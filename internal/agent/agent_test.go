package agent_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"testing"
	"time"

	api "github.com/kumatch-sandbox/proglog/api/v1"
	"github.com/kumatch-sandbox/proglog/internal/agent"
	"github.com/kumatch-sandbox/proglog/internal/config"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestAgent(t *testing.T) {
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	var agents []*agent.Agent
	for i := 0; i < 3; i++ {
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		dataDir, err := os.MkdirTemp("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].Config.BindAddr)
		}

		a, err := agent.New(agent.Config{
			NodeName:        fmt.Sprintf("agent-%d", i),
			StartJoinAddr:   startJoinAddrs,
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			DataDir:         dataDir,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
			Bootstrap:       i == 0,
		})
		require.NoError(t, err)

		agents = append(agents, a)
	}
	defer func() {
		for _, a := range agents {
			err := a.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(a.Config.DataDir))
		}
	}()

	// ノード同士がお互いを発見する時間を確保
	time.Sleep(3 * time.Second)

	// リーダーへ１つのレコードを生成
	leaderClient := client(t, agents[0], peerTLSConfig)
	ctx := context.Background()
	want := []byte("hello, world")
	produceRes, err := leaderClient.Produce(
		ctx,
		&api.ProduceRequest{
			Record: &api.Record{Value: want},
		},
	)
	require.NoError(t, err)

	// レプリケーションの完了を待つ
	time.Sleep(3 * time.Second)

	// フォロワー（レプリカサーバ）でコードがレプリケーションされたことを確認
	followerClient := client(t, agents[1], peerTLSConfig)
	consumeRes, err := followerClient.Consume(
		ctx,
		&api.ConsumeRequest{Offset: produceRes.Offset},
	)
	require.NoError(t, err)
	require.Equal(t, consumeRes.Record.Value, want)

	// 追加のレプリケーション待ち
	time.Sleep(3 * time.Second)

	// フォロワーでのレプリケーションに対し、リーダー側へ再レプリケーションが行われないことを確認
	consumeRes, err = leaderClient.Consume(
		ctx,
		&api.ConsumeRequest{Offset: produceRes.Offset + 1},
	)
	require.Nil(t, consumeRes)
	require.Error(t, err)
	got := status.Code(err)
	want2 := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, got, want2)
}

func client(t *testing.T, agent *agent.Agent, tlsConfig *tls.Config) api.LogClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)

	conn, err := grpc.Dial(rpcAddr, opts...)
	require.NoError(t, err)

	client := api.NewLogClient(conn)

	return client
}
