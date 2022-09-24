package loadbalance_test

import (
	"net"
	"testing"

	"github.com/kumatch-sandbox/proglog/internal/config"
	"github.com/kumatch-sandbox/proglog/internal/loadbalance"
	"github.com/kumatch-sandbox/proglog/internal/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	api "github.com/kumatch-sandbox/proglog/api/v1"
)

func TestResolver(t *testing.T) {
	listen, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// テスト用リゾルバが発見する対象となるサーバ情報を返すサーバを構築
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	srv, err := server.NewGrpcServer(&server.Config{
		GetServerer: &getServers{},
	}, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go srv.Serve(listen)

	// テスト用リゾルバを構築
	conn := &clientConn{}
	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)
	clientCreds := credentials.NewTLS(clientTLSConfig)
	opts := resolver.BuildOptions{
		DialCreds: clientCreds,
	}

	r := &loadbalance.Resolver{}
	_, err = r.Build(
		resolver.Target{
			Endpoint: listen.Addr().String(),
		},
		conn,
		opts,
	)
	require.NoError(t, err)

	// リゾルバ構築後にサーバを見つけていることを確認
	wantState := resolver.State{
		Addresses: []resolver.Address{
			{Addr: "localhost:9001", Attributes: attributes.New("is_leader", true)},
			{Addr: "localhost:9002", Attributes: attributes.New("is_leader", false)},
		},
	}
	require.Equal(t, wantState, conn.state)

	conn.state.Addresses = nil
	r.ResolveNow(resolver.ResolveNowOptions{})
	require.Equal(t, wantState, conn.state)
}

type getServers struct{}

func (s *getServers) GetServers() ([]*api.Server, error) {
	return []*api.Server{
		{Id: "leader", RpcAddr: "localhost:9001", IsLeader: true},
		{Id: "follower", RpcAddr: "localhost:9002", IsLeader: false},
	}, nil
}

type clientConn struct {
	resolver.ClientConn
	state resolver.State
}

func (c *clientConn) UpdateState(state resolver.State) error {
	c.state = state
	return nil
}

func (c *clientConn) ReportError(err error) {}

func (c *clientConn) NewAddress(addrs []resolver.Address) {}

func (c *clientConn) NewServiceConfig(config string) {}

func (c *clientConn) ParseServiceConfig(config string) *serviceconfig.ParseResult {
	return nil
}
