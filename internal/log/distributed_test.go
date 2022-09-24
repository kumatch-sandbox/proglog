package log_test

import (
	"fmt"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	api "github.com/kumatch-sandbox/proglog/api/v1"
	"github.com/kumatch-sandbox/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
)

func TestMultipleNodes(t *testing.T) {
	nodeCount := 3
	ports := dynaport.Get(nodeCount)
	distributedLogs := make([]*log.DistributedLog, 0, 3)

	// サーバ x3 の準備
	for i := 0; i < nodeCount; i++ {
		id := fmt.Sprintf("%d", i)
		dataDir, err := os.MkdirTemp("", "distributed-log-test")
		require.NoError(t, err)
		defer func(dir string) {
			_ = os.RemoveAll(dir)
		}(dataDir)

		listen, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[i]))
		require.NoError(t, err)

		config := log.Config{}
		config.Raft.StreamLayer = log.NewStreamLayer(listen, nil, nil)
		config.Raft.LocalID = raft.ServerID(id)
		config.Raft.HeartbeatTimeout = 50 * time.Millisecond
		config.Raft.ElectionTimeout = 50 * time.Millisecond
		config.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
		config.Raft.CommitTimeout = 5 * time.Millisecond
		if i == 0 {
			config.Raft.Bootstrap = true
		}

		dl, err := log.NewDistributedLog(dataDir, config)
		require.NoError(t, err)

		if i != 0 {
			// リーダーであろうサーバに対して自身を参加させる
			err = distributedLogs[0].Join(id, listen.Addr().String())
			require.NoError(t, err)
		} else {
			// 自分がクラスタリーダーに選出されるのを待つ
			err = dl.WaitForLeader(3 * time.Second)
			require.NoError(t, err)
		}

		distributedLogs = append(distributedLogs, dl)
	}

	// 準備できたサーバ x3 がクラスタ参加中として参照できる状態になっていることを確認
	servers, err := distributedLogs[0].GetServers()
	require.NoError(t, err)
	require.Equal(t, 3, len(servers))
	require.True(t, servers[0].IsLeader)
	require.False(t, servers[1].IsLeader)
	require.False(t, servers[2].IsLeader)

	leader := distributedLogs[0]

	// リーダーのサーバにレコードを追加していって、Raft経由で複製処理できたことを確認
	records := []*api.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
		{Value: []byte("third")},
	}
	for _, record := range records {
		offset, err := leader.Append(record)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			for i := 0; i < nodeCount; i++ {
				got, err := distributedLogs[i].Read(offset)
				if err != nil {
					return false
				}

				record.Offset = offset

				if !reflect.DeepEqual(got.Value, record.Value) {
					return false
				}
			}

			return true
		}, 500*time.Millisecond, 50*time.Millisecond)
	}

	// クラスタからサーバを１つ離脱させる
	err = leader.Leave("1")
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	record := &api.Record{Value: []byte("final")}
	offset, err := leader.Append(record)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// クラスタ離脱済みサーバは読めない
	leavedRes, err := distributedLogs[1].Read(offset)
	require.IsType(t, api.ErrOffsetOutOfRange{}, err)
	require.Nil(t, leavedRes)

	// クラスタ参加中サーバは読める
	joinedRes, err := distributedLogs[2].Read(offset)
	require.NoError(t, err)
	require.Equal(t, record.Value, joinedRes.Value)
	require.Equal(t, offset, joinedRes.Offset)

	// クラスタ参加中サーバが更新される
	servers, err = distributedLogs[0].GetServers()
	require.NoError(t, err)
	require.Equal(t, 2, len(servers))
	require.True(t, servers[0].IsLeader)
	require.False(t, servers[1].IsLeader)
}
