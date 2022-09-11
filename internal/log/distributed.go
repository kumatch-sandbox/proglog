package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/protobuf/proto"

	api "github.com/kumatch-sandbox/proglog/api/v1"
)

type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

type DistributedLog struct {
	config  Config
	log     *Log
	raftLog *logStore
	raft    *raft.Raft
}

func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	dl := &DistributedLog{config: config}

	if err := dl.setupLog(dataDir); err != nil {
		return nil, err
	}

	if err := dl.setupRaft(dataDir); err != nil {
		return nil, err
	}

	return dl, nil
}

func (dl *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	var err error
	dl.log, err = NewLog(logDir, dl.config)

	return err
}

func (dl *DistributedLog) setupRaft(dataDir string) error {
	var err error
	raftDir := filepath.Join(dataDir, "raft")

	// Raft が与えられたコマンドを適用する有限ステートマシン
	fsm := &fsm{log: dl.log}

	// Raft がそれらのコマンドを保存するログストア (log store)
	logDir := filepath.Join(raftDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	logConfig := dl.config
	logConfig.Segment.InitialOffset = 1
	dl.raftLog, err = newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	// Raft がクラスタの構成（クラスタ内のサーバやアドレスなど）を保存ずる安定ストア (stable store)
	stableDir := filepath.Join(raftDir, "stable")
	stableStore, err := raftboltdb.NewBoltStore(stableDir)
	if err != nil {
		return err
	}

	// Raft がコンパクトなスナップショットを保存するスナップショットストア (snapshot store)
	// 効率的にデータ復旧をするための使われる
	retain := 1
	snapshotStore, err := raft.NewFileSnapshotStore(raftDir, retain, os.Stderr)
	if err != nil {
		return err
	}

	// Raft が他の Raft サーバと接続するために使うトランスポート
	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		dl.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	// その他 Raft 設定
	config := raft.DefaultConfig()
	config.LocalID = dl.config.Raft.LocalID
	// 以下設定値は動作環境次第で変更、あるいはテスト実行時に効率あげる目的で都合よく差し替えられるように
	if dl.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = dl.config.Raft.HeartbeatTimeout
	}
	if dl.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = dl.config.Raft.ElectionTimeout
	}
	if dl.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = dl.config.Raft.LeaderLeaseTimeout
	}
	if dl.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = dl.config.Raft.CommitTimeout
	}

	// Raft インスタンス作成のために、上記までのすべての用意、設定する必要がある
	dl.raft, err = raft.NewRaft(
		config,
		fsm,
		dl.raftLog,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return err
	}

	hasState, err := raft.HasExistingState(dl.raftLog, stableStore, snapshotStore)
	if err != nil {
		return err
	}

	// 設定値で起動フラグを立てて、サーバ起動のためのブートストラップ処理を行わせる
	if dl.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{
				{ID: config.LocalID, Address: transport.LocalAddr()},
			},
		}
		err = dl.raft.BootstrapCluster(config).Error()
	}

	return err
}

// Append は Log 型と同じAPIにして互換性を持たせている
func (dl *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := dl.apply(AppendRequestType, &api.ProduceRequest{Record: record})
	if err != nil {
		return 0, err
	}

	return res.(*api.ProduceResponse).Offset, nil
}

func (dl *DistributedLog) apply(reqType RequestType, req proto.Message) (interface{}, error) {
	var buf bytes.Buffer

	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}

	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}

	// Raft でコマンド実行（FSM で定義した内容が実施される）
	timeout := 10 * time.Second
	future := dl.raft.Apply(buf.Bytes(), timeout)
	if future.Error() != nil {
		return nil, future.Error()
	}

	// FSM の Apply メソッドが返したものが得られる
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}

	return res, nil
}

func (dl *DistributedLog) Read(offset uint64) (*api.Record, error) {
	// ここでは緩やかな一貫性で良しとして、Raft を経由せず直接自分のを返している
	return dl.log.Read(offset)
}

func (dl *DistributedLog) Join(id, addr string) error {
	configFuture := dl.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}

	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				// サーバはすでに参加している
				return nil
			}

			// 既存のサーバを取り除く
			removeFuture := dl.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}

	addFuture := dl.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}

	return nil
}

func (dl *DistributedLog) Leave(id string) error {
	removeFuture := dl.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

func (dl *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutCh := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCh:
			return fmt.Errorf("timeout")
		case <-ticker.C:
			if leaderAddr := dl.raft.Leader(); leaderAddr != "" {
				// 選出完了
				return nil
			}
		}
	}
}

func (dl *DistributedLog) Close() error {
	f := dl.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}

	if err := dl.raftLog.Log.Close(); err != nil {
		return err
	}

	return dl.log.Close()
}

var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log *Log
}

func (f *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])

	switch reqType {
	case AppendRequestType:
		return f.applyAppend(buf[1:])
	}

	return nil
}

func (f *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest

	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}

	offset, err := f.log.Append(req.Record)
	if err != nil {
		return err
	}

	return &api.ProduceResponse{Offset: offset}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()

	return &snapshot{reader: r}, nil
}

func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)

	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		size := int64(enc.Uint64(b))
		if _, err := io.CopyN(&buf, r, size); err != nil {
			return err
		}

		record := &api.Record{}
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}

		if i == 0 {
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}

		if _, err := f.log.Append(record); err != nil {
			return err
		}

		buf.Reset()
	}

	return nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		return err
	}

	return sink.Close()
}

func (s *snapshot) Release() {}

var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*Log
}

func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}

	return &logStore{log}, nil
}

func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

func (l *logStore) LastIndex() (uint64, error) {
	return l.HighestOffset()
}

func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}

	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term

	return nil
}

func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := l.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		}); err != nil {
			return err
		}
	}

	return nil
}

func (l *logStore) DeleteRange(min, max uint64) error {
	return l.Truncate(max)
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func NewStreamLayer(ln net.Listener, serverTLSConfig *tls.Config, peerTLSConfig *tls.Config) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

const RaftRPC = 1

func (s *StreamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}

	conn, err := dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}

	// Raft RPC であることを特定する
	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}

	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}

	return conn, err
}

// Accept は net.Listener の interface.
func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}

	b := make([]byte, 1)
	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal([]byte{byte(RaftRPC)}, b) {
		return nil, fmt.Errorf("not a raft rpc")
	}

	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}

	return conn, nil
}

// Close は net.Listener の interface.
func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

// Addr は net.Listener の interface.
func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}
