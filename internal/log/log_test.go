package log

import (
	//"io"

	"io"
	"os"
	"testing"

	api "github.com/kumatch-sandbox/proglog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, l *Log){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader":                            testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := os.MkdirTemp("", "log_test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32

			l, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, l)
		})
	}
}

func testAppendRead(t *testing.T, l *Log) {
	record := &api.Record{Value: []byte("hello, world")}

	off, err := l.Append(record)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err := l.Read(off)
	require.NoError(t, err)
	require.Equal(t, record.Value, read.Value)
	require.NoError(t, l.Close())
}

func testOutOfRangeErr(t *testing.T, l *Log) {
	read, err := l.Read(1)
	require.Nil(t, read)
	require.Error(t, err)
	require.NoError(t, l.Close())
}

func testInitExisting(t *testing.T, l *Log) {
	record := &api.Record{Value: []byte("hello, world")}

	for i := 0; i < 3; i++ {
		_, err := l.Append(record)
		require.NoError(t, err)
	}
	require.NoError(t, l.Close())

	// 現在のログのオフセット状態の確認
	off, err := l.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = l.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	// 上記で保存したデータを指定したログの復元
	x, err := NewLog(l.Dir, l.Config)
	require.NoError(t, err)
	off, err = x.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = x.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), off)

	require.NoError(t, x.Close())
}

func testReader(t *testing.T, l *Log) {
	record := &api.Record{Value: []byte("hello, world")}
	p, _ := proto.Marshal(record)

	off, err := l.Append(record)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := l.Reader()
	b, err := io.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:len(p)+lenWidth], read)
	require.NoError(t, err)
	require.Equal(t, record.Value, read.Value)
	require.NoError(t, l.Close())
}

func testTruncate(t *testing.T, l *Log) {
	record := &api.Record{Value: []byte("hello, world")}

	for i := 0; i < 3; i++ {
		_, err := l.Append(record)
		require.NoError(t, err)
	}

	err := l.Truncate(1)
	require.NoError(t, err)

	_, err = l.Read(0)
	require.Error(t, err)
	_, err = l.Read(1)
	require.Error(t, err)
	_, err = l.Read(2)
	require.NoError(t, err)

	require.NoError(t, l.Close())
}
