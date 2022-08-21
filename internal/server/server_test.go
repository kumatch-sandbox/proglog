package server

import (
	"context"
	"flag"
	"net"
	"os"
	"testing"
	"time"

	api "github.com/kumatch-sandbox/proglog/api/v1"
	"github.com/kumatch-sandbox/proglog/internal/auth"
	"github.com/kumatch-sandbox/proglog/internal/config"
	"github.com/kumatch-sandbox/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var debug = flag.Bool("debug", false, "Enable observability for debugging.")

func TestMain(m *testing.M) {
	flag.Parse()

	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}

		zap.ReplaceGlobals(logger)
	}

	os.Exit(m.Run())
}

func TestServer(t *testing.T) {
	scenarios := map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"consume past log boundary fails":                    testConsumePastBoundary,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"unauthorized fails":                                 testUnauthorized,
	}

	for scenario, fn := range scenarios {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()

			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func newClient(csrPath, keyPath string, listen net.Listener) (*grpc.ClientConn, api.LogClient, []grpc.DialOption, error) {
	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: csrPath,
		KeyFile:  keyPath,
		CAFile:   config.CAFile,
		Server:   false,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	conn, err := grpc.Dial(listen.Addr().String(), opts...)
	if err != nil {
		return nil, nil, nil, err
	}

	client := api.NewLogClient(conn)

	return conn, client, opts, nil
}

func newTelemetryExporter(t *testing.T) (*exporter.LogExporter, error) {
	metricsLogFile, err := os.CreateTemp("", "metrics-*.log")
	if err != nil {
		return nil, err
	}
	t.Logf("metrics log file: %s", metricsLogFile.Name())

	tracesLogFile, err := os.CreateTemp("", "traces-*.log")
	if err != nil {
		return nil, err
	}
	t.Logf("traces log file: %s", tracesLogFile.Name())

	telemetryExporter, err := exporter.NewLogExporter(
		exporter.Options{
			MetricsLogFile:    metricsLogFile.Name(),
			TracesLogFile:     tracesLogFile.Name(),
			ReportingInterval: time.Second,
		},
	)
	if err != nil {
		return nil, err
	}

	return telemetryExporter, nil
}

func setupTest(t *testing.T, fn func(*Config)) (
	rootClient api.LogClient,
	nobodyClient api.LogClient,
	cfg *Config, teardown func(),
) {
	t.Helper()

	listen, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var rootConn, nobodyConn *grpc.ClientConn

	// setup clients (root and nobody)
	rootConn, rootClient, _, err = newClient(config.RootClientCertFile, config.RootClientKeyFile, listen)
	require.NoError(t, err)
	nobodyConn, nobodyClient, _, err = newClient(config.NobodyClientCertFile, config.NobodyClientKeyFile, listen)
	require.NoError(t, err)

	// setup server
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: listen.Addr().String(),
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	commitLog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer, err := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	require.NoError(t, err)

	var telemetryExporter *exporter.LogExporter
	if *debug {
		telemetryExporter, err = newTelemetryExporter(t)
		require.NoError(t, err)

		err = telemetryExporter.Start()
		require.NoError(t, err)
	}

	cfg = &Config{
		CommitLog:  commitLog,
		Authorizer: authorizer,
	}
	if fn != nil {
		fn(cfg)
	}
	server, err := NewGrpcServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(listen)
	}()

	teardown = func() {
		rootConn.Close()
		nobodyConn.Close()
		server.Stop()
		listen.Close()

		if telemetryExporter != nil {
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Stop()
			telemetryExporter.Close()
		}
	}

	return rootClient, nobodyClient, cfg, teardown
}

func testProduceConsume(t *testing.T, client, _ api.LogClient, conifg *Config) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("Hello, world"),
	}

	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: want,
		},
	)
	require.NoError(t, err)
	want.Offset = produce.Offset

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset,
	})
	require.NoError(t, err)
	require.Equal(t, want.Value, consume.Record.Value)
	require.Equal(t, want.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, conifg *Config) {
	ctx := context.Background()

	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: &api.Record{Value: []byte("hello")},
		},
	)
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	require.Error(t, err)
	if consume != nil {
		t.Fatal("consume not nil")
	}

	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, want, got)
}

func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{
		{Value: []byte("first message"), Offset: 0},
		{Value: []byte("second message"), Offset: 1},
	}

	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)

			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Offset != uint64(offset) {
				t.Fatalf("got offset %d, want %d.", res.Offset, offset)
			}
		}
	}

	{
		stream, err := client.ConsumeStream(
			ctx,
			&api.ConsumeRequest{Offset: 0},
		)
		require.NoError(t, err)

		for offset, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(offset),
			})
		}
	}
}

func testUnauthorized(t *testing.T, _, client api.LogClient, config *Config) {
	ctx := context.Background()

	produce, err := client.Produce(ctx,
		&api.ProduceRequest{
			Record: &api.Record{Value: []byte("hello, world")},
		},
	)
	require.Nil(t, produce)

	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	require.Equal(t, gotCode, wantCode)
}
