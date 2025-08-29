package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os/signal"
	"strings"
	"syscall"
	"time"

	loggerpb "github.com/example/user-platform/api/gen/go/logger/v1"
	loggergrpc "github.com/example/user-platform/internal/logger/grpc"
	"github.com/example/user-platform/internal/logger/service"
	"github.com/example/user-platform/pkg/config"
	"github.com/example/user-platform/pkg/kafka"
	"github.com/example/user-platform/pkg/log"
	"github.com/gocql/gocql"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	health "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

const (
	gracefulTimeout = 10 * time.Second
	queryTimeout    = 5 * time.Second
)

// ----------------------------------------------------------------------------
// main
// ----------------------------------------------------------------------------

func main() {
	logger := log.NewLogger()
	defer log.Sync(logger)

	// Root ctx that cancels on SIGINT/SIGTERM (needed for Vault + shutdown)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// -------------------------
	// Vault-only configuration
	// -------------------------
	// Build a Vault client WITHOUT env usage. Replace addr/token/TLS as needed.
	vaultAddr := "http://vault:8200" // <-- set appropriately
	vaultToken := "root"             // <-- or pre-auth via Kubernetes/AppRole and set on client
	vaultTLS := &tls.Config{
		MinVersion: tls.VersionTLS12,
		// Root CAs / mTLS as needed. Avoid InsecureSkipVerify in production.
	}

	vcli, err := config.NewVaultClient(vaultAddr, vaultToken, vaultTLS)
	if err != nil {
		logger.Fatal("vault client init failed", zap.Error(err))
	}

	paths := config.DefaultKVPaths() // adjust if your Vault layout differs
	cfg, err := config.LoadConfigFromVault(ctx, vcli, paths)
	if err != nil {
		logger.Fatal("failed to load config from vault", zap.Error(err))
	}

	// Cassandra
	cass, err := setupCassandra(ctx, cfg.CassandraHostList(), cfg.CassandraKeyspace, logger)
	if err != nil {
		logger.Fatal("cassandra setup failed", zap.Error(err))
	}
	if cass != nil {
		defer cass.Close()
		if err := ensureAuditSchema(ctx, cass); err != nil {
			logger.Fatal("ensuring audit schema failed", zap.Error(err))
		}
	}

	// Kafka (lightweight producer wrapper)
	producer := kafka.NewProducer(cfg.KafkaBrokerList(), kafka.UserAuditTopic)
	defer producer.Close()

	// Services
	auditSvc := service.NewAuditService(producer, cass)
	grpcServer := newGRPCServer(auditSvc)

	addr := normalizeListenAddr(cfg.LoggerServiceAddr)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal("failed to listen", zap.String("addr", addr), zap.Error(err))
	}
	logger.Info("logger-service gRPC listening", zap.String("addr", addr))

	// Serve in foreground until ctx canceled or server error
	errCh := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		// graceful with deadline, then force
		done := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(done)
		}()

		select {
		case <-done:
			logger.Info("logger-service graceful shutdown complete")
		case <-time.After(gracefulTimeout):
			logger.Warn("graceful shutdown timed out; forcing stop")
			grpcServer.Stop()
		}

	case err := <-errCh:
		logger.Fatal("grpc server exited with error", zap.Error(err))
	}
}

// ----------------------------------------------------------------------------
// Cassandra
// ----------------------------------------------------------------------------

func setupCassandra(ctx context.Context, hosts []string, keyspace string, logger *zap.Logger) (*gocql.Session, error) {
	if len(hosts) == 0 {
		return nil, nil // optional dependency
	}

	// Ensure keyspace first (connect without keyspace)
	if err := ensureKeyspace(ctx, hosts, keyspace); err != nil {
		return nil, fmt.Errorf("ensure keyspace: %w", err)
	}

	cluster := gocql.NewCluster(hosts...)
	// Performance & resiliency knobs
	cluster.ProtoVersion = 4
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 5 * time.Second
	cluster.NumConns = 2
	cluster.DisableInitialHostLookup = true
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}
	cluster.ReconnectionPolicy = &gocql.ExponentialReconnectionPolicy{
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     10 * time.Second,
	}
	cluster.Keyspace = keyspace
	cluster.DefaultTimestamp = true

	sess, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("connect to keyspace: %w", err)
	}
	logger.Info("connected to cassandra", zap.Strings("hosts", hosts), zap.String("keyspace", keyspace))
	return sess, nil
}

func ensureKeyspace(ctx context.Context, hosts []string, keyspace string) error {
	cc := gocql.NewCluster(hosts...)
	cc.Timeout = 5 * time.Second
	cc.ConnectTimeout = 5 * time.Second
	s, err := cc.CreateSession()
	if err != nil {
		return err
	}
	defer s.Close()

	ddl := fmt.Sprintf(
		`CREATE KEYSPACE IF NOT EXISTS %s
		 WITH replication = {'class':'SimpleStrategy','replication_factor':1}`,
		keyspace,
	)

	cctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	return s.Query(ddl).WithContext(cctx).Exec()
}

func ensureAuditSchema(ctx context.Context, s *gocql.Session) error {
	const ddl = `CREATE TABLE IF NOT EXISTS audit_logs (
		id text,
		ts timestamp,
		user_id text,
		event_type text,
		correlation_id text,
		payload_json text,
		created_at timestamp,
		PRIMARY KEY (id)
	)`
	cctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	return s.Query(ddl).WithContext(cctx).Exec()
}

// ----------------------------------------------------------------------------
// gRPC server
// ----------------------------------------------------------------------------

func newGRPCServer(auditSvc *service.AuditService) *grpc.Server {
	// Conservative keepalive & limits for stability under load
	kaep := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}
	kas := keepalive.ServerParameters{
		MaxConnectionIdle:     2 * time.Minute,
		MaxConnectionAge:      30 * time.Minute,
		MaxConnectionAgeGrace: 2 * time.Minute,
		Time:                  30 * time.Second,
		Timeout:               10 * time.Second,
	}

	opts := []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kas),
		grpc.MaxRecvMsgSize(8 << 20), // 8MB
		grpc.MaxSendMsgSize(8 << 20),
	}

	s := grpc.NewServer(opts...)

	loggerServer := loggergrpc.NewLoggerServer(auditSvc)
	loggerpb.RegisterLoggerServiceServer(s, loggerServer)

	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(s, hs)

	// Enable reflection in non-prod only if you prefer; left on here for convenience
	reflection.Register(s)

	return s
}

// ----------------------------------------------------------------------------
// Misc
// ----------------------------------------------------------------------------

func normalizeListenAddr(cfgAddr string) string {
	const def = ":50052"
	if cfgAddr == "" {
		return def
	}
	// If already ":port" or "host:port", respect it.
	if strings.HasPrefix(cfgAddr, ":") {
		return cfgAddr
	}
	if _, port, err := net.SplitHostPort(cfgAddr); err == nil && port != "" {
		return ":" + port
	}
	// If it's "50052" (bare port), normalize to ":50052".
	if !strings.Contains(cfgAddr, ":") {
		return ":" + cfgAddr
	}
	return def
}
