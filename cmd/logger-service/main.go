package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	loggerpb "github.com/example/user-platform/api/gen/go/logger/v1"
	loggrpc "github.com/example/user-platform/internal/logger/grpc"
	logsvc "github.com/example/user-platform/internal/logger/service"
	"github.com/example/user-platform/pkg/kafka"
	"github.com/example/user-platform/pkg/log"
	"github.com/gocql/gocql"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

const gracefulTimeout = 10 * time.Second

func main() {
	logger := log.NewLogger()
	defer log.Sync(logger)

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cassHosts := os.Getenv("CASSANDRA_HOSTS")
	cassKeyspace := os.Getenv("CASSANDRA_KEYSPACE")
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	grpcAddr := os.Getenv("LOGGER_SERVICE_ADDR")
	if strings.TrimSpace(grpcAddr) == "" {
		grpcAddr = ":50052"
	}

	hostList := splitAndTrim(cassHosts)
	if len(hostList) == 0 {
		hostList = []string{"cassandra:9042"}
	}
	cluster := gocql.NewCluster(hostList...)
	if cassKeyspace != "" {
		cluster.Keyspace = cassKeyspace
	}
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		logger.Fatal("cassandra create session", zap.Error(err))
	}
	defer session.Close()

	logService, err := logsvc.NewLoggerService(session)
	if err != nil {
		logger.Fatal("init logger service", zap.Error(err))
	}

	brokerList := splitAndTrim(kafkaBrokers)
	if len(brokerList) == 0 {
		brokerList = []string{"kafka:9092"}
	}
	byteConsumer := kafka.NewByteAuditConsumer(brokerList, kafka.UserAuditTopic, "logger-service", 1, func(ctx context.Context, msg []byte) error {
		var ev kafka.AuditEvent
		if err := json.Unmarshal(msg, &ev); err != nil {
			logger.Warn("failed to unmarshal audit event", zap.Error(err))
			return nil
		}
		if err := logService.WriteAudit(ctx, ev); err != nil {
			logger.Error("write audit failed", zap.Error(err))
			return err
		}
		return nil
	})
	go byteConsumer.Start(rootCtx)
	defer func() { _ = byteConsumer.Close() }()

	grpcServer := newGRPCServer()
	loggerServer := loggrpc.NewServer(logService)
	loggerpb.RegisterLoggerServiceServer(grpcServer, loggerServer)
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", normalizeListenAddr(grpcAddr, ":50052"))
	if err != nil {
		logger.Fatal("failed to listen", zap.String("addr", grpcAddr), zap.Error(err))
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info("logger-service gRPC listening", zap.String("addr", lis.Addr().String()))
		if err := grpcServer.Serve(lis); err != nil {
			errCh <- err
		}
	}()
	select {
	case <-rootCtx.Done():
	case err := <-errCh:
		logger.Error("grpc serve error", zap.Error(err))
	}
	done := make(chan struct{})
	go func() { grpcServer.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(gracefulTimeout):
		grpcServer.Stop()
	}
	logger.Info("logger-service shutdown complete")
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func newGRPCServer() *grpc.Server {
	kaep := keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}
	kas := keepalive.ServerParameters{
		MaxConnectionIdle:     2 * time.Minute,
		MaxConnectionAge:      30 * time.Minute,
		MaxConnectionAgeGrace: 2 * time.Minute,
		Time:                  30 * time.Second,
		Timeout:               10 * time.Second,
	}
	return grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kas),
		grpc.MaxRecvMsgSize(8<<20),
		grpc.MaxSendMsgSize(8<<20),
	)
}

func normalizeListenAddr(cfgAddr, def string) string {
	addr := strings.TrimSpace(cfgAddr)
	if addr == "" {
		return def
	}
	if strings.HasPrefix(addr, ":") {
		return addr
	}
	if h, _, err := net.SplitHostPort(addr); err == nil {
		if strings.TrimSpace(h) == "" {
			return def
		}
		return addr
	}
	if !strings.Contains(addr, ":") {
		return ":" + addr
	}
	return def
}
