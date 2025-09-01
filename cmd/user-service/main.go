// cmd/user-service/main.go
package main

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	userpb "github.com/example/user-platform/api/gen/go/user/v1"
	usergrpc "github.com/example/user-platform/internal/user/grpc"
	userrepo "github.com/example/user-platform/internal/user/repo"
	usersvc "github.com/example/user-platform/internal/user/service"
	"github.com/example/user-platform/pkg/auth"
	"github.com/example/user-platform/pkg/config"
	"github.com/example/user-platform/pkg/kafka"
	"github.com/example/user-platform/pkg/log"
	"github.com/example/user-platform/pkg/validation"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	// Redis cache (optional)
	"github.com/redis/go-redis/v9"

	health "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	gracefulTimeout  = 10 * time.Second
	pgConnectTimeout = 5 * time.Second
	pgQueryTimeout   = 5 * time.Second
)

func main() {
	logger := log.NewLogger()
	defer log.Sync(logger)

	// Root context cancels on SIGINT/SIGTERM
	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ----- Vault config (no env) -----
	vaultAddr := "http://vault:8200"
	vaultToken := "root"
	vaultTLS := &tls.Config{MinVersion: tls.VersionTLS12}

	vcli, err := config.NewVaultClient(vaultAddr, vaultToken, vaultTLS)
	if err != nil {
		logger.Fatal("vault client init failed", zap.Error(err))
	}
	paths := config.DefaultKVPaths()
	cfg, err := config.LoadConfigFromVault(rootCtx, vcli, paths)
	if err != nil {
		logger.Fatal("failed to load config from vault", zap.Error(err))
	}

	// ----- Auth -----
	privPath, err := writeTempPEM("jwt-priv", normalizePEM(cfg.JWTPrivateKeyPEM))
	if err != nil {
		logger.Fatal("write private pem", zap.Error(err))
	}
	defer os.Remove(privPath)
	pubPath, err := writeTempPEM("jwt-pub", normalizePEM(cfg.JWTPublicKeyPEM))
	if err != nil {
		logger.Fatal("write public pem", zap.Error(err))
	}
	defer os.Remove(pubPath)
	tm, err := auth.NewTokenManager(privPath, pubPath)
	if err != nil {
		logger.Fatal("failed to init token manager", zap.Error(err))
	}

	// ----- Postgres -----
	pgCfg, err := pgxpool.ParseConfig(cfg.DSN())
	if err != nil {
		logger.Fatal("invalid postgres dsn", zap.Error(err))
	}
	pgCfg.MaxConns = 20
	pgCfg.MinConns = 2
	pgCfg.HealthCheckPeriod = 30 * time.Second
	pgCfg.MaxConnIdleTime = 5 * time.Minute
	pgCfg.MaxConnLifetime = 60 * time.Minute

	pool, err := pgxpool.NewWithConfig(rootCtx, pgCfg)
	if err != nil {
		logger.Fatal("create postgres pool", zap.Error(err))
	}
	defer pool.Close()
	{
		ctx, cancel := context.WithTimeout(rootCtx, pgConnectTimeout)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			logger.Fatal("unable to ping postgres", zap.Error(err))
		}
	}
	_ = pgQueryTimeout

	// ----- Kafka Producer (audit only; NO Cassandra here) -----
	prod := kafka.NewProducer(cfg.KafkaBrokerList(), kafka.ProducerOptions{
		Topic: kafka.UserAuditTopic, // "user.audit.v1"
		// (other opts are fine to leave default)
	})
	defer prod.Close()

	// ----- Optional Redis cache -----
	var cache usersvc.Cache
	var rdb *redis.Client
	if cfg.RedisHost != "" {
		addr := cfg.RedisHost
		if cfg.RedisPort != "" && !strings.Contains(addr, ":") {
			addr = addr + ":" + cfg.RedisPort
		}
		rdb = redis.NewClient(&redis.Options{
			Addr:         addr,
			Password:     cfg.RedisPass,
			DB:           0,
			DialTimeout:  800 * time.Millisecond,
			ReadTimeout:  1 * time.Second,
			WriteTimeout: 1 * time.Second,
			PoolSize:     32,
			MinIdleConns: 4,
		})
		ctx, cancel := context.WithTimeout(rootCtx, 1*time.Second)
		if err := rdb.Ping(ctx).Err(); err != nil {
			logger.Warn("redis not available; continuing without cache", zap.Error(err))
			_ = rdb.Close()
			rdb = nil
		}
		cancel()
		if rdb != nil {
			cache = NewRedisCache(rdb)
			logger.Info("redis cache enabled", zap.String("addr", addr))
		}
	}
	defer func() {
		if rdb != nil {
			_ = rdb.Close()
		}
	}()

	// ----- Repository + Validator + Domain Service -----
	repository := userrepo.NewPostgresRepo(pool)
	validator := validation.NewValidator()
	usrSvc := usersvc.NewUserService(repository, tm, validator, logger, cache)

	// ----- Wire audit emitter -----
	auditSvc := usersvc.NewAuditService(prod, nil) // Cassandra session not used here
	usrSvc.Audit = auditSvc

	// ----- gRPC server (+ health + reflection) -----
	grpcServer := newGRPCServer()
	userpb.RegisterUserServiceServer(grpcServer, usergrpc.NewUserServer(usrSvc))

	hs := health.NewServer()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, hs)
	reflection.Register(grpcServer)

	addr := normalizeListenAddr(cfg.UserServiceAddr, ":50051")
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Fatal("failed to listen", zap.String("addr", addr), zap.Error(err))
	}

	// Serve
	errCh := make(chan error, 1)
	go func() {
		logger.Info("user-service gRPC listening", zap.String("addr", addr))
		if err := grpcServer.Serve(lis); err != nil {
			errCh <- err
		}
	}()

	// Graceful shutdown
	select {
	case <-rootCtx.Done():
	case err := <-errCh:
		logger.Error("grpc serve error", zap.Error(err))
	}

	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(gracefulTimeout):
		grpcServer.Stop()
	}

	logger.Info("user-service shutdown complete")
}

// ---------------- helpers ----------------

func newGRPCServer() *grpc.Server {
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
	return grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kas),
		grpc.MaxRecvMsgSize(8<<20),
		grpc.MaxSendMsgSize(8<<20),
	)
}

func normalizeListenAddr(cfgAddr, def string) string {
	if cfgAddr == "" {
		return def
	}
	if strings.HasPrefix(cfgAddr, ":") {
		return cfgAddr
	}
	if _, _, err := net.SplitHostPort(cfgAddr); err == nil {
		return cfgAddr
	}
	if !strings.Contains(cfgAddr, ":") {
		return ":" + cfgAddr
	}
	return def
}

func normalizePEM(s string) string {
	t := strings.TrimSpace(s)
	t = strings.ReplaceAll(t, "\r\n", "\n")
	t = strings.ReplaceAll(t, "\\n", "\n")
	return t
}

func writeTempPEM(prefix, pem string) (string, error) {
	f, err := os.CreateTemp("", prefix+"-*.pem")
	if err != nil {
		return "", err
	}
	if err := os.Chmod(f.Name(), 0o600); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", err
	}
	if _, err := f.WriteString(pem); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return "", err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

/* ---------- tiny Redis cache adapter ---------- */

type RedisCache struct{ rdb *redis.Client }

func NewRedisCache(rdb *redis.Client) *RedisCache { return &RedisCache{rdb: rdb} }

func (c *RedisCache) Get(ctx context.Context, key string) (string, bool, error) {
	val, err := c.rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	return val, err == nil, err
}

func (c *RedisCache) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	return c.rdb.Set(ctx, key, value, ttl).Err()
}

func (c *RedisCache) Del(ctx context.Context, keys ...string) error {
	if len(keys) == 0 {
		return nil
	}
	return c.rdb.Del(ctx, keys...).Err()
}
