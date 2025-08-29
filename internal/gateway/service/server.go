package service

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	loggerpb "github.com/example/user-platform/api/gen/go/logger/v1"
	userpb "github.com/example/user-platform/api/gen/go/user/v1"
	"github.com/example/user-platform/internal/gateway/httpserver"
	"github.com/example/user-platform/pkg/kafka"
	"github.com/example/user-platform/pkg/sse"
	"github.com/gin-gonic/gin"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func RunHTTPServer(ctx context.Context, httpAddr, userGRPC, loggerGRPC string, brokers []string) error {
	// ── gRPC connections ───────────────────────────────
	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	userConn, err := grpc.DialContext(ctx, userGRPC, dialOpts...)
	if err != nil {
		return fmt.Errorf("dial user service: %w", err)
	}
	defer userConn.Close()
	userClient := userpb.NewUserServiceClient(userConn)

	loggerConn, err := grpc.DialContext(ctx, loggerGRPC, dialOpts...)
	if err != nil {
		return fmt.Errorf("dial logger service: %w", err)
	}
	defer loggerConn.Close()

	// grpc-gateway mux
	gwmux := runtime.NewServeMux()
	if err := userpb.RegisterUserServiceHandlerFromEndpoint(ctx, gwmux, userGRPC, dialOpts); err != nil {
		return fmt.Errorf("register user handler: %w", err)
	}
	if err := loggerpb.RegisterLoggerServiceHandlerFromEndpoint(ctx, gwmux, loggerGRPC, dialOpts); err != nil {
		return fmt.Errorf("register logger handler: %w", err)
	}

	// ── SSE hub + Kafka consumer ───────────────────────
	hub := sse.NewHub()
	consumer := kafka.NewAuditConsumer(
		brokers,
		kafka.UserAuditTopic,
		"gateway-group",
		2,
		func(ctx context.Context, msg []byte) error {
			hub.Broadcast(string(msg))
			return nil
		},
	)
	go func() { consumer.Start(ctx) }()

	// ── Gin router ─────────────────────────────────────
	router := gin.Default()
	httpserver.RegisterRoutes(router, userClient, hub, gwmux)

	// ── HTTP server ────────────────────────────────────
	srv := &http.Server{
		Addr:              httpAddr,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Graceful shutdown
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown: %v", err)
		}
		_ = consumer.Close()
	}()

	log.Printf("HTTP gateway (Gin) listening on %s", httpAddr)
	return srv.ListenAndServe()
}
