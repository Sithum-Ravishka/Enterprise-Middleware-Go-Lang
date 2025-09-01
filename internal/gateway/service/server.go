package service

import (
	"context"
	"encoding/json"
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
	handler := func(ctx context.Context, evt kafka.AuditEvent) error {
		// Re-emit the original event JSON to clients
		b, err := json.Marshal(evt)
		if err != nil {
			return err
		}
		hub.Broadcast(string(b))
		return nil
	}

	opts := kafka.ConsumerOpts{
		Brokers: brokers,
		Topic:   kafka.UserAuditTopic,
		GroupID: "gateway-group",
		// optional tuning:
		MinBytes: 1 << 10,  // 1KB
		MaxBytes: 10 << 20, // 10MB
		// DLQTopic: "user.audit.dlq.v1",
	}

	consumer := kafka.NewAuditConsumer(opts, handler, nil)
	// Start launches its own goroutine; no need to wrap in another go-routine.
	consumer.Start(ctx)

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
