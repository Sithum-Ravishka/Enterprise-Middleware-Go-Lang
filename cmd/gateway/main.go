package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/example/user-platform/internal/gateway/service"
)

type config struct {
	httpAddr   string
	userGRPC   string
	loggerGRPC string
	brokers    []string
}

func main() {
	cfg := loadConfig()

	// Context cancels automatically on SIGINT/SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Run server; exit on first error or signal
	grp, ctx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		return service.RunHTTPServer(ctx, cfg.httpAddr, cfg.userGRPC, cfg.loggerGRPC, cfg.brokers)
	})

	// Wait until a signal or server error
	if err := grp.Wait(); err != nil && !isContextCanceled(ctx) {
		log.Printf("gateway http server exited with error: %v", err)
	}

	// Give background work up to 10s to honor ctx cancel
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	<-shutdownCtx.Done()
	if shutdownCtx.Err() == context.DeadlineExceeded {
		log.Println("forced shutdown after timeout")
	}
}

func loadConfig() config {
	var cfg config

	httpDefault := getEnv("HTTP_ADDR", ":8080")
	userDefault := getEnv("USER_SERVICE_ADDR", "user-service:50051")
	loggerDefault := getEnv("LOGGER_SERVICE_ADDR", "logger-service:9092")
	brokersDefault := getEnv("KAFKA_BROKERS", "kafka:9092")

	flag.StringVar(&cfg.httpAddr, "http", httpDefault, "HTTP listen address")
	flag.StringVar(&cfg.userGRPC, "user-grpc", userDefault, "user-service gRPC address")
	flag.StringVar(&cfg.loggerGRPC, "logger-grpc", loggerDefault, "logger-service gRPC address")

	// Parse brokers directly into slice (avoids temp CSV string)
	flag.Func("brokers", "Kafka broker list (comma-separated)", func(s string) error {
		cfg.brokers = splitAndTrimOrDefault(s, brokersDefault)
		return nil
	})

	flag.Parse()

	// If brokers flag wasn’t provided, derive from env/default
	if len(cfg.brokers) == 0 {
		cfg.brokers = splitAndTrimOrDefault(brokersDefault, brokersDefault)
	}
	return cfg
}

// --- helpers ---

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func splitAndTrimOrDefault(input, def string) []string {
	s := input
	if strings.TrimSpace(s) == "" {
		s = def
	}
	parts := strings.Split(s, ",")
	out := outPool(parts) // pre-size result
	for _, p := range parts {
		if v := strings.TrimSpace(p); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// outPool preallocates with a likely-good capacity to reduce reallocations.
func outPool(parts []string) []string {
	// Assume at least half survive trimming; adjust as needed.
	capacity := len(parts)
	if capacity < 4 {
		capacity = 4
	}
	return make([]string, 0, capacity)
}

func isContextCanceled(ctx context.Context) bool {
	return ctx.Err() == context.Canceled || ctx.Err() == context.DeadlineExceeded
}
