package retry

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// WithBackoff retries a function with exponential backoff and jitter.
func WithBackoff(ctx context.Context, fn func() error, attempts int) error {
	var err error
	for i := 0; i < attempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		// Wait before retrying
		backoff := time.Duration(math.Pow(2, float64(i))) * 100 * time.Millisecond
		jitter := time.Duration(rand.Intn(100)) * time.Millisecond
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff + jitter):
		}
	}
	return err
}
