package log

import (
	"go.uber.org/zap"
)

// NewLogger returns a new zap logger configured for JSON output.
func NewLogger() *zap.Logger {
	config := zap.NewProductionConfig()
	config.Encoding = "json"
	config.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	config.OutputPaths = []string{"stdout"}
	logger, _ := config.Build()
	return logger
}

// WithRequestID adds a request ID field to the logger.
func WithRequestID(logger *zap.Logger, requestID string) *zap.Logger {
	return logger.With(zap.String("request_id", requestID))
}

// Sync flushes any buffered log entries.
func Sync(logger *zap.Logger) {
	_ = logger.Sync()
}
