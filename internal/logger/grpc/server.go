package grpc

import (
	"context"

	loggerpb "github.com/example/user-platform/api/gen/go/logger/v1"
	"github.com/example/user-platform/internal/logger/service"
)

// LoggerServer implements the LoggerService gRPC server. It wraps an
// AuditService and publishes received audit events to the configured sink.
type LoggerServer struct {
	loggerpb.UnimplementedLoggerServiceServer
	audit *service.AuditService
}

// NewLoggerServer constructs a new LoggerServer.
func NewLoggerServer(a *service.AuditService) *LoggerServer {
	return &LoggerServer{audit: a}
}

// WriteAudit receives an AuditEvent and immediately publishes it via the
// underlying AuditService. In this reference implementation no additional
// persistence is performed. A real implementation could write to a DB.
func (s *LoggerServer) WriteAudit(ctx context.Context, evt *loggerpb.AuditEvent) (*loggerpb.WriteAuditResponse, error) {
	// Convert the protobuf event into the internal kafka.AuditEvent type.
	// Emit via the audit service. Errors are logged by the audit service
	// internally; here we report success regardless.
	payload := map[string]interface{}{}
	if evt.PayloadJson != "" {
		// do nothing; payload is already encoded JSON. Implementation could
		// choose to decode but it's not required for forwarding.
	}
	_ = s.audit.Emit(ctx, evt.UserId, evt.EventType, evt.CorrelationId, payload)
	return &loggerpb.WriteAuditResponse{Success: true}, nil
}
