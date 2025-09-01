package grpc

import (
	"context"

	loggerpb "github.com/example/user-platform/api/gen/go/logger/v1"
	"github.com/example/user-platform/internal/logger/service"
	"github.com/example/user-platform/pkg/kafka"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	loggerpb.UnimplementedLoggerServiceServer
	svc *service.LoggerService
}

func NewServer(svc *service.LoggerService) *Server { return &Server{svc: svc} }

func (s *Server) WriteAudit(ctx context.Context, req *loggerpb.AuditEvent) (*loggerpb.WriteAuditResponse, error) {
	if s == nil || s.svc == nil {
		return nil, status.Error(codes.Unavailable, "logger service unavailable")
	}
	ev := kafka.AuditEvent{
		ID:          req.GetId(),
		Timestamp:   req.GetTs(),
		LogLevel:    req.GetLogLevel(),
		Message:     req.GetMessage(),
		ServiceName: req.GetServiceName(),
		APIEndpoint: req.GetApiEndpoint(),
		HTTPMethod:  req.GetHttpMethod(),
		UserID:      req.GetUserId(),
		TraceID:     req.GetTraceId(),
		Reason:      req.GetReason(),
	}
	if err := s.svc.WriteAudit(ctx, ev); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &loggerpb.WriteAuditResponse{Success: true}, nil
}
