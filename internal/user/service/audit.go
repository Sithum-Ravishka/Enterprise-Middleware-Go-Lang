package service

import (
	"context"
	"encoding/json"
	"time"

	"github.com/example/user-platform/pkg/kafka"
	"github.com/google/uuid"
)

type AuditEmitter interface {
	// Emit a single audit log event using the new schema.
	Emit(ctx context.Context,
		userID string,
		logLevel string,
		message string,
		serviceName string,
		apiEndpoint string,
		httpMethod string,
		traceID string,
		reason string,
	) error
}

type AuditService struct {
	producer *kafka.Producer
	sess     any // reserved for future use
}

func NewAuditService(producer *kafka.Producer, session any) *AuditService {
	return &AuditService{producer: producer, sess: session}
}

func (a *AuditService) Emit(
	ctx context.Context,
	userID string,
	logLevel string,
	message string,
	serviceName string,
	apiEndpoint string,
	httpMethod string,
	traceID string,
	reason string,
) error {
	if a == nil || a.producer == nil {
		return nil
	}

	ev := kafka.AuditEvent{
		ID:          uuid.NewString(),  // uuid string (generated at producer)
		Timestamp:   time.Now().Unix(), // unix seconds (UTC)
		LogLevel:    logLevel,          // INFO|WARN|ERROR|DEBUG
		Message:     message,
		ServiceName: serviceName,
		APIEndpoint: apiEndpoint,
		HTTPMethod:  httpMethod,
		UserID:      userID, // "Public-User" if unknown
		TraceID:     traceID,
		Reason:      reason,
	}

	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}

	// Use userID as the Kafka key (unchanged behavior)
	return a.producer.Publish(ctx, []byte(userID), data)
}
