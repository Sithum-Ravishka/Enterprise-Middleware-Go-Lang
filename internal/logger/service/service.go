package service

import (
	"context"
	"errors"

	"github.com/example/user-platform/pkg/kafka"
	"github.com/gocql/gocql"
)

type LoggerService struct{ session *gocql.Session }

func NewLoggerService(sess *gocql.Session) (*LoggerService, error) {
	if sess == nil {
		return nil, errors.New("nil Cassandra session")
	}
	ls := &LoggerService{session: sess}
	cql := `CREATE TABLE IF NOT EXISTS audit_events (
        id text PRIMARY KEY,
        ts bigint,
        log_level text,
        message text,
        service_name text,
        api_endpoint text,
        http_method text,
        user_id text,
        trace_id text,
        reason text
    )`
	if err := ls.session.Query(cql).Exec(); err != nil {
		return nil, err
	}
	return ls, nil
}

func (s *LoggerService) WriteAudit(ctx context.Context, ev kafka.AuditEvent) error {
	if s == nil || s.session == nil {
		return errors.New("logger service not initialized")
	}
	return s.session.Query(
		`INSERT INTO audit_events (id, ts, log_level, message, service_name, api_endpoint, http_method, user_id, trace_id, reason)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ev.ID, ev.Timestamp, ev.LogLevel, ev.Message, ev.ServiceName, ev.APIEndpoint, ev.HTTPMethod, ev.UserID, ev.TraceID, ev.Reason,
	).WithContext(ctx).Exec()
}
