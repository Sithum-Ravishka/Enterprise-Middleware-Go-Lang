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
        user_id text,
        event_type text,
        correlation_id text,
        payload_json text
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
		`INSERT INTO audit_events (id, ts, user_id, event_type, correlation_id, payload_json)
         VALUES (?, ?, ?, ?, ?, ?)`,
		ev.ID, ev.Timestamp, ev.UserID, ev.EventType, ev.CorrelationID, ev.PayloadJSON,
	).WithContext(ctx).Exec()
}
