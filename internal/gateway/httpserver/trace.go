package httpserver

import "context"

type traceIDCtxKey struct{}
type clientIDCtxKey struct{}

// ContextWithTraceID stores a trace id in context
func ContextWithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDCtxKey{}, traceID)
}

// TraceIDFromContext retrieves the trace id from context
func TraceIDFromContext(ctx context.Context) (string, bool) {
	v := ctx.Value(traceIDCtxKey{})
	if s, ok := v.(string); ok && s != "" {
		return s, true
	}
	return "", false
}

// ContextWithClientID stores a client id in context
func ContextWithClientID(ctx context.Context, clientID string) context.Context {
	return context.WithValue(ctx, clientIDCtxKey{}, clientID)
}

// ClientIDFromContext retrieves the client id from context
func ClientIDFromContext(ctx context.Context) (string, bool) {
	v := ctx.Value(clientIDCtxKey{})
	if s, ok := v.(string); ok && s != "" {
		return s, true
	}
	return "", false
}
