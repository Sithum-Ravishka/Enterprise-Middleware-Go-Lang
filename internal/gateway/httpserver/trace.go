package httpserver

import "context"

type traceIDCtxKey struct{}

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
