package trace

import (
	"context"

	"google.golang.org/grpc/metadata"
)

const (
	MetaTraceID    = "x-trace-id"
	MetaAPIPath    = "x-api-endpoint"
	MetaHTTPMethod = "x-http-method"
	MetaClientID   = "x-client-id" // 👈 new
)

type Meta struct {
	TraceID    string
	APIPath    string
	HTTPMethod string
	ClientID   string // 👈 new
}

// InjectToOutgoing adds the tracing fields to outgoing gRPC metadata.
func InjectToOutgoing(ctx context.Context, traceID, apiPath, httpMethod, clientID string) context.Context {
	if traceID == "" && apiPath == "" && httpMethod == "" && clientID == "" {
		return ctx
	}
	pairs := []string{}
	if traceID != "" {
		pairs = append(pairs, MetaTraceID, traceID)
	}
	if apiPath != "" {
		pairs = append(pairs, MetaAPIPath, apiPath)
	}
	if httpMethod != "" {
		pairs = append(pairs, MetaHTTPMethod, httpMethod)
	}
	if clientID != "" {
		pairs = append(pairs, MetaClientID, clientID)
	}
	return metadata.AppendToOutgoingContext(ctx, pairs...)
}

// ExtractFromIncoming reads the tracing fields from incoming gRPC metadata.
func ExtractFromIncoming(ctx context.Context) Meta {
	var m Meta
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(MetaTraceID); len(v) > 0 {
			m.TraceID = v[0]
		}
		if v := md.Get(MetaAPIPath); len(v) > 0 {
			m.APIPath = v[0]
		}
		if v := md.Get(MetaHTTPMethod); len(v) > 0 {
			m.HTTPMethod = v[0]
		}
		if v := md.Get(MetaClientID); len(v) > 0 {
			m.ClientID = v[0]
		}
	}
	return m
}
