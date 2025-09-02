package httpserver

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const (
	traceIDKey  = "trace_id"
	clientIDKey = "client_id"
)

func TraceMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Trace ID (generated server-side if missing)
		traceID := c.GetHeader("X-Trace-Id")
		if traceID == "" {
			traceID = uuid.New().String()
		}

		// Client ID (frontend generates if possible, otherwise fallback)
		clientID := c.GetHeader("X-Client-Id")
		if clientID == "" {
			clientID = uuid.New().String()
		}

		// Save in gin context
		c.Set(traceIDKey, traceID)
		c.Set(clientIDKey, clientID)

		// Reflect in response headers (useful for debugging / frontend reading)
		c.Writer.Header().Set("X-Trace-Id", traceID)
		c.Writer.Header().Set("X-Client-Id", clientID)

		// Inject into std context so gRPC / services can propagate
		ctx := ContextWithTraceID(c.Request.Context(), traceID)
		ctx = ContextWithClientID(ctx, clientID)
		c.Request = c.Request.WithContext(ctx)

		c.Next()
	}
}

func getTraceID(c *gin.Context) string {
	if v, ok := c.Get(traceIDKey); ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func getClientID(c *gin.Context) string {
	if v, ok := c.Get(clientIDKey); ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
