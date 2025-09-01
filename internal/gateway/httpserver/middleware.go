package httpserver

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

const traceIDKey = "trace_id"

func TraceMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := c.GetHeader("X-Trace-Id")
		if traceID == "" {
			traceID = uuid.New().String()
		}

		// save in gin context
		c.Set(traceIDKey, traceID)
		// also add to response headers
		c.Writer.Header().Set("X-Trace-Id", traceID)
		// inject into std context so gRPC interceptors can grab it
		ctx := ContextWithTraceID(c.Request.Context(), traceID)
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
