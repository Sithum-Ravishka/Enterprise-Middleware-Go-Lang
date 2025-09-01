package httpserver

import (
	userpb "github.com/example/user-platform/api/gen/go/user/v1"

	"github.com/example/user-platform/pkg/sse"
	"github.com/gin-gonic/gin"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
)

// RegisterRoutes attaches all HTTP endpoints.
func RegisterRoutes(
	router *gin.Engine,
	userClient userpb.UserServiceClient,
	audit AuditEmitter,
	hub *sse.Hub,
	gwmux *runtime.ServeMux,
) {
	// tracing middleware
	router.Use(TraceMiddleware())

	// CORS
	router.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Origin, Content-Type, Authorization, X-Trace-Id")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	// health
	router.GET("/healthz", healthHandler())

	// SSE events
	router.GET("/events", gin.WrapH(sse.Handler(hub)))

	// user endpoints
	router.POST("/v1/register", registerHandler(userClient, audit))
	router.POST("/v1/login", loginHandler(userClient, audit))
	router.GET("/v1/profile/:id", profileHandler(userClient, audit))
	router.POST("/v1/refresh", refreshHandler(userClient, audit))
	router.POST("/v1/logout", logoutHandler(userClient, audit))
}
