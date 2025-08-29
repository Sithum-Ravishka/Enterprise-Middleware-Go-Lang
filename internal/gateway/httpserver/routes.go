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
	hub *sse.Hub,
	gwmux *runtime.ServeMux,
) {

	// add a middleware before routes
	router.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Origin, Content-Type, Authorization")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	// health check
	router.GET("/healthz", healthHandler())

	// SSE events
	router.GET("/events", gin.WrapH(sse.Handler(hub)))

	// User endpoints
	router.POST("/v1/register", registerHandler(userClient))
	router.POST("/v1/login", loginHandler(userClient))
	router.GET("/v1/profile/:id", profileHandler(userClient))
	router.POST("/v1/refresh", refreshHandler(userClient))
	router.POST("/v1/logout", logoutHandler(userClient))
}
