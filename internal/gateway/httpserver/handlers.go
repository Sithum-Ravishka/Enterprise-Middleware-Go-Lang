package httpserver

import (
	"net/http"

	userpb "github.com/example/user-platform/api/gen/go/user/v1"
	intErr "github.com/example/user-platform/pkg/errors"
	"github.com/example/user-platform/pkg/trace"
	"github.com/gin-gonic/gin"
)

// healthHandler for /healthz
func healthHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		intErr.JSONSuccess(c, http.StatusOK, "OK", gin.H{"status": "ok"})
	}
}

func registerHandler(userClient userpb.UserServiceClient, audit AuditEmitter) gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := getTraceID(c)

		var body struct {
			Email    string `json:"email"`
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			_ = audit.Emit(c.Request.Context(),
				"", "WARN", "register payload bind failed",
				"gateway-service", c.FullPath(), c.Request.Method, traceID, "user-register")
			intErr.HTTPError(c, http.StatusBadRequest, err)
			return
		}

		// 👇 inject into outgoing gRPC metadata (trace_id, api_endpoint, http_method)
		ctx := trace.InjectToOutgoing(c.Request.Context(), traceID, c.FullPath(), c.Request.Method)

		resp, err := userClient.Register(ctx, &userpb.RegisterRequest{
			Email:    body.Email,
			Username: body.Username,
			Password: body.Password,
		})
		if err != nil {
			_ = audit.Emit(c.Request.Context(),
				"", "ERROR", "register RPC failed",
				"gateway-service", c.FullPath(), c.Request.Method, traceID, "user-register")
			intErr.HTTPError(c, http.StatusConflict, err)
			return
		}

		_ = audit.Emit(c.Request.Context(),
			resp.GetUserId(), "INFO", "user register successful",
			"gateway-service", c.FullPath(), c.Request.Method, traceID, "user-register")

		intErr.JSONSuccess(c, http.StatusOK, "User registered successfully", gin.H{
			"user_id": resp.GetUserId(),
		})
	}
}

func loginHandler(userClient userpb.UserServiceClient, audit AuditEmitter) gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := getTraceID(c)

		var body struct {
			Email    string `json:"email"`
			Password string `json:"password"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			_ = audit.Emit(c.Request.Context(),
				"", "WARN", "login payload bind failed",
				"gateway", c.FullPath(), c.Request.Method, traceID, "user-login")
			intErr.HTTPError(c, http.StatusBadRequest, err)
			return
		}

		resp, err := userClient.Login(c.Request.Context(), &userpb.LoginRequest{
			Email:    body.Email,
			Password: body.Password,
		})
		if err != nil {
			_ = audit.Emit(c.Request.Context(),
				"", "ERROR", "login RPC failed",
				"gateway", c.FullPath(), c.Request.Method, traceID, "user-login")
			intErr.HTTPError(c, http.StatusUnauthorized, err)
			return
		}

		_ = audit.Emit(c.Request.Context(),
			"", "INFO", "login successful",
			"gateway", c.FullPath(), c.Request.Method, traceID, "user-login")

		intErr.JSONSuccess(c, http.StatusOK, "Login successful", gin.H{
			"access_token":  resp.GetAccessToken(),
			"refresh_token": resp.GetRefreshToken(),
		})
	}
}

func profileHandler(userClient userpb.UserServiceClient, audit AuditEmitter) gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := getTraceID(c)
		id := c.Param("id")

		resp, err := userClient.GetProfile(c.Request.Context(), &userpb.GetProfileRequest{UserId: id})
		if err != nil {
			_ = audit.Emit(c.Request.Context(),
				id, "WARN", "get profile RPC failed",
				"gateway", c.FullPath(), c.Request.Method, traceID, "user-profile")
			intErr.HTTPError(c, http.StatusNotFound, err)
			return
		}

		_ = audit.Emit(c.Request.Context(),
			resp.GetUserId(), "INFO", "get profile successful",
			"gateway", c.FullPath(), c.Request.Method, traceID, "user-profile")

		intErr.JSONSuccess(c, http.StatusOK, "User retrieved successfully", gin.H{
			"id":       resp.GetUserId(),
			"name":     resp.GetUsername(),
			"email":    resp.GetEmail(),
			"username": resp.GetUsername(),
		})
	}
}

func refreshHandler(userClient userpb.UserServiceClient, audit AuditEmitter) gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := getTraceID(c)

		var body struct {
			RefreshToken string `json:"refresh_token"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			_ = audit.Emit(c.Request.Context(),
				"", "WARN", "refresh payload bind failed",
				"gateway", c.FullPath(), c.Request.Method, traceID, "token-refresh")
			intErr.HTTPError(c, http.StatusBadRequest, err)
			return
		}

		resp, err := userClient.RefreshSession(c.Request.Context(),
			&userpb.RefreshSessionRequest{RefreshToken: body.RefreshToken})
		if err != nil {
			_ = audit.Emit(c.Request.Context(),
				"", "ERROR", "refresh RPC failed",
				"gateway", c.FullPath(), c.Request.Method, traceID, "token-refresh")
			intErr.HTTPError(c, http.StatusUnauthorized, err)
			return
		}

		_ = audit.Emit(c.Request.Context(),
			"", "INFO", "token refresh successful",
			"gateway", c.FullPath(), c.Request.Method, traceID, "token-refresh")

		intErr.JSONSuccess(c, http.StatusOK, "Token refreshed successfully", gin.H{
			"access_token":  resp.GetAccessToken(),
			"refresh_token": resp.GetRefreshToken(),
		})
	}
}

func logoutHandler(userClient userpb.UserServiceClient, audit AuditEmitter) gin.HandlerFunc {
	return func(c *gin.Context) {
		traceID := getTraceID(c)

		var body struct {
			RefreshToken string `json:"refresh_token"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			_ = audit.Emit(c.Request.Context(),
				"", "WARN", "logout payload bind failed",
				"gateway", c.FullPath(), c.Request.Method, traceID, "user-logout")
			intErr.HTTPError(c, http.StatusBadRequest, err)
			return
		}

		if _, err := userClient.Logout(c.Request.Context(),
			&userpb.LogoutRequest{RefreshToken: body.RefreshToken}); err != nil {
			_ = audit.Emit(c.Request.Context(),
				"", "ERROR", "logout RPC failed",
				"gateway", c.FullPath(), c.Request.Method, traceID, "user-logout")
			intErr.HTTPError(c, http.StatusUnauthorized, err)
			return
		}

		_ = audit.Emit(c.Request.Context(),
			"", "INFO", "logout successful",
			"gateway", c.FullPath(), c.Request.Method, traceID, "user-logout")

		intErr.JSONSuccess(c, http.StatusOK, "Logged out successfully", gin.H{
			"ok": true,
		})
	}
}
