package httpserver

import (
	"net/http"

	userpb "github.com/example/user-platform/api/gen/go/user/v1"
	intErr "github.com/example/user-platform/pkg/errors"
	"github.com/gin-gonic/gin"
)

// healthHandler for /healthz
func healthHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		intErr.JSONSuccess(c, http.StatusOK, "OK", gin.H{"status": "ok"})
	}
}

func registerHandler(userClient userpb.UserServiceClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			Email    string `json:"email"`
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			intErr.HTTPError(c, http.StatusBadRequest, err)
			return
		}

		resp, err := userClient.Register(c.Request.Context(), &userpb.RegisterRequest{
			Email:    body.Email,
			Username: body.Username,
			Password: body.Password,
		})
		if err != nil {
			intErr.HTTPError(c, http.StatusConflict, err)
			return
		}

		intErr.JSONSuccess(c, http.StatusOK, "User registered successfully", gin.H{
			"user_id": resp.GetUserId(),
		})
	}
}

func loginHandler(userClient userpb.UserServiceClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			Email    string `json:"email"`
			Password string `json:"password"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			intErr.HTTPError(c, http.StatusBadRequest, err)
			return
		}

		resp, err := userClient.Login(c.Request.Context(), &userpb.LoginRequest{
			Email:    body.Email,
			Password: body.Password,
		})
		if err != nil {
			intErr.HTTPError(c, http.StatusUnauthorized, err)
			return
		}

		intErr.JSONSuccess(c, http.StatusOK, "Login successful", gin.H{
			"access_token":  resp.GetAccessToken(),
			"refresh_token": resp.GetRefreshToken(),
		})
	}
}

func profileHandler(userClient userpb.UserServiceClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")

		resp, err := userClient.GetProfile(c.Request.Context(), &userpb.GetProfileRequest{UserId: id})
		if err != nil {
			intErr.HTTPError(c, http.StatusNotFound, err)
			return
		}

		intErr.JSONSuccess(c, http.StatusOK, "User retrieved successfully", gin.H{
			"id":       resp.GetUserId(),
			"name":     resp.GetUsername(),
			"email":    resp.GetEmail(),
			"username": resp.GetUsername(),
		})
	}
}

func refreshHandler(userClient userpb.UserServiceClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			RefreshToken string `json:"refresh_token"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			intErr.HTTPError(c, http.StatusBadRequest, err)
			return
		}

		resp, err := userClient.RefreshSession(c.Request.Context(),
			&userpb.RefreshSessionRequest{RefreshToken: body.RefreshToken})
		if err != nil {
			intErr.HTTPError(c, http.StatusUnauthorized, err)
			return
		}

		intErr.JSONSuccess(c, http.StatusOK, "Token refreshed successfully", gin.H{
			"access_token":  resp.GetAccessToken(),
			"refresh_token": resp.GetRefreshToken(),
		})
	}
}

func logoutHandler(userClient userpb.UserServiceClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			RefreshToken string `json:"refresh_token"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			intErr.HTTPError(c, http.StatusBadRequest, err)
			return
		}

		if _, err := userClient.Logout(c.Request.Context(),
			&userpb.LogoutRequest{RefreshToken: body.RefreshToken}); err != nil {
			intErr.HTTPError(c, http.StatusUnauthorized, err)
			return
		}

		intErr.JSONSuccess(c, http.StatusOK, "Logged out successfully", gin.H{
			"ok": true,
		})
	}
}
