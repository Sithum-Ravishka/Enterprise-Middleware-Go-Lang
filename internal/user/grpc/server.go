package grpc

import (
	"context"

	userpb "github.com/example/user-platform/api/gen/go/user/v1"
	usersvc "github.com/example/user-platform/internal/user/service"
	"github.com/google/uuid"
)

// UserServer implements userpb.UserServiceServer.
type UserServer struct {
	userpb.UnimplementedUserServiceServer
	svc *usersvc.UserService
}

func NewUserServer(svc *usersvc.UserService) *UserServer {
	return &UserServer{svc: svc}
}

func (s *UserServer) Register(ctx context.Context, req *userpb.RegisterRequest) (*userpb.RegisterResponse, error) {
	// Map to your domain service call. Example:
	id, err := s.svc.Register(ctx, req.GetEmail(), req.GetUsername(), req.GetPassword())
	if err != nil {
		return nil, err
	}
	return &userpb.RegisterResponse{UserId: id}, nil
}

func (s *UserServer) Login(ctx context.Context, req *userpb.LoginRequest) (*userpb.LoginResponse, error) {
	access, refresh, err := s.svc.Login(ctx, req.GetEmail(), req.GetPassword())
	if err != nil {
		return nil, err
	}
	return &userpb.LoginResponse{
		AccessToken:  access,
		RefreshToken: refresh,
	}, nil
}

func (s *UserServer) GetProfile(ctx context.Context, req *userpb.GetProfileRequest) (*userpb.GetProfileResponse, error) {
	u, err := s.svc.GetProfile(ctx, req.GetUserId())
	if err != nil {
		return nil, err
	}
	return &userpb.GetProfileResponse{
		UserId:   uuid.UUID(u.ID.Bytes).String(),
		Email:    u.Email,
		Username: u.Username,
	}, nil
}

func (s *UserServer) RefreshSession(ctx context.Context, req *userpb.RefreshSessionRequest) (*userpb.RefreshSessionResponse, error) {
	access, refresh, err := s.svc.RefreshSession(ctx, req.GetRefreshToken())
	if err != nil {
		return nil, err
	}
	return &userpb.RefreshSessionResponse{
		AccessToken:  access,
		RefreshToken: refresh,
	}, nil
}

func (s *UserServer) Logout(ctx context.Context, req *userpb.LogoutRequest) (*userpb.LogoutResponse, error) {
	if err := s.svc.Logout(ctx, req.GetRefreshToken()); err != nil {
		return nil, err
	}
	// If your proto defines an empty message payload, just return a zero-value struct.
	return &userpb.LogoutResponse{}, nil
}
