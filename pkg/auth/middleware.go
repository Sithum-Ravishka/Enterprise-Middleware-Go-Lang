package auth

import (
    "context"
    "strings"

    "github.com/example/user-platform/pkg/errors"
    "go.uber.org/zap"
    "google.golang.org/grpc"
)

// UnaryServerInterceptor returns a gRPC interceptor for authentication.
func UnaryServerInterceptor(tm *TokenManager, logger *zap.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		md, ok := grpcMDFromIncomingContext(ctx)
		if !ok {
			logger.Warn("no metadata found")
			return nil, errors.StatusFromCode(errors.ErrInvalidCredentials, "")
		}
		authHeader := ""
		if values, ok := md["authorization"]; ok && len(values) > 0 {
			authHeader = values[0]
		}
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			return nil, errors.StatusFromCode(errors.ErrInvalidCredentials, "")
		}
		sub, err := tm.Verify(parts[1])
		if err != nil {
			return nil, errors.StatusFromCode(errors.ErrInvalidCredentials, "")
		}
		ctx = context.WithValue(ctx, "user_id", sub)
		return handler(ctx, req)
	}
}

// grpcMDFromIncomingContext extracts metadata from context.
func grpcMDFromIncomingContext(ctx context.Context) (map[string][]string, bool) {
	md, ok := ctx.Value("grpc.Metadata").(map[string][]string)
	return md, ok
}
