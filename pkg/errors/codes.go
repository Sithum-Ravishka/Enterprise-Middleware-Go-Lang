package errors

// Code represents internal error codes used throughout the service.
type Code string

const (
	// General
	ErrInternal    Code = "internal_error"
	ErrValidation  Code = "validation_error"
	ErrNotFound    Code = "not_found"
	ErrRateLimited Code = "rate_limited"

	// Auth/User
	ErrEmailInUse         Code = "email_in_use"
	ErrInvalidCredentials Code = "invalid_credentials"
	ErrWeakPassword       Code = "weak_password"
)
