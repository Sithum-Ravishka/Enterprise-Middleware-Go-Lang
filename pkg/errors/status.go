package errors

import (
	"net/http"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/types/known/durationpb"
)

// FieldViolation describes a single invalid field used for BadRequest details.
type FieldViolation struct {
	Field       string
	Description string
}

// baseStatus builds a status with a mapped gRPC code and your standard message.
func baseStatus(code Code) *status.Status {
	switch code {
	case ErrEmailInUse:
		return status.New(codes.AlreadyExists, Message(code))
	case ErrInvalidCredentials:
		return status.New(codes.Unauthenticated, Message(code))
	case ErrWeakPassword, ErrValidation:
		return status.New(codes.InvalidArgument, Message(code))
	case ErrRateLimited:
		return status.New(codes.ResourceExhausted, Message(code))
	case ErrNotFound:
		return status.New(codes.NotFound, Message(code))
	default:
		return status.New(codes.Internal, Message(ErrInternal))
	}
}

// withCommonDetails attaches RequestInfo + ErrorInfo (and any extra details).
func withCommonDetails(st *status.Status, requestID string, code Code, extra ...protoadapt.MessageV1) *status.Status {
	details := []protoadapt.MessageV1{
		&errdetails.RequestInfo{RequestId: requestID},
		&errdetails.ErrorInfo{Reason: string(code), Domain: "user-platform"},
	}
	details = append(details, extra...)

	if st2, err := st.WithDetails(details...); err == nil {
		return st2
	}
	return st
}

// StatusFromCode emits a minimal status with RequestInfo + ErrorInfo attached.
func StatusFromCode(code Code, requestID string) error {
	st := baseStatus(code)
	st = withCommonDetails(st, requestID, code)
	return st.Err()
}

/* ---------- Rich helpers you can call from services ---------- */

// Validation builds INVALID_ARGUMENT with google.rpc.BadRequest field violations.
func Validation(requestID string, violations ...FieldViolation) error {
	st := baseStatus(ErrValidation)

	br := &errdetails.BadRequest{}
	for _, v := range violations {
		br.FieldViolations = append(br.FieldViolations, &errdetails.BadRequest_FieldViolation{
			Field:       v.Field,
			Description: v.Description,
		})
	}

	st = withCommonDetails(st, requestID, ErrValidation, br)
	return st.Err()
}

// EmailAlreadyInUse builds ALREADY_EXISTS with ErrorInfo and BadRequest(field=email).
func EmailAlreadyInUse(requestID, email string) error {
	st := baseStatus(ErrEmailInUse)
	br := &errdetails.BadRequest{
		FieldViolations: []*errdetails.BadRequest_FieldViolation{
			{Field: "email", Description: "already registered"},
		},
	}
	st = withCommonDetails(st, requestID, ErrEmailInUse, br)
	return st.Err()
}

// WeakPassword returns INVALID_ARGUMENT with a field violation on "password".
func WeakPassword(requestID, description string) error {
	if description == "" {
		description = "must be at least 12 characters"
	}
	return Validation(requestID, FieldViolation{Field: "password", Description: description})
}

// RateLimited builds RESOURCE_EXHAUSTED with RetryInfo so clients know when to retry.
func RateLimited(requestID string, retryAfter time.Duration) error {
	st := baseStatus(ErrRateLimited)
	ri := &errdetails.RetryInfo{RetryDelay: durationpb.New(retryAfter)}
	st = withCommonDetails(st, requestID, ErrRateLimited, ri)
	return st.Err()
}

// NotFound builds NOT_FOUND with optional resource hint in ResourceInfo.
func NotFound(requestID, resource string) error {
	st := baseStatus(ErrNotFound)
	if resource != "" {
		st = withCommonDetails(st, requestID, ErrNotFound, &errdetails.ResourceInfo{
			ResourceType: "resource",
			ResourceName: resource,
		})
	} else {
		st = withCommonDetails(st, requestID, ErrNotFound)
	}
	return st.Err()
}

// Internal builds INTERNAL with common details.
func Internal(requestID string) error {
	st := baseStatus(ErrInternal)
	st = withCommonDetails(st, requestID, ErrInternal)
	return st.Err()
}

// HTTPStatus maps an internal Code to an HTTP status.
func HTTPStatus(code Code) int {
	switch code {
	case ErrValidation:
		return http.StatusBadRequest
	case ErrEmailInUse:
		return http.StatusConflict
	case ErrNotFound:
		return http.StatusNotFound
	case ErrInvalidCredentials:
		return http.StatusUnauthorized
	case ErrRateLimited:
		return http.StatusTooManyRequests
	default:
		return http.StatusInternalServerError
	}
}

// -------- helpers (private) --------

func extractCodeAndRequestID(st *status.Status) (Code, string) {
	var (
		code      = ErrInternal
		requestID string
	)

	for _, d := range st.Details() {
		switch v := d.(type) {
		case *errdetails.ErrorInfo:
			if r := v.GetReason(); r != "" {
				code = Code(r)
			}
			if id := v.GetMetadata()["request_id"]; id != "" && requestID == "" {
				requestID = id
			}
		case *errdetails.RequestInfo:
			if id := v.GetRequestId(); id != "" && requestID == "" {
				requestID = id
			}
		}
	}
	return code, requestID
}

func fallbackCodeFromGRPC(c codes.Code) Code {
	switch c {
	case codes.InvalidArgument:
		return ErrValidation
	case codes.AlreadyExists:
		return ErrEmailInUse
	case codes.NotFound:
		return ErrNotFound
	case codes.Unauthenticated:
		return ErrInvalidCredentials
	case codes.ResourceExhausted:
		return ErrRateLimited
	default:
		return ErrInternal
	}
}

// -------- refactored Parse --------

// Parse extracts (Code, requestID, grpc.Code) from an error produced here.
func Parse(err error) (code Code, requestID string, grpcCode codes.Code) {
	st, ok := status.FromError(err)
	if !ok {
		return ErrInternal, "", codes.Internal
	}

	grpcCode = st.Code()
	code, requestID = extractCodeAndRequestID(st)

	if code == ErrInternal { // no Reason set -> map from grpc code
		code = fallbackCodeFromGRPC(grpcCode)
	}
	return
}
