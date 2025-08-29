package errors

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//
// ---------- SUCCESS RESPONSE ----------
//

type successEnvelope struct {
	Status    string      `json:"status"`               // "success"
	Code      int         `json:"code"`                 // HTTP status code
	Message   string      `json:"message"`              // human-friendly message
	Data      interface{} `json:"data,omitempty"`       // response payload
	Timestamp string      `json:"timestamp"`            // UTC timestamp
	Meta      interface{} `json:"meta,omitempty"`       // optional (pagination, etc.)
	RequestID string      `json:"request_id,omitempty"` // correlation id
}

// JSONSuccess writes a standardized success response.
func JSONSuccess(c *gin.Context, httpCode int, message string, data any, meta ...any) {
	env := successEnvelope{
		Status:    "success",
		Code:      httpCode,
		Message:   message,
		Data:      data,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		RequestID: c.GetString("request_id"),
	}

	if len(meta) > 0 {
		env.Meta = meta[0]
	}

	c.JSON(httpCode, env)
}

//
// ---------- ERROR RESPONSE ----------
//

type errorEnvelope struct {
	Error errorPayload `json:"error"`
	Meta  interface{}  `json:"meta,omitempty"` // optional extra context
}

type errorPayload struct {
	Code      int         `json:"code"`                 // HTTP status code
	Message   string      `json:"message"`              // user-facing message
	Details   interface{} `json:"details,omitempty"`    // structured details (field, reason)
	RequestID string      `json:"request_id,omitempty"` // correlation id
	Timestamp string      `json:"timestamp"`            // UTC timestamp
}

// HTTPError converts gRPC errors into the standard error envelope.
func HTTPError(c *gin.Context, defaultHTTP int, err error, meta ...any) {
	st, ok := status.FromError(err)
	if !ok {
		// fallback: not a gRPC error
		writeError(c, defaultHTTP, err.Error(), nil, c.GetString("request_id"), meta...)
		return
	}

	httpCode := grpcToHTTP(st.Code())
	if httpCode == 0 {
		httpCode = defaultHTTP
	}

	// Extract structured details if present
	var details any
	for _, d := range st.Details() {
		switch v := d.(type) {
		case *errdetails.BadRequest:
			if len(v.FieldViolations) == 1 {
				details = map[string]string{
					"field":  v.FieldViolations[0].GetField(),
					"reason": v.FieldViolations[0].GetDescription(),
				}
			} else if len(v.FieldViolations) > 1 {
				type violation struct {
					Field  string `json:"field"`
					Reason string `json:"reason"`
				}
				var out []violation
				for _, fv := range v.FieldViolations {
					out = append(out, violation{
						Field:  fv.GetField(),
						Reason: fv.GetDescription(),
					})
				}
				details = map[string]any{"violations": out}
			}
		}
	}

	writeError(c, httpCode, st.Message(), details, c.GetString("request_id"), meta...)
}

// writeError creates the standardized error response.
func writeError(c *gin.Context, httpCode int, message string, details any, requestID string, meta ...any) {
	env := errorEnvelope{
		Error: errorPayload{
			Code:      httpCode,
			Message:   message,
			Details:   details,
			RequestID: requestID,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		},
	}
	if len(meta) > 0 {
		env.Meta = meta[0]
	}

	c.JSON(httpCode, env)
}

//
// ---------- GRPC to HTTP MAPPING ----------
//

func grpcToHTTP(c codes.Code) int {
	switch c {
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.AlreadyExists:
		return http.StatusConflict
	case codes.NotFound:
		return http.StatusNotFound
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.FailedPrecondition:
		return http.StatusPreconditionFailed
	case codes.Aborted:
		return http.StatusConflict
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.Canceled:
		return http.StatusBadRequest // some APIs use 499, but non-standard
	case codes.Internal, codes.DataLoss, codes.Unknown:
		return http.StatusInternalServerError
	default:
		return 0
	}
}
