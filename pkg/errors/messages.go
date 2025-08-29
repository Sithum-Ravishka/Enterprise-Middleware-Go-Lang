package errors

// user-facing messages for error codes (single place)
var messages = map[Code]string{
	ErrInternal:           "Something went wrong on our side. Please try again.",
	ErrValidation:         "Invalid input. Please check your request and try again.",
	ErrEmailInUse:         "That email is already registered. Try signing in.",
	ErrInvalidCredentials: "We couldn’t verify your email or password.",
	ErrWeakPassword:       "Your password needs at least 12 characters with a mix of letters and numbers.",
	ErrRateLimited:        "Too many attempts. Please wait a moment and try again.",
	ErrNotFound:           "The requested resource was not found.",
}

func Message(code Code) string {
	if msg, ok := messages[code]; ok {
		return msg
	}
	return messages[ErrInternal]
}
