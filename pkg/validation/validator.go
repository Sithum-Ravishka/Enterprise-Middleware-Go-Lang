package validation

import (
	"regexp"

	"github.com/go-playground/validator/v10"

	pkgerr "github.com/example/user-platform/pkg/errors"
)

// Validator wraps go-playground/validator and custom rules.
type Validator struct {
	v *validator.Validate
}

// NewValidator creates a validator instance with custom rules.
func NewValidator() *Validator {
	v := validator.New()

	// Custom password rule:
	// - min length 12
	// - at least one lowercase, uppercase, digit, and special char.
	_ = v.RegisterValidation("password", func(fl validator.FieldLevel) bool {
		s := fl.Field().String()
		if len(s) < 12 {
			return false
		}
		var (
			lower = regexp.MustCompile(`[a-z]`).MatchString
			upper = regexp.MustCompile(`[A-Z]`).MatchString
			digit = regexp.MustCompile(`[0-9]`).MatchString
			spec  = regexp.MustCompile(`[^a-zA-Z0-9]`).MatchString
		)
		return lower(s) && upper(s) && digit(s) && spec(s)
	})

	return &Validator{v: v}
}

// tagMessage maps validator tags to human-friendly messages.
func tagMessage(tag string) string {
	switch tag {
	case "required":
		return "This field is required."
	case "email":
		return "Please enter a valid email address."
	case "min":
		return "This value is too short."
	case "max":
		return "This value is too long."
	case "password":
		return "Password must be at least 12 characters and include uppercase, lowercase, a number, and a special character."
	default:
		return "This value is invalid."
	}
}

// ValidateRegister validates inputs for registration and returns a standardized error
// with FieldViolations if any rule fails. requestID can be "" if not available.
func (v *Validator) ValidateRegister(requestID, email, username, password string) error {
	// Define struct with tags (includes custom "password" rule).
	var in = struct {
		Email    string `validate:"required,email"`
		Username string `validate:"required,min=3,max=32"`
		Password string `validate:"required,password"`
	}{
		Email:    email,
		Username: username,
		Password: password,
	}

	if err := v.v.Struct(in); err != nil {
		if verrs, ok := err.(validator.ValidationErrors); ok {
			violations := make([]pkgerr.FieldViolation, 0, len(verrs))
			for _, fe := range verrs {
				// fe.Field() returns struct field name; you can map to json tags if you prefer:
				field := fe.Field() // "Email", "Username", "Password"
				violations = append(violations, pkgerr.FieldViolation{
					Field:       field,
					Description: tagMessage(fe.Tag()),
				})
			}
			return pkgerr.Validation(requestID, violations...)
		}
		// Fallback (unexpected)
		return pkgerr.Internal(requestID)
	}
	return nil
}
