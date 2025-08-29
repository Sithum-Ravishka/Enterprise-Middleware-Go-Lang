package auth

import (
    "golang.org/x/crypto/bcrypt"
)

// HashPassword hashes a plaintext password using bcrypt.
// Bcrypt is a widely used password hashing function that incorporates a salt
// and work factor. The DefaultCost provided by the bcrypt package is
// sufficient for most applications and automatically generates a random salt.
func HashPassword(password string) (string, error) {
    hashed, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
    if err != nil {
        return "", err
    }
    return string(hashed), nil
}

// ComparePassword compares a bcrypt hashed password with its possible
// plaintext equivalent. It returns true on success, or false if the password
// does not match the hash. The error return can be inspected for more
// detailed failure reasons but should generally be treated as a mismatch.
func ComparePassword(hash, password string) (bool, error) {
    err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(password))
    if err != nil {
        return false, err
    }
    return true, nil
}
