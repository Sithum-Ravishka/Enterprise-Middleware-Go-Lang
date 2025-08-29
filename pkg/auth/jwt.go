package auth

import (
    "crypto/rand"
    "crypto/rsa"
    "errors"
    "fmt"
    "io/ioutil"
    "time"

    jwt "github.com/golang-jwt/jwt/v5"
)

// TokenManager handles issuing and verifying JWT tokens.
type TokenManager struct {
	privateKey *rsa.PrivateKey
	publicKey  *rsa.PublicKey
}

// NewTokenManager initializes a TokenManager from key file paths.
func NewTokenManager(privateKeyPath, publicKeyPath string) (*TokenManager, error) {
    privData, err := ioutil.ReadFile(privateKeyPath)
    if err != nil {
        return nil, fmt.Errorf("read private key: %w", err)
    }
    pubData, err := ioutil.ReadFile(publicKeyPath)
    if err != nil {
        return nil, fmt.Errorf("read public key: %w", err)
    }

    privKey, err := jwt.ParseRSAPrivateKeyFromPEM(privData)
    if err != nil {
        return nil, fmt.Errorf("parse private key: %w", err)
    }
    pubKey, err := jwt.ParseRSAPublicKeyFromPEM(pubData)
    if err != nil {
        return nil, fmt.Errorf("parse public key: %w", err)
    }

    return &TokenManager{
        privateKey: privKey,
        publicKey:  pubKey,
    }, nil
}

// Generate creates access and refresh tokens for a user.
func (t *TokenManager) Generate(userID string) (string, string, error) {
    // Create access token valid for 15 minutes
    accessToken := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
        "sub": userID,
        "exp": time.Now().Add(15 * time.Minute).Unix(),
    })
    refreshToken := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
        "sub": userID,
        "exp": time.Now().Add(7 * 24 * time.Hour).Unix(),
    })
    access, err := accessToken.SignedString(t.privateKey)
    if err != nil {
        return "", "", err
    }
    refresh, err := refreshToken.SignedString(t.privateKey)
    if err != nil {
        return "", "", err
    }
    return access, refresh, nil
}

// Verify verifies the token and returns the user ID.
func (t *TokenManager) Verify(token string) (string, error) {
    // Parse and validate the token. jwt.Parse uses the token's header to
    // determine the signing algorithm and expects the keyfunc to return the
    // matching public key. If the token is invalid or the claims are not
    // structured as expected, an error will be returned.
    parsed, err := jwt.Parse(token, func(token *jwt.Token) (interface{}, error) {
        return t.publicKey, nil
    })
    if err != nil {
        return "", fmt.Errorf("invalid token: %w", err)
    }
    if !parsed.Valid {
        return "", errors.New("invalid token")
    }
    claims, ok := parsed.Claims.(jwt.MapClaims)
    if !ok {
        return "", errors.New("invalid claims")
    }
    sub, ok := claims["sub"].(string)
    if !ok {
        return "", errors.New("invalid subject")
    }
    return sub, nil
}

// GenerateKeyPair generates a new RSA key pair (for testing).
func GenerateKeyPair() (*rsa.PrivateKey, *rsa.PublicKey, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}
	return priv, &priv.PublicKey, nil
}
