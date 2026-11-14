// Package auth authenticates clients for routes that require it (Phase 3).
//
// Two Authenticators are planned:
//   - API key: reads the X-API-Key header, hashes it with SHA-256 and looks
//     the hex digest up in a map built from config.AuthSettings.APIKeys.
//     The map lookup is on a hash of the secret, so lookup time does not
//     depend on how many leading bytes of a guessed key are correct.
//   - JWT: reads "Authorization: Bearer <token>", verifies it with
//     golang-jwt/jwt/v5 restricted to the single configured algorithm
//     (jwt.WithValidMethods), checks exp/nbf with a small leeway, and the
//     issuer/audience when configured.
//
// Dependency rule: auth may import internal/config (for its settings types)
// and the JWT library; nothing else internal (docs/architecture.md).
//
// Planned helpers (Phase 3):
//
//	func NewAPIKey(keys []config.APIKey) Authenticator
//	func NewJWT(s config.JWTSettings, now func() time.Time) (Authenticator, error)
//	func WithIdentity(ctx context.Context, id Identity) context.Context
//	func IdentityFrom(ctx context.Context) (Identity, bool)
package auth

import (
	"errors"
	"net/http"
)

// HeaderAPIKey is the request header carrying an API key.
const HeaderAPIKey = "X-API-Key"

// Errors returned by Authenticate. The proxy answers 401 for both and sends
// the same generic body, so a caller cannot learn which check failed.
var (
	// ErrMissingCredentials: no API key header / no bearer token.
	ErrMissingCredentials = errors.New("auth: missing credentials")
	// ErrInvalidCredentials: unknown key, bad signature, expired token,
	// wrong issuer or audience, or missing client-ID claim.
	ErrInvalidCredentials = errors.New("auth: invalid credentials")
)

// Identity is the authenticated client.
type Identity struct {
	// ClientID is the stable client identifier (API key's client_id or the
	// JWT's client-ID claim). It is the rate-limit identity.
	ClientID string
	// Method is "api_key" or "jwt", for logs and metrics.
	Method string
}

// Authenticator validates the credentials on a request.
//
// Implementations must be safe for concurrent use, must not modify the
// request, and must not do network I/O (keys are loaded at construction,
// when the snapshot is built). Authenticate returns an error wrapping
// ErrMissingCredentials or ErrInvalidCredentials on failure.
type Authenticator interface {
	Authenticate(r *http.Request) (Identity, error)
}
