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
// Amended rules (architecture.md section 11, rows 9 and 10):
//   - NewAPIKey fails on a sha256 that is not 64 hex characters; zero keys is
//     allowed and rejects every request as invalid. Several X-API-Key header
//     values are invalid. Error text never contains the presented key.
//   - JWT: exp is required; leeway is 30 s; NewJWT errors on a missing or
//     < 32 byte HS256 secret, and on an RS256 key that is not RSA >= 2048 bits.
//     A non-"Bearer" Authorization scheme counts as missing credentials; an
//     empty bearer token is invalid. getenv, readFile and now default to
//     os.Getenv, os.ReadFile and time.Now when nil.
//   - ClientIPKey: port stripped, IPv4 as-is, ::ffff: IPv4-mapped un-mapped,
//     other IPv6 masked to its /64 and rendered like "2001:db8:1:2::/64";
//     unparseable input is returned unchanged.
//
// Dependency rule: auth may import internal/config (for its settings types)
// and the JWT library; nothing else internal (docs/architecture.md).
//
// Planned helpers (Phase 3):
//
//	func NewAPIKey(keys []config.APIKey) (Authenticator, error)
//	func NewJWT(s config.JWTSettings, getenv func(string) string,
//	    readFile func(string) ([]byte, error), now func() time.Time) (Authenticator, error)
//	func ClientIPKey(remoteAddr string) string
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
	// wrong issuer or audience, or a missing, empty or non-string client-ID claim.
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
