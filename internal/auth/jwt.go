package auth

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/amitkhanal596/tollgate/internal/config"
)

const (
	// jwtLeeway absorbs clock skew between the issuer and the gateway for
	// exp and nbf; 30 s is small enough that a revoked short-lived token
	// does not linger noticeably.
	jwtLeeway = 30 * time.Second
	// minHMACSecretBytes: HS256 secrets shorter than the 256-bit hash
	// output weaken the MAC and are within reach of offline brute force.
	minHMACSecretBytes = 32
	// minRSABits: RSA below 2048 bits is considered breakable (NIST SP 800-131A).
	minRSABits = 2048
	// defaultClientIDClaim is used when JWTSettings.ClientIDClaim is empty.
	defaultClientIDClaim = "sub"
)

// jwtAuth verifies "Authorization: Bearer <jwt>" tokens. All fields are set
// once in NewJWT and never written again, so Authenticate is safe for
// concurrent use without locks (jwt.Parser only reads its options).
type jwtAuth struct {
	parser      *jwt.Parser
	keyfunc     jwt.Keyfunc
	clientClaim string
}

// NewJWT builds a JWT Authenticator. It fails closed: an unknown algorithm,
// an HS256 secret that is missing or shorter than 32 bytes, or an RS256 key
// that is not an RSA public key of at least 2048 bits is an error, and the
// returned Authenticator is nil. getenv, readFile and now default to
// os.Getenv, os.ReadFile and time.Now when nil.
func NewJWT(settings config.JWTSettings, getenv func(string) string, readFile func(string) ([]byte, error), now func() time.Time) (Authenticator, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	if readFile == nil {
		readFile = os.ReadFile
	}
	if now == nil {
		now = time.Now
	}

	var keyfunc jwt.Keyfunc
	switch settings.Algorithm {
	case config.JWTHS256:
		if settings.HMACSecretEnv == "" {
			return nil, errors.New("auth: jwt HS256 requires hmac_secret_env")
		}
		secret := getenv(settings.HMACSecretEnv)
		// The error names the variable and the length rule, never the value.
		if len(secret) < minHMACSecretBytes {
			return nil, fmt.Errorf("auth: jwt HS256 secret in $%s is missing or shorter than %d bytes", settings.HMACSecretEnv, minHMACSecretBytes)
		}
		key := []byte(secret)
		keyfunc = func(t *jwt.Token) (any, error) {
			// Defence in depth on top of WithValidMethods: only ever hand the
			// HMAC secret to the HMAC verifier for exactly HS256.
			if m, ok := t.Method.(*jwt.SigningMethodHMAC); !ok || m.Alg() != jwt.SigningMethodHS256.Alg() {
				return nil, errors.New("unexpected signing method")
			}
			return key, nil
		}
	case config.JWTRS256:
		if settings.PublicKeyFile == "" {
			return nil, errors.New("auth: jwt RS256 requires public_key_file")
		}
		data, err := readFile(settings.PublicKeyFile)
		if err != nil {
			return nil, fmt.Errorf("auth: jwt RS256 public key: %w", err)
		}
		pub, err := parseRSAPublicKey(data)
		if err != nil {
			return nil, fmt.Errorf("auth: jwt RS256 public key %s: %w", settings.PublicKeyFile, err)
		}
		keyfunc = func(t *jwt.Token) (any, error) {
			// Defence in depth on top of WithValidMethods: *SigningMethodRSAPSS
			// is a different type, so PS256 cannot reach this key either.
			if m, ok := t.Method.(*jwt.SigningMethodRSA); !ok || m.Alg() != jwt.SigningMethodRS256.Alg() {
				return nil, errors.New("unexpected signing method")
			}
			return pub, nil
		}
	default:
		return nil, fmt.Errorf("auth: jwt algorithm %q is not supported (want %s or %s)", settings.Algorithm, config.JWTHS256, config.JWTRS256)
	}

	opts := []jwt.ParserOption{
		// Pin the single configured algorithm: blocks "alg: none" and
		// HS/RS confusion before the keyfunc is ever called.
		jwt.WithValidMethods([]string{string(settings.Algorithm)}),
		// A token without exp would be valid forever if leaked.
		jwt.WithExpirationRequired(),
		jwt.WithLeeway(jwtLeeway),
		// Injected clock keeps tests deterministic.
		jwt.WithTimeFunc(now),
	}
	if settings.Issuer != "" {
		opts = append(opts, jwt.WithIssuer(settings.Issuer))
	}
	if settings.Audience != "" {
		opts = append(opts, jwt.WithAudience(settings.Audience))
	}

	claim := settings.ClientIDClaim
	if claim == "" {
		claim = defaultClientIDClaim
	}
	return &jwtAuth{parser: jwt.NewParser(opts...), keyfunc: keyfunc, clientClaim: claim}, nil
}

// parseRSAPublicKey decodes a PEM "PUBLIC KEY" (PKIX) or "RSA PUBLIC KEY"
// (PKCS#1) block and requires an RSA key of at least minRSABits.
func parseRSAPublicKey(data []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("no PEM block found")
	}
	var pub *rsa.PublicKey
	switch block.Type {
	case "PUBLIC KEY":
		k, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse PKIX public key: %w", err)
		}
		rk, ok := k.(*rsa.PublicKey)
		if !ok {
			return nil, fmt.Errorf("key is %T, want RSA", k)
		}
		pub = rk
	case "RSA PUBLIC KEY":
		k, err := x509.ParsePKCS1PublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parse PKCS#1 public key: %w", err)
		}
		pub = k
	default:
		return nil, fmt.Errorf("PEM block type %q, want PUBLIC KEY or RSA PUBLIC KEY", block.Type)
	}
	if bits := pub.N.BitLen(); bits < minRSABits {
		return nil, fmt.Errorf("RSA key is %d bits, want at least %d", bits, minRSABits)
	}
	return pub, nil
}

// Authenticate implements Authenticator.
func (a *jwtAuth) Authenticate(r *http.Request) (Identity, error) {
	vals := r.Header.Values("Authorization")
	if len(vals) == 0 || vals[0] == "" {
		return Identity{}, fmt.Errorf("%w: no Authorization header", ErrMissingCredentials)
	}
	if len(vals) > 1 {
		// Several Authorization headers are ambiguous; refuse rather than guess.
		return Identity{}, fmt.Errorf("%w: multiple Authorization headers", ErrInvalidCredentials)
	}

	// RFC 7235: the auth scheme is case-insensitive. Another scheme (Basic,
	// or a bare token) means no bearer credentials were presented.
	scheme, tok, _ := strings.Cut(vals[0], " ")
	if !strings.EqualFold(scheme, "Bearer") {
		return Identity{}, fmt.Errorf("%w: Authorization scheme is not Bearer", ErrMissingCredentials)
	}
	tok = strings.TrimSpace(tok)
	if tok == "" {
		return Identity{}, fmt.Errorf("%w: empty bearer token", ErrInvalidCredentials)
	}

	claims := jwt.MapClaims{}
	if _, err := a.parser.ParseWithClaims(tok, claims, a.keyfunc); err != nil {
		// Library errors can quote attacker-controlled header fields (e.g. the
		// alg name), so only a fixed reason string is surfaced.
		return Identity{}, fmt.Errorf("%w: %s", ErrInvalidCredentials, jwtFailureReason(err))
	}

	// A missing, empty or non-string claim must not yield an identity: it
	// would collapse many clients into one rate-limit bucket.
	id, ok := claims[a.clientClaim].(string)
	if !ok || id == "" {
		return Identity{}, fmt.Errorf("%w: client-ID claim %q missing, empty or not a string", ErrInvalidCredentials, a.clientClaim)
	}
	// The ID becomes the X-Tollgate-Client-ID upstream header: CR/LF would be
	// header injection, and any control byte would make the transport refuse
	// the request (a 502 blamed on a healthy backend). A fixed reason; the
	// claim value is attacker-chosen and never echoed.
	if !config.ValidClientID(id) {
		return Identity{}, fmt.Errorf("%w: client-ID claim %q is too long or contains control characters", ErrInvalidCredentials, a.clientClaim)
	}
	return Identity{ClientID: id, Method: "jwt"}, nil
}

// jwtFailureReason maps a jwt library error to a fixed, log-safe string.
// Order matters: claim errors are joined under ErrTokenInvalidClaims, so the
// specific causes are checked first.
func jwtFailureReason(err error) string {
	switch {
	case errors.Is(err, jwt.ErrTokenMalformed):
		return "malformed token"
	case errors.Is(err, jwt.ErrTokenUnverifiable):
		return "unverifiable token"
	case errors.Is(err, jwt.ErrTokenSignatureInvalid):
		return "invalid signature or algorithm"
	case errors.Is(err, jwt.ErrTokenExpired):
		return "token expired"
	case errors.Is(err, jwt.ErrTokenNotValidYet):
		return "token not valid yet"
	case errors.Is(err, jwt.ErrTokenInvalidIssuer):
		return "invalid issuer"
	case errors.Is(err, jwt.ErrTokenInvalidAudience):
		return "invalid audience"
	case errors.Is(err, jwt.ErrTokenRequiredClaimMissing):
		return "required claim missing"
	default:
		return "invalid token"
	}
}
