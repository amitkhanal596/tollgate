package auth

// Supplementary tests added by the Phase 3 builder for paths the
// test-writer's suite does not reach (construction edge cases, defaults,
// PEM variants, header ambiguity, log-safe error reasons).

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"

	"github.com/amitkhanal596/tollgate/internal/config"
)

func TestNewAPIKeyExtraConstruction(t *testing.T) {
	h := hashKey("k")
	tests := []struct {
		name    string
		keys    []config.APIKey
		wantErr bool
	}{
		{"empty client id", []config.APIKey{{ClientID: "", SHA256: h}}, true},
		{"duplicate hash", []config.APIKey{{ClientID: "a", SHA256: h}, {ClientID: "b", SHA256: h}}, true},
		{"duplicate hash differing only in case", []config.APIKey{{ClientID: "a", SHA256: h}, {ClientID: "b", SHA256: strings.ToUpper(h)}}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, err := NewAPIKey(tc.keys)
			if (err != nil) != tc.wantErr {
				t.Fatalf("a=%v err=%v, wantErr %v", a, err, tc.wantErr)
			}
		})
	}
}

func TestAPIKeyUpperCaseHashAccepted(t *testing.T) {
	a, err := NewAPIKey([]config.APIKey{{ClientID: "a", SHA256: strings.ToUpper(hashKey("secret-key"))}})
	if err != nil {
		t.Fatal(err)
	}
	if id, err := a.Authenticate(keyReq("secret-key")); err != nil || id.ClientID != "a" {
		t.Fatalf("id=%+v err=%v", id, err)
	}
}

func TestNewJWTExtraConstruction(t *testing.T) {
	key := genRSA(t, 2048)
	pkcs1 := pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: x509.MarshalPKCS1PublicKey(&key.PublicKey)})
	pkcs1Small := pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: x509.MarshalPKCS1PublicKey(&genRSA(t, 1024).PublicKey)})
	tests := []struct {
		name     string
		settings config.JWTSettings
		file     []byte
		wantErr  bool
	}{
		{"hs256 without env name", config.JWTSettings{Algorithm: config.JWTHS256}, nil, true},
		{"rs256 without file", config.JWTSettings{Algorithm: config.JWTRS256}, nil, true},
		{"rs256 PKCS#1 ok", rsSettings(), pkcs1, false},
		{"rs256 PKCS#1 1024-bit", rsSettings(), pkcs1Small, true},
		{"rs256 PKCS#1 garbage", rsSettings(), pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: []byte("x")}), true},
		{"rs256 PKIX garbage", rsSettings(), pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: []byte("x")}), true},
		{"rs256 private key block", rsSettings(), pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, err := NewJWT(tc.settings, envOf(nil), fileOf(pubKeyPath, tc.file), fixedNow)
			if (err != nil) != tc.wantErr || (err != nil && a != nil) {
				t.Fatalf("a=%v err=%v, wantErr %v", a, err, tc.wantErr)
			}
		})
	}
}

func TestNewJWTPKCS1Verifies(t *testing.T) {
	key := genRSA(t, 2048)
	pkcs1 := pem.EncodeToMemory(&pem.Block{Type: "RSA PUBLIC KEY", Bytes: x509.MarshalPKCS1PublicKey(&key.PublicKey)})
	a, err := NewJWT(rsSettings(), nil, fileOf(pubKeyPath, pkcs1), fixedNow)
	if err != nil {
		t.Fatal(err)
	}
	if id, err := a.Authenticate(bearer("Bearer " + sign(t, jwt.SigningMethodRS256, key, claims(nil)))); err != nil || id.ClientID != "client-1" {
		t.Fatalf("id=%+v err=%v", id, err)
	}
}

// Nil getenv/readFile/now fall back to the os/time defaults.
func TestNewJWTNilDefaults(t *testing.T) {
	t.Setenv(testSecretEnv, testSecret)
	a, err := NewJWT(hsSettings(), nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	// The real clock is used, so the fixed-time test claims (2026-01-02) would
	// be expired; a token with no exp proves the parser ran with real checks.
	tok := sign(t, jwt.SigningMethodHS256, []byte(testSecret), claims(map[string]any{"exp": nil}))
	if _, err := a.Authenticate(bearer("Bearer " + tok)); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("err = %v, want ErrInvalidCredentials", err)
	}
	if _, err := NewJWT(rsSettings(), nil, nil, nil); err == nil {
		t.Fatal("os.ReadFile of a nonexistent path must fail")
	}
}

func TestJWTMultipleAuthorizationHeaders(t *testing.T) {
	a := mustJWT(t, hsSettings())
	good := sign(t, jwt.SigningMethodHS256, []byte(testSecret), claims(nil))
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header["Authorization"] = []string{"Bearer " + good, "Bearer " + good}
	if _, err := a.Authenticate(r); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("err = %v, want ErrInvalidCredentials", err)
	}
	r.Header["Authorization"] = []string{"Bearer"}
	if _, err := a.Authenticate(r); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("bare scheme: err = %v, want ErrInvalidCredentials", err)
	}
}

// The library's own error quotes the token's alg header ("signing method
// HS384 is invalid"); only a fixed reason may reach the returned error.
func TestJWTErrorDoesNotEchoHeaderFields(t *testing.T) {
	a := mustJWT(t, hsSettings())
	tok := sign(t, jwt.SigningMethodHS384, []byte(testSecret), claims(nil))
	_, err := a.Authenticate(bearer("Bearer " + tok))
	if !errors.Is(err, ErrInvalidCredentials) || strings.Contains(err.Error(), "HS384") {
		t.Fatalf("err = %v", err)
	}
}

func TestJWTFailureReason(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{jwt.ErrTokenMalformed, "malformed token"},
		{jwt.ErrTokenUnverifiable, "unverifiable token"},
		{jwt.ErrTokenSignatureInvalid, "invalid signature or algorithm"},
		{fmt.Errorf("x: %w", jwt.ErrTokenExpired), "token expired"},
		{jwt.ErrTokenNotValidYet, "token not valid yet"},
		{jwt.ErrTokenInvalidIssuer, "invalid issuer"},
		{jwt.ErrTokenInvalidAudience, "invalid audience"},
		{jwt.ErrTokenRequiredClaimMissing, "required claim missing"},
		{errors.New("other"), "invalid token"},
	}
	for _, tc := range tests {
		if got := jwtFailureReason(tc.err); got != tc.want {
			t.Errorf("jwtFailureReason(%v) = %q, want %q", tc.err, got, tc.want)
		}
	}
}

func TestClientIPKeyExtra(t *testing.T) {
	tests := []struct{ in, want string }{
		{"203.0.113.7", "203.0.113.7"},
		{"2001:db8:1:2::9", "2001:db8:1:2::/64"},
		{"[fe80::1%en0]:80", "fe80::/64"},
		{"::ffff:198.51.100.1", "198.51.100.1"},
	}
	for _, tc := range tests {
		if got := ClientIPKey(tc.in); got != tc.want {
			t.Errorf("ClientIPKey(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
