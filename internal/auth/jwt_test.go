package auth

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/amitkhanal596/tollgate/internal/config"
)

const (
	testSecretEnv = "TEST_JWT_SECRET"
	testSecret    = "0123456789abcdef0123456789abcdef-secret" // >= 32 bytes
)

var testNow = time.Date(2026, 1, 2, 12, 0, 0, 0, time.UTC)

func fixedNow() time.Time { return testNow }

func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func hsEnv() func(string) string { return envOf(map[string]string{testSecretEnv: testSecret}) }

func hsSettings() config.JWTSettings {
	return config.JWTSettings{Algorithm: config.JWTHS256, HMACSecretEnv: testSecretEnv}
}

func mustJWT(t *testing.T, s config.JWTSettings) Authenticator {
	t.Helper()
	a, err := NewJWT(s, hsEnv(), nil, fixedNow)
	if err != nil {
		t.Fatalf("NewJWT: %v", err)
	}
	return a
}

func claims(extra map[string]any) jwt.MapClaims {
	c := jwt.MapClaims{
		"sub": "client-1",
		"exp": testNow.Add(time.Hour).Unix(),
	}
	for k, v := range extra {
		if v == nil {
			delete(c, k)
		} else {
			c[k] = v
		}
	}
	return c
}

func sign(t *testing.T, m jwt.SigningMethod, key any, c jwt.MapClaims) string {
	t.Helper()
	s, err := jwt.NewWithClaims(m, c).SignedString(key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return s
}

func bearer(h string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	if h != "" {
		r.Header.Set("Authorization", h)
	}
	return r
}

func TestJWTHS256(t *testing.T) {
	ts := func(d time.Duration) int64 { return testNow.Add(d).Unix() }
	full := config.JWTSettings{
		Algorithm: config.JWTHS256, HMACSecretEnv: testSecretEnv,
		Issuer: "https://issuer.example", Audience: "tollgate",
	}
	custom := hsSettings()
	custom.ClientIDClaim = "client"

	tests := []struct {
		name     string
		settings config.JWTSettings
		claims   jwt.MapClaims
		key      string // HMAC key; default testSecret
		wantID   string
		wantErr  error
	}{
		{"valid default claim", hsSettings(), claims(nil), "", "client-1", nil},
		{"valid custom claim", custom, claims(map[string]any{"client": "svc-9"}), "", "svc-9", nil},
		{"custom claim configured but only sub present", custom, claims(nil), "", "", ErrInvalidCredentials},
		{"issuer and audience match", full, claims(map[string]any{"iss": "https://issuer.example", "aud": "tollgate"}), "", "client-1", nil},
		{"audience in list", full, claims(map[string]any{"iss": "https://issuer.example", "aud": []string{"other", "tollgate"}}), "", "client-1", nil},
		{"wrong issuer", full, claims(map[string]any{"iss": "evil", "aud": "tollgate"}), "", "", ErrInvalidCredentials},
		{"missing issuer", full, claims(map[string]any{"aud": "tollgate"}), "", "", ErrInvalidCredentials},
		{"wrong audience", full, claims(map[string]any{"iss": "https://issuer.example", "aud": "other"}), "", "", ErrInvalidCredentials},
		{"missing audience", full, claims(map[string]any{"iss": "https://issuer.example"}), "", "", ErrInvalidCredentials},
		{"issuer ignored when not configured", hsSettings(), claims(map[string]any{"iss": "anything"}), "", "client-1", nil},
		{"expired well past leeway", hsSettings(), claims(map[string]any{"exp": ts(-60 * time.Second)}), "", "", ErrInvalidCredentials},
		{"expired 10s ago within 30s leeway", hsSettings(), claims(map[string]any{"exp": ts(-10 * time.Second)}), "", "client-1", nil},
		{"expired 5m ago", hsSettings(), claims(map[string]any{"exp": ts(-5 * time.Minute)}), "", "", ErrInvalidCredentials},
		{"nbf 5m in future", hsSettings(), claims(map[string]any{"nbf": ts(5 * time.Minute)}), "", "", ErrInvalidCredentials},
		{"nbf 10s in future within leeway", hsSettings(), claims(map[string]any{"nbf": ts(10 * time.Second)}), "", "client-1", nil},
		{"nbf in past", hsSettings(), claims(map[string]any{"nbf": ts(-time.Minute)}), "", "client-1", nil},
		{"no exp", hsSettings(), claims(map[string]any{"exp": nil}), "", "", ErrInvalidCredentials},
		{"wrong signature", hsSettings(), claims(nil), "another-secret-another-secret-12345", "", ErrInvalidCredentials},
		{"missing client claim", hsSettings(), claims(map[string]any{"sub": nil}), "", "", ErrInvalidCredentials},
		{"empty client claim", hsSettings(), claims(map[string]any{"sub": ""}), "", "", ErrInvalidCredentials},
		{"numeric client claim", hsSettings(), claims(map[string]any{"sub": 12345}), "", "", ErrInvalidCredentials},
		{"object client claim", hsSettings(), claims(map[string]any{"sub": map[string]any{"a": "b"}}), "", "", ErrInvalidCredentials},
		{"array client claim", hsSettings(), claims(map[string]any{"sub": []string{"a"}}), "", "", ErrInvalidCredentials},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := mustJWT(t, tc.settings)
			key := tc.key
			if key == "" {
				key = testSecret
			}
			tok := sign(t, jwt.SigningMethodHS256, []byte(key), tc.claims)
			id, err := a.Authenticate(bearer("Bearer " + tok))
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				if strings.Contains(err.Error(), tok) {
					t.Fatal("error leaks token")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if id.ClientID != tc.wantID || id.Method != "jwt" {
				t.Fatalf("identity = %+v, want %q/jwt", id, tc.wantID)
			}
		})
	}
}

func TestJWTBearerParsing(t *testing.T) {
	a := mustJWT(t, hsSettings())
	good := sign(t, jwt.SigningMethodHS256, []byte(testSecret), claims(nil))
	tests := []struct {
		name    string
		header  string
		wantErr error
	}{
		{"valid", "Bearer " + good, nil},
		{"lower-case scheme", "bearer " + good, nil},
		{"upper-case scheme", "BEARER " + good, nil},
		{"missing header", "", ErrMissingCredentials},
		{"basic scheme is missing", "Basic dXNlcjpwYXNz", ErrMissingCredentials},
		{"token without scheme is missing", good, ErrMissingCredentials},
		{"empty token", "Bearer ", ErrInvalidCredentials},
		{"garbage token", "Bearer not-a-jwt", ErrInvalidCredentials},
		{"truncated token", "Bearer " + good[:len(good)-10], ErrInvalidCredentials},
		{"two dots only", "Bearer ..", ErrInvalidCredentials},
		{"header and payload only", "Bearer " + strings.Join(strings.Split(good, ".")[:2], "."), ErrInvalidCredentials},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// httptest request trims nothing; "Bearer " keeps its trailing space.
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			if tc.header != "" {
				r.Header["Authorization"] = []string{tc.header}
			}
			id, err := a.Authenticate(r)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil || id.ClientID != "client-1" {
				t.Fatalf("id=%+v err=%v", id, err)
			}
		})
	}
}

func TestJWTConcurrent(t *testing.T) {
	a := mustJWT(t, hsSettings())
	tok := sign(t, jwt.SigningMethodHS256, []byte(testSecret), claims(nil))
	done := make(chan struct{})
	for i := 0; i < 32; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 50; j++ {
				if id, err := a.Authenticate(bearer("Bearer " + tok)); err != nil || id.ClientID != "client-1" {
					t.Errorf("id=%+v err=%v", id, err)
					return
				}
			}
		}()
	}
	for i := 0; i < 32; i++ {
		<-done
	}
}
