package auth

import (
	"errors"
	"strings"
	"testing"

	"github.com/golang-jwt/jwt/v5"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// D10: a client ID travels into rate-limit keys, logs and the
// X-Tollgate-Client-ID upstream header, so it must be a sane string:
// no control bytes (< 0x20, 0x7f) and at most 256 bytes.

func TestJWTClientIDClaimShape(t *testing.T) {
	tests := []struct {
		name    string
		sub     string
		wantErr bool
	}{
		{"email-like printable ASCII", "user@example.com", false},
		{"utf-8 name", "müller", false},
		{"exactly 256 bytes", strings.Repeat("a", 256), false},
		{"257 bytes", strings.Repeat("a", 257), true},
		{"4096 bytes", strings.Repeat("a", 4096), true},
		{"CRLF header injection", "alice\r\nX-Admin: 1", true},
		{"NUL", "a\x00b", true},
		{"tab", "a\tb", true},
		{"newline", "a\nb", true},
		{"DEL", "a\x7fb", true},
		{"ESC", "a\x1bb", true},
		{"leading control byte", "\x01alice", true},
		{"trailing control byte", "alice\x1f", true},
	}
	a := mustJWT(t, hsSettings())
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tok := sign(t, jwt.SigningMethodHS256, []byte(testSecret), claims(map[string]any{"sub": tc.sub}))
			id, err := a.Authenticate(bearer("Bearer " + tok))
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidCredentials) {
					t.Fatalf("err = %v (id %q), want ErrInvalidCredentials", err, id.ClientID)
				}
				if id.ClientID != "" {
					t.Errorf("rejected token still produced identity %q", id.ClientID)
				}
				return
			}
			if err != nil || id.ClientID != tc.sub {
				t.Fatalf("got (%q, %v), want (%q, nil)", id.ClientID, err, tc.sub)
			}
		})
	}
}

func TestNewAPIKeyRejectsBadClientID(t *testing.T) {
	tests := []struct {
		name     string
		clientID string
		wantErr  bool
	}{
		{"plain", "alice", false},
		{"utf-8", "müller", false},
		{"exactly 256 bytes", strings.Repeat("a", 256), false},
		{"257 bytes", strings.Repeat("a", 257), true},
		{"CRLF", "alice\r\nX-Admin: 1", true},
		{"NUL", "a\x00b", true},
		{"tab", "a\tb", true},
		{"DEL", "a\x7fb", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewAPIKey([]config.APIKey{{ClientID: tc.clientID, SHA256: hashKey("k")}})
			if (err != nil) != tc.wantErr {
				t.Fatalf("NewAPIKey err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
