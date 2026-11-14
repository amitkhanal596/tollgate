package auth

import (
	"errors"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestJWTRS256(t *testing.T) {
	key := genRSA(t, 2048)
	other := genRSA(t, 2048)
	a, err := NewJWT(rsSettings(), nil, fileOf(pubKeyPath, pubPEM(t, &key.PublicKey)), fixedNow)
	if err != nil {
		t.Fatalf("NewJWT: %v", err)
	}
	tests := []struct {
		name    string
		tok     string
		wantID  string
		wantErr error
	}{
		{"valid", sign(t, jwt.SigningMethodRS256, key, claims(nil)), "client-1", nil},
		{"different key", sign(t, jwt.SigningMethodRS256, other, claims(nil)), "", ErrInvalidCredentials},
		{"expired", sign(t, jwt.SigningMethodRS256, key, claims(map[string]any{"exp": testNow.Add(-time.Hour).Unix()})), "", ErrInvalidCredentials},
		{"no exp", sign(t, jwt.SigningMethodRS256, key, claims(map[string]any{"exp": nil})), "", ErrInvalidCredentials},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id, err := a.Authenticate(bearer("Bearer " + tc.tok))
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				return
			}
			if err != nil || id.ClientID != tc.wantID || id.Method != "jwt" {
				t.Fatalf("id=%+v err=%v", id, err)
			}
		})
	}
}
