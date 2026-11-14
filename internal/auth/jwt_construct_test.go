package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"strings"
	"testing"

	"github.com/amitkhanal596/tollgate/internal/config"
)

func TestNewJWTFailsClosed(t *testing.T) {
	rsa2048 := pubPEM(t, &genRSA(t, 2048).PublicKey)
	rsa1024 := pubPEM(t, &genRSA(t, 1024).PublicKey)
	ec, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	ecDER, _ := x509.MarshalPKIXPublicKey(&ec.PublicKey)
	ecPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: ecDER})

	shortSecret := "short-secret-31-bytes-xxxxxxxxx"[:31]
	tests := []struct {
		name     string
		settings config.JWTSettings
		env      map[string]string
		file     func(string) ([]byte, error)
		wantErr  bool
	}{
		{"hs256 ok", hsSettings(), map[string]string{testSecretEnv: testSecret}, nil, false},
		{"hs256 exactly 32 bytes ok", hsSettings(), map[string]string{testSecretEnv: strings.Repeat("a", 32)}, nil, false},
		{"hs256 env unset", hsSettings(), map[string]string{}, nil, true},
		{"hs256 env empty", hsSettings(), map[string]string{testSecretEnv: ""}, nil, true},
		{"hs256 secret 31 bytes", hsSettings(), map[string]string{testSecretEnv: shortSecret}, nil, true},
		{"rs256 ok", rsSettings(), nil, fileOf(pubKeyPath, rsa2048), false},
		{"rs256 file missing", rsSettings(), nil, func(string) ([]byte, error) { return nil, errors.New("no such file") }, true},
		{"rs256 empty file", rsSettings(), nil, fileOf(pubKeyPath, nil), true},
		{"rs256 not PEM", rsSettings(), nil, fileOf(pubKeyPath, []byte("hello")), true},
		{"rs256 EC key", rsSettings(), nil, fileOf(pubKeyPath, ecPEM), true},
		{"rs256 1024-bit RSA", rsSettings(), nil, fileOf(pubKeyPath, rsa1024), true},
		{"unknown algorithm", config.JWTSettings{Algorithm: "ES256", HMACSecretEnv: testSecretEnv}, map[string]string{testSecretEnv: testSecret}, nil, true},
		{"empty algorithm", config.JWTSettings{HMACSecretEnv: testSecretEnv}, map[string]string{testSecretEnv: testSecret}, nil, true},
		{"none algorithm", config.JWTSettings{Algorithm: "none"}, nil, nil, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, err := NewJWT(tc.settings, envOf(tc.env), tc.file, fixedNow)
			if !tc.wantErr {
				if err != nil || a == nil {
					t.Fatalf("a=%v err=%v", a, err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error (must fail closed)")
			}
			if a != nil {
				t.Fatal("authenticator must be nil on error")
			}
			for _, secret := range tc.env {
				if secret != "" && strings.Contains(err.Error(), secret) {
					t.Fatalf("error %q leaks secret", err)
				}
			}
		})
	}
}
