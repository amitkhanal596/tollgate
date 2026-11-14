package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"testing"

	"github.com/golang-jwt/jwt/v5"

	"github.com/amitkhanal596/tollgate/internal/config"
)

func genRSA(t *testing.T, bits int) *rsa.PrivateKey {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func pubPEM(t *testing.T, k *rsa.PublicKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(k)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}

const pubKeyPath = "/keys/pub.pem"

func rsSettings() config.JWTSettings {
	return config.JWTSettings{Algorithm: config.JWTRS256, PublicKeyFile: pubKeyPath}
}

func fileOf(path string, data []byte) func(string) ([]byte, error) {
	return func(p string) ([]byte, error) {
		if p != path {
			return nil, errors.New("file not found: " + p)
		}
		return data, nil
	}
}

func TestJWTAlgorithmConfusion(t *testing.T) {
	rsaKey := genRSA(t, 2048)
	pub := pubPEM(t, &rsaKey.PublicKey)

	hsAuth := mustJWT(t, hsSettings())
	rsAuth, err := NewJWT(rsSettings(), nil, fileOf(pubKeyPath, pub), fixedNow)
	if err != nil {
		t.Fatalf("NewJWT rs256: %v", err)
	}

	noneTok := func() string {
		return sign(t, jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, claims(nil))
	}
	tests := []struct {
		name string
		a    Authenticator
		tok  string
	}{
		{"alg none vs HS256", hsAuth, noneTok()},
		{"alg none vs RS256", rsAuth, noneTok()},
		{"HS256 signed with RSA public PEM as secret vs RS256", rsAuth, sign(t, jwt.SigningMethodHS256, pub, claims(nil))},
		{"RS256 token vs HS256 authenticator", hsAuth, sign(t, jwt.SigningMethodRS256, rsaKey, claims(nil))},
		{"HS384 vs HS256 authenticator", hsAuth, sign(t, jwt.SigningMethodHS384, []byte(testSecret), claims(nil))},
		{"HS512 vs HS256 authenticator", hsAuth, sign(t, jwt.SigningMethodHS512, []byte(testSecret), claims(nil))},
		{"RS384 vs RS256 authenticator", rsAuth, sign(t, jwt.SigningMethodRS384, rsaKey, claims(nil))},
		{"PS256 vs RS256 authenticator", rsAuth, sign(t, jwt.SigningMethodPS256, rsaKey, claims(nil))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.a.Authenticate(bearer("Bearer " + tc.tok)); !errors.Is(err, ErrInvalidCredentials) {
				t.Fatalf("err = %v, want ErrInvalidCredentials", err)
			}
		})
	}
}
