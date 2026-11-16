package config

import (
	"errors"
	"strings"
	"testing"
)

// G1 (known issue K2): the JWT key material must match the algorithm. HS256
// needs hmac_secret_env and must not carry public_key_file; RS256 is the
// reverse. The rules apply whenever auth.jwt is present.
func TestValidateFixRound4_JWTKeyMaterial(t *testing.T) {
	tests := []struct {
		name      string
		jwt       *JWTSettings
		wantInMsg string // "" means valid
	}{
		{"HS256 with secret env", &JWTSettings{Algorithm: JWTHS256, HMACSecretEnv: "S"}, ""},
		{"RS256 with public key file", &JWTSettings{Algorithm: JWTRS256, PublicKeyFile: "/k.pem"}, ""},
		{"HS256 without secret env", &JWTSettings{Algorithm: JWTHS256}, "auth.jwt.hmac_secret_env"},
		{"HS256 with only a key file", &JWTSettings{Algorithm: JWTHS256, PublicKeyFile: "/k.pem"}, "auth.jwt.hmac_secret_env"},
		{"HS256 with both", &JWTSettings{Algorithm: JWTHS256, HMACSecretEnv: "S", PublicKeyFile: "/k.pem"}, "auth.jwt.public_key_file"},
		{"RS256 without key file", &JWTSettings{Algorithm: JWTRS256}, "auth.jwt.public_key_file"},
		{"RS256 with only a secret env", &JWTSettings{Algorithm: JWTRS256, HMACSecretEnv: "S"}, "auth.jwt.public_key_file"},
		{"RS256 with both", &JWTSettings{Algorithm: JWTRS256, HMACSecretEnv: "S", PublicKeyFile: "/k.pem"}, "auth.jwt.hmac_secret_env"},
	}
	for _, tc := range tests {
		for _, routeUsesJWT := range []bool{true, false} {
			name := tc.name + "/route uses jwt"
			if !routeUsesJWT {
				name = tc.name + "/no route uses jwt"
			}
			t.Run(name, func(t *testing.T) {
				c := validConfig()
				c.Auth.JWT = tc.jwt
				if !routeUsesJWT {
					for i := range c.Routes {
						if c.Routes[i].Auth == AuthJWT {
							c.Routes[i].Auth = AuthNone
						}
					}
				}
				err := c.Validate()
				if tc.wantInMsg == "" {
					if err != nil {
						t.Fatalf("want valid, got %v", err)
					}
					return
				}
				if err == nil {
					t.Fatalf("want error mentioning %q, got nil", tc.wantInMsg)
				}
				if !errors.Is(err, ErrInvalid) {
					t.Errorf("error does not wrap ErrInvalid: %v", err)
				}
				if !strings.Contains(err.Error(), tc.wantInMsg) {
					t.Errorf("error %q does not name %q", err, tc.wantInMsg)
				}
			})
		}
	}
}
