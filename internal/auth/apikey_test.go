package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/amitkhanal596/tollgate/internal/config"
)

func hashKey(k string) string {
	h := sha256.Sum256([]byte(k))
	return hex.EncodeToString(h[:])
}

func keyReq(values ...string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	for _, v := range values {
		r.Header.Add(HeaderAPIKey, v)
	}
	return r
}

func newTestAPIKey(t *testing.T) Authenticator {
	t.Helper()
	a, err := NewAPIKey([]config.APIKey{
		{ClientID: "alice", SHA256: hashKey("alice-secret-key-123")},
		{ClientID: "bob", SHA256: hashKey("bob-secret-key-456")},
	})
	if err != nil {
		t.Fatalf("NewAPIKey: %v", err)
	}
	return a
}

func TestAPIKeyAuthenticate(t *testing.T) {
	tests := []struct {
		name    string
		headers []string // values added to X-API-Key; nil = header absent
		wantID  string
		wantErr error
	}{
		{"valid alice", []string{"alice-secret-key-123"}, "alice", nil},
		{"valid bob", []string{"bob-secret-key-456"}, "bob", nil},
		{"missing header", nil, "", ErrMissingCredentials},
		{"empty header value", []string{""}, "", ErrMissingCredentials},
		{"unknown key", []string{"nope"}, "", ErrInvalidCredentials},
		{"prefix of valid key", []string{"alice-secret-key-12"}, "", ErrInvalidCredentials},
		{"suffix of valid key", []string{"lice-secret-key-123"}, "", ErrInvalidCredentials},
		{"upper-case variant", []string{"ALICE-SECRET-KEY-123"}, "", ErrInvalidCredentials},
		{"valid key plus trailing char", []string{"alice-secret-key-123x"}, "", ErrInvalidCredentials},
		{"the hash itself is not a key", []string{hashKey("alice-secret-key-123")}, "", ErrInvalidCredentials},
		{"multiple header values rejected", []string{"alice-secret-key-123", "bob-secret-key-456"}, "", ErrInvalidCredentials},
		{"multiple values, same key twice, rejected", []string{"alice-secret-key-123", "alice-secret-key-123"}, "", ErrInvalidCredentials},
	}
	a := newTestAPIKey(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			id, err := a.Authenticate(keyReq(tc.headers...))
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err = %v, want %v", err, tc.wantErr)
				}
				for _, h := range tc.headers {
					if h != "" && strings.Contains(err.Error(), h) {
						t.Fatalf("error %q leaks presented key", err)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if id.ClientID != tc.wantID || id.Method != "api_key" {
				t.Fatalf("identity = %+v, want ClientID %q Method api_key", id, tc.wantID)
			}
		})
	}
}

func TestAPIKeyDoesNotModifyRequest(t *testing.T) {
	a := newTestAPIKey(t)
	r := keyReq("alice-secret-key-123")
	if _, err := a.Authenticate(r); err != nil {
		t.Fatal(err)
	}
	if got := r.Header.Get(HeaderAPIKey); got != "alice-secret-key-123" {
		t.Fatalf("request header modified: %q", got)
	}
}

func TestNewAPIKeyConstruction(t *testing.T) {
	good := hashKey("k")
	tests := []struct {
		name    string
		keys    []config.APIKey
		wantErr bool
	}{
		{"valid", []config.APIKey{{ClientID: "a", SHA256: good}}, false},
		{"zero keys ok", nil, false},
		{"empty slice ok", []config.APIKey{}, false},
		{"too short", []config.APIKey{{ClientID: "a", SHA256: good[:63]}}, true},
		{"too long", []config.APIKey{{ClientID: "a", SHA256: good + "0"}}, true},
		{"non-hex", []config.APIKey{{ClientID: "a", SHA256: strings.Repeat("z", 64)}}, true},
		{"empty", []config.APIKey{{ClientID: "a", SHA256: ""}}, true},
		{"one bad among good", []config.APIKey{{ClientID: "a", SHA256: good}, {ClientID: "b", SHA256: "abc"}}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, err := NewAPIKey(tc.keys)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected constructor error")
				}
				return
			}
			if err != nil || a == nil {
				t.Fatalf("NewAPIKey: a=%v err=%v", a, err)
			}
		})
	}
}

func TestAPIKeyZeroKeysRejectsEverything(t *testing.T) {
	a, err := NewAPIKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Authenticate(keyReq("anything")); !errors.Is(err, ErrInvalidCredentials) {
		t.Fatalf("err = %v, want ErrInvalidCredentials", err)
	}
	if _, err := a.Authenticate(keyReq()); !errors.Is(err, ErrMissingCredentials) {
		t.Fatalf("err = %v, want ErrMissingCredentials", err)
	}
}

// Upper-case hex in config is not the documented form (lower-case); the
// constructor may accept or reject it, but a lower-case digest must work.
func TestAPIKeyConfigHoldsOnlyHash(t *testing.T) {
	h := hashKey("alice-secret-key-123")
	if h != strings.ToLower(h) || len(h) != 64 {
		t.Fatal("test helper must produce lower-case 64-hex")
	}
	if strings.Contains(h, "alice-secret-key-123") {
		t.Fatal("plaintext in config")
	}
}

func TestAPIKeyConcurrent(t *testing.T) {
	a := newTestAPIKey(t)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				key, want := "alice-secret-key-123", "alice"
				if i%2 == 1 {
					key, want = "bob-secret-key-456", "bob"
				}
				id, err := a.Authenticate(keyReq(key))
				if err != nil || id.ClientID != want {
					t.Errorf("goroutine %d: id=%+v err=%v", i, id, err)
					return
				}
				if _, err := a.Authenticate(keyReq("bad")); !errors.Is(err, ErrInvalidCredentials) {
					t.Errorf("bad key err = %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}
