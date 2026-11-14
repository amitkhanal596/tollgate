package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// apiKeyAuth authenticates requests by the X-API-Key header. It is immutable
// after construction, so concurrent Authenticate calls need no locking.
type apiKeyAuth struct {
	// byDigest maps the raw SHA-256 digest of an accepted key to its client ID.
	byDigest map[[sha256.Size]byte]string
}

// NewAPIKey builds an API-key Authenticator from the configured key hashes.
// Every SHA256 must be 64 hex characters and every ClientID non-empty; zero
// keys is allowed and rejects every presented key as invalid.
func NewAPIKey(keys []config.APIKey) (Authenticator, error) {
	m := make(map[[sha256.Size]byte]string, len(keys))
	for i, k := range keys {
		// Errors name the entry by index and client ID only: the hash is not
		// secret, but there is no reason to echo key material into logs.
		if len(k.SHA256) != hex.EncodedLen(sha256.Size) {
			return nil, fmt.Errorf("auth: api_keys[%d] (client %q): sha256 must be %d hex characters", i, k.ClientID, hex.EncodedLen(sha256.Size))
		}
		var d [sha256.Size]byte
		if _, err := hex.Decode(d[:], []byte(k.SHA256)); err != nil {
			return nil, fmt.Errorf("auth: api_keys[%d] (client %q): sha256 is not valid hex", i, k.ClientID)
		}
		// An empty identity would merge unrelated clients into one rate-limit bucket.
		if k.ClientID == "" {
			return nil, fmt.Errorf("auth: api_keys[%d]: client_id is empty", i)
		}
		// One key mapping to two clients would make the identity ambiguous.
		if prev, dup := m[d]; dup {
			return nil, fmt.Errorf("auth: api_keys[%d] (client %q): sha256 duplicates client %q", i, k.ClientID, prev)
		}
		m[d] = k.ClientID
	}
	return &apiKeyAuth{byDigest: m}, nil
}

// Authenticate implements Authenticator.
func (a *apiKeyAuth) Authenticate(r *http.Request) (Identity, error) {
	vals := r.Header.Values(HeaderAPIKey)
	switch {
	case len(vals) == 0 || (len(vals) == 1 && vals[0] == ""):
		return Identity{}, fmt.Errorf("%w: no %s header", ErrMissingCredentials, HeaderAPIKey)
	case len(vals) > 1:
		// Several values are ambiguous (which one did a fronting proxy check?), so refuse them all.
		return Identity{}, fmt.Errorf("%w: multiple %s values", ErrInvalidCredentials, HeaderAPIKey)
	}

	// Timing safety: the map lookup compares SHA-256(presented key) against
	// stored digests, so any early-exit in the comparison reveals only how
	// many leading bytes of a hash match, which an attacker cannot steer
	// toward a stored key without a SHA-256 preimage.
	d := sha256.Sum256([]byte(vals[0]))
	id, ok := a.byDigest[d]
	if !ok {
		// The presented key is deliberately not included in the error.
		return Identity{}, fmt.Errorf("%w: unknown api key", ErrInvalidCredentials)
	}
	return Identity{ClientID: id, Method: "api_key"}, nil
}
