package controlplane

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// ---- C1 / C3: store-level conformance additions -----------------------------
//
// These extend the conformance suite without editing it: the same functions run
// against MemStore and (when TOLLGATE_TEST_DATABASE_URL is set) PostgresStore.

func fixRound4StoreConformance(t *testing.T, newStore func(t *testing.T) Store) {
	t.Helper()
	cases := []struct {
		name string
		fn   func(t *testing.T, ns func(*testing.T) Store)
	}{
		{"TotalSizeCap", scTotalSizeCap},
		{"TotalSizeCapBoundary", scTotalSizeBoundary},
		{"RollbackIsAlsoCapped", scRollbackCapNotNeeded},
		{"NULCharacterRejected", scNUL},
	}
	for _, c := range cases {
		run(t, c.name, func(t *testing.T) { c.fn(t, newStore) })
	}
}

func TestFixRound4_StoreConformance_MemStore(t *testing.T) {
	defer failOnPanic(t)
	fixRound4StoreConformance(t, func(t *testing.T) Store { return NewMemStore() })
}

func TestFixRound4_StoreConformance_PostgresStore(t *testing.T) {
	defer failOnPanic(t)
	fixRound4StoreConformance(t, postgresStoreFactory(t))
}

// Long host labels keep the number of backends (and so validation time) low.
var (
	longLabelA = strings.Repeat("a", 63)
	longLabelB = strings.Repeat("b", 63)
	longLabelC = strings.Repeat("c", 63)
)

// backendsOfSize returns distinct backend URLs whose JSON array encoding is
// about target bytes. tag keeps the hosts of different upstreams distinct.
func backendsOfSize(tag string, target int) []string {
	var out []string
	size := 2 // brackets
	for i := 0; size < target; i++ {
		u := fmt.Sprintf("http://h%s-%d.%s.%s.%s.example.com:8080", tag, i, longLabelA, longLabelB, longLabelC)
		out = append(out, u)
		size += len(u) + 3 // quotes and comma
	}
	return out
}

// mutBigUpstream adds an upstream named name whose JSON is about target bytes.
func mutBigUpstream(name string, target int) func(*config.Config) error {
	return func(c *config.Config) error {
		c.Upstreams = append(c.Upstreams, config.Upstream{Name: name, Backends: backendsOfSize(name, target)})
		return nil
	}
}

const bigUpstreamBytes = 900_000

// scTotalSizeCap writes ~900 KB upstreams until the document crosses
// MaxConfigBytes: that write must fail with ErrInvalid, store nothing and
// consume no version.
func scTotalSizeCap(t *testing.T, ns func(*testing.T) Store) {
	s := ns(t)
	crossing := MaxConfigBytes/bigUpstreamBytes + 1 // 5 for a 4 MiB cap
	for i := 1; i <= crossing; i++ {
		_, err := s.Update(testCtx(t), UpdateRequest{}, mutBigUpstream(fmt.Sprintf("big%d", i), bigUpstreamBytes))
		if i < crossing {
			if err != nil {
				t.Fatalf("write %d (document well under the cap): %v", i, err)
			}
			continue
		}
		if !errors.Is(err, config.ErrInvalid) {
			t.Fatalf("write %d crossing the %d byte cap: err = %v, want errors.Is(config.ErrInvalid)", i, MaxConfigBytes, err)
		}
	}
	if h := mustHead(t, s); h.Version != int64(crossing-1) || len(h.Upstreams) != crossing-1 {
		t.Fatalf("head after rejected write: version %d with %d upstreams, want %d / %d", h.Version, len(h.Upstreams), crossing-1, crossing-1)
	}
	if l, _ := s.List(testCtx(t), 50, 0); len(l) != crossing-1 {
		t.Fatalf("%d versions stored, want %d", len(l), crossing-1)
	}
	// Removing weight makes room again, and the rejected write consumed no version.
	c, err := s.Update(testCtx(t), UpdateRequest{}, func(c *config.Config) error {
		c.Upstreams = c.Upstreams[:1]
		return nil
	})
	if err != nil || c.Version != int64(crossing) {
		t.Fatalf("shrinking write = (%v, %v), want version %d", c, err, crossing)
	}
}

// docSize is the size the stores measure: Version stamped, Normalized, JSON.
func docSize(t *testing.T, c *config.Config, version int64) int {
	t.Helper()
	cp := *c
	cp.Version = version
	cp.Normalize()
	b, err := json.Marshal(&cp)
	if err != nil {
		t.Fatal(err)
	}
	return len(b)
}

// exactSizeMut builds a mutation whose first-version document marshals to
// exactly want bytes, by sizing the host of one padding backend.
func exactSizeMut(t *testing.T, want int) func(*config.Config) error {
	t.Helper()
	build := func(pad int) *config.Config {
		return &config.Config{Upstreams: []config.Upstream{
			{Name: "fill", Backends: backendsOfSize("fill", want-6000)},
			{Name: "pad", Backends: []string{"http://p" + strings.Repeat("a", pad) + ".example.com:80"}},
		}}
	}
	base := docSize(t, build(1), 1)
	pad := 1 + want - base
	if pad < 1 {
		t.Fatalf("fixture too large: base %d, want %d", base, want)
	}
	cfg := build(pad)
	if got := docSize(t, cfg, 1); got != want {
		t.Fatalf("fixture size %d, want exactly %d", got, want)
	}
	return func(c *config.Config) error {
		c.Upstreams = append(c.Upstreams, cfg.Upstreams...)
		return nil
	}
}

// scTotalSizeBoundary pins "more than MaxConfigBytes": exactly the cap is
// accepted, one byte more is rejected.
func scTotalSizeBoundary(t *testing.T, ns func(*testing.T) Store) {
	t.Run("exactly at the cap", func(t *testing.T) {
		s := ns(t)
		if _, err := s.Update(testCtx(t), UpdateRequest{}, exactSizeMut(t, MaxConfigBytes)); err != nil {
			t.Fatalf("document of exactly MaxConfigBytes rejected: %v", err)
		}
	})
	t.Run("one byte over", func(t *testing.T) {
		s := ns(t)
		_, err := s.Update(testCtx(t), UpdateRequest{}, exactSizeMut(t, MaxConfigBytes+1))
		if !errors.Is(err, config.ErrInvalid) {
			t.Fatalf("document of MaxConfigBytes+1 bytes: err = %v, want errors.Is(config.ErrInvalid)", err)
		}
		if h := mustHead(t, s); h.Version != 0 {
			t.Fatalf("head version %d after rejected write, want 0", h.Version)
		}
	})
}

// scRollbackCapNotNeeded: a rollback copies an already-stored document, so a
// rollback of a stored (hence under-cap) version must keep working at the cap.
func scRollbackCapNotNeeded(t *testing.T, ns func(*testing.T) Store) {
	s := ns(t)
	mustUpdate(t, s, UpdateRequest{}, exactSizeMut(t, MaxConfigBytes))
	mustUpdate(t, s, UpdateRequest{}, func(c *config.Config) error { c.Upstreams = c.Upstreams[:1]; return nil })
	if _, err := s.Rollback(testCtx(t), 1, UpdateRequest{}); err != nil {
		t.Fatalf("rollback to a stored version exactly at the cap: %v", err)
	}
}

// nulMutations put a NUL into a string that passes every other validation rule
// today. Postgres cannot store it (JSONB and TEXT reject U+0000).
var nulMutations = []struct {
	name string
	mut  func(*config.Config) error
}{
	{"consul service", func(c *config.Config) error {
		c.Upstreams = append(c.Upstreams, config.Upstream{Name: "c", Consul: &config.ConsulService{Service: "svc\x00x"}})
		return nil
	}},
	{"consul tag", func(c *config.Config) error {
		c.Upstreams = append(c.Upstreams, config.Upstream{Name: "c", Consul: &config.ConsulService{Service: "svc", Tag: "a\x00"}})
		return nil
	}},
	{"jwt issuer", func(c *config.Config) error {
		c.Auth.JWT = &config.JWTSettings{Algorithm: config.JWTHS256, HMACSecretEnv: "S", Issuer: "iss\x00"}
		return nil
	}},
	{"jwt hmac_secret_env", func(c *config.Config) error {
		c.Auth.JWT = &config.JWTSettings{Algorithm: config.JWTHS256, HMACSecretEnv: "S\x00"}
		return nil
	}},
}

func scNUL(t *testing.T, ns func(*testing.T) Store) {
	for _, tc := range nulMutations {
		run(t, tc.name, func(t *testing.T) {
			s := ns(t)
			mustUpdate(t, s, UpdateRequest{}, mutSeed)
			_, err := s.Update(testCtx(t), UpdateRequest{}, tc.mut)
			if !errors.Is(err, config.ErrInvalid) {
				t.Fatalf("err = %v, want errors.Is(config.ErrInvalid)", err)
			}
			if h := mustHead(t, s); h.Version != 1 {
				t.Fatalf("head version %d after rejected write, want 1", h.Version)
			}
			// The store is still usable and consumed no version (a failed
			// Postgres statement must not leave a poisoned connection or lock).
			if c := mustUpdate(t, s, UpdateRequest{}, mutAddUpstream("fine")); c.Version != 2 {
				t.Fatalf("next version = %d, want 2", c.Version)
			}
		})
	}
}
