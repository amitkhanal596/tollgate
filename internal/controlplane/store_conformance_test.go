package controlplane

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// storeConformance is the suite every Store implementation must pass
// (store.go doc). newStore returns a fresh, empty, isolated store.
func storeConformance(t *testing.T, newStore func(t *testing.T) Store) {
	t.Helper()
	cases := []struct {
		name string
		fn   func(t *testing.T, ns func(*testing.T) Store)
	}{
		{"EmptyStore", scEmpty},
		{"GaplessMonotonicVersions", scGapless},
		{"StoredConfigVersionField", scVersionField},
		{"UpdateDeepCopies", scDeepCopy},
		{"MutateErrorStoresNothing", scMutateError},
		{"MutateCalledOnceAndVersionOverwritten", scMutateOnce},
		{"ValidationRejected", scValidation},
		{"InvalidFirstUpdateConsumesNoVersion", scInvalidFirst},
		{"NormalizeApplied", scNormalize},
		{"IfMatch", scIfMatch},
		{"IfMatchZeroOnlyOnEmpty", scIfMatchZero},
		{"Get", scGet},
		{"ListPagingAndMetadata", scList},
		{"ListLimitClamping", scListClamp},
		{"Rollback", scRollback},
		{"RollbackTargets", scRollbackTargets},
		{"RollbackIfMatch", scRollbackIfMatch},
		{"CanceledContextWritesNothing", scCanceled},
		{"Ping", scPing},
		{"ConcurrentUpdatesNoLostUpdate", scConcurrentUpdates},
		{"ConcurrentSameIfMatchOneWinner", scConcurrentIfMatch},
		{"ConcurrentMixedWritersGapless", scMixedWriters},
	}
	for _, c := range cases {
		run(t, c.name, func(t *testing.T) { c.fn(t, newStore) })
	}
}

func TestStoreConformance_MemStore(t *testing.T) {
	defer failOnPanic(t)
	storeConformance(t, func(t *testing.T) Store { return NewMemStore() })
}

// TestStoreConformance_PostgresStore runs the same suite against Postgres. It
// is skipped unless TOLLGATE_TEST_DATABASE_URL is set (and the binary was
// built with -tags postgres); see store_postgres_*_test.go.
func TestStoreConformance_PostgresStore(t *testing.T) {
	defer failOnPanic(t)
	storeConformance(t, postgresStoreFactory(t))
}

// ---- helpers ---------------------------------------------------------------

func mustUpdate(t *testing.T, s Store, req UpdateRequest, m func(*config.Config) error) *config.Config {
	t.Helper()
	c, err := s.Update(testCtx(t), req, m)
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	return c
}

func mustHead(t *testing.T, s Store) *config.Config {
	t.Helper()
	c, err := s.Head(testCtx(t))
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	return c
}

func i64(v int64) *int64 { return &v }

// ---- cases -----------------------------------------------------------------

func scEmpty(t *testing.T, ns func(*testing.T) Store) {
	s := ns(t)
	ctx := testCtx(t)
	head, err := s.Head(ctx)
	if err != nil || head == nil {
		t.Fatalf("Head on empty store = %v, %v; want empty config, nil", head, err)
	}
	if head.Version != 0 || len(head.Routes) != 0 || len(head.Upstreams) != 0 {
		t.Fatalf("empty head = %+v, want version 0 and no routes/upstreams", head)
	}
	l, err := s.List(ctx, 10, 0)
	if err != nil || l == nil || len(l) != 0 {
		t.Fatalf("List on empty store = %#v, %v; want empty non-nil slice", l, err)
	}
	for _, v := range []int64{-1, 0, 1} {
		if _, err := s.Get(ctx, v); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get(%d) err = %v, want ErrNotFound", v, err)
		}
	}
	for _, to := range []int64{0, 1} {
		if _, err := s.Rollback(ctx, to, UpdateRequest{}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Rollback(%d) on empty store err = %v, want ErrNotFound", to, err)
		}
	}
}

func scGapless(t *testing.T, ns func(*testing.T) Store) {
	s := ns(t)
	ctx := testCtx(t)
	for i := 1; i <= 5; i++ {
		c := mustUpdate(t, s, UpdateRequest{}, mutAddUpstream(fmt.Sprintf("u%d", i)))
		if c.Version != int64(i) {
			t.Fatalf("update #%d returned version %d", i, c.Version)
		}
		if h := mustHead(t, s); h.Version != int64(i) || len(h.Upstreams) != i {
			t.Fatalf("after update #%d head = version %d with %d upstreams", i, h.Version, len(h.Upstreams))
		}
	}
	l, err := s.List(ctx, 50, 0)
	if err != nil || len(l) != 5 {
		t.Fatalf("List = %d entries, %v", len(l), err)
	}
	for i, vi := range l {
		if want := int64(5 - i); vi.Version != want {
			t.Fatalf("List[%d].Version = %d, want %d (descending, gapless)", i, vi.Version, want)
		}
	}
}

func scVersionField(t *testing.T, ns func(*testing.T) Store) {
	s := ns(t)
	for i := 1; i <= 3; i++ {
		mustUpdate(t, s, UpdateRequest{}, mutAddUpstream(fmt.Sprintf("u%d", i)))
	}
	for v := int64(1); v <= 3; v++ {
		c, err := s.Get(testCtx(t), v)
		if err != nil || c.Version != v || len(c.Upstreams) != int(v) {
			t.Fatalf("Get(%d) = %+v, %v; want Version == %d and %d upstreams", v, c, err, v, v)
		}
	}
}

func scDeepCopy(t *testing.T, ns func(*testing.T) Store) {
	s := ns(t)
	got := mustUpdate(t, s, UpdateRequest{}, mutSeed)
	want := canonJSON(got)

	// Mutating the value Update returned must not reach the store.
	got.Upstreams[0].Backends[0] = "http://evil:1"
	got.Upstreams = append(got.Upstreams, testUpstream("evil"))
	got.Routes[0].Name = "evil"
	if h := mustHead(t, s); canonJSON(h) != want {
		t.Fatalf("mutating Update's result changed the store:\n got %s\nwant %s", canonJSON(h), want)
	}

	// Same for Head and Get results.
	h1 := mustHead(t, s)
	h1.Upstreams[0].Backends[0] = "http://evil:2"
	h1.Routes = nil
	if h2 := mustHead(t, s); canonJSON(h2) != want {
		t.Fatalf("mutating Head's result changed the store")
	}
	g, err := s.Get(testCtx(t), 1)
	if err != nil {
		t.Fatal(err)
	}
	g.Upstreams[0].Name = "evil"
	if g2, _ := s.Get(testCtx(t), 1); canonJSON(g2) != want {
		t.Fatalf("mutating Get's result changed the store")
	}
}

func scMutateError(t *testing.T, ns func(*testing.T) Store) {
	s := ns(t)
	mustUpdate(t, s, UpdateRequest{}, mutSeed)
	before := canonJSON(mustHead(t, s))

	sentinel := errors.New("boom")
	calls := 0
	_, err := s.Update(testCtx(t), UpdateRequest{Author: "a"}, func(c *config.Config) error {
		calls++
		c.Upstreams = append(c.Upstreams, testUpstream("half-applied"))
		return sentinel
	})
	if !errors.Is(err, sentinel) { // "returned UNCHANGED"
		t.Fatalf("err = %v, want exactly the mutate error", err)
	}
	if calls != 1 {
		t.Fatalf("mutate called %d times", calls)
	}
	if got := canonJSON(mustHead(t, s)); got != before {
		t.Fatalf("failed mutate changed the store:\n got %s\nwant %s", got, before)
	}
	if l, _ := s.List(testCtx(t), 10, 0); len(l) != 1 {
		t.Fatalf("failed mutate created a version: %d versions", len(l))
	}

	// Wrapped sentinels pass through for the HTTP layer (404 / 400).
	nf := fmt.Errorf("%w: route x", ErrNotFound)
	if _, err := s.Update(testCtx(t), UpdateRequest{}, func(*config.Config) error { return nf }); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	inv := fmt.Errorf("%w: nope", config.ErrInvalid)
	if _, err := s.Update(testCtx(t), UpdateRequest{}, func(*config.Config) error { return inv }); !errors.Is(err, config.ErrInvalid) {
		t.Fatalf("err = %v, want config.ErrInvalid", err)
	}
	// And the next write still gets the next gapless version.
	if c := mustUpdate(t, s, UpdateRequest{}, mutAddUpstream("ok")); c.Version != 2 {
		t.Fatalf("version after failed writes = %d, want 2", c.Version)
	}
}

func scMutateOnce(t *testing.T, ns func(*testing.T) Store) {
	s := ns(t)
	calls := 0
	c := mustUpdate(t, s, UpdateRequest{}, func(c *config.Config) error {
		calls++
		c.Version = 999 // must be overwritten with head+1
		return mutSeed(c)
	})
	if calls != 1 || c.Version != 1 {
		t.Fatalf("calls = %d, version = %d; want 1 and 1", calls, c.Version)
	}
	// mutate sees a deep copy of head: it starts with the previous content.
	mustUpdate(t, s, UpdateRequest{}, func(c *config.Config) error {
		if len(c.Upstreams) != 1 || len(c.Routes) != 1 || c.Version != 1 {
			t.Errorf("mutate saw %+v, want a copy of head (v1)", c)
		}
		return nil
	})
}

// invalidMutations each turn the seed config (upstream web, route api) into
// an invalid one.
var invalidMutations = []struct {
	name string
	mut  func(*config.Config) error
}{
	{"route to unknown upstream", mutAddRoute("r", "/r", "nope")},
	{"duplicate upstream name", mutAddUpstream("web")},
	{"empty upstream name", mutAddUpstream("")},
	{"bad backend url", func(c *config.Config) error {
		c.Upstreams = append(c.Upstreams, config.Upstream{Name: "bad", Backends: []string{"not a url"}})
		return nil
	}},
	{"path prefix without slash", mutAddRoute("r", "r", "web")},
	{"duplicate route name", mutAddRoute("api", "/other", "web")},
	{"duplicate host+path_prefix", mutAddRoute("r2", "/api", "web")},
	{"unknown auth mode", func(c *config.Config) error {
		c.Routes = append(c.Routes, config.Route{Name: "r", PathPrefix: "/r", Upstream: "web", Auth: "magic"})
		return nil
	}},
}

// TestInvalidMutationsAreInvalid guards the fixtures above against config.Validate.
func TestInvalidMutationsAreInvalid(t *testing.T) {
	for _, tc := range invalidMutations {
		var c config.Config
		_ = mutSeed(&c)
		_ = tc.mut(&c)
		c.Normalize()
		if err := c.Validate(); !errors.Is(err, config.ErrInvalid) {
			t.Errorf("%s: Validate = %v, want ErrInvalid", tc.name, err)
		}
	}
}

func scValidation(t *testing.T, ns func(*testing.T) Store) {
	for _, tc := range invalidMutations {
		run(t, tc.name, func(t *testing.T) {
			s := ns(t)
			mustUpdate(t, s, UpdateRequest{}, mutSeed)
			_, err := s.Update(testCtx(t), UpdateRequest{}, tc.mut)
			if !errors.Is(err, config.ErrInvalid) {
				t.Fatalf("err = %v, want errors.Is(config.ErrInvalid)", err)
			}
			if h := mustHead(t, s); h.Version != 1 {
				t.Fatalf("head version %d after rejected update, want 1", h.Version)
			}
			if l, _ := s.List(testCtx(t), 10, 0); len(l) != 1 {
				t.Fatalf("%d versions after rejected update, want 1", len(l))
			}
			if c := mustUpdate(t, s, UpdateRequest{}, mutAddUpstream("fine")); c.Version != 2 {
				t.Fatalf("next version = %d, want 2 (rejected update consumed a version)", c.Version)
			}
		})
	}
}

func scInvalidFirst(t *testing.T, ns func(*testing.T) Store) {
	s := ns(t)
	if _, err := s.Update(testCtx(t), UpdateRequest{}, mutAddRoute("r", "/r", "nope")); !errors.Is(err, config.ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
	if h := mustHead(t, s); h.Version != 0 {
		t.Fatalf("head version = %d, want 0", h.Version)
	}
	if c := mustUpdate(t, s, UpdateRequest{}, mutSeed); c.Version != 1 {
		t.Fatalf("first valid version = %d, want 1", c.Version)
	}
}

func scNormalize(t *testing.T, ns func(*testing.T) Store) {
	s := ns(t)
	c := mustUpdate(t, s, UpdateRequest{}, func(c *config.Config) error {
		c.Upstreams = append(c.Upstreams, config.Upstream{Name: "web", Backends: []string{seedBackend}})
		c.Routes = append(c.Routes, config.Route{Name: "r", Host: "API.Example.COM", PathPrefix: "/r", Upstream: "web"})
		return nil
	})
	for label, got := range map[string]*config.Config{"returned": c, "head": mustHead(t, s)} {
		if got.Routes[0].Host != "api.example.com" || got.Routes[0].Auth != config.AuthNone {
			t.Errorf("%s route not normalized: %+v", label, got.Routes[0])
		}
		if got.Upstreams[0].Algorithm != config.AlgorithmRoundRobin {
			t.Errorf("%s upstream not normalized: %+v", label, got.Upstreams[0])
		}
	}
}

func scIfMatch(t *testing.T, ns func(*testing.T) Store) {
	cases := []struct {
		name    string
		ifMatch *int64
		wantErr error // nil = success
	}{
		{"nil is unconditional", nil, nil},
		{"equal to head", i64(2), nil},
		{"stale", i64(1), ErrVersionConflict},
		{"ahead of head", i64(3), ErrVersionConflict},
		{"zero on non-empty store", i64(0), ErrVersionConflict},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			s := ns(t)
			mustUpdate(t, s, UpdateRequest{}, mutAddUpstream("u1"))
			mustUpdate(t, s, UpdateRequest{}, mutAddUpstream("u2"))
			c, err := s.Update(testCtx(t), UpdateRequest{IfMatch: tc.ifMatch}, mutAddUpstream("u3"))
			if tc.wantErr == nil {
				if err != nil || c.Version != 3 {
					t.Fatalf("got %v, %v; want version 3", c, err)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if h := mustHead(t, s); h.Version != 2 || len(h.Upstreams) != 2 {
				t.Fatalf("conflicting write stored something: head %d with %d upstreams", h.Version, len(h.Upstreams))
			}
			if l, _ := s.List(testCtx(t), 10, 0); len(l) != 2 {
				t.Fatalf("%d versions, want 2", len(l))
			}
		})
	}
	run(t, "conflict message names both versions", func(t *testing.T) {
		s := ns(t)
		mustUpdate(t, s, UpdateRequest{}, mutAddUpstream("u1"))
		mustUpdate(t, s, UpdateRequest{}, mutAddUpstream("u2"))
		_, err := s.Update(testCtx(t), UpdateRequest{IfMatch: i64(1)}, mutAddUpstream("x"))
		// store.go: "controlplane: version conflict: if-match 41, head is 42"
		if err == nil || !strings.Contains(err.Error(), "if-match 1") || !strings.Contains(err.Error(), "head is 2") {
			t.Fatalf("err = %v, want message naming 'if-match 1' and 'head is 2'", err)
		}
	})
	run(t, "conflict is checked before mutate runs", func(t *testing.T) {
		s := ns(t)
		mustUpdate(t, s, UpdateRequest{}, mutAddUpstream("u1"))
		called := false
		_, err := s.Update(testCtx(t), UpdateRequest{IfMatch: i64(7)}, func(*config.Config) error { called = true; return ErrNotFound })
		if !errors.Is(err, ErrVersionConflict) || called {
			t.Fatalf("err = %v, mutate called = %v; want conflict without calling mutate", err, called)
		}
	})
}

func scIfMatchZero(t *testing.T, ns func(*testing.T) Store) {
	s := ns(t)
	if _, err := s.Update(testCtx(t), UpdateRequest{IfMatch: i64(1)}, mutSeed); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("IfMatch=1 on empty store: err = %v, want conflict", err)
	}
	if c, err := s.Update(testCtx(t), UpdateRequest{IfMatch: i64(0)}, mutSeed); err != nil || c.Version != 1 {
		t.Fatalf("IfMatch=0 on empty store: %v, %v; want version 1", c, err)
	}
	if _, err := s.Update(testCtx(t), UpdateRequest{IfMatch: i64(0)}, mutAddUpstream("x")); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("IfMatch=0 on non-empty store: err = %v, want conflict", err)
	}
}

func scGet(t *testing.T, ns func(*testing.T) Store) {
	s := ns(t)
	mustUpdate(t, s, UpdateRequest{}, mutSeed)
	mustUpdate(t, s, UpdateRequest{}, mutAddUpstream("extra"))
	v1, err := s.Get(testCtx(t), 1)
	if err != nil || v1.Version != 1 || len(v1.Upstreams) != 1 || len(v1.Routes) != 1 {
		t.Fatalf("Get(1) = %+v, %v", v1, err)
	}
	v2, err := s.Get(testCtx(t), 2)
	if err != nil || v2.Version != 2 || len(v2.Upstreams) != 2 {
		t.Fatalf("Get(2) = %+v, %v", v2, err)
	}
	for _, v := range []int64{-5, 0, 3, 99} {
		if c, err := s.Get(testCtx(t), v); !errors.Is(err, ErrNotFound) || c != nil {
			t.Fatalf("Get(%d) = %v, %v; want nil, ErrNotFound", v, c, err)
		}
	}
}

func scList(t *testing.T, ns func(*testing.T) Store) {
	s := ns(t)
	ctx := testCtx(t)
	start := time.Now()
	for i := 1; i <= 7; i++ {
		req := UpdateRequest{}
		if i%2 == 1 {
			req.Author = fmt.Sprintf("author-%d", i)
			req.Comment = fmt.Sprintf("comment %d", i)
		}
		mustUpdate(t, s, req, mutAddUpstream(fmt.Sprintf("u%d", i)))
	}
	// A rollback to 3 becomes version 8.
	if _, err := s.Rollback(ctx, 3, UpdateRequest{Author: "rb", Comment: "back to 3"}); err != nil {
		t.Fatal(err)
	}

	versions := func(l []VersionInfo) []int64 {
		out := make([]int64, 0, len(l))
		for _, v := range l {
			out = append(out, v.Version)
		}
		return out
	}
	cases := []struct {
		name   string
		limit  int
		before int64
		want   []int64
	}{
		{"first page", 3, 0, []int64{8, 7, 6}},
		{"before 6", 3, 6, []int64{5, 4, 3}},
		{"before 3", 3, 3, []int64{2, 1}},
		{"before 2", 5, 2, []int64{1}},
		{"before 1 is empty", 5, 1, []int64{}},
		{"negative before means from head", 2, -4, []int64{8, 7}},
		{"before beyond head", 2, 100, []int64{8, 7}},
		{"limit 1", 1, 0, []int64{8}},
		{"limit 0 takes the default (50)", 0, 0, []int64{8, 7, 6, 5, 4, 3, 2, 1}},
		{"negative limit takes the default", -3, 0, []int64{8, 7, 6, 5, 4, 3, 2, 1}},
		{"huge limit is clamped, not an error", 1 << 30, 0, []int64{8, 7, 6, 5, 4, 3, 2, 1}},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			l, err := s.List(ctx, tc.limit, tc.before)
			if err != nil || l == nil {
				t.Fatalf("List = %v, %v", l, err)
			}
			got := versions(l)
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("versions = %v, want %v", got, tc.want)
			}
		})
	}

	all, _ := s.List(ctx, 50, 0)
	byV := map[int64]VersionInfo{}
	for _, v := range all {
		byV[v.Version] = v
	}
	if a := byV[3]; a.Author != "author-3" || a.Comment != "comment 3" || a.RolledBackFrom != nil {
		t.Errorf("v3 = %+v", a)
	}
	if a := byV[4]; a.Author != "" || a.Comment != "" {
		t.Errorf("v4 should have empty author/comment: %+v", a)
	}
	rb := byV[8]
	if rb.Author != "rb" || rb.Comment != "back to 3" || rb.RolledBackFrom == nil || *rb.RolledBackFrom != 3 {
		t.Errorf("v8 = %+v, want author rb, comment, RolledBackFrom 3", rb)
	}
	var prev time.Time
	for v := int64(1); v <= 8; v++ {
		ci := byV[v].CreatedAt
		if ci.IsZero() {
			t.Fatalf("v%d CreatedAt is zero", v)
		}
		if _, off := ci.Zone(); off != 0 {
			t.Errorf("v%d CreatedAt not UTC: %v", v, ci)
		}
		if ci.Before(start.Add(-5*time.Minute)) || ci.After(time.Now().Add(5*time.Minute)) {
			t.Errorf("v%d CreatedAt %v is not near now", v, ci)
		}
		if ci.Before(prev) {
			t.Errorf("v%d CreatedAt %v is before v%d's %v", v, ci, v-1, prev)
		}
		prev = ci
	}
}

func scListClamp(t *testing.T, ns func(*testing.T) Store) {
	if testing.Short() {
		t.Skip("writes 520 versions")
	}
	s := ns(t)
	for i := 1; i <= 520; i++ {
		mustUpdate(t, s, UpdateRequest{}, func(c *config.Config) error {
			// Keep the document tiny: replace instead of grow.
			c.Upstreams = []config.Upstream{testUpstream(fmt.Sprintf("u%d", i))}
			return nil
		})
	}
	if l, _ := s.List(testCtx(t), 0, 0); len(l) != DefaultListLimit {
		t.Errorf("default page = %d entries, want %d", len(l), DefaultListLimit)
	}
	if l, _ := s.List(testCtx(t), 100000, 0); len(l) != MaxListLimit || l[0].Version != 520 {
		t.Errorf("clamped page = %d entries, want %d", len(l), MaxListLimit)
	}
	if l, _ := s.List(testCtx(t), MaxListLimit, 0); len(l) != MaxListLimit {
		t.Errorf("max page = %d entries", len(l))
	}
}

func scRollback(t *testing.T, ns func(*testing.T) Store) {
	s := ns(t)
	ctx := testCtx(t)
	mustUpdate(t, s, UpdateRequest{}, mutSeed)                       // v1
	mustUpdate(t, s, UpdateRequest{}, mutAddRoute("b", "/b", "web")) // v2
	mustUpdate(t, s, UpdateRequest{}, mutAddRoute("c", "/c", "web")) // v3
	v1, _ := s.Get(ctx, 1)
	v2, _ := s.Get(ctx, 2)
	v3, _ := s.Get(ctx, 3)

	c, err := s.Rollback(ctx, 1, UpdateRequest{Author: "bob", Comment: "undo"})
	if err != nil {
		t.Fatal(err)
	}
	if c.Version != 4 || canonBody(c) != canonBody(v1) {
		t.Fatalf("Rollback(1) = %s, want version 4 with v1's body %s", canonJSON(c), canonJSON(v1))
	}
	if h := mustHead(t, s); h.Version != 4 || canonBody(h) != canonBody(v1) {
		t.Fatalf("head after rollback = %s", canonJSON(h))
	}
	// History is untouched.
	if g, _ := s.Get(ctx, 3); canonJSON(g) != canonJSON(v3) {
		t.Fatalf("v3 changed by rollback")
	}
	l, _ := s.List(ctx, 10, 0)
	if len(l) != 4 || l[0].Version != 4 || l[0].RolledBackFrom == nil || *l[0].RolledBackFrom != 1 || l[0].Author != "bob" || l[0].Comment != "undo" {
		t.Fatalf("List[0] = %+v", l[0])
	}
	for _, vi := range l[1:] {
		if vi.RolledBackFrom != nil {
			t.Errorf("v%d RolledBackFrom = %d, want nil", vi.Version, *vi.RolledBackFrom)
		}
	}

	// to == 0 means head-1: head is 4 (copy of v1), so target is v3.
	c, err = s.Rollback(ctx, 0, UpdateRequest{})
	if err != nil || c.Version != 5 || canonBody(c) != canonBody(v3) {
		t.Fatalf("Rollback(0) = %v, %v; want version 5 with v3's body", c, err)
	}
	if l, _ := s.List(ctx, 1, 0); l[0].RolledBackFrom == nil || *l[0].RolledBackFrom != 3 {
		t.Fatalf("Rollback(0) RolledBackFrom = %v, want 3", l[0].RolledBackFrom)
	}
	// Result is a fresh value.
	c.Routes[0].Name = "mutated"
	if g := mustHead(t, s); g.Routes[0].Name == "mutated" {
		t.Fatal("Rollback result aliases stored state")
	}
	_ = v2
}

func scRollbackTargets(t *testing.T, ns func(*testing.T) Store) {
	s := ns(t)
	ctx := testCtx(t)
	mustUpdate(t, s, UpdateRequest{}, mutSeed) // v1: head <= 1
	if _, err := s.Rollback(ctx, 0, UpdateRequest{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Rollback(0) with head 1: err = %v, want ErrNotFound (version 0 is not a row)", err)
	}
	if h := mustHead(t, s); h.Version != 1 {
		t.Fatalf("failed rollback changed head to %d", h.Version)
	}
	mustUpdate(t, s, UpdateRequest{}, mutAddUpstream("x")) // v2
	for _, to := range []int64{99, 3, -1} {
		if _, err := s.Rollback(ctx, to, UpdateRequest{}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Rollback(%d) err = %v, want ErrNotFound", to, err)
		}
	}
	if h := mustHead(t, s); h.Version != 2 {
		t.Fatalf("failed rollbacks changed head to %d", h.Version)
	}
	// to == head is allowed and re-publishes head as a new version.
	c, err := s.Rollback(ctx, 2, UpdateRequest{})
	if err != nil || c.Version != 3 {
		t.Fatalf("Rollback(head) = %v, %v; want version 3", c, err)
	}
	if v2, _ := s.Get(ctx, 2); canonBody(c) != canonBody(v2) {
		t.Fatalf("Rollback(head) body differs from head")
	}
	if l, _ := s.List(ctx, 1, 0); l[0].RolledBackFrom == nil || *l[0].RolledBackFrom != 2 {
		t.Fatalf("RolledBackFrom = %v, want 2", l[0].RolledBackFrom)
	}
}

func scRollbackIfMatch(t *testing.T, ns func(*testing.T) Store) {
	s := ns(t)
	ctx := testCtx(t)
	mustUpdate(t, s, UpdateRequest{}, mutSeed)
	mustUpdate(t, s, UpdateRequest{}, mutAddUpstream("x"))
	if _, err := s.Rollback(ctx, 1, UpdateRequest{IfMatch: i64(1)}); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale IfMatch: err = %v, want conflict", err)
	}
	// IfMatch is checked before the target is resolved.
	if _, err := s.Rollback(ctx, 99, UpdateRequest{IfMatch: i64(1)}); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale IfMatch + missing target: err = %v, want conflict (checked first)", err)
	}
	if h := mustHead(t, s); h.Version != 2 {
		t.Fatalf("conflicting rollback wrote: head %d", h.Version)
	}
	if c, err := s.Rollback(ctx, 1, UpdateRequest{IfMatch: i64(2)}); err != nil || c.Version != 3 {
		t.Fatalf("matching IfMatch: %v, %v", c, err)
	}
}

func scCanceled(t *testing.T, ns func(*testing.T) Store) {
	s := ns(t)
	mustUpdate(t, s, UpdateRequest{}, mutSeed)
	mustUpdate(t, s, UpdateRequest{}, mutAddUpstream("x"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Update(ctx, UpdateRequest{}, mutAddUpstream("never")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Update with canceled ctx: err = %v, want context.Canceled", err)
	}
	if _, err := s.Rollback(ctx, 1, UpdateRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Rollback with canceled ctx: err = %v, want context.Canceled", err)
	}
	if h := mustHead(t, s); h.Version != 2 {
		t.Fatalf("canceled writes changed head to %d", h.Version)
	}
}

func scPing(t *testing.T, ns func(*testing.T) Store) {
	s := ns(t)
	if err := s.Ping(testCtx(t)); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func scConcurrentUpdates(t *testing.T, ns func(*testing.T) Store) {
	const goroutines, perG = 16, 25
	s := ns(t)
	ctx := testCtx(t)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		returned []int64
		writers  sync.WaitGroup
	)
	start := make(chan struct{})
	for g := 0; g < goroutines; g++ {
		writers.Add(1)
		go func(g int) {
			defer writers.Done()
			<-start
			for i := 0; i < perG; i++ {
				c, err := s.Update(ctx, UpdateRequest{}, mutAddUpstream(fmt.Sprintf("g%d-i%d", g, i)))
				if err != nil {
					t.Errorf("g%d update %d: %v", g, i, err)
					return
				}
				mu.Lock()
				returned = append(returned, c.Version)
				mu.Unlock()
			}
		}(g)
	}
	// Readers must never see a torn or uncommitted state: every update adds
	// exactly one upstream, so head.Version == len(head.Upstreams) always, and
	// versions never go backwards (guarantee 4).
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var last int64
			for {
				select {
				case <-stop:
					return
				default:
				}
				h, err := s.Head(ctx)
				if err != nil {
					t.Errorf("reader Head: %v", err)
					return
				}
				if int(h.Version) != len(h.Upstreams) {
					t.Errorf("torn head: version %d with %d upstreams", h.Version, len(h.Upstreams))
					return
				}
				if h.Version < last {
					t.Errorf("head went backwards: %d after %d", h.Version, last)
					return
				}
				last = h.Version
			}
		}()
	}
	close(start)
	writers.Wait()
	close(stop)
	wg.Wait()

	head := mustHead(t, s)
	if head.Version != goroutines*perG {
		t.Fatalf("final head = %d, want %d (lost updates)", head.Version, goroutines*perG)
	}
	names := map[string]bool{}
	for _, u := range head.Upstreams {
		names[u.Name] = true
	}
	for g := 0; g < goroutines; g++ {
		for i := 0; i < perG; i++ {
			if n := fmt.Sprintf("g%d-i%d", g, i); !names[n] {
				t.Fatalf("lost update: upstream %s missing", n)
			}
		}
	}
	sort.Slice(returned, func(i, j int) bool { return returned[i] < returned[j] })
	for i, v := range returned {
		if v != int64(i+1) {
			t.Fatalf("returned versions not gapless/unique at index %d: %d", i, v)
		}
	}
	l, err := s.List(ctx, MaxListLimit, 0)
	if err != nil || len(l) != goroutines*perG {
		t.Fatalf("List = %d entries, %v", len(l), err)
	}
	for i, vi := range l {
		if vi.Version != int64(goroutines*perG-i) {
			t.Fatalf("List not gapless at %d: %d", i, vi.Version)
		}
	}
}

func scConcurrentIfMatch(t *testing.T, ns func(*testing.T) Store) {
	const goroutines = 16
	s := ns(t)
	mustUpdate(t, s, UpdateRequest{}, mutAddUpstream("base")) // head 1

	start := make(chan struct{})
	errs := make([]error, goroutines)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			<-start
			_, errs[g] = s.Update(testCtx(t), UpdateRequest{IfMatch: i64(1)}, mutAddUpstream(fmt.Sprintf("racer%d", g)))
		}(g)
	}
	close(start)
	wg.Wait()

	wins, conflicts := 0, 0
	for g, err := range errs {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, ErrVersionConflict):
			conflicts++
		default:
			t.Errorf("goroutine %d: unexpected error %v", g, err)
		}
	}
	if wins != 1 || conflicts != goroutines-1 {
		t.Fatalf("wins = %d, conflicts = %d; want 1 and %d", wins, conflicts, goroutines-1)
	}
	if h := mustHead(t, s); h.Version != 2 || len(h.Upstreams) != 2 {
		t.Fatalf("head = version %d with %d upstreams, want 2 and 2", h.Version, len(h.Upstreams))
	}
}

func scMixedWriters(t *testing.T, ns func(*testing.T) Store) {
	const goroutines, ops = 8, 10
	s := ns(t)
	mustUpdate(t, s, UpdateRequest{}, mutSeed)
	mustUpdate(t, s, UpdateRequest{}, mutAddUpstream("x"))

	start := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			<-start
			for i := 0; i < ops; i++ {
				var err error
				if (g+i)%2 == 0 {
					_, err = s.Update(testCtx(t), UpdateRequest{}, mutAddUpstream(fmt.Sprintf("m%d-%d", g, i)))
				} else {
					_, err = s.Rollback(testCtx(t), 1, UpdateRequest{})
				}
				if err != nil {
					t.Errorf("g%d op %d: %v", g, i, err)
					return
				}
			}
		}(g)
	}
	close(start)
	wg.Wait()

	want := int64(2 + goroutines*ops)
	if h := mustHead(t, s); h.Version != want {
		t.Fatalf("head = %d, want %d", h.Version, want)
	}
	l, err := s.List(testCtx(t), MaxListLimit, 0)
	if err != nil || int64(len(l)) != want {
		t.Fatalf("List = %d entries, %v; want %d", len(l), err, want)
	}
	for i, vi := range l {
		if vi.Version != want-int64(i) {
			t.Fatalf("versions not gapless at %d: %d", i, vi.Version)
		}
		if vi.RolledBackFrom != nil && *vi.RolledBackFrom != 1 {
			t.Fatalf("v%d RolledBackFrom = %d, want 1", vi.Version, *vi.RolledBackFrom)
		}
	}
}
