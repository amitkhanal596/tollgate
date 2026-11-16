package proxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
	"github.com/amitkhanal596/tollgate/internal/discovery"
)

// Acknowledger wiring (docs/architecture.md section 5, config/ack.go,
// section 11 row 2): the reconcile loop reports the outcome of every
// config-triggered build, and only those.

type ackEvent struct {
	kind    string // "applied" or "rejected"
	version int64
	err     error
	liveAt  int64 // Gateway.Version() at the moment of the call
	gid     uint64
}

// ackSource is a fakeSource that also implements config.Acknowledger and
// records every call.
type ackSource struct {
	*fakeSource
	g *Gateway

	mu      sync.Mutex
	events  []ackEvent
	inCall  atomic.Int32
	overlap atomic.Bool
}

func (s *ackSource) record(kind string, v int64, err error) {
	if s.inCall.Add(1) > 1 {
		s.overlap.Store(true)
	}
	defer s.inCall.Add(-1)
	ev := ackEvent{kind: kind, version: v, err: err, liveAt: s.g.Version(), gid: igGoroutineID()}
	s.mu.Lock()
	s.events = append(s.events, ev)
	s.mu.Unlock()
}

func (s *ackSource) Applied(v int64)           { s.record("applied", v, nil) }
func (s *ackSource) Rejected(v int64, e error) { s.record("rejected", v, e) }

func (s *ackSource) snapshot() []ackEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ackEvent(nil), s.events...)
}

func (s *ackSource) waitEvents(t *testing.T, n int) []ackEvent {
	t.Helper()
	waitFor(t, 3*time.Second, "acknowledgements", func() bool { return len(s.snapshot()) >= n })
	return s.snapshot()
}

// stableEvents asserts the number of events stays exactly n for a while
// ("exactly once" cannot be proven by looking only once).
func (s *ackSource) stableEvents(t *testing.T, n int) {
	t.Helper()
	consistently(t, 100*time.Millisecond, "no further acknowledgement", func() bool { return len(s.snapshot()) == n })
}

// startAck runs a Gateway against an ackSource.
func startAck(t *testing.T, opts Options) (*Gateway, *ackSource) {
	t.Helper()
	g := New(opts)
	src := &ackSource{fakeSource: newFakeSource(), g: g}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx, src) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Logf("Run returned: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after cleanup cancel")
		}
	})
	return g, src
}

func wantEvent(t *testing.T, got ackEvent, kind string, version int64) {
	t.Helper()
	if got.kind != kind || got.version != version {
		t.Fatalf("ack = %s(%d), want %s(%d)", got.kind, got.version, kind, version)
	}
}

// A good config: Applied exactly once, after the swap was visible.
func TestAckAppliedOnceAfterSwap(t *testing.T) {
	a := echoUpstream(t, "A")
	g, src := startAck(t, Options{})
	src.push(cfgOf(1, []config.Route{rt("r", "", "/", "u")}, up("u", a.URL)))

	evs := src.waitEvents(t, 1)
	wantEvent(t, evs[0], "applied", 1)
	if evs[0].liveAt != 1 {
		t.Errorf("Version() was %d when Applied(1) ran, want 1 (Applied comes AFTER the swap)", evs[0].liveAt)
	}
	if evs[0].err != nil {
		t.Errorf("Applied carried an error: %v", evs[0].err)
	}
	src.stableEvents(t, 1)
	if !g.Ready() {
		t.Error("not ready after Applied")
	}
}

// Configs that cannot be built: Rejected exactly once with a non-nil error,
// the previous config keeps serving, and no Applied is reported for them.
func TestAckRejectedWhenBuildFails(t *testing.T) {
	a := echoUpstream(t, "A")
	good := func(v int64) *config.Config {
		return cfgOf(v, []config.Route{rt("r", "", "/", "u")}, up("u", a.URL))
	}
	unknownUpstream := func(v int64) *config.Config {
		return cfgOf(v, []config.Route{rt("r", "", "/", "ghost")}, up("u", a.URL))
	}
	jwtEnvUnset := func(v int64) *config.Config {
		c := cfgOf(v, []config.Route{
			rt("r", "", "/", "u"),
			authRoute("secure", "/secure", "u", config.AuthJWT, nil),
		}, up("u", a.URL))
		return withAuth(c, config.AuthSettings{JWT: jwtSettings()})
	}

	tests := []struct {
		name      string
		bad       func(int64) *config.Config
		firstGood bool // apply a good v1 before the bad v2
	}{
		{"unknown upstream after a good config", unknownUpstream, true},
		{"jwt secret env unset after a good config", jwtEnvUnset, true},
		{"unknown upstream as the very first config", unknownUpstream, false},
		{"jwt secret env unset as the very first config", jwtEnvUnset, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Getenv always empty: the JWT HMAC secret is "unset".
			g, src := startAck(t, Options{Getenv: func(string) string { return "" }})
			badVersion, prev := int64(1), int64(0)
			var evs []ackEvent
			if tc.firstGood {
				src.push(good(1))
				evs = src.waitEvents(t, 1)
				wantEvent(t, evs[0], "applied", 1)
				badVersion, prev = 2, 1
			}
			src.push(tc.bad(badVersion))
			evs = src.waitEvents(t, len(evs)+1)
			last := evs[len(evs)-1]
			wantEvent(t, last, "rejected", badVersion)
			if last.err == nil {
				t.Error("Rejected was called with a nil error")
			}
			if last.liveAt != prev || g.Version() != prev {
				t.Errorf("Version() = %d (at ack: %d), want it unchanged at %d", g.Version(), last.liveAt, prev)
			}
			if got := g.Ready(); got != tc.firstGood {
				t.Errorf("Ready() = %v, want %v", got, tc.firstGood)
			}
			src.stableEvents(t, len(evs)) // once, and no Applied for the bad version

			// A later good config is applied and acknowledged normally.
			src.push(good(badVersion + 1))
			evs = src.waitEvents(t, len(evs)+1)
			wantEvent(t, evs[len(evs)-1], "applied", badVersion+1)
		})
	}
}

// The loop reports from one goroutine, never concurrently, in delivery order.
func TestAckOrderAndGoroutine(t *testing.T) {
	a := echoUpstream(t, "A")
	_, src := startAck(t, Options{})
	mk := func(v int64, upstream string) *config.Config {
		return cfgOf(v, []config.Route{rt("r", "", "/", upstream)}, up("u", a.URL))
	}
	// v1 ok, v2 bad, v3 ok, v4 ok, v5 bad, v6 ok. Each is pushed only after
	// the previous one was acknowledged, so latest-wins coalescing cannot
	// swallow one.
	steps := []struct {
		cfg  *config.Config
		kind string
	}{
		{mk(1, "u"), "applied"}, {mk(2, "ghost"), "rejected"}, {mk(3, "u"), "applied"},
		{mk(4, "u"), "applied"}, {mk(5, "ghost"), "rejected"}, {mk(6, "u"), "applied"},
	}
	for i, st := range steps {
		src.push(st.cfg)
		evs := src.waitEvents(t, i+1)
		wantEvent(t, evs[i], st.kind, st.cfg.Version)
	}
	src.stableEvents(t, len(steps))

	evs := src.snapshot()
	self := igGoroutineID()
	for i, ev := range evs {
		if ev.gid != evs[0].gid {
			t.Errorf("ack %d ran on goroutine %d, ack 0 on %d: want one reconcile goroutine", i, ev.gid, evs[0].gid)
		}
		if ev.gid == self {
			t.Errorf("ack %d ran on the test goroutine", i)
		}
	}
	if src.overlap.Load() {
		t.Error("Applied/Rejected were called concurrently")
	}
}

// A ConfigSource without Acknowledger (FileSource) must keep working, for
// good and bad configs alike.
func TestAckSourceWithoutAcknowledgerWorks(t *testing.T) {
	a := echoUpstream(t, "A")
	h := startRun(t) // plain fakeSource: no Applied/Rejected methods
	if _, ok := any(h.src).(config.Acknowledger); ok {
		t.Fatal("test bug: fakeSource must not implement Acknowledger")
	}
	h.src.push(cfgOf(1, []config.Route{rt("r", "", "/", "ghost")}, up("u", a.URL)))
	consistently(t, 100*time.Millisecond, "bad config not applied", func() bool { return h.g.Version() == 0 })
	h.apply(t, cfgOf(2, []config.Route{rt("r", "", "/", "u")}, up("u", a.URL)))
	srv := httptest.NewServer(h.g)
	defer srv.Close()
	if code := getCode(t, newClient(t), srv.URL+"/"); code != http.StatusOK {
		t.Fatalf("status %d, want 200", code)
	}
}

// Rebuilds caused by discovery are not acknowledged (ack.go contract 2),
// even when they fail.
func TestAckDiscoveryRebuildsAreNotAcknowledged(t *testing.T) {
	a, b := echoUpstream(t, "A"), echoUpstream(t, "B")

	t.Run("successful rebuild", func(t *testing.T) {
		res := &igResolver{closeOnCancel: true}
		g, src := startAck(t, Options{Resolver: res})
		src.push(discCfg(1, discUp("users", "users", "", "http")))
		wantEvent(t, src.waitEvents(t, 1)[0], "applied", 1)

		w := res.waitWatches(t, 1)
		srv := httptest.NewServer(g)
		defer srv.Close()
		sets := []struct {
			name string
			eps  []discovery.Endpoint
			want string // body of the first answer once the set is live
		}{
			{"only A", []discovery.Endpoint{igEP("a", a)}, "A"},
			{"only B", []discovery.Endpoint{igEP("b", b)}, "B"},
		}
		for _, set := range sets {
			w.send(set.eps)
			waitFor(t, 3*time.Second, "endpoint set "+set.name+" to take effect", func() bool {
				return igCount(t, srv.URL, 3)[set.want] == 3
			})
		}
		src.stableEvents(t, 1)
		if g.Version() != 1 {
			t.Errorf("Version() = %d, want 1 (discovery does not create versions)", g.Version())
		}
	})

	t.Run("failing rebuild", func(t *testing.T) {
		// The JWT secret disappears from the environment after v1 was built,
		// so any later rebuild fails. A discovery update triggers one: it
		// must NOT be reported as Rejected, must not change the version,
		// and the old snapshot (with its working JWT auth) keeps serving.
		var secretSet atomic.Bool
		secretSet.Store(true)
		getenv := func(k string) string {
			if k == testJWTEnv && secretSet.Load() {
				return testJWTSecret
			}
			return ""
		}
		res := &igResolver{closeOnCancel: true}
		g, src := startAck(t, Options{Resolver: res, Getenv: getenv})
		u := discUp("users", "users", "", "http")
		cfg := withAuth(cfgOf(1, []config.Route{authRoute("r", "/", "users", config.AuthJWT, nil)}, u),
			config.AuthSettings{JWT: jwtSettings()})
		src.push(cfg)
		wantEvent(t, src.waitEvents(t, 1)[0], "applied", 1)
		w := res.waitWatches(t, 1)
		w.send([]discovery.Endpoint{igEP("a", a)})
		srv := httptest.NewServer(g)
		defer srv.Close()
		tok := "Bearer " + goodToken(t, "alice")
		get := func() int {
			req, _ := http.NewRequest(http.MethodGet, srv.URL+"/x", nil)
			req.Header.Set("Authorization", tok)
			resp, err := newClient(t).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			return resp.StatusCode
		}
		waitFor(t, 3*time.Second, "first endpoint set to be served", func() bool { return get() == 200 })

		secretSet.Store(false)
		w.send([]discovery.Endpoint{igEP("a", a), igEP("b", b)})
		time.Sleep(150 * time.Millisecond) // give the (failing) rebuild time to run
		src.stableEvents(t, 1)
		if g.Version() != 1 {
			t.Errorf("Version() = %d, want 1", g.Version())
		}
		if code := get(); code != 200 {
			t.Errorf("status %d after a failed discovery rebuild, want 200 from the old snapshot", code)
		}
	})
}
