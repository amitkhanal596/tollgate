package proxy

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
	"github.com/amitkhanal596/tollgate/internal/discovery"
)

// Discovery wiring (ADR 0007, architecture.md sections 5 and 7, section 11
// rows 4 and 17): Options.Resolver feeds the backends of `consul:` upstreams.

func startDisc(t *testing.T, opts Options, res *igResolver, cfg *config.Config) (*harness, *httptest.Server) {
	t.Helper()
	opts.Resolver = res
	return startGatewayWith(t, opts, cfg)
}

// igBackendURLs lists the backend URLs of upstream in the live snapshot.
func igBackendURLs(g *Gateway, upstream string) []string {
	s := g.snap.Load()
	if s == nil || s.upstreams[upstream] == nil {
		return nil
	}
	var out []string
	for _, b := range s.upstreams[upstream].backends {
		out = append(out, b.URL.String())
	}
	slices.Sort(out)
	return out
}

// igStop cancels the harness and checks that Run returns within d. The
// result is put back so the harness cleanup still sees it.
func igStop(t *testing.T, h *harness, d time.Duration) {
	t.Helper()
	h.cancel()
	select {
	case err := <-h.done:
		h.done <- err
	case <-time.After(d):
		t.Fatalf("Run did not return within %v after cancel", d)
	}
}

type igResult struct {
	code int
	body string
	err  error
}

// igHold fires GET /hold requests one at a time until one is blocked on b
// (b.holding == 1; every other backend answers /hold at once) and returns the
// channel that delivers that request's result.
func igHold(t *testing.T, base string, b *ctrlBackend) <-chan igResult {
	t.Helper()
	for i := 0; i < 20; i++ {
		ch := make(chan igResult, 1)
		go func() {
			c := &http.Client{Timeout: 10 * time.Second}
			resp, err := c.Get(base + "/hold")
			if err != nil {
				ch <- igResult{err: err}
				return
			}
			defer resp.Body.Close()
			buf := make([]byte, 64)
			n, _ := resp.Body.Read(buf)
			ch <- igResult{code: resp.StatusCode, body: string(buf[:n])}
		}()
		deadline := time.Now().Add(300 * time.Millisecond)
		for time.Now().Before(deadline) {
			if b.holding.Load() >= 1 {
				return ch
			}
			select {
			case <-ch: // answered by another backend; try again
				deadline = time.Time{}
			case <-time.After(2 * time.Millisecond):
			}
		}
	}
	t.Fatal("could not park a request on the target backend")
	return nil
}

// Before the first endpoint set: the config is applied (Version set) but the
// gateway is not ready yet, and the route has no backends, so 503.
func TestDiscoveryBeforeFirstEndpointSet(t *testing.T) {
	res := &igResolver{closeOnCancel: true}
	h, srv := startDisc(t, Options{}, res, discCfg(1, discUp("users", "users", "", "http")))

	// Fix round 5 (D1, ADR 0007 Startup): the config is applied, but the
	// gateway is NOT ready until the service has answered once (or the
	// discovery-ready timeout passes).
	if h.g.Ready() || h.g.Version() != 1 {
		t.Fatalf("Ready=%v Version=%d, want not ready at version 1", h.g.Ready(), h.g.Version())
	}
	w := res.waitWatches(t, 1)
	if want := (discovery.Query{Service: "users", Tag: ""}); w.q != want {
		t.Errorf("Watch query = %+v, want %+v", w.q, want)
	}
	if code := getCode(t, newClient(t), srv.URL+"/x"); code != http.StatusServiceUnavailable {
		t.Errorf("status %d before any endpoint set, want 503", code)
	}
}

func TestDiscoveryBalancesAcrossEndpointsWithoutNewVersion(t *testing.T) {
	a, b := echoUpstream(t, "A"), echoUpstream(t, "B")
	res := &igResolver{closeOnCancel: true}
	h, srv := startDisc(t, Options{}, res, discCfg(1, discUp("users", "users", "", "http")))
	w := res.waitWatches(t, 1)

	w.send([]discovery.Endpoint{igEP("n1/a", a), igEP("n2/b", b)})
	waitFor(t, 3*time.Second, "both endpoints to serve", func() bool {
		got := igCount(t, srv.URL, 4)
		return got["A"] > 0 && got["B"] > 0
	})
	got := igCount(t, srv.URL, 20)
	if got["A"] != 10 || got["B"] != 10 {
		t.Errorf("distribution over 20 requests = %v, want A:10 B:10 (round robin)", got)
	}
	if v := h.g.Version(); v != 1 {
		t.Errorf("Version() = %d, want 1: endpoint changes are not config versions", v)
	}
}

// [A,B] -> [B,C]: A leaves, C joins, and B keeps its Backend (health and
// in-flight state), proven by a request parked on B across the change.
func TestDiscoveryEndpointChangeKeepsSurvivorState(t *testing.T) {
	a, b, c := newCtrlBackend(t, "A"), newCtrlBackend(t, "B"), newCtrlBackend(t, "C")
	hold := make(chan struct{})
	b.hold.Store(&hold)
	var once atomic.Bool
	release := func() {
		if once.CompareAndSwap(false, true) {
			close(hold)
		}
	}
	defer release()

	res := &igResolver{closeOnCancel: true}
	h, srv := startDisc(t, Options{}, res, discCfg(1, discUp("users", "users", "", "http")))
	w := res.waitWatches(t, 1)
	ep := func(id string, x *ctrlBackend) discovery.Endpoint {
		return discovery.Endpoint{ID: id, Address: hostport(x.srv)}
	}

	w.send([]discovery.Endpoint{ep("n/a", a), ep("n/b", b)})
	cl := newClient(t)
	waitFor(t, 3*time.Second, "A and B to serve", func() bool {
		burst(t, cl, srv.URL, "/x", 4)
		return a.hits.Load() > 0 && b.hits.Load() > 0
	})

	held := igHold(t, srv.URL, b)
	if got := inFlight(t, h.g, "users", b.url()); got != 1 {
		t.Fatalf("in-flight on B = %d with one parked request, want 1", got)
	}

	w.send([]discovery.Endpoint{ep("n/b", b), ep("n/c", c)})
	waitFor(t, 3*time.Second, "A out of rotation and C in", func() bool {
		return quietFor(t, cl, srv.URL, a, 10) && c.hits.Load() > 0
	})
	if got := inFlight(t, h.g, "users", b.url()); got != 1 {
		t.Errorf("in-flight on B after the change = %d, want 1: B's Backend must survive the endpoint change", got)
	}

	release()
	select {
	case r := <-held:
		if r.err != nil || r.code != 200 || r.body != "B" {
			t.Errorf("parked request finished with %+v, want 200 \"B\"", r)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("parked request on B did not complete after the endpoint change")
	}
	waitFor(t, 2*time.Second, "in-flight accounting on B to end at 0", func() bool {
		return inFlight(t, h.g, "users", b.url()) == 0
	})
	if v := h.g.Version(); v != 1 {
		t.Errorf("Version() = %d, want 1", v)
	}
}

// Consul reporting zero passing instances is trusted: 503, and recovery works.
func TestDiscoveryEmptySetIs503(t *testing.T) {
	a := echoUpstream(t, "A")
	res := &igResolver{closeOnCancel: true}
	_, srv := startDisc(t, Options{}, res, discCfg(1, discUp("users", "users", "", "http")))
	w := res.waitWatches(t, 1)
	cl := newClient(t)

	w.send([]discovery.Endpoint{igEP("n/a", a)})
	waitFor(t, 3*time.Second, "200 from A", func() bool { return getCode(t, cl, srv.URL+"/x") == 200 })

	w.send([]discovery.Endpoint{}) // empty, not nil: "zero passing instances"
	waitFor(t, 3*time.Second, "503 after the empty set", func() bool { return getCode(t, cl, srv.URL+"/x") == 503 })
	for i := 0; i < 5; i++ {
		if code := getCode(t, cl, srv.URL+"/x"); code != 503 {
			t.Fatalf("request %d with an empty set: %d, want 503", i, code)
		}
	}

	w.send([]discovery.Endpoint{igEP("n/a", a)})
	waitFor(t, 3*time.Second, "200 again", func() bool { return getCode(t, cl, srv.URL+"/x") == 200 })
}

// Section 11 row 17: Consul can list two instances with the same address.
func TestDiscoveryDuplicateAddressesCollapseToOneBackend(t *testing.T) {
	a, c := echoUpstream(t, "A"), echoUpstream(t, "C")
	res := &igResolver{closeOnCancel: true}
	h, srv := startDisc(t, Options{}, res, discCfg(1, discUp("users", "users", "", "http")))
	w := res.waitWatches(t, 1)

	w.send([]discovery.Endpoint{igEP("n1/a-1", a), igEP("n2/a-2", a), igEP("n3/c", c)})
	waitFor(t, 3*time.Second, "both distinct backends to serve", func() bool {
		got := igCount(t, srv.URL, 4)
		return got["A"] > 0 && got["C"] > 0
	})
	got := igCount(t, srv.URL, 30)
	if got["A"] != 15 || got["C"] != 15 {
		t.Errorf("distribution = %v, want A:15 C:15 (a duplicated address must not get double weight)", got)
	}
	if urls := igBackendURLs(h.g, "users"); len(urls) != 2 {
		t.Errorf("snapshot has backends %v, want exactly 2", urls)
	}
}

func TestDiscoveryWatchLifecycle(t *testing.T) {
	a := echoUpstream(t, "A")

	t.Run("one watch per distinct service and tag", func(t *testing.T) {
		res := &igResolver{closeOnCancel: true}
		u1 := discUp("u1", "users", "", "http")
		u2 := discUp("u2", "users", "", "https") // same (service, tag): shares the watch
		u3 := discUp("u3", "users", "canary", "http")
		cfg := cfgOf(1, []config.Route{rt("r1", "", "/u1", "u1"), rt("r2", "", "/u2", "u2"), rt("r3", "", "/u3", "u3")}, u1, u2, u3)
		h, _ := startDisc(t, Options{}, res, cfg)

		res.waitWatches(t, 2)
		consistently(t, 150*time.Millisecond, "exactly two watches", func() bool { return res.count() == 2 })
		var qs []discovery.Query
		for i := 0; i < res.count(); i++ {
			qs = append(qs, res.watch(i).q)
		}
		for _, want := range []discovery.Query{{Service: "users"}, {Service: "users", Tag: "canary"}} {
			if !slices.Contains(qs, want) {
				t.Errorf("no Watch for %+v; got %+v", want, qs)
			}
		}

		// The shared watch feeds both upstreams, each with its own scheme.
		var shared *igWatch
		for i := 0; i < res.count(); i++ {
			if res.watch(i).q.Tag == "" {
				shared = res.watch(i)
			}
		}
		shared.send([]discovery.Endpoint{{ID: "n/a", Address: "127.0.0.1:9101"}})
		waitFor(t, 3*time.Second, "both upstreams to get the endpoint", func() bool {
			return len(igBackendURLs(h.g, "u1")) == 1 && len(igBackendURLs(h.g, "u2")) == 1
		})
		if got := igBackendURLs(h.g, "u1"); got[0] != "http://127.0.0.1:9101" {
			t.Errorf("u1 backends = %v, want http://127.0.0.1:9101", got)
		}
		if got := igBackendURLs(h.g, "u2"); got[0] != "https://127.0.0.1:9101" {
			t.Errorf("u2 backends = %v, want https://127.0.0.1:9101", got)
		}
	})

	t.Run("unreferenced service cancels its watch; re-adding starts a fresh one", func(t *testing.T) {
		res := &igResolver{closeOnCancel: true}
		h, _ := startDisc(t, Options{}, res, discCfg(1, discUp("users", "users", "", "http")))
		w0 := res.waitWatches(t, 1)

		// v2 has only a static upstream.
		h.apply(t, cfgOf(2, []config.Route{rt("r", "", "/", "s")}, up("s", a.URL)))
		waitFor(t, 3*time.Second, "watch 0 to be cancelled", w0.cancelled)
		consistently(t, 100*time.Millisecond, "no new watch while unreferenced", func() bool { return res.count() == 1 })

		h.apply(t, discCfg(3, discUp("users", "users", "", "http")))
		w1 := res.waitWatches(t, 2)
		if w1.cancelled() {
			t.Error("the fresh watch was cancelled")
		}
		if !w0.cancelled() {
			t.Error("the old watch came back to life")
		}
		if w1.q != w0.q {
			t.Errorf("fresh watch query %+v, want %+v", w1.q, w0.q)
		}
	})

	t.Run("a config that still references the service keeps the watch and the latest endpoints", func(t *testing.T) {
		b := echoUpstream(t, "B")
		res := &igResolver{closeOnCancel: true}
		h, srv := startDisc(t, Options{}, res, discCfg(1, discUp("users", "users", "", "http")))
		w := res.waitWatches(t, 1)
		w.send([]discovery.Endpoint{igEP("n/b", b)})
		waitFor(t, 3*time.Second, "B to serve", func() bool { return igCount(t, srv.URL, 1)["B"] == 1 })

		// v2 changes the balancing algorithm only; the resolver says nothing new.
		u := discUp("users", "users", "", "http")
		u.Algorithm = config.AlgorithmLeastConn
		h.apply(t, discCfg(2, u))

		consistently(t, 150*time.Millisecond, "watch kept, no restart", func() bool {
			return res.count() == 1 && !w.cancelled()
		})
		if got := igCount(t, srv.URL, 5); got["B"] != 5 {
			t.Errorf("after the config change: %v, want all 5 answered by B (the loop must keep the latest endpoints)", got)
		}
	})

	t.Run("a changed tag replaces the watch", func(t *testing.T) {
		res := &igResolver{closeOnCancel: true}
		h, _ := startDisc(t, Options{}, res, discCfg(1, discUp("users", "users", "", "http")))
		w0 := res.waitWatches(t, 1)
		h.apply(t, discCfg(2, discUp("users", "users", "v2", "http")))
		w1 := res.waitWatches(t, 2)
		waitFor(t, 3*time.Second, "old watch cancelled", w0.cancelled)
		if want := (discovery.Query{Service: "users", Tag: "v2"}); w1.q != want {
			t.Errorf("new watch query %+v, want %+v", w1.q, want)
		}
		if w1.cancelled() {
			t.Error("new watch cancelled")
		}
	})

	t.Run("shutdown cancels every watch", func(t *testing.T) {
		res := &igResolver{closeOnCancel: true}
		cfg := cfgOf(1, []config.Route{rt("r1", "", "/a", "u1"), rt("r2", "", "/b", "u2")},
			discUp("u1", "users", "", "http"), discUp("u2", "orders", "", "http"))
		h, _ := startDisc(t, Options{}, res, cfg)
		res.waitWatches(t, 2)
		igStop(t, h, 3*time.Second)
		for i := 0; i < res.count(); i++ {
			if !res.watch(i).cancelled() {
				t.Errorf("watch %d (%+v) still live after Run returned", i, res.watch(i).q)
			}
		}
	})
}

// Section 11 row 4: a forwarder with an unconsumed update must never block
// shutdown. 100 iterations, each racing an update against the cancel; both
// resolver styles (closing its channel like the real one, and never closing).
func TestDiscoveryShutdownWithPendingUpdateDoesNotHang(t *testing.T) {
	a := echoUpstream(t, "A")
	for _, closeOnCancel := range []bool{true, false} {
		name := "resolver never closes its channel"
		if closeOnCancel {
			name = "resolver closes its channel on cancel"
		}
		t.Run(name, func(t *testing.T) {
			for i := 0; i < 100; i++ {
				res := &igResolver{closeOnCancel: closeOnCancel}
				g := New(Options{Resolver: res})
				src := newFakeSource()
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() { done <- g.Run(ctx, src) }()
				src.push(discCfg(1, discUp("users", "users", "", "http")))
				w := res.waitWatches(t, 1)
				waitFor(t, 3*time.Second, "config applied", func() bool { return g.Version() == 1 })

				eps := []discovery.Endpoint{igEP("n/a", a)}
				go w.send(eps)
				cancel()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Fatalf("iteration %d: Run did not return within 2s with an update racing the cancel", i)
				}
				if !w.cancelled() {
					t.Fatalf("iteration %d: watch ctx not cancelled after Run returned", i)
				}
			}
		})
	}
}

// Section 11 row 4: an update that arrives late on a cancelled watch (an
// older generation) must never overwrite what the current watch says.
func TestDiscoveryStaleGenerationIsIgnored(t *testing.T) {
	a, b, c := newCtrlBackend(t, "A"), newCtrlBackend(t, "B"), newCtrlBackend(t, "C")
	res := &igResolver{closeOnCancel: false} // dead watches stay open: late sends are possible
	h, srv := startDisc(t, Options{}, res, discCfg(1, discUp("users", "users", "", "http")))
	cl := newClient(t)
	w0 := res.waitWatches(t, 1)
	w0.send([]discovery.Endpoint{{ID: "n/c", Address: hostport(c.srv)}})
	waitFor(t, 3*time.Second, "C to serve", func() bool { return getCode(t, cl, srv.URL+"/x") == 200 })

	// v2 drops the service, v3 re-adds it: a new watch, a new generation.
	h.apply(t, cfgOf(2, []config.Route{rt("r", "", "/", "s")}, up("s", c.url())))
	waitFor(t, 3*time.Second, "old watch cancelled", w0.cancelled)
	h.apply(t, discCfg(3, discUp("users", "users", "", "http")))
	w1 := res.waitWatches(t, 2)

	// Late update on the OLD watch: must be ignored.
	w0.send([]discovery.Endpoint{{ID: "n/a", Address: hostport(a.srv)}})
	consistently(t, 200*time.Millisecond, "A never receives traffic from a stale update", func() bool {
		_ = getCode(t, cl, srv.URL+"/x")
		return a.hits.Load() == 0
	})

	w1.send([]discovery.Endpoint{{ID: "n/b", Address: hostport(b.srv)}})
	waitFor(t, 3*time.Second, "the new watch's endpoints to be used", func() bool { return getCode(t, cl, srv.URL+"/x") == 200 && b.hits.Load() > 0 })
	w0.send([]discovery.Endpoint{{ID: "n/a", Address: hostport(a.srv)}}) // late again
	consistently(t, 150*time.Millisecond, "only the current watch's endpoints are used", func() bool {
		_ = getCode(t, cl, srv.URL+"/x")
		return a.hits.Load() == 0
	})

	// Stress the same race: the stale send runs concurrently with the swap
	// that cancels its watch. Whatever the forwarder does, A stays unused.
	prev := w1
	version := int64(3)
	for i := 0; i < 20; i++ {
		old := prev
		n0 := res.count()
		version++
		go old.send([]discovery.Endpoint{{ID: "n/a", Address: hostport(a.srv)}})
		h.apply(t, cfgOf(version, []config.Route{rt("r", "", "/", "s")}, up("s", c.url())))
		version++
		h.apply(t, discCfg(version, discUp("users", "users", "", "http")))
		prev = res.waitWatches(t, n0+1) // the fresh watch of this re-add
		prev.send([]discovery.Endpoint{{ID: "n/b", Address: hostport(b.srv)}})
		waitFor(t, 3*time.Second, "B to serve", func() bool { return getCode(t, cl, srv.URL+"/x") == 200 })
		old.send([]discovery.Endpoint{{ID: "n/a", Address: hostport(a.srv)}})
		for j := 0; j < 5; j++ {
			_ = getCode(t, cl, srv.URL+"/x")
		}
		if got := a.hits.Load(); got != 0 {
			t.Fatalf("iteration %d: A received %d requests from a stale-generation update", i, got)
		}
	}
}

// Active health checks apply to discovered backends like static ones.
func TestDiscoveryHealthChecksApplyToDiscoveredBackends(t *testing.T) {
	a, b := newCtrlBackend(t, "A"), newCtrlBackend(t, "B")
	res := &igResolver{closeOnCancel: true}
	u := discUp("users", "users", "", "http")
	u.HealthCheck = hcFast()
	_, srv := startDisc(t, Options{}, res, discCfg(1, u))
	w := res.waitWatches(t, 1)
	w.send([]discovery.Endpoint{
		{ID: "n/a", Address: hostport(a.srv)}, {ID: "n/b", Address: hostport(b.srv)},
	})
	cl := newClient(t)
	waitFor(t, 3*time.Second, "both discovered backends to serve", func() bool {
		burst(t, cl, srv.URL, "/x", 4)
		return a.hits.Load() > 0 && b.hits.Load() > 0
	})

	b.setDown()
	waitFor(t, 5*time.Second, "B to be marked down by its active check", func() bool { return quietFor(t, cl, srv.URL, b, 9) })
	for i, code := range burst(t, cl, srv.URL, "/x", 30) {
		if code != 200 {
			t.Fatalf("request %d after B was marked down: %d, want 200", i, code)
		}
	}

	b.setUp()
	before := b.hits.Load()
	waitFor(t, 5*time.Second, "B to receive traffic again", func() bool {
		burst(t, cl, srv.URL, "/x", 6)
		return b.hits.Load() > before
	})
}

// The scheme of the consul block builds the backend URLs. Observed through
// the live snapshot: a TLS round trip would need the gateway to trust a test
// CA, which Options deliberately does not offer.
func TestDiscoverySchemeBuildsBackendURLs(t *testing.T) {
	tests := []struct {
		scheme, want string
	}{
		{"", "http"}, // documented default
		{"http", "http"},
		{"https", "https"},
	}
	for _, tc := range tests {
		t.Run("scheme="+tc.scheme, func(t *testing.T) {
			res := &igResolver{closeOnCancel: true}
			h, _ := startDisc(t, Options{}, res, discCfg(1, discUp("users", "users", "", tc.scheme)))
			w := res.waitWatches(t, 1)
			w.send([]discovery.Endpoint{{ID: "n/a", Address: "127.0.0.1:9201"}, {ID: "n/b", Address: "127.0.0.1:9202"}})
			want := []string{tc.want + "://127.0.0.1:9201", tc.want + "://127.0.0.1:9202"}
			waitFor(t, 3*time.Second, "backend URLs "+strings.Join(want, ","), func() bool {
				return slices.Equal(igBackendURLs(h.g, "users"), want)
			})
		})
	}
}

// No Resolver: the config still applies, the upstream is empty (503) and the
// operator gets a warning.
func TestDiscoveryNilResolverApplies503AndWarns(t *testing.T) {
	var logs igBuf
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	h := startRunWith(t, Options{Logger: logger}) // Resolver nil
	h.apply(t, discCfg(1, discUp("users", "users", "", "http")))
	srv := httptest.NewServer(h.g)
	defer srv.Close()

	if !h.g.Ready() {
		t.Error("not ready after a config with a consul upstream and no Resolver")
	}
	if code := getCode(t, newClient(t), srv.URL+"/x"); code != http.StatusServiceUnavailable {
		t.Errorf("status %d, want 503", code)
	}
	h.apply(t, discCfg(2, discUp("users", "users", "", "http")))
	if h.g.Version() != 2 {
		t.Errorf("Version() = %d, want 2", h.g.Version())
	}
	warned := false
	for _, m := range logs.jsonLines(t) {
		if m["level"] == "WARN" && strings.Contains(logsLine(m), "users") {
			warned = true
		}
	}
	if !warned {
		t.Errorf("no WARN log naming the upstream; logs:\n%s", logs.String())
	}
}

func logsLine(m map[string]any) string {
	var sb strings.Builder
	for k, v := range m {
		sb.WriteString(k)
		sb.WriteString("=")
		if s, ok := v.(string); ok {
			sb.WriteString(s)
		}
		sb.WriteString(" ")
	}
	return sb.String()
}
