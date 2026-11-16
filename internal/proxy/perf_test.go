package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/amitkhanal596/tollgate/internal/config"
	"github.com/amitkhanal596/tollgate/internal/metrics"
)

// Performance regression guards for the proxy hot path (Phase 8 throughput
// investigation): upstream connection reuse under concurrency, and the cost
// of one proxied request with one and with three backends.

// countingBackend is an upstream that counts the TCP connections it accepts,
// so a test can tell reused keep-alive connections from fresh dials.
type countingBackend struct {
	srv   *httptest.Server
	conns atomic.Int64 // connections accepted (StateNew transitions)
}

func newCountingBackend(tb testing.TB, h http.HandlerFunc) *countingBackend {
	tb.Helper()
	cb := &countingBackend{}
	cb.srv = httptest.NewUnstartedServer(h)
	cb.srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			cb.conns.Add(1)
		}
	}
	cb.srv.Start()
	tb.Cleanup(cb.srv.Close)
	return cb
}

// perfGateway runs a production-like Gateway (metrics on, access log off,
// as in the benchmark stack) with one round-robin route /p over backends,
// fronted by a real HTTP server. It returns the gateway's base URL.
func perfGateway(tb testing.TB, backends []*countingBackend) string {
	tb.Helper()
	urls := make([]string, len(backends))
	for i, b := range backends {
		urls[i] = b.srv.URL
	}
	front := httptest.NewServer(perfHandler(tb, urls...))
	tb.Cleanup(front.Close)
	return front.URL
}

// perfHandler is the Gateway behind perfGateway, with route /p over urls.
func perfHandler(tb testing.TB, urls ...string) http.Handler {
	tb.Helper()
	g := New(Options{Metrics: metrics.New(prometheus.NewRegistry())})
	src := newFakeSource()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx, src) }()
	tb.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			tb.Logf("Run returned: %v", err)
		}
	})
	cfg := cfgOf(1, []config.Route{rt("p", "", "/p", "u")}, up("u", urls...))
	src.push(cfg)
	for deadline := time.Now().Add(3 * time.Second); g.Version() != cfg.Version; {
		if time.Now().After(deadline) {
			tb.Fatal("config was not applied")
		}
		time.Sleep(time.Millisecond)
	}
	return g
}

// bigPoolClient never closes its own connections to the gateway, so any
// connection churn measured is on the gateway's upstream side.
func bigPoolClient(tb testing.TB) *http.Client {
	tb.Helper()
	tr := &http.Transport{MaxIdleConns: 4096, MaxIdleConnsPerHost: 4096}
	tb.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: 10 * time.Second}
}

// N requests held in flight together at the backends, then released, M times
// over. A gateway that keeps its upstream connections reuses them from round
// to round: the backends see at most N connections in total. One that keeps
// fewer idle connections than it just used closes the surplus after every
// round and dials them again in the next one (connect cost, a TIME_WAIT
// socket per close, accept load on the backend): it would show up to
// 64 + (N - 64) x M here with the old 64-per-host pool.
//
// Each run uses roughly 1.3k ephemeral ports (client->gateway and
// gateway->backend sockets, each left in TIME_WAIT). A high -count on macOS
// (about 16k ephemeral ports, 15 s TIME_WAIT) can exhaust them; the test then
// fails with a dial error instead of hanging, but the failure is an artefact
// of the machine, not of the gateway.
func TestUpstreamConnectionsReusedUnderConcurrency(t *testing.T) {
	const n, rounds = 300, 4
	for _, nb := range []int{1, 3} {
		t.Run(fmt.Sprintf("backends=%d", nb), func(t *testing.T) {
			// gate holds every request at the backend until all n of the
			// round have arrived, so concurrency is exactly n (no reuse
			// within a round is possible, which makes the count exact).
			var arrived atomic.Int64
			var relMu sync.Mutex
			release := make(chan struct{})
			handler := func(w http.ResponseWriter, r *http.Request) {
				relMu.Lock()
				ch := release
				relMu.Unlock()
				arrived.Add(1)
				// Also leave when the gateway gives up on the request, so a
				// failed test can never leave a handler blocked forever.
				select {
				case <-ch:
				case <-r.Context().Done():
					return
				}
				_, _ = io.WriteString(w, "ok")
			}
			backends := make([]*countingBackend, nb)
			for i := range backends {
				backends[i] = newCountingBackend(t, handler)
			}
			url := perfGateway(t, backends) + "/p"
			client := bigPoolClient(t)
			// Registered after the servers' cleanups, so it runs BEFORE them
			// (cleanups are LIFO): whatever way the test ends, the current
			// gate is open and httptest.Server.Close has nothing to wait for.
			t.Cleanup(func() {
				relMu.Lock()
				defer relMu.Unlock()
				close(release)
			})

			for r := 0; r < rounds; r++ {
				arrived.Store(0)
				var wg sync.WaitGroup
				errs := make(chan error, n)
				for i := 0; i < n; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						resp, err := client.Get(url)
						if err != nil {
							errs <- err
							return
						}
						_, _ = io.Copy(io.Discard, resp.Body)
						_ = resp.Body.Close()
						if resp.StatusCode != http.StatusOK {
							errs <- fmt.Errorf("status %d", resp.StatusCode)
						}
					}()
				}
				// Wait for all n to arrive, but fail fast with the real error
				// if any request fails instead of waiting for the timeout.
				deadline := time.Now().Add(10 * time.Second)
				for arrived.Load() != n {
					select {
					case err := <-errs:
						t.Fatalf("round %d: request failed with %d of %d in flight: %v", r, arrived.Load(), n, err)
					default:
					}
					if time.Now().After(deadline) {
						t.Fatalf("round %d: only %d of %d requests in flight after 10s", r, arrived.Load(), n)
					}
					time.Sleep(2 * time.Millisecond)
				}
				relMu.Lock()
				close(release)
				release = make(chan struct{})
				relMu.Unlock()
				wg.Wait()
				close(errs)
				for err := range errs {
					t.Fatalf("round %d: %v", r, err)
				}
				// Let the transport put the connections back in its pool
				// (it does so asynchronously after the body is read).
				time.Sleep(50 * time.Millisecond)
			}

			var total int64
			for _, b := range backends {
				total += b.conns.Load()
			}
			if total > n {
				t.Errorf("backends accepted %d connections for %d rounds of %d concurrent requests; want <= %d (connections must be reused, not re-dialled every round)",
					total, rounds, n, n)
			}
		})
	}
}

// BenchmarkProxy measures one proxied GET through the full gateway (HTTP
// server, routing, round robin, ReverseProxy, metrics) to 1 and 3 trivial
// backends, at high client concurrency. dials/op is the upstream
// connections opened per request: it should stay near 0 once warm.
//
//	go test -run '^$' -bench BenchmarkProxy -benchmem ./internal/proxy/
func BenchmarkProxy(b *testing.B) {
	for _, nb := range []int{1, 3} {
		b.Run(fmt.Sprintf("backends=%d", nb), func(b *testing.B) {
			backends := make([]*countingBackend, nb)
			for i := range backends {
				backends[i] = newCountingBackend(b, func(w http.ResponseWriter, _ *http.Request) {
					_, _ = io.WriteString(w, "ok")
				})
			}
			url := perfGateway(b, backends) + "/p"
			client := bigPoolClient(b)
			// 32 goroutines per P: enough in-flight requests to need far
			// more than a handful of upstream connections per backend.
			b.SetParallelism(32)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					resp, err := client.Get(url)
					if err != nil {
						b.Error(err)
						return
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}
			})
			b.StopTimer()
			var dials int64
			for _, be := range backends {
				dials += be.conns.Load()
			}
			b.ReportMetric(float64(dials)/float64(b.N), "dials/op")
		})
	}
}

// The pool must be able to keep every connection it opens, and must not cap
// connections at all. A cap makes long-lived streams (SSE) hold slots: request
// N+1 then waits inside the transport until the route's deadline, the client
// gets a 504 that passive health blames on a healthy backend, and streams on
// one route starve other routes that share the transport. The churn fix comes
// from the idle pool alone: every connection opened for a burst of up to 1024
// concurrent requests can be kept (the old 64-per-host pool could not).
func TestUpstreamPoolHasNoConnectionCapAndKeepsBursts(t *testing.T) {
	tr := newUpstreamTransport(transportKey{dial: time.Second, responseHeader: time.Second})
	if tr.MaxConnsPerHost != 0 {
		t.Errorf("MaxConnsPerHost = %d, want 0 (no cap: streams must not starve other requests)", tr.MaxConnsPerHost)
	}
	if tr.MaxIdleConnsPerHost < 1024 {
		t.Errorf("MaxIdleConnsPerHost = %d, want >= 1024 (every connection of a 1024-request burst must be kept)", tr.MaxIdleConnsPerHost)
	}
	if tr.MaxIdleConns != 0 {
		t.Errorf("MaxIdleConns = %d, want 0 (a global cap below the per-host sum evicts and churns)", tr.MaxIdleConns)
	}
}

// Behavioural guard for the no-cap decision: with K long-lived SSE streams
// open to one backend, a plain request to the same backend through ANOTHER
// route that shares the same timeouts (so the same transport) must complete
// promptly with 200, and the backend must not be ejected. This passes today
// because K is far below the old cap of 1024; it exists to catch a cap being
// reintroduced at a small value.
func TestStreamsDoNotStarveOtherRoutesOnSameBackend(t *testing.T) {
	const k = 50
	hold := make(chan struct{})
	var streams atomic.Int64
	be := newCountingBackend(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stream" {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: hi\n\n")
			w.(http.Flusher).Flush()
			streams.Add(1)
			select {
			case <-hold:
			case <-r.Context().Done():
			}
			return
		}
		_, _ = io.WriteString(w, "plain")
	})
	// Runs before the backend's Close (LIFO), freeing the stream handlers.
	t.Cleanup(func() { close(hold) })

	g := New(Options{Metrics: metrics.New(prometheus.NewRegistry())})
	src := newFakeSource()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx, src) }()
	cfg := cfgOf(1, []config.Route{rt("s", "", "/stream", "u"), rt("p", "", "/plain", "u")}, up("u", be.srv.URL))
	src.push(cfg)
	front := httptest.NewServer(g)
	// Cleanups run LIFO: front.Close needs the streams released first, which
	// the hold cleanup above does only after this one; so cancel the gateway
	// (which aborts upstream requests) and close client connections first.
	client := bigPoolClient(t)
	t.Cleanup(func() {
		client.CloseIdleConnections()
		front.CloseClientConnections()
		front.Close()
		cancel()
		<-done
	})
	waitFor(t, 3*time.Second, "config applied", func() bool { return g.Version() == cfg.Version })

	streamCtx, stopStreams := context.WithCancel(context.Background())
	t.Cleanup(stopStreams)
	for i := 0; i < k; i++ {
		req, _ := http.NewRequestWithContext(streamCtx, http.MethodGet, front.URL+"/stream", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("opening stream %d: %v", i, err)
		}
		defer func() { _ = resp.Body.Close() }()
	}
	waitFor(t, 5*time.Second, "all streams open at the backend", func() bool { return streams.Load() == k })

	start := time.Now()
	pctx, pcancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer pcancel()
	req, _ := http.NewRequestWithContext(pctx, http.MethodGet, front.URL+"/plain", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("plain request with %d streams open: %v", k, err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(body) != "plain" {
		t.Errorf("plain request with %d streams open = %d %q, want 200 \"plain\"", k, resp.StatusCode, body)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("plain request took %v with %d streams open, want prompt", d, k)
	}
	// The backend must still be selectable (not ejected by passive health).
	for i := 0; i < 5; i++ {
		resp, err := client.Get(front.URL + "/plain")
		if err != nil {
			t.Fatalf("follow-up %d: %v", i, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("follow-up %d = %d, want 200 (backend ejected?)", i, resp.StatusCode)
		}
	}
}

// writeCountingListener counts Write calls on every accepted connection:
// each one is a write syscall, and on a real network usually a TCP segment.
type writeCountingListener struct {
	net.Listener
	writes atomic.Int64
}

func (l *writeCountingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &writeCountingConn{Conn: c, writes: &l.writes}, nil
}

type writeCountingConn struct {
	net.Conn
	writes *atomic.Int64
}

func (c *writeCountingConn) Write(p []byte) (int, error) {
	c.writes.Add(1)
	return c.Conn.Write(p)
}

// A small response with a Content-Length must reach the client in ONE write.
// With FlushInterval -1 the gateway flushed after every body write: 2 writes
// per small response (39-40 for 20), and flushing was 23% of its CPU at 12k
// rps (Phase 8 profile). The 100 ms interval keeps it at one write because
// the copy finishes and stops the flush timer before it fires.
func TestSmallResponseIsOneWrite(t *testing.T) {
	be := newCountingBackend(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok") // net/http sets Content-Length: 2
	})
	front := httptest.NewUnstartedServer(perfHandler(t, be.srv.URL))
	cl := &writeCountingListener{Listener: front.Listener}
	front.Listener = cl
	front.Start()
	t.Cleanup(front.Close)
	client := bigPoolClient(t)

	const n = 20
	for i := 0; i < n; i++ {
		resp, err := client.Get(front.URL + "/p")
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || string(body) != "ok" {
			t.Fatalf("got %d %q", resp.StatusCode, body)
		}
	}
	if got := cl.writes.Load(); got != n {
		t.Errorf("%d writes to the client for %d small responses, want %d (one per response)", got, n, n)
	}
}

// Streams must still be relayed as they are produced, not when they end:
// SSE, and any response without a Content-Length (chunked). The backend
// sends the first piece and then holds the response open until the test
// ends; the client must see that first piece well before. This guards the
// response recorder's Flush/FlushError/Unwrap plumbing (the ReverseProxy's
// flushes must reach the client connection through it), NOT the value of
// FlushInterval: a chunked response is flushed by the proxy regardless.
func TestStreamedResponsesAreNotBuffered(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
	}{
		{"server-sent events", "text/event-stream"},
		{"chunked, no content-length", "text/plain"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hold := make(chan struct{})
			be := newCountingBackend(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				_, _ = io.WriteString(w, "data: first\n\n")
				w.(http.Flusher).Flush() // no Content-Length: chunked
				select {
				case <-hold:
				case <-r.Context().Done():
				}
			})
			// Registered after the backend, so it runs first and frees the
			// handler before the backend server's Close waits for it.
			t.Cleanup(func() { close(hold) })
			url := perfGateway(t, []*countingBackend{be}) + "/p"

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			resp, err := bigPoolClient(t).Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			got := make(chan string, 1)
			go func() {
				buf := make([]byte, len("data: first\n\n"))
				n, _ := io.ReadFull(resp.Body, buf)
				got <- string(buf[:n])
			}()
			select {
			case s := <-got:
				if s != "data: first\n\n" {
					t.Errorf("first piece = %q", s)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("the first piece of a streamed response was held back by the gateway")
			}
		})
	}
}

// FlushInterval is 100 ms so that a response with a Content-Length whose body
// arrives slowly is not held back: with 0 the ReverseProxy writes into the
// server's 4 KiB buffer and the headers (and anything up to 4 KiB) sit there
// until the body completes. Both tests below FAIL with FlushInterval 0.

// newSlowBackend writes status+headers (Content-Length length) immediately and
// then hands the response to body() to write the body as slowly as it likes.
func newSlowBackend(t *testing.T, length int, body func(w http.ResponseWriter, fl http.Flusher)) *countingBackend {
	t.Helper()
	return newCountingBackend(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(length))
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		body(w, w.(http.Flusher))
	})
}

// A long poll that sends its headers first and the body one second later:
// the client must see the headers long before the body.
func TestKnownLengthHeadersAreNotHeldUntilBodyCompletes(t *testing.T) {
	be := newSlowBackend(t, 2, func(w http.ResponseWriter, fl http.Flusher) {
		time.Sleep(time.Second)
		_, _ = io.WriteString(w, "ok")
	})
	url := perfGateway(t, []*countingBackend{be}) + "/p"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	start := time.Now()
	resp, err := bigPoolClient(t).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if d := time.Since(start); d > 500*time.Millisecond {
		t.Errorf("response headers arrived after %v, want < 500ms (the backend sent them at once; the gateway held them until the body completed)", d)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "ok" {
		t.Errorf("body = %q, want ok", body)
	}
}

// A known-length body that trickles in (128 B every 20 ms, 8 KiB in ~1.3 s)
// must reach the client as it arrives, not when it is complete. The pieces are
// small enough that the server's 4 KiB write buffer takes ~640 ms to fill, so
// with FlushInterval 0 the first byte arrives after that (256 B pieces fill it
// in 320 ms, too close to the threshold to tell the two apart).
func TestKnownLengthTrickleReachesClientIncrementally(t *testing.T) {
	const size, piece, gap = 8 * 1024, 128, 20 * time.Millisecond
	var firstWrite atomic.Int64 // unix nanos of the backend's first body write
	be := newSlowBackend(t, size, func(w http.ResponseWriter, fl http.Flusher) {
		chunk := make([]byte, piece)
		for sent := 0; sent < size; sent += piece {
			if sent == 0 {
				firstWrite.Store(time.Now().UnixNano())
			}
			if _, err := w.Write(chunk); err != nil {
				return
			}
			fl.Flush()
			time.Sleep(gap)
		}
	})
	url := perfGateway(t, []*countingBackend{be}) + "/p"

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := bigPoolClient(t).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	buf := make([]byte, 1)
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		t.Fatal(err)
	}
	first := time.Now()
	fw := time.Unix(0, firstWrite.Load())
	if lag := first.Sub(fw); lag > 500*time.Millisecond {
		t.Errorf("first body byte reached the client %v after the backend's first write, want < 500ms (body of ~1.3s was held until complete)", lag)
	}
	n, _ := io.Copy(io.Discard, resp.Body)
	if n+1 != size {
		t.Errorf("body length = %d, want %d", n+1, size)
	}
}
