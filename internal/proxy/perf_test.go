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
func TestUpstreamConnectionsReusedUnderConcurrency(t *testing.T) {
	const n, rounds = 300, 4
	for _, nb := range []int{1, 3} {
		t.Run(fmt.Sprintf("backends=%d", nb), func(t *testing.T) {
			// gate holds every request at the backend until all n of the
			// round have arrived, so concurrency is exactly n (no reuse
			// within a round is possible, which makes the count exact).
			var arrived atomic.Int64
			release := make(chan struct{})
			var relMu sync.Mutex
			handler := func(w http.ResponseWriter, _ *http.Request) {
				relMu.Lock()
				ch := release
				relMu.Unlock()
				arrived.Add(1)
				<-ch
				_, _ = io.WriteString(w, "ok")
			}
			backends := make([]*countingBackend, nb)
			for i := range backends {
				backends[i] = newCountingBackend(t, handler)
			}
			url := perfGateway(t, backends) + "/p"
			client := bigPoolClient(t)

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
				waitFor(t, 10*time.Second, "all requests in flight", func() bool { return arrived.Load() == n })
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

// The pool must be able to keep every connection it may open (see the
// comment on the constants in transport.go); otherwise bursts churn.
func TestUpstreamPoolKeepsEveryConnectionItOpens(t *testing.T) {
	tr := newUpstreamTransport(transportKey{dial: time.Second, responseHeader: time.Second})
	if tr.MaxConnsPerHost <= 0 {
		t.Fatalf("MaxConnsPerHost = %d, want a positive bound", tr.MaxConnsPerHost)
	}
	if tr.MaxIdleConnsPerHost < tr.MaxConnsPerHost {
		t.Errorf("MaxIdleConnsPerHost %d < MaxConnsPerHost %d: connections above the idle cap are closed after every burst",
			tr.MaxIdleConnsPerHost, tr.MaxConnsPerHost)
	}
	if tr.MaxIdleConns != 0 && tr.MaxIdleConns < tr.MaxIdleConnsPerHost {
		t.Errorf("MaxIdleConns %d < MaxIdleConnsPerHost %d", tr.MaxIdleConns, tr.MaxIdleConnsPerHost)
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
// With FlushInterval -1 the gateway flushed the headers from a timer goroutine
// and again after each body write: 2-3 writes per response, which was 23% of
// its CPU at 12k rps (Phase 8 profile).
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
// ends; the client must see that first piece well before.
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
