package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/balancer"
	"github.com/amitkhanal596/tollgate/internal/config"
)

// Special fakeBackend modes; any positive value is an HTTP status to return.
const (
	modeHang    = -1 // block until the probe gives up or the test releases
	modePattern = -2 // statuses taken from pattern, cycling
)

// fakeBackend is an httptest server whose health endpoint can be flipped
// between 200, 500, hanging, patterned, and closed.
type fakeBackend struct {
	srv     *httptest.Server
	mode    atomic.Int32
	hits    atomic.Int64
	release chan struct{}
	once    sync.Once

	mu       sync.Mutex
	paths    []string
	pathMode map[string]int
	pattern  []int
	patIdx   int
}

func newFakeBackend(t testing.TB) *fakeBackend {
	t.Helper()
	f := &fakeBackend{release: make(chan struct{}), pathMode: map[string]int{}}
	f.mode.Store(http.StatusOK)
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.close)
	return f
}

func (f *fakeBackend) handle(w http.ResponseWriter, r *http.Request) {
	f.hits.Add(1)
	f.mu.Lock()
	f.paths = append(f.paths, r.URL.Path)
	mode, ok := f.pathMode[r.URL.Path]
	if !ok {
		mode = int(f.mode.Load())
	}
	if mode == modePattern && len(f.pattern) > 0 {
		mode = f.pattern[f.patIdx%len(f.pattern)]
		f.patIdx++
	}
	f.mu.Unlock()

	if mode == modeHang {
		select {
		case <-r.Context().Done():
		case <-f.release:
		}
		return
	}
	w.WriteHeader(mode)
}

func (f *fakeBackend) set(mode int) { f.mode.Store(int32(mode)) }

func (f *fakeBackend) setPath(path string, mode int) {
	f.mu.Lock()
	f.pathMode[path] = mode
	f.mu.Unlock()
}

func (f *fakeBackend) setPattern(p ...int) {
	f.mu.Lock()
	f.pattern, f.patIdx = p, 0
	f.mu.Unlock()
	f.set(modePattern)
}

func (f *fakeBackend) hitsOn(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, p := range f.paths {
		if p == path {
			n++
		}
	}
	return n
}

// close shuts the server down (connection refused afterwards). Idempotent.
func (f *fakeBackend) close() {
	f.once.Do(func() {
		close(f.release)
		f.srv.Close()
	})
}

func (f *fakeBackend) backend(t testing.TB, upstream string) *balancer.Backend {
	t.Helper()
	u, err := url.Parse(f.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return balancer.NewBackend(upstream+"|"+f.srv.URL, u)
}

// check returns fast-probe settings. Timeout is generous relative to loopback
// latency under -race so healthy backends never flake; hang tests override it.
func check(path string, rise, fall int) config.HealthCheck {
	return config.HealthCheck{
		Path:               path,
		Interval:           config.Duration(20 * time.Millisecond),
		Timeout:            config.Duration(150 * time.Millisecond),
		HealthyThreshold:   rise,
		UnhealthyThreshold: fall,
	}
}

func newClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
		// A 3xx must be seen as a 3xx, not followed.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

type change struct {
	id string
	up bool
}

type recorder struct {
	mu     sync.Mutex
	events []change
}

func (r *recorder) onChange(b *balancer.Backend, up bool) {
	r.mu.Lock()
	r.events = append(r.events, change{b.ID, up})
	r.mu.Unlock()
}

func (r *recorder) snapshot() []change {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]change(nil), r.events...)
}

// harness runs a Checker in the background and stops it on cleanup.
type harness struct {
	c      *Checker
	rec    *recorder
	cancel context.CancelFunc
	done   chan error
}

func newHarness(t testing.TB, client *http.Client) *harness {
	t.Helper()
	if client == nil {
		client = newClient()
	}
	rec := &recorder{}
	return &harness{c: NewChecker(client, time.Now, rec.onChange), rec: rec}
}

func (h *harness) start(t testing.TB) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.done = make(chan error, 1)
	go func() { h.done <- h.c.Run(ctx) }()
	t.Cleanup(func() { h.stop(t) })
}

// stop cancels Run and returns its error (nil if already stopped).
func (h *harness) stop(t testing.TB) error {
	t.Helper()
	if h.cancel == nil {
		return nil
	}
	h.cancel()
	h.cancel = nil
	select {
	case err := <-h.done:
		return err
	case <-time.After(5 * time.Second):
		t.Error("Run did not return within 5s of cancel")
		return nil
	}
}

func eventually(t testing.TB, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out after %v waiting for: %s", d, what)
	}
}

// always asserts cond holds continuously for d.
func always(t testing.TB, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if !cond() {
			t.Fatalf("violated within %v: %s", d, what)
		}
		time.Sleep(time.Millisecond)
	}
}

const wait = 3 * time.Second

func goroutinesSettleTo(t testing.TB, baseline int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baseline {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	buf := make([]byte, 1<<16)
	buf = buf[:runtime.Stack(buf, true)]
	t.Fatalf("goroutines = %d, want <= %d after shutdown\n%s", runtime.NumGoroutine(), baseline, buf)
}

func runtimeGoroutines() int { return runtime.NumGoroutine() }
