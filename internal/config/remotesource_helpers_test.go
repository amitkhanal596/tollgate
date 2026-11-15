package config

// Test scaffolding for RemoteSource (docs/architecture.md §5.1): a fake
// control plane built on httptest, a recording Sleep, and small frame helpers.
//
// The literal strings below are duplicated from internal/controlplane/api.go
// on purpose: internal/config must not import any other internal package.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	rsStreamPath   = "/v1/config/stream"                // controlplane stream endpoint
	rsStatusFormat = "/v1/dataplanes/%s/status"         // controlplane status endpoint
	rsHeartbeat    = ": heartbeat\n\n"                  // controlplane.HeartbeatComment
	rsToken        = "tok-0123456789abcdef0123456789ab" // data-plane token used by tests
	rsNode         = "gw-7"
)

// rsReq is what the fake control plane saw on one stream request.
type rsReq struct {
	method, path, node, since, auth, accept, lastEventID string
	hasSince                                             bool
}

// rsConn is one accepted stream request, scripted by the test.
type rsConn struct {
	req    rsReq
	frames chan []byte
	stop   chan bool     // true = abort abruptly, false = end normally
	done   chan struct{} // closed when the server handler returns (client went away or we ended it)
	wrote  atomic.Int64
}

func (c *rsConn) send(s string) { c.frames <- []byte(s) }

// sendBytewise delivers s one byte per write, to exercise partial reads.
func (c *rsConn) sendBytewise(s string) {
	for i := 0; i < len(s); i++ {
		c.frames <- []byte{s[i]}
	}
}
func (c *rsConn) end()   { c.stop <- false }
func (c *rsConn) abort() { c.stop <- true }

func (c *rsConn) waitDone(t *testing.T, what string) {
	t.Helper()
	select {
	case <-c.done:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s (server never saw the request end)", what)
	}
}

// rsStep scripts the response to one stream request.
type rsStep func(cp *fakeCP, w http.ResponseWriter, r *http.Request, c *rsConn)

func rsStreamHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	_ = http.NewResponseController(w).Flush()
}

// rsHold is the default step: a live stream the test feeds through rsConn.
func rsHold(cp *fakeCP, w http.ResponseWriter, r *http.Request, c *rsConn) {
	rc := http.NewResponseController(w)
	rsStreamHeaders(w)
	for {
		select {
		case b := <-c.frames:
			if _, err := w.Write(b); err != nil {
				return
			}
			_ = rc.Flush()
		case abort := <-c.stop:
			if abort {
				panic(http.ErrAbortHandler) // closes the TCP connection mid-chunked-body
			}
			return
		case <-r.Context().Done():
			return
		case <-cp.quit:
			return
		}
	}
}

// rsStatus answers with a plain status code (500, 401, 200 with a wrong media type, ...).
func rsStatus(code int, contentType string) rsStep {
	return func(_ *fakeCP, w http.ResponseWriter, _ *http.Request, _ *rsConn) {
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte("nope"))
	}
}

// rsThen opens a stream, writes frames, then ends it normally or abruptly.
func rsThen(frames string, abort bool) rsStep {
	return func(_ *fakeCP, w http.ResponseWriter, _ *http.Request, _ *rsConn) {
		rsStreamHeaders(w)
		_, _ = w.Write([]byte(frames))
		_ = http.NewResponseController(w).Flush()
		if abort {
			panic(http.ErrAbortHandler)
		}
	}
}

// rsEndless writes a never-terminated line until the client hangs up.
func rsEndless(cp *fakeCP, w http.ResponseWriter, r *http.Request, c *rsConn) {
	rsStreamHeaders(w)
	chunk := bytes.Repeat([]byte("a"), 64<<10)
	rc := http.NewResponseController(w)
	for r.Context().Err() == nil && c.wrote.Load() < 512<<20 {
		select {
		case <-cp.quit:
			return
		default:
		}
		n, err := w.Write(chunk)
		c.wrote.Add(int64(n))
		if err != nil {
			return
		}
		_ = rc.Flush()
	}
}

// rsReport mirrors the status body of §6.6 (the source has its own copy).
type rsReport struct {
	Applied      int64  `json:"applied"`
	LastRejected int64  `json:"last_rejected"`
	Error        string `json:"error,omitempty"`
}

type rsStatusReq struct {
	rsReport
	method, path, auth, ctype string
}

// fakeCP is a fake control plane: GET /v1/config/stream and
// POST /v1/dataplanes/{node}/status.
type fakeCP struct {
	t     *testing.T
	srv   *httptest.Server
	token string
	quit  chan struct{}
	once  sync.Once

	mu          sync.Mutex
	steps       []rsStep
	conns       []*rsConn
	reports     []rsStatusReq
	statusErrs  []string
	hangElapsed []time.Duration

	statusCode atomic.Int32 // 0 = 204
	statusHang atomic.Bool
}

func newFakeCP(t *testing.T) *fakeCP {
	t.Helper()
	cp := &fakeCP{t: t, token: rsToken, quit: make(chan struct{})}
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+rsStreamPath, cp.handleStream)
	mux.HandleFunc("POST /v1/dataplanes/{node}/status", cp.handleStatus)
	cp.srv = httptest.NewServer(mux)
	t.Cleanup(func() {
		cp.close()
		cp.mu.Lock()
		defer cp.mu.Unlock()
		for _, e := range cp.statusErrs {
			t.Errorf("status endpoint protocol error: %s", e)
		}
	})
	return cp
}

func (cp *fakeCP) close() {
	cp.once.Do(func() { close(cp.quit) })
	cp.srv.Close()
}

// queue appends scripted responses for the next stream requests, in order.
// When the queue is empty every request gets a live rsHold stream.
func (cp *fakeCP) queue(steps ...rsStep) {
	cp.mu.Lock()
	cp.steps = append(cp.steps, steps...)
	cp.mu.Unlock()
}

func (cp *fakeCP) handleStream(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	c := &rsConn{
		frames: make(chan []byte, 4096),
		stop:   make(chan bool, 1),
		done:   make(chan struct{}),
	}
	_, c.req.hasSince = q["since"]
	c.req.method, c.req.path = r.Method, r.URL.Path
	c.req.node, c.req.since = q.Get("node"), q.Get("since")
	c.req.auth, c.req.accept = r.Header.Get("Authorization"), r.Header.Get("Accept")
	c.req.lastEventID = r.Header.Get("Last-Event-ID")

	cp.mu.Lock()
	authed := c.req.auth == "Bearer "+cp.token
	var step rsStep
	if authed && len(cp.steps) > 0 {
		step, cp.steps = cp.steps[0], cp.steps[1:]
	}
	cp.conns = append(cp.conns, c)
	cp.mu.Unlock()
	defer close(c.done)

	if !authed {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	if step == nil {
		step = rsHold
	}
	step(cp, w, r, c)
}

func (cp *fakeCP) handleStatus(w http.ResponseWriter, r *http.Request) {
	var rep rsStatusReq
	rep.method, rep.path = r.Method, r.URL.EscapedPath()
	rep.auth, rep.ctype = r.Header.Get("Authorization"), r.Header.Get("Content-Type")
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields() // pins the exact StatusReport shape
	if err := dec.Decode(&rep.rsReport); err != nil {
		cp.mu.Lock()
		cp.statusErrs = append(cp.statusErrs, "undecodable body: "+err.Error())
		cp.mu.Unlock()
	}
	if rep.auth != "Bearer "+cp.token {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	cp.mu.Lock()
	cp.reports = append(cp.reports, rep)
	cp.mu.Unlock()
	if cp.statusHang.Load() {
		start := time.Now()
		select {
		case <-r.Context().Done():
		case <-cp.quit:
		}
		cp.mu.Lock()
		cp.hangElapsed = append(cp.hangElapsed, time.Since(start))
		cp.mu.Unlock()
		return
	}
	if code := int(cp.statusCode.Load()); code != 0 {
		w.WriteHeader(code)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (cp *fakeCP) connCount() int {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return len(cp.conns)
}

// waitConn blocks until the n-th (1-based) stream request has arrived.
func (cp *fakeCP) waitConn(t *testing.T, n int) *rsConn {
	t.Helper()
	waitFor(t, 3*time.Second, fmt.Sprintf("stream connection #%d", n), func() bool { return cp.connCount() >= n })
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return cp.conns[n-1]
}

func (cp *fakeCP) allReports() []rsStatusReq {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	return append([]rsStatusReq(nil), cp.reports...)
}

func (cp *fakeCP) waitReport(t *testing.T, what string, pred func(rsStatusReq) bool) rsStatusReq {
	t.Helper()
	var got rsStatusReq
	waitFor(t, 3*time.Second, "status report: "+what, func() bool {
		for _, r := range cp.allReports() {
			if pred(r) {
				got = r
				return true
			}
		}
		return false
	})
	return got
}

// rsSleeper is the injected Sleep: it records every requested duration and
// returns quickly (or blocks until ctx is done when block is set).
type rsSleeper struct {
	mu      sync.Mutex
	calls   []time.Duration
	block   bool
	entered chan struct{}
}

func newSleeper() *rsSleeper { return &rsSleeper{entered: make(chan struct{}, 1024)} }

func (s *rsSleeper) Sleep(ctx context.Context, d time.Duration) error {
	s.mu.Lock()
	s.calls = append(s.calls, d)
	block := s.block
	s.mu.Unlock()
	select {
	case s.entered <- struct{}{}:
	default:
	}
	if block {
		<-ctx.Done()
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Millisecond): // small real delay so a refusing server is not a hot loop
		return nil
	}
}

func (s *rsSleeper) durations() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Duration(nil), s.calls...)
}

// rsFlaky is a RoundTripper that fails like a refused connection while down.
type rsFlaky struct {
	down atomic.Bool
	base *http.Transport
}

func newFlaky() *rsFlaky { return &rsFlaky{base: &http.Transport{DisableKeepAlives: true}} }

func (f *rsFlaky) RoundTrip(r *http.Request) (*http.Response, error) {
	if f.down.Load() {
		return nil, errors.New("dial tcp 127.0.0.1:1: connect: connection refused (simulated)")
	}
	return f.base.RoundTrip(r)
}

// rsLogBuf captures slog output safely across goroutines.
type rsLogBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *rsLogBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}
func (l *rsLogBuf) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }
func (l *rsLogBuf) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(l, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// rsHarness bundles a fake control plane, a sleeper and a RemoteSource.
type rsHarness struct {
	t      *testing.T
	cp     *fakeCP
	sl     *rsSleeper
	src    *RemoteSource
	opts   RemoteOptions
	ch     <-chan *Config
	cancel context.CancelFunc
}

// rsTryNew calls the constructor and converts a panic (the Phase 4 stub) into
// a value, so an unimplemented source fails each test cleanly instead of
// crashing the whole test binary.
func rsTryNew(opts RemoteOptions) (s *RemoteSource, err error, panicked any) {
	defer func() { panicked = recover() }()
	s, err = NewRemoteSource(opts)
	return
}

func rsNew(t *testing.T, opts RemoteOptions) *RemoteSource {
	t.Helper()
	s, err, p := rsTryNew(opts)
	if p != nil {
		t.Fatalf("NewRemoteSource panicked: %v", p)
	}
	if err != nil {
		t.Fatalf("NewRemoteSource: %v", err)
	}
	return s
}

// newRS builds a harness with fast, deterministic options (BackoffMin 100ms,
// BackoffMax 400ms, Rand always 0, recording Sleep); mods tweak them.
func newRS(t *testing.T, mods ...func(*RemoteOptions)) *rsHarness {
	t.Helper()
	cp := newFakeCP(t)
	sl := newSleeper()
	opts := RemoteOptions{
		BaseURL:       cp.srv.URL,
		Token:         cp.token,
		NodeID:        rsNode,
		BackoffMin:    100 * time.Millisecond,
		BackoffMax:    400 * time.Millisecond,
		IdleTimeout:   30 * time.Second,
		StatusTimeout: 2 * time.Second,
		Logger:        quietLogger(),
		Sleep:         sl.Sleep,
		Rand:          func() float64 { return 0 },
	}
	for _, m := range mods {
		m(&opts)
	}
	return &rsHarness{t: t, cp: cp, sl: sl, opts: opts, src: rsNew(t, opts)}
}

func (h *rsHarness) watch() <-chan *Config {
	h.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	h.t.Cleanup(cancel)
	ch, err := h.src.Watch(ctx)
	if err != nil {
		h.t.Fatalf("Watch: %v", err)
	}
	h.ch = ch
	return ch
}

func (h *rsHarness) expectVersion(want int64) *Config {
	h.t.Helper()
	c := recv(h.t, h.ch)
	if c.Version != want {
		h.t.Fatalf("delivered version %d, want %d", c.Version, want)
	}
	return c
}

func (h *rsHarness) quiet() { h.t.Helper(); expectQuiet(h.t, h.ch, 100*time.Millisecond) }

// drainClosed waits for the channel to close (ignoring any buffered value).
func rsDrainClosed(t *testing.T, ch <-chan *Config, within time.Duration) {
	t.Helper()
	deadline := time.After(within)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatalf("channel not closed within %v of cancel", within)
		}
	}
}

// ---- frames -----------------------------------------------------------

// rsCfgJSON is a minimal valid wire config at the given version.
func rsCfgJSON(v int64) string {
	return fmt.Sprintf(`{"version":%d,"routes":[{"name":"r","path_prefix":"/","upstream":"u"}],`+
		`"upstreams":[{"name":"u","backends":["http://127.0.0.1:9001"]}]}`, v)
}

// rsBigCfgJSON is a valid config with n routes (to make large events and
// LKG files whose torn writes would be visible).
func rsBigCfgJSON(v int64, n int) string {
	var b strings.Builder
	fmt.Fprintf(&b, `{"version":%d,"routes":[`, v)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"name":"r%d","path_prefix":"/p%d","upstream":"u"}`, i, i)
	}
	b.WriteString(`],"upstreams":[{"name":"u","backends":["http://127.0.0.1:9001"]}]}`)
	return b.String()
}

func rsFrameData(v int64, data string) string {
	return fmt.Sprintf("id: %d\nevent: config\ndata: %s\n\n", v, data)
}

func rsFrame(v int64) string { return rsFrameData(v, rsCfgJSON(v)) }

// ---- LKG helpers ------------------------------------------------------

// rsWriteLKG writes a valid LKG file holding version v (as the source would).
func rsWriteLKG(t *testing.T, path string, v int64) *Config {
	t.Helper()
	cfg, err := ParseJSON([]byte(rsCfgJSON(v)))
	if err != nil {
		t.Fatalf("ParseJSON: %v", err)
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return cfg
}

// rsReadLKG returns the version stored in the LKG file, if it is present and valid.
func rsReadLKG(path string) (int64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	c, err := ParseJSON(b)
	if err != nil {
		return 0, err
	}
	return c.Version, nil
}

func rsWaitLKGVersion(t *testing.T, path string, want int64) {
	t.Helper()
	waitFor(t, 3*time.Second, fmt.Sprintf("LKG file at version %d", want), func() bool {
		v, err := rsReadLKG(path)
		return err == nil && v == want
	})
}

func rsDirNames(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
	}
	return names
}
