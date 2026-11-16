package controlplane

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// ---- SSE test client ------------------------------------------------------------

type sseFrame struct {
	raw, id, event, data string
	comment              bool
}

type sseClient struct {
	resp   *http.Response
	frames chan sseFrame // closed when the body ends
	cancel context.CancelFunc
}

type streamEnv struct {
	*env
	ts *httptest.Server
}

func newStreamEnv(t *testing.T, mods ...func(*Options)) *streamEnv {
	t.Helper()
	e := newEnv(t, mods...)
	ts := httptest.NewServer(e.h)
	t.Cleanup(func() {
		e.hub.Close() // ends open streams so ts.Close cannot hang
		ts.CloseClientConnections()
		ts.Close()
	})
	return &streamEnv{env: e, ts: ts}
}

// dial opens the stream with the given token and returns the response (not
// yet read) and a cancel func.
func (se *streamEnv) dial(t *testing.T, token, query string, hdr ...string) (*http.Response, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, "GET", se.ts.URL+"/v1/config/stream"+query, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	tr := &http.Transport{DisableKeepAlives: true}
	t.Cleanup(tr.CloseIdleConnections)
	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		cancel()
		t.Fatalf("stream request: %v", err)
	}
	return resp, cancel
}

// open dials with the data-plane token, requires 200 and starts a frame reader.
func (se *streamEnv) open(t *testing.T, query string, hdr ...string) *sseClient {
	t.Helper()
	resp, cancel := se.dial(t, dpTok, query, hdr...)
	if resp.StatusCode != 200 {
		cancel()
		t.Fatalf("stream status %d, want 200", resp.StatusCode)
	}
	c := &sseClient{resp: resp, frames: make(chan sseFrame, 4096), cancel: cancel}
	go func() {
		defer close(c.frames)
		br := bufio.NewReader(resp.Body)
		var lines []string
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			if line == "\n" {
				if len(lines) > 0 {
					c.frames <- parseFrame(lines)
				}
				lines = nil
				continue
			}
			lines = append(lines, line)
		}
	}()
	t.Cleanup(c.close)
	return c
}

func parseFrame(lines []string) sseFrame {
	f := sseFrame{raw: strings.Join(lines, "") + "\n", comment: true}
	for _, l := range lines {
		l = strings.TrimSuffix(l, "\n")
		switch {
		case strings.HasPrefix(l, ":"):
		case strings.HasPrefix(l, "id: "):
			f.id, f.comment = strings.TrimPrefix(l, "id: "), false
		case strings.HasPrefix(l, "event: "):
			f.event, f.comment = strings.TrimPrefix(l, "event: "), false
		case strings.HasPrefix(l, "data: "):
			f.data, f.comment = strings.TrimPrefix(l, "data: "), false
		default:
			f.comment = false
		}
	}
	return f
}

func (c *sseClient) close() {
	c.cancel()
	_ = c.resp.Body.Close()
}

func (c *sseClient) next(t *testing.T, d time.Duration) sseFrame {
	t.Helper()
	select {
	case f, ok := <-c.frames:
		if !ok {
			t.Fatalf("stream ended, wanted a frame")
		}
		return f
	case <-time.After(d):
		t.Fatalf("no frame within %v", d)
	}
	return sseFrame{}
}

// nextConfig skips heartbeat comments and returns the next config frame.
func (c *sseClient) nextConfig(t *testing.T, d time.Duration) sseFrame {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		f := c.next(t, time.Until(deadline))
		if !f.comment {
			return f
		}
	}
}

// expectSilence fails if any frame (even a comment) or EOF arrives within d.
func (c *sseClient) expectSilence(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case f, ok := <-c.frames:
		if !ok {
			t.Fatalf("stream ended unexpectedly")
		}
		t.Fatalf("unexpected frame: %q", f.raw)
	case <-time.After(d):
	}
}

func (c *sseClient) expectEOF(t *testing.T, d time.Duration) {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case _, ok := <-c.frames:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatalf("stream still open after %v", d)
		}
	}
}

// parseConfigFrame validates framing and strictly decodes the data line.
func parseConfigFrame(t *testing.T, f sseFrame) *config.Config {
	t.Helper()
	if f.comment || f.event != EventConfig {
		t.Fatalf("not a config frame: %q", f.raw)
	}
	if strings.Contains(f.data, "\n") {
		t.Fatalf("data spans lines: %q", f.raw)
	}
	var c config.Config
	if err := decodeStrict([]byte(f.data), &c); err != nil {
		t.Fatalf("data does not decode strictly into config.Config: %v\n%s", err, f.data)
	}
	if f.id != strconv.FormatInt(c.Version, 10) {
		t.Fatalf("id %q does not match data version %d", f.id, c.Version)
	}
	want := fmt.Sprintf("id: %d\nevent: config\ndata: %s\n\n", c.Version, f.data)
	if f.raw != want {
		t.Fatalf("frame not byte-exact:\n got %q\nwant %q", f.raw, want)
	}
	return &c
}

const wait = 3 * time.Second
const quiet = 150 * time.Millisecond

// ---- tests ------------------------------------------------------------------------

func TestStream_Headers(t *testing.T) {
	defer failOnPanic(t)
	se := newStreamEnv(t)
	resp, cancel := se.dial(t, dpTok, "?node=gw-1")
	defer cancel()
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Errorf("Content-Type = %q", ct)
	}
	if got := resp.Header.Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", got)
	}
	if got := resp.Header.Get("X-Accel-Buffering"); got != "no" {
		t.Errorf("X-Accel-Buffering = %q, want no", got)
	}
}

func TestStream_BadParameters(t *testing.T) {
	defer failOnPanic(t)
	cases := []struct {
		name  string
		query string
		hdr   []string
	}{
		{"node missing", "", nil},
		{"node missing, since present", "?since=0", nil},
		{"node empty", "?node=", nil},
		{"node starts with dash", "?node=-bad", nil},
		{"node starts with dot", "?node=.bad", nil},
		{"node with space", "?node=has%20space", nil},
		{"node with slash", "?node=a%2Fb", nil},
		{"node with unicode", "?node=n%C3%B6de", nil},
		{"node too long", "?node=" + strings.Repeat("a", MaxNodeIDBytes+1), nil},
		{"since not a number", "?node=gw&since=abc", nil},
		{"since negative", "?node=gw&since=-1", nil},
		{"since fractional", "?node=gw&since=1.5", nil},
		{"since overflow", "?node=gw&since=99999999999999999999", nil},
		{"Last-Event-ID not a number", "?node=gw", []string{"Last-Event-ID", "abc"}},
		{"Last-Event-ID negative", "?node=gw", []string{"Last-Event-ID", "-2"}},
		{"bad since even though Last-Event-ID is fine", "?node=gw&since=zzz", []string{"Last-Event-ID", "1"}},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			e := newEnv(t)
			rec := do(t, e.h, "GET", "/v1/config/stream"+tc.query, dpTok, "", tc.hdr...)
			if rec.Code != 400 {
				t.Fatalf("status %d, want 400", rec.Code)
			}
			errBody(t, rec)
			if len(e.nodes.List()) != 0 {
				t.Fatalf("rejected request registered a node")
			}
		})
	}
}

func TestStream_ValidNodeIDs(t *testing.T) {
	defer failOnPanic(t)
	for _, node := range []string{"gw-1", "a", "9", "pod.name_x-1", "A", strings.Repeat("a", MaxNodeIDBytes)} {
		run(t, node, func(t *testing.T) {
			se := newStreamEnv(t)
			c := se.open(t, "?node="+node)
			_ = c
		})
	}
}

func TestStream_InitialEvent(t *testing.T) {
	defer failOnPanic(t)
	cases := []struct {
		name  string
		head  int
		query string
		hdr   []string
		want  int64 // 0 = nothing sent
	}{
		{"empty store, since 0: nothing (version 0 is never streamed)", 0, "?node=gw&since=0", nil, 0},
		{"empty store, since omitted", 0, "?node=gw", nil, 0},
		{"head 3, since 1: only the head, not 2", 3, "?node=gw&since=1", nil, 3},
		{"head 3, since 0", 3, "?node=gw&since=0", nil, 3},
		{"head 3, since omitted", 3, "?node=gw", nil, 3},
		{"head 3, since 2", 3, "?node=gw&since=2", nil, 3},
		{"head 3, since == head", 3, "?node=gw&since=3", nil, 0},
		{"head 3, since > head", 3, "?node=gw&since=5", nil, 0},
		{"Last-Event-ID wins: head already applied", 3, "?node=gw&since=0", []string{"Last-Event-ID", "3"}, 0},
		{"Last-Event-ID wins: older than since", 3, "?node=gw&since=3", []string{"Last-Event-ID", "1"}, 3},
		{"Last-Event-ID alone", 3, "?node=gw", []string{"Last-Event-ID", "2"}, 3},
		{"empty Last-Event-ID is ignored", 3, "?node=gw&since=3", []string{"Last-Event-ID", ""}, 0},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			se := newStreamEnv(t)
			se.seedStore(t, tc.head) // committed but never published to the hub
			c := se.open(t, tc.query, tc.hdr...)
			if tc.want == 0 {
				c.expectSilence(t, quiet)
				return
			}
			f := c.next(t, wait)
			cfg := parseConfigFrame(t, f)
			if cfg.Version != tc.want || len(cfg.Upstreams) != int(tc.want) {
				t.Fatalf("got v%d with %d upstreams, want v%d", cfg.Version, len(cfg.Upstreams), tc.want)
			}
			// Byte-exact against what the store holds (json.Marshal of the Config).
			head, _ := se.store.Head(testCtx(t))
			b, _ := json.Marshal(head)
			if f.data != string(b) {
				t.Fatalf("data differs from json.Marshal(head):\n got %s\nwant %s", f.data, b)
			}
			c.expectSilence(t, quiet) // never replays skipped versions
		})
	}
}

func TestStream_NothingUntilNewerThanSince(t *testing.T) {
	defer failOnPanic(t)
	run(t, "since == head: next write arrives", func(t *testing.T) {
		se := newStreamEnv(t)
		se.seedStore(t, 3)
		c := se.open(t, "?node=gw&since=3")
		c.expectSilence(t, quiet)
		if v := se.putUpstream(t, "new"); v != 4 {
			t.Fatalf("version %d", v)
		}
		if cfg := parseConfigFrame(t, c.next(t, wait)); cfg.Version != 4 {
			t.Fatalf("got v%d", cfg.Version)
		}
	})
	run(t, "since > head: versions <= since are not sent (database wiped, ADR 0005)", func(t *testing.T) {
		se := newStreamEnv(t)
		se.seedStore(t, 3)
		c := se.open(t, "?node=gw&since=5")
		c.expectSilence(t, quiet)
		se.putUpstream(t, "w4") // v4
		se.putUpstream(t, "w5") // v5
		c.expectSilence(t, quiet)
		if v := se.putUpstream(t, "w6"); v != 6 {
			t.Fatalf("version %d", v)
		}
		if cfg := parseConfigFrame(t, c.next(t, wait)); cfg.Version != 6 {
			t.Fatalf("got v%d, want v6", cfg.Version)
		}
		// since is what the node reports as applied until it says otherwise.
		waitFor(t, wait, "node listed", func() bool { _, ok := findNode(se.listNodes(t), "gw"); return ok })
		if n, _ := findNode(se.listNodes(t), "gw"); n.AppliedVersion != 5 {
			t.Fatalf("applied_version = %d, want 5 (from since)", n.AppliedVersion)
		}
	})
}

func TestStream_PushAfterConnect(t *testing.T) {
	defer failOnPanic(t)
	se := newStreamEnv(t)
	c := se.open(t, "?node=gw&since=0")
	c.expectSilence(t, quiet)
	for v := 1; v <= 3; v++ {
		start := time.Now()
		se.putUpstream(t, fmt.Sprintf("p%d", v))
		cfg := parseConfigFrame(t, c.next(t, wait))
		if cfg.Version != int64(v) || len(cfg.Upstreams) != v {
			t.Fatalf("got v%d with %d upstreams, want v%d", cfg.Version, len(cfg.Upstreams), v)
		}
		if d := time.Since(start); d > 2*time.Second {
			t.Fatalf("push took %v", d)
		}
	}
}

func TestStream_BurstEndsOnLatestStrictlyIncreasing(t *testing.T) {
	defer failOnPanic(t)
	cases := []struct {
		name  string
		write func(se *streamEnv) int64 // performs the writes, returns final version
	}{
		{"20 sequential writes", func(se *streamEnv) int64 {
			for i := 0; i < 20; i++ {
				se.putUpstream(t, fmt.Sprintf("s%d", i))
			}
			return 20
		}},
		{"8 writers x 5 concurrent writes", func(se *streamEnv) int64 {
			var wg sync.WaitGroup
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; i < 5; i++ {
						name := fmt.Sprintf("c%d-%d", g, i)
						if rec := se.admin(t, "PUT", "/v1/upstreams/"+name, upstreamJSON(name)); rec.Code != 201 {
							t.Errorf("PUT %s: %d %s", name, rec.Code, rec.Body)
						}
					}
				}(g)
			}
			wg.Wait()
			return 40
		}},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			se := newStreamEnv(t)
			c := se.open(t, "?node=gw&since=0")
			final := tc.write(se)
			var last int64
			deadline := time.Now().Add(5 * time.Second)
			for last < final {
				cfg := parseConfigFrame(t, c.nextConfig(t, time.Until(deadline)))
				if cfg.Version <= last {
					t.Fatalf("version %d after %d: not strictly increasing", cfg.Version, last)
				}
				if len(cfg.Upstreams) != int(cfg.Version) {
					t.Fatalf("v%d has %d upstreams (frame mixes versions)", cfg.Version, len(cfg.Upstreams))
				}
				last = cfg.Version
			}
			c.expectSilence(t, quiet)
		})
	}
}

func TestStream_TwoClientsBothReceive(t *testing.T) {
	defer failOnPanic(t)
	se := newStreamEnv(t)
	a := se.open(t, "?node=gw-a&since=0")
	b := se.open(t, "?node=gw-b&since=0")
	se.putUpstream(t, "x")
	for name, c := range map[string]*sseClient{"a": a, "b": b} {
		if cfg := parseConfigFrame(t, c.next(t, wait)); cfg.Version != 1 {
			t.Fatalf("%s got v%d", name, cfg.Version)
		}
	}
	waitFor(t, wait, "both nodes listed", func() bool { return len(se.listNodes(t)) == 2 })
}

func TestStream_Heartbeat(t *testing.T) {
	defer failOnPanic(t)
	run(t, "idle stream sends only heartbeat comments", func(t *testing.T) {
		se := newStreamEnv(t, func(o *Options) { o.Heartbeat = 10 * time.Millisecond })
		c := se.open(t, "?node=gw&since=0") // empty store: no config event
		for i := 0; i < 3; i++ {
			f := c.next(t, wait)
			if !f.comment || f.raw != HeartbeatComment {
				t.Fatalf("frame %q, want exactly %q", f.raw, HeartbeatComment)
			}
		}
		se.putUpstream(t, "x")
		if cfg := parseConfigFrame(t, c.nextConfig(t, wait)); cfg.Version != 1 {
			t.Fatalf("got v%d", cfg.Version)
		}
	})
	run(t, "pending head is sent before the first heartbeat", func(t *testing.T) {
		se := newStreamEnv(t, func(o *Options) { o.Heartbeat = 300 * time.Millisecond })
		se.seedStore(t, 2)
		c := se.open(t, "?node=gw&since=0")
		parseConfigFrame(t, c.next(t, wait)) // fails if a heartbeat came first
	})
	run(t, "heartbeats keep the node's last_seen fresh", func(t *testing.T) {
		se := newStreamEnv(t, func(o *Options) { o.Heartbeat = 10 * time.Millisecond })
		se.open(t, "?node=gw&since=0")
		var first time.Time
		waitFor(t, wait, "node listed", func() bool {
			n, ok := findNode(se.listNodes(t), "gw")
			first = n.LastSeen
			return ok
		})
		waitFor(t, wait, "last_seen to advance", func() bool {
			n, _ := findNode(se.listNodes(t), "gw")
			return n.LastSeen.After(first)
		})
	})
}

func TestStream_NodeRegistry(t *testing.T) {
	defer failOnPanic(t)
	se := newStreamEnv(t)
	se.seedStore(t, 2)
	c := se.open(t, "?node=gw-7&since=1")
	parseConfigFrame(t, c.next(t, wait))

	var n NodeStatus
	waitFor(t, wait, "node connected with offered version", func() bool {
		var ok bool
		n, ok = findNode(se.listNodes(t), "gw-7")
		return ok && n.Connected && n.LastOfferedVersion == 2
	})
	if n.ConnectedAt == nil || n.LastSeen.IsZero() || n.AppliedVersion != 1 {
		t.Fatalf("%+v, want connected_at set and applied_version 1 (from since)", n)
	}
	// Pushed versions are recorded as offered.
	se.putUpstream(t, "x")
	parseConfigFrame(t, c.next(t, wait))
	waitFor(t, wait, "offered 3", func() bool {
		n, _ = findNode(se.listNodes(t), "gw-7")
		return n.LastOfferedVersion == 3
	})

	c.close()
	waitFor(t, wait, "node disconnected", func() bool {
		n, _ = findNode(se.listNodes(t), "gw-7")
		return !n.Connected
	})
	if n.LastOfferedVersion != 3 || n.ConnectedAt == nil {
		t.Fatalf("after disconnect: %+v (still listed with its history)", n)
	}
	// A reconnect makes it connected again.
	se.open(t, "?node=gw-7&since=3")
	waitFor(t, wait, "node reconnected", func() bool {
		n, _ = findNode(se.listNodes(t), "gw-7")
		return n.Connected && n.AppliedVersion == 3
	})
}

func TestDataPlanes_ListEmptyIsArray(t *testing.T) {
	defer failOnPanic(t)
	e := newEnv(t)
	rec := e.admin(t, "GET", "/v1/dataplanes", "")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"dataplanes":[]`) {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	if rec.Header().Get("ETag") != "" {
		t.Fatalf("ETag %q on /v1/dataplanes, want none", rec.Header().Get("ETag"))
	}
}

func TestStatusReport(t *testing.T) {
	defer failOnPanic(t)
	maxErr := strings.Repeat("e", MaxReportErrorBytes)
	cases := []struct {
		name       string
		node       string
		body       string
		wantStatus int
	}{
		{"full report", "gw-1", `{"applied":2,"last_rejected":3,"error":"build: boom"}`, 204},
		{"applied only", "gw-1", `{"applied":2}`, 204},
		{"zeros", "gw-1", `{"applied":0,"last_rejected":0}`, 204},
		{"empty object", "gw-1", `{}`, 204},
		{"error at the limit", "gw-1", `{"error":"` + maxErr + `"}`, 204},
		{"error over the limit is rejected", "gw-1", `{"error":"` + maxErr + `x"}`, 400},
		{"malformed", "gw-1", `{"applied":`, 400},
		{"empty body", "gw-1", ``, 400},
		{"unknown field", "gw-1", `{"applied":1,"bogus":2}`, 400},
		{"wrong type", "gw-1", `{"applied":"1"}`, 400},
		{"negative applied", "gw-1", `{"applied":-1}`, 400},
		{"negative last_rejected", "gw-1", `{"last_rejected":-1}`, 400},
		{"trailing garbage", "gw-1", `{} {}`, 400},
		{"invalid node id", "-bad", `{"applied":1}`, 400},
		{"too long node id", strings.Repeat("a", MaxNodeIDBytes+1), `{"applied":1}`, 400},
		{"body over 1 MiB", "gw-1", oversizeBody(`{}`), 413},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			e := newEnv(t)
			rec := do(t, e.h, "POST", "/v1/dataplanes/"+tc.node+"/status", dpTok, tc.body)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status %d, want %d; %s", rec.Code, tc.wantStatus, rec.Body)
			}
			if tc.wantStatus == 204 {
				if rec.Body.Len() != 0 {
					t.Fatalf("204 with body %q", rec.Body)
				}
				return
			}
			errBody(t, rec)
			if len(e.nodes.List()) != 0 {
				t.Fatalf("rejected report registered a node: %+v", e.nodes.List())
			}
		})
	}

	run(t, "listed with applied/rejected/error, not connected", func(t *testing.T) {
		e := newEnv(t)
		rec := do(t, e.h, "POST", "/v1/dataplanes/gw-1/status", dpTok, `{"applied":2,"last_rejected":3,"error":"build: boom"}`)
		if rec.Code != 204 {
			t.Fatalf("status %d", rec.Code)
		}
		n, ok := findNode(e.listNodes(t), "gw-1")
		if !ok || n.Connected || n.ConnectedAt != nil || n.AppliedVersion != 2 || n.RejectedVersion != 3 || n.LastError != "build: boom" || n.LastSeen.IsZero() {
			t.Fatalf("%+v", n)
		}
	})
	run(t, "latest report wins, zeros clear", func(t *testing.T) {
		e := newEnv(t)
		do(t, e.h, "POST", "/v1/dataplanes/gw-1/status", dpTok, `{"applied":2,"last_rejected":3,"error":"x"}`)
		do(t, e.h, "POST", "/v1/dataplanes/gw-1/status", dpTok, `{"applied":4}`)
		n, _ := findNode(e.listNodes(t), "gw-1")
		if n.AppliedVersion != 4 || n.RejectedVersion != 0 || n.LastError != "" {
			t.Fatalf("%+v", n)
		}
	})
	run(t, "admin token on the status endpoint is 401 and records nothing", func(t *testing.T) {
		e := newEnv(t)
		if rec := do(t, e.h, "POST", "/v1/dataplanes/gw-1/status", adminTok, `{"applied":1}`); rec.Code != 401 {
			t.Fatalf("status %d", rec.Code)
		}
		if len(e.nodes.List()) != 0 {
			t.Fatal("registered a node")
		}
	})
	run(t, "data-plane token cannot list nodes", func(t *testing.T) {
		e := newEnv(t)
		if rec := do(t, e.h, "GET", "/v1/dataplanes", dpTok, ""); rec.Code != 401 {
			t.Fatalf("status %d", rec.Code)
		}
	})
	run(t, "report for a streaming node updates the same entry", func(t *testing.T) {
		se := newStreamEnv(t)
		se.seedStore(t, 1)
		c := se.open(t, "?node=gw-1&since=0")
		parseConfigFrame(t, c.next(t, wait))
		do(t, se.h, "POST", "/v1/dataplanes/gw-1/status", dpTok, `{"applied":1}`)
		waitFor(t, wait, "applied 1 and connected", func() bool {
			n, _ := findNode(se.listNodes(t), "gw-1")
			return n.Connected && n.AppliedVersion == 1 && n.LastOfferedVersion == 1
		})
	})
}

func TestStream_AuthOnRealConnection(t *testing.T) {
	defer failOnPanic(t)
	se := newStreamEnv(t)
	for name, tok := range map[string]string{"none": "", "admin": adminTok, "wrong": strings.Repeat("q", 44)} {
		resp, cancel := se.dial(t, tok, "?node=gw")
		_ = resp.Body.Close()
		cancel()
		if resp.StatusCode != 401 {
			t.Errorf("%s token: status %d, want 401", name, resp.StatusCode)
		}
	}
	// And the data-plane token works (open requires 200).
	se.open(t, "?node=gw")
}

func TestStream_HeadFailureIs500BeforeAnyStreamBytes(t *testing.T) {
	defer failOnPanic(t)
	fs := &faultyStore{Store: NewMemStore(), err: fmt.Errorf("pq: connection refused to 10.0.0.5")}
	e := newEnvWithStore(t, fs)
	rec := do(t, e.h, "GET", "/v1/config/stream?node=gw", dpTok, "")
	if rec.Code != 500 {
		t.Fatalf("status %d, want 500", rec.Code)
	}
	if got := strings.TrimSpace(rec.Body.String()); got != `{"error":"internal error"}` {
		t.Fatalf("body %s", got)
	}
	if ct := rec.Header().Get("Content-Type"); strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type %q on an error", ct)
	}
	if len(e.nodes.List()) != 0 {
		t.Fatal("a failed stream registered a node")
	}
}

func TestStream_HubCloseEndsStreams(t *testing.T) {
	defer failOnPanic(t)
	se := newStreamEnv(t)
	se.seedStore(t, 1)
	var cs []*sseClient
	for i := 0; i < 3; i++ {
		c := se.open(t, fmt.Sprintf("?node=gw-%d&since=1", i))
		cs = append(cs, c)
	}
	se.hub.Close() // what http.Server.Shutdown triggers via RegisterOnShutdown
	for i, c := range cs {
		func() {
			defer func() {
				if t.Failed() {
					t.Logf("client %d", i)
				}
			}()
			c.expectEOF(t, wait)
		}()
	}
	waitFor(t, wait, "all nodes disconnected", func() bool {
		for _, n := range se.listNodes(t) {
			if n.Connected {
				return false
			}
		}
		return true
	})
	// A stream opened after Close ends right away too.
	late := se.open(t, "?node=late&since=1")
	late.expectEOF(t, wait)
}

func TestStream_ClientDisconnectReleasesResources(t *testing.T) {
	defer failOnPanic(t)
	se := newStreamEnv(t)
	se.seedStore(t, 1)
	// Warm up lazily-started goroutines (server accept loop, client plumbing).
	se.open(t, "?node=warm&since=1").close()
	time.Sleep(100 * time.Millisecond)
	base := runtime.NumGoroutine()

	const n = 20
	var cs []*sseClient
	for i := 0; i < n; i++ {
		cs = append(cs, se.open(t, fmt.Sprintf("?node=leak-%d&since=1", i)))
	}
	waitFor(t, wait, "all connected", func() bool {
		c := 0
		for _, ns := range se.listNodes(t) {
			if ns.Connected {
				c++
			}
		}
		return c >= n
	})
	if g := runtime.NumGoroutine(); g < base+n {
		t.Logf("goroutines while connected: %d (base %d)", g, base)
	}
	for _, c := range cs {
		c.close()
	}
	waitFor(t, 5*time.Second, "node registry to show all disconnected", func() bool {
		for _, ns := range se.listNodes(t) {
			if ns.Connected {
				return false
			}
		}
		return true
	})
	waitFor(t, 5*time.Second, "goroutines to return to baseline", func() bool {
		return runtime.NumGoroutine() <= base+3
	})
	// Publishing after everyone left must be harmless and cheap.
	for i := 0; i < 5; i++ {
		se.putUpstream(t, fmt.Sprintf("after%d", i))
	}
}
