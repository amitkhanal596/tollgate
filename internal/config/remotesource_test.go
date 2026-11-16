package config

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

// ---- 1. constructor validation and defaults -----------------------------

func TestRemoteOptionsValidation(t *testing.T) {
	good := func() RemoteOptions {
		return RemoteOptions{BaseURL: "http://cp.example:9000", Token: "tok", NodeID: "gw-1"}
	}
	tests := []struct {
		name    string
		mutate  func(o *RemoteOptions)
		wantErr bool
	}{
		{"minimal valid", func(o *RemoteOptions) {}, false},
		{"base url with root path", func(o *RemoteOptions) { o.BaseURL = "http://cp.example:9000/" }, false},
		{"https", func(o *RemoteOptions) { o.BaseURL = "https://cp.example" }, false},
		{"ipv6 host", func(o *RemoteOptions) { o.BaseURL = "http://[::1]:9000" }, false},
		{"node id digit first", func(o *RemoteOptions) { o.NodeID = "7" }, false},
		{"node id with dot underscore dash", func(o *RemoteOptions) { o.NodeID = "a.b_c-d" }, false},
		{"node id 128 bytes", func(o *RemoteOptions) { o.NodeID = strings.Repeat("a", 128) }, false},
		{"negative durations and sizes take defaults", func(o *RemoteOptions) {
			o.BackoffMin, o.BackoffMax, o.IdleTimeout, o.StatusTimeout, o.MaxEventBytes = -1, -1, -1, -1, -5
		}, false},
		{"backoff min equals max", func(o *RemoteOptions) { o.BackoffMin, o.BackoffMax = time.Second, time.Second }, false},
		{"backoff min below default max", func(o *RemoteOptions) { o.BackoffMin = time.Second }, false},
		{"http client without timeout", func(o *RemoteOptions) { o.HTTPClient = &http.Client{} }, false},

		{"missing base url", func(o *RemoteOptions) { o.BaseURL = "" }, true},
		{"missing token", func(o *RemoteOptions) { o.Token = "" }, true},
		{"missing node id", func(o *RemoteOptions) { o.NodeID = "" }, true},
		{"unparseable url", func(o *RemoteOptions) { o.BaseURL = "://bad" }, true},
		{"not a url", func(o *RemoteOptions) { o.BaseURL = "not a url" }, true},
		{"no scheme", func(o *RemoteOptions) { o.BaseURL = "cp.example:9000" }, true},
		{"ftp scheme", func(o *RemoteOptions) { o.BaseURL = "ftp://cp.example" }, true},
		{"no host", func(o *RemoteOptions) { o.BaseURL = "http://" }, true},
		{"path other than root", func(o *RemoteOptions) { o.BaseURL = "http://cp.example/api" }, true},
		{"path with trailing slash", func(o *RemoteOptions) { o.BaseURL = "http://cp.example/api/" }, true},
		{"query", func(o *RemoteOptions) { o.BaseURL = "http://cp.example?x=1" }, true},
		{"fragment", func(o *RemoteOptions) { o.BaseURL = "http://cp.example#f" }, true},
		{"node id leading dash", func(o *RemoteOptions) { o.NodeID = "-a" }, true},
		{"node id leading dot", func(o *RemoteOptions) { o.NodeID = ".a" }, true},
		{"node id slash", func(o *RemoteOptions) { o.NodeID = "a/b" }, true},
		{"node id space", func(o *RemoteOptions) { o.NodeID = "a b" }, true},
		{"node id question mark", func(o *RemoteOptions) { o.NodeID = "a?b" }, true},
		{"node id non-ascii", func(o *RemoteOptions) { o.NodeID = "é" }, true},
		{"node id 129 bytes", func(o *RemoteOptions) { o.NodeID = strings.Repeat("a", 129) }, true},
		{"http client with timeout kills the stream", func(o *RemoteOptions) {
			o.HTTPClient = &http.Client{Timeout: time.Second}
		}, true},
		{"backoff min above defaulted max (30s)", func(o *RemoteOptions) { o.BackoffMin = time.Minute }, true},
		{"backoff min above max", func(o *RemoteOptions) { o.BackoffMin, o.BackoffMax = 5*time.Second, time.Second }, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o := good()
			tc.mutate(&o)
			s, p, err := rsTryNew(o)
			if p != nil {
				t.Fatalf("NewRemoteSource panicked: %v", p)
			}
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				if s != nil {
					t.Error("a failed constructor must return a nil source")
				}
				return
			}
			if err != nil || s == nil {
				t.Fatalf("expected success, got %v", err)
			}
		})
	}
}

// Defaults are pinned through behaviour (the fields are unexported): with
// zero BackoffMin/BackoffMax the schedule is 500ms doubling to 30s, equal
// jitter with Rand()==0 gives cap/2.
func TestRemoteDefaultBackoffSchedule(t *testing.T) {
	h := newRS(t, func(o *RemoteOptions) { o.BackoffMin, o.BackoffMax = 0, 0 })
	h.cp.close() // connection refused on every attempt
	h.watch()
	want := []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second,
		4 * time.Second, 8 * time.Second, 15 * time.Second, 15 * time.Second}
	waitFor(t, 5*time.Second, "8 backoff sleeps", func() bool { return len(h.sl.durations()) >= len(want) })
	if got := h.sl.durations()[:len(want)]; !reflect.DeepEqual(got, want) {
		t.Fatalf("default sleeps = %v, want %v", got, want)
	}
}

// Nil Sleep and Rand use real timers and math/rand: the first retry waits in
// [BackoffMin/2, BackoffMin) = [250ms, 500ms).
func TestRemoteDefaultSleepAndRand(t *testing.T) {
	h := newRS(t, func(o *RemoteOptions) { o.BackoffMin, o.BackoffMax, o.Sleep, o.Rand = 0, 0, nil, nil })
	h.cp.queue(rsStatus(500, ""))
	h.watch()
	c1 := h.cp.waitConn(t, 1)
	start := time.Now()
	h.cp.waitConn(t, 2)
	if d := time.Since(start); d < 200*time.Millisecond || d > 2*time.Second {
		t.Fatalf("default first retry after %v, want within [250ms, 500ms) (+ scheduling slack)", d)
	}
	_ = c1
}

// Canceling during the default (real) backoff wait returns promptly.
func TestRemoteCancelDuringDefaultSleep(t *testing.T) {
	h := newRS(t, func(o *RemoteOptions) { o.BackoffMin, o.BackoffMax, o.Sleep = 10*time.Second, 30*time.Second, nil })
	h.cp.queue(rsStatus(500, ""))
	ch := h.watch()
	h.cp.waitConn(t, 1)
	h.cancel()
	rsDrainClosed(t, ch, time.Second)
}

// MaxEventBytes defaults to 8 MiB: exactly that much data is fine (it is
// just not a valid config), one byte more is a protocol error.
func TestRemoteDefaultMaxEventBytes(t *testing.T) {
	const limit = 8 << 20
	tests := []struct {
		name      string
		dataBytes int
		wantDrop  bool
	}{
		{"exactly 8 MiB stays connected", limit, false},
		{"8 MiB plus one drops the connection", limit + 1, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newRS(t, func(o *RemoteOptions) { o.MaxEventBytes = 0 })
			h.watch()
			c1 := h.cp.waitConn(t, 1)
			c1.send("event: config\ndata: " + strings.Repeat("x", tc.dataBytes) + "\n\n")
			if tc.wantDrop {
				h.cp.waitConn(t, 2).send(rsFrame(1))
				c1.waitDone(t, "oversized event to close the connection")
			} else {
				c1.send(rsFrame(1))
			}
			h.expectVersion(1)
			if !tc.wantDrop && h.cp.connCount() != 1 {
				t.Fatalf("connection was dropped (%d conns)", h.cp.connCount())
			}
		})
	}
}

// ---- 2. ConfigSource rules ---------------------------------------------

func TestRemoteSecondWatch(t *testing.T) {
	h := newRS(t)
	h.watch()
	ch, err := h.src.Watch(context.Background())
	if !errors.Is(err, ErrAlreadyWatching) || ch != nil {
		t.Fatalf("second Watch = (%v, %v), want (nil, ErrAlreadyWatching)", ch, err)
	}
}

// Rule 2/3: an unreachable control plane is transient; nothing is delivered
// until it is reachable, then the first value is the stream's head.
func TestRemoteUnreachableIsTransient(t *testing.T) {
	fl := newFlaky()
	fl.down.Store(true)
	h := newRS(t, func(o *RemoteOptions) { o.HTTPClient = &http.Client{Transport: fl} })
	h.watch() // must not return an error (newRS's watch fatals otherwise)
	waitFor(t, 3*time.Second, "retries while down", func() bool { return len(h.sl.durations()) >= 2 })
	h.quiet()
	if h.cp.connCount() != 0 {
		t.Fatal("no request should have reached the server while down")
	}
	fl.down.Store(false)
	c1 := h.cp.waitConn(t, 1)
	c1.send(rsFrame(3)) // head
	h.expectVersion(3)
}

func TestRemoteLatestWins(t *testing.T) {
	h := newRS(t)
	ch := h.watch()
	if cap(ch) != 1 {
		t.Fatalf("channel capacity %d, want 1", cap(ch))
	}
	c1 := h.cp.waitConn(t, 1)
	for v := int64(1); v <= 5; v++ {
		c1.send(rsFrame(v))
	}
	time.Sleep(300 * time.Millisecond) // consumer idle: let the source process all five
	h.expectVersion(5)                 // intermediates were replaced, the latest is never lost
	h.quiet()
	c1.send(rsFrame(6))
	h.expectVersion(6)
}

// Rule 6: a delivered *Config is never touched again by the source.
func TestRemoteDeliveredConfigNotMutated(t *testing.T) {
	dir := t.TempDir()
	h := newRS(t, func(o *RemoteOptions) { o.LKGPath = dir + "/lkg.json" })
	h.watch()
	c1 := h.cp.waitConn(t, 1)
	c1.send(rsFrame(1))
	cfg := h.expectVersion(1)
	snap, _ := json.Marshal(cfg)

	stop := make(chan struct{})
	finished := make(chan struct{})
	go func() { // concurrent reads let -race catch any source-side write
		defer close(finished)
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = json.Marshal(cfg)
			}
		}
	}()
	h.src.Applied(1) // the worker marshals the same pointer for the LKG file
	for v := int64(2); v <= 4; v++ {
		c1.send(rsFrame(v))
	}
	c1.abort()
	h.cp.waitConn(t, 2).send(rsFrame(1)) // stale resend must not touch it either
	rsWaitLKGVersion(t, dir+"/lkg.json", 1)
	close(stop)
	<-finished
	if after, _ := json.Marshal(cfg); string(after) != string(snap) {
		t.Fatalf("delivered config was mutated:\nbefore %s\nafter  %s", snap, after)
	}
}

// Rule 7: closed only after cancel; goroutines gone; the response body is
// closed (the server handler sees the request end).
func TestRemoteShutdown(t *testing.T) {
	runtime.GC()
	before := runtime.NumGoroutine()
	tr := newFlaky()
	cp := newFakeCP(t)
	src := rsNew(t, RemoteOptions{BaseURL: cp.srv.URL, Token: cp.token, NodeID: rsNode, Logger: quietLogger(),
		HTTPClient: &http.Client{Transport: tr}, BackoffMin: 10 * time.Millisecond, BackoffMax: 20 * time.Millisecond,
		LKGPath: t.TempDir() + "/lkg.json"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := src.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	c1 := cp.waitConn(t, 1)
	c1.send(rsFrame(1))
	recv(t, ch)
	src.Applied(1)
	expectQuiet(t, ch, 100*time.Millisecond) // open while ctx is live
	cancel()
	rsDrainClosed(t, ch, 2*time.Second)
	c1.waitDone(t, "request context to end after cancel (body closed)")
	// Calls after shutdown must not panic or start goroutines.
	src.Applied(1)
	src.Rejected(2, context.Canceled)
	_ = src.Connected()
	cp.close()
	tr.base.CloseIdleConnections()
	waitFor(t, 3*time.Second, "goroutines to exit", func() bool {
		runtime.GC()
		return runtime.NumGoroutine() <= before
	})
}

func TestRemoteCancelBeforeConsumerReads(t *testing.T) {
	h := newRS(t)
	ch := h.watch()
	h.cp.waitConn(t, 1).send(rsFrame(1))
	time.Sleep(100 * time.Millisecond)
	h.cancel()
	rsDrainClosed(t, ch, 2*time.Second) // at most one buffered value, then closed
}

// ---- 3. version filtering ------------------------------------------------

func TestRemoteVersionFiltering(t *testing.T) {
	steps := []struct {
		send    int64
		deliver bool
	}{
		{5, true}, {5, false}, {4, false}, {0, false}, {6, true}, {100, true}, {99, false}, {100, false}, {101, true},
	}
	h := newRS(t)
	h.watch()
	c1 := h.cp.waitConn(t, 1)
	var last int64
	for _, s := range steps {
		c1.send(rsFrame(s.send))
		if s.deliver {
			c := h.expectVersion(s.send)
			if c.Version <= last {
				t.Fatalf("not strictly increasing: %d after %d", c.Version, last)
			}
			last = c.Version
		} else {
			h.quiet()
		}
	}
	if h.cp.connCount() != 1 {
		t.Errorf("dropping stale events must not reconnect (%d conns)", h.cp.connCount())
	}
}

// A control-plane version is a deliberate commit: empty configs are delivered
// (FileSource's zero-route guard does not apply).
func TestRemoteAcceptsZeroRouteConfig(t *testing.T) {
	h := newRS(t)
	h.watch()
	c1 := h.cp.waitConn(t, 1)
	c1.send(rsFrameData(1, `{"version":1,"routes":[],"upstreams":[]}`))
	if c := h.expectVersion(1); len(c.Routes) != 0 {
		t.Fatalf("routes = %v", c.Routes)
	}
	c1.send(rsFrameData(2, `{"version":2}`))
	h.expectVersion(2)
}

// ---- 4. request shape and the cursor rule --------------------------------

func TestRemoteRequestShape(t *testing.T) {
	h := newRS(t)
	h.watch()
	r := h.cp.waitConn(t, 1).req
	if r.method != http.MethodGet || r.path != rsStreamPath {
		t.Errorf("request = %s %s", r.method, r.path)
	}
	if r.auth != "Bearer "+rsToken {
		t.Errorf("Authorization = %q", r.auth)
	}
	if r.node != rsNode {
		t.Errorf("node = %q, want %q", r.node, rsNode)
	}
	if !r.hasSince || r.since != "0" {
		t.Errorf("since = %q (present=%v), want an explicit 0 on a fresh start", r.since, r.hasSince)
	}
	if r.accept != "text/event-stream" {
		t.Errorf("Accept = %q", r.accept)
	}
	if r.lastEventID != "" {
		t.Errorf("source must send since only, got Last-Event-ID %q", r.lastEventID)
	}
}

// ADR 0005 / §11 row 2: since advances only on Applied, never on receipt.
func TestRemoteCursorAdvancesOnlyOnApplied(t *testing.T) {
	dir := t.TempDir()
	h := newRS(t, func(o *RemoteOptions) { o.LKGPath = dir + "/lkg.json" })
	h.watch()
	c1 := h.cp.waitConn(t, 1)
	c1.send(rsFrame(5))
	h.expectVersion(5)

	c1.abort() // delivered but never applied
	c2 := h.cp.waitConn(t, 2)
	if c2.req.since != "0" {
		t.Fatalf("since after unacknowledged delivery = %q, want 0", c2.req.since)
	}
	c2.send(rsFrame(5)) // server re-offers head: already delivered in this process
	h.quiet()

	h.src.Applied(5)
	c2.abort()
	c3 := h.cp.waitConn(t, 3)
	if c3.req.since != "5" {
		t.Fatalf("since after Applied(5) = %q, want 5", c3.req.since)
	}
	c3.send(rsFrame(6))
	h.expectVersion(6)
	h.src.Applied(6)
	h.cp.waitReport(t, "applied 6", func(r rsStatusReq) bool { return r.Applied == 6 })

	h.src.Applied(5) // late, lower ack: ignored, cursor and LKG never move backwards
	c3.abort()
	c4 := h.cp.waitConn(t, 4)
	if c4.req.since != "6" {
		t.Fatalf("since after lower Applied = %q, want 6", c4.req.since)
	}
	if v, err := rsReadLKG(dir + "/lkg.json"); err != nil || v != 6 {
		t.Fatalf("LKG = (%d, %v), want version 6", v, err)
	}
}

func TestRemoteWrongTokenKeepsRetrying(t *testing.T) {
	h := newRS(t, func(o *RemoteOptions) { o.Token = "wrong-token" })
	ch := h.watch()
	h.cp.waitConn(t, 3)
	for i := 1; i <= 3; i++ {
		if a := h.cp.waitConn(t, i).req.auth; a != "Bearer wrong-token" {
			t.Errorf("conn %d Authorization = %q", i, a)
		}
	}
	expectQuiet(t, ch, 50*time.Millisecond)
	if len(h.sl.durations()) < 2 {
		t.Fatalf("expected backoff between attempts, sleeps = %v", h.sl.durations())
	}
}

// ---- 8. reconnect and backoff -------------------------------------------

func TestRemoteBackoffSchedule(t *testing.T) {
	fails := map[string]rsStep{
		"500":                   rsStatus(500, "text/plain"),
		"401":                   rsStatus(401, "application/json"),
		"200 but not an stream": rsStatus(200, "text/html"),
		"204":                   rsStatus(204, ""),
	}
	want := []time.Duration{50, 100, 200, 200, 200}
	for i := range want {
		want[i] *= time.Millisecond // BackoffMin 100ms, BackoffMax 400ms, Rand 0 => cap/2
	}
	for name, step := range fails {
		t.Run(name, func(t *testing.T) {
			h := newRS(t)
			h.cp.queue(step, step, step, step, step)
			ch := h.watch()
			h.cp.waitConn(t, 6) // 5 failures, then a live stream
			if got := h.sl.durations(); !reflect.DeepEqual(got, want) {
				t.Fatalf("sleeps = %v, want %v", got, want)
			}
			expectQuiet(t, ch, 20*time.Millisecond)
		})
	}
	t.Run("connection refused", func(t *testing.T) {
		h := newRS(t)
		h.cp.close()
		h.watch()
		waitFor(t, 3*time.Second, "5 sleeps", func() bool { return len(h.sl.durations()) >= 5 })
		if got := h.sl.durations()[:5]; !reflect.DeepEqual(got, want) {
			t.Fatalf("sleeps = %v, want %v", got, want)
		}
	})
}

// Equal jitter: d = cap/2 + Rand()*cap/2, always in [cap/2, cap).
func TestRemoteBackoffEqualJitter(t *testing.T) {
	rands := []float64{0.5, 0.25, 0.999, 0, 0.75}
	caps := []time.Duration{100, 200, 400, 400, 400}
	var i int
	h := newRS(t, func(o *RemoteOptions) {
		o.Rand = func() float64 { r := rands[i%len(rands)]; i++; return r } // only the reader goroutine calls it
	})
	step := rsStatus(500, "")
	h.cp.queue(step, step, step, step, step)
	h.watch()
	h.cp.waitConn(t, 6)
	got := h.sl.durations()
	if len(got) != 5 {
		t.Fatalf("sleeps = %v", got)
	}
	for k, d := range got {
		c := caps[k] * time.Millisecond
		want := c/2 + time.Duration(rands[k]*float64(c)/2)
		if diff := d - want; diff < -time.Microsecond || diff > time.Microsecond {
			t.Errorf("sleep %d = %v, want %v (cap %v, rand %v)", k, d, want, c, rands[k])
		}
		if d < c/2 || d >= c {
			t.Errorf("sleep %d = %v outside [cap/2, cap) = [%v, %v)", k, d, c/2, c)
		}
	}
}

// Only a complete frame (event or comment, even a dropped event) resets the
// backoff; a stream that ends without one does not.
func TestRemoteBackoffReset(t *testing.T) {
	f500 := rsStatus(500, "")
	ms := func(v ...int) []time.Duration {
		out := make([]time.Duration, len(v))
		for i, x := range v {
			out[i] = time.Duration(x) * time.Millisecond
		}
		return out
	}
	tests := []struct {
		name  string
		steps []rsStep
		want  []time.Duration
	}{
		{"heartbeat resets", []rsStep{f500, f500, f500, rsThen(rsHeartbeat, false), f500}, ms(50, 100, 200, 50, 100)},
		{"config event resets", []rsStep{f500, f500, rsThen(rsFrame(1), false), f500}, ms(50, 100, 50, 100)},
		{"dropped event still resets", []rsStep{f500, rsThen("event: message\ndata: x\n\n", false), f500}, ms(50, 50, 100)},
		{"empty streams do not reset", []rsStep{rsThen("", false), rsThen("", false), rsThen("", false)}, ms(50, 100, 200)},
		{"partial frame does not reset", []rsStep{rsThen("id: 1\nevent: config\n", false), rsThen("id: 1\nevent: config\n", true)}, ms(50, 100)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newRS(t)
			h.cp.queue(tc.steps...)
			h.watch()
			h.cp.waitConn(t, len(tc.steps)+1)
			if got := h.sl.durations(); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("sleeps = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRemoteReconnectsAfterClose(t *testing.T) {
	for _, abort := range []bool{false, true} {
		name := map[bool]string{false: "clean end", true: "abrupt close"}[abort]
		t.Run(name, func(t *testing.T) {
			h := newRS(t)
			h.watch()
			c1 := h.cp.waitConn(t, 1)
			c1.send(rsFrame(1))
			h.expectVersion(1)
			if abort {
				c1.abort()
			} else {
				c1.end()
			}
			c2 := h.cp.waitConn(t, 2)
			c2.send(rsFrame(2))
			h.expectVersion(2)
			if len(h.sl.durations()) != 1 {
				t.Fatalf("expected exactly one backoff sleep, got %v", h.sl.durations())
			}
		})
	}
}

// An event cut off mid-way is never delivered.
func TestRemoteTruncatedEventNotDelivered(t *testing.T) {
	h := newRS(t)
	h.watch()
	c1 := h.cp.waitConn(t, 1)
	c1.send("id: 7\nevent: config\ndata: " + rsCfgJSON(7)[:20])
	c1.abort()
	c2 := h.cp.waitConn(t, 2)
	h.quiet()
	c2.send(rsFrame(8))
	h.expectVersion(8)
}

func TestRemoteCancelDuringBackoffReturnsPromptly(t *testing.T) {
	h := newRS(t)
	h.sl.block = true
	h.cp.queue(rsStatus(500, ""))
	ch := h.watch()
	select {
	case <-h.sl.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("Sleep never called")
	}
	h.cancel()
	rsDrainClosed(t, ch, time.Second)
}

// ---- 9. liveness ----------------------------------------------------------

func TestRemoteIdleTimeoutReconnects(t *testing.T) {
	tests := []struct {
		name    string
		prelude string
	}{
		{"silent from the start", ""},
		{"silent after a heartbeat", rsHeartbeat},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newRS(t, func(o *RemoteOptions) { o.IdleTimeout = 150 * time.Millisecond })
			h.watch()
			c1 := h.cp.waitConn(t, 1)
			if tc.prelude != "" {
				c1.send(tc.prelude)
			}
			c2 := h.cp.waitConn(t, 2)
			c1.waitDone(t, "idle connection to be dropped by the client")
			if len(h.sl.durations()) < 1 {
				t.Error("idle reconnect must go through backoff")
			}
			c2.send(rsFrame(1))
			h.expectVersion(1)
		})
	}
}

func TestRemoteHeartbeatsKeepConnectionAlive(t *testing.T) {
	h := newRS(t, func(o *RemoteOptions) { o.IdleTimeout = 250 * time.Millisecond })
	h.watch()
	c1 := h.cp.waitConn(t, 1)
	for i := 0; i < 20; i++ { // 1s of heartbeats, four idle timeouts
		c1.send(rsHeartbeat)
		time.Sleep(50 * time.Millisecond)
		if h.cp.connCount() != 1 {
			t.Fatalf("reconnected despite heartbeats (after %d)", i)
		}
	}
	if !h.src.Connected() {
		t.Error("Connected() = false on a healthy stream")
	}
	c1.send(rsFrame(1))
	h.expectVersion(1)
}

func TestRemoteConnected(t *testing.T) {
	h := newRS(t)
	if h.src.Connected() {
		t.Fatal("Connected() before Watch")
	}
	h.watch()
	c1 := h.cp.waitConn(t, 1)
	time.Sleep(50 * time.Millisecond)
	if h.src.Connected() {
		t.Fatal("Connected() true before any frame arrived (§5.1: open and has delivered a frame)")
	}
	c1.send(rsHeartbeat)
	waitFor(t, 2*time.Second, "Connected() after a heartbeat", h.src.Connected)
	c1.abort()
	c2 := h.cp.waitConn(t, 2)
	waitFor(t, 2*time.Second, "Connected() false after the stream dropped", func() bool { return !h.src.Connected() })
	c2.send(rsHeartbeat)
	waitFor(t, 2*time.Second, "Connected() on the new stream", h.src.Connected)
	h.cancel()
	rsDrainClosed(t, h.ch, 2*time.Second)
	if h.src.Connected() {
		t.Fatal("Connected() after shutdown")
	}
}

// ---- 10. SSE parsing and per-event handling -------------------------------

func TestRemoteEventParsing(t *testing.T) {
	j7 := rsCfgJSON(7)
	comma := strings.Index(j7, ",") + 1
	tests := []struct {
		name   string
		frames string
		want   int64 // 0 = nothing delivered
	}{
		{"LF endings", rsFrame(7), 7},
		{"CRLF endings", "id: 7\r\nevent: config\r\ndata: " + j7 + "\r\n\r\n", 7},
		{"no id field", "event: config\ndata: " + j7 + "\n\n", 7},
		{"no space after colon", "id:7\nevent:config\ndata:" + j7 + "\n\n", 7},
		{"two spaces: only one is stripped", "id: 7\nevent: config\ndata:  " + j7 + "\n\n", 7},
		{"data before event and id", "data: " + j7 + "\nevent: config\nid: 7\n\n", 7},
		{"comments and unknown fields inside an event",
			": hi\nid: 7\nretry: 3000\nfoo: bar\nevent: config\n: mid\ndata: " + j7 + "\n\n", 7},
		{"heartbeat before the event", rsHeartbeat + rsFrame(7), 7},
		{"extra blank lines", "\n\n" + rsFrame(7), 7},
		{"last id wins", "id: 99\nid: 7\nevent: config\ndata: " + j7 + "\n\n", 7},
		{"multi-line data is joined", "id: 7\nevent: config\ndata: " + j7[:comma] + "\ndata: " + j7[comma:] + "\n\n", 7},

		{"unknown top-level field", rsFrameData(7, `{"version":7,"bogus":1,"routes":[],"upstreams":[]}`), 0},
		{"unknown nested field", rsFrameData(7, `{"version":7,"routes":[{"name":"r","path_prefix":"/","upstream":"u","nope":1}],"upstreams":[{"name":"u","backends":["http://127.0.0.1:9001"]}]}`), 0},
		{"fails Validate (missing upstream)", rsFrameData(7, `{"version":7,"routes":[{"name":"r","path_prefix":"/","upstream":"ghost"}],"upstreams":[]}`), 0},
		{"not json", rsFrameData(7, "hello"), 0},
		{"trailing garbage", rsFrameData(7, j7+" {}"), 0},
		{"empty data", "id: 7\nevent: config\ndata:\n\n", 0},
		{"no data line", "id: 7\nevent: config\n\n", 0},
		{"multi-line join uses a newline, not concatenation", "id: 12\nevent: config\ndata: {\"version\":1\ndata: 2}\n\n", 0},
		{"id does not match version", "id: 8\nevent: config\ndata: " + j7 + "\n\n", 0},
		{"id not canonical", "id: 07\nevent: config\ndata: " + j7 + "\n\n", 0},
		{"id present but empty", "id:\nevent: config\ndata: " + j7 + "\n\n", 0},
		{"default event type is ignored", "data: " + j7 + "\n\n", 0},
		{"explicit message type is ignored", "event: message\ndata: " + j7 + "\n\n", 0},
		{"unknown event type is ignored", "event: ping\ndata: " + j7 + "\n\n", 0},
		{"retry only", "retry: 10\n\n", 0},
		{"version zero", rsFrameData(0, rsCfgJSON(0)), 0},
		{"negative version", rsFrameData(-3, rsCfgJSON(-3)), 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newRS(t)
			h.watch()
			c1 := h.cp.waitConn(t, 1)
			c1.send(tc.frames)
			if tc.want != 0 {
				h.expectVersion(tc.want)
			} else {
				h.quiet()
			}
			// A bad event must not poison the stream: the next one is delivered
			// on the same connection (§5.1 step 1: "The connection stays open").
			c1.send(rsFrame(1000))
			h.expectVersion(1000)
			if h.cp.connCount() != 1 {
				t.Fatalf("connection was dropped (%d conns)", h.cp.connCount())
			}
		})
	}
}

// An event is dispatched by its blank line, not before; partial writes are fine.
func TestRemoteEventDispatchedByBlankLine(t *testing.T) {
	h := newRS(t)
	h.watch()
	c1 := h.cp.waitConn(t, 1)
	c1.send("id: 7\nevent: config\ndata: " + rsCfgJSON(7) + "\n")
	h.quiet()
	c1.send("\n")
	h.expectVersion(7)
	c1.sendBytewise(strings.ReplaceAll(rsFrame(8), "\n", "\r\n"))
	h.expectVersion(8)
}

func TestRemoteEventSizeLimits(t *testing.T) {
	const limit = 1024
	bigValid := rsFrameData(7, rsBigCfgJSON(7, 40)) // valid, but far over 1 KiB
	tests := []struct {
		name     string
		frames   string
		wantDrop bool
	}{
		{"data exactly at the limit stays connected", "event: config\ndata: " + strings.Repeat("x", limit) + "\n\n", false},
		{"data one byte over", "event: config\ndata: " + strings.Repeat("x", limit+1) + "\n\n", true},
		{"valid config over the limit", bigValid, true},
		{"accumulated multi-line data over the limit",
			"event: config\n" + strings.Repeat("data: "+strings.Repeat("x", 400)+"\n", 3) + "\n", true},
		{"one overlong comment line", ": " + strings.Repeat("x", 2000) + "\n\n", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newRS(t, func(o *RemoteOptions) { o.MaxEventBytes = limit })
			h.watch()
			c1 := h.cp.waitConn(t, 1)
			c1.send(tc.frames)
			if !tc.wantDrop {
				c1.send(rsFrame(1))
				h.expectVersion(1)
				if h.cp.connCount() != 1 {
					t.Fatal("connection dropped")
				}
				return
			}
			c2 := h.cp.waitConn(t, 2)
			c1.waitDone(t, "oversized event to close the connection")
			if len(h.sl.durations()) < 1 {
				t.Error("a protocol error must go through backoff")
			}
			c2.send(rsFrame(1))
			h.expectVersion(1) // the oversized event (v7) was not delivered
		})
	}
	t.Run("endless line is not buffered without bound", func(t *testing.T) {
		h := newRS(t, func(o *RemoteOptions) { o.MaxEventBytes = limit })
		h.cp.queue(rsEndless)
		h.watch()
		c1 := h.cp.waitConn(t, 1)
		c2 := h.cp.waitConn(t, 2)
		c1.waitDone(t, "endless line to close the connection")
		// Socket buffers absorb some bytes, but the client must stop reading
		// long before the 512 MiB the server is willing to write.
		if n := c1.wrote.Load(); n > 64<<20 {
			t.Fatalf("server wrote %d bytes before the client hung up", n)
		}
		c2.send(rsFrame(1))
		h.expectVersion(1)
	})
}
