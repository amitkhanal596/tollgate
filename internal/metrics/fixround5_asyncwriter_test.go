package metrics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// fr5Sink is an io.Writer test double. With gate != nil every Write blocks
// until gate is closed (or forever if it never is).
type fr5Sink struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	gate    chan struct{}
	entered atomic.Int64 // Write calls that started
}

func (s *fr5Sink) Write(p []byte) (int, error) {
	s.entered.Add(1)
	if s.gate != nil {
		<-s.gate
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *fr5Sink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func fr5Lines(s string) []string {
	s = strings.TrimSuffix(s, "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

func fr5Ctx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func TestFixRound5_AsyncWriterDeliversInOrder(t *testing.T) {
	defer guard(t)
	sink := &fr5Sink{}
	w := NewAsyncWriter(sink, 1000, nil)
	var want []string
	for i := 0; i < 500; i++ {
		line := fmt.Sprintf("line-%04d", i)
		want = append(want, line)
		n, err := w.Write([]byte(line + "\n"))
		if err != nil || n != len(line)+1 {
			t.Fatalf("Write #%d = (%d, %v), want (%d, nil)", i, n, err, len(line)+1)
		}
	}
	if err := w.Close(fr5Ctx(t, 5*time.Second)); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := fr5Lines(sink.String())
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("got %d lines, want %d, in order; first got %q", len(got), len(want), got[:min(3, len(got))])
	}
}

// A sink that blocks forever must not slow the writers down: the buffer
// fills, the rest is dropped (onDrop once per drop) and Write still reports
// success so a logger never turns the drop into an error path.
func TestFixRound5_AsyncWriterBlockedSinkDropsInsteadOfBlocking(t *testing.T) {
	defer guard(t)
	const (
		queueLines = 16
		writes     = 10000
	)
	sink := &fr5Sink{gate: make(chan struct{})}
	var drops atomic.Int64
	w := NewAsyncWriter(sink, queueLines, func() { drops.Add(1) })

	start := time.Now()
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < writes; i++ {
			if n, err := w.Write([]byte(fmt.Sprintf("l%d\n", i))); err != nil || n == 0 {
				t.Errorf("Write #%d = (%d, %v), want success even when dropped", i, n, err)
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(sink.gate) // let the goroutine out before failing
		t.Fatalf("%d Writes did not finish in 5s: Write blocks on a stuck sink", writes)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Errorf("%d Writes took %v with a stuck sink, want near-instant", writes, el)
	}
	// Everything not queued was dropped, exactly once each. The queue holds
	// queueLines and the drain goroutine may hold one more (in the sink).
	accepted := int64(writes) - drops.Load()
	if accepted < queueLines || accepted > queueLines+1 {
		t.Errorf("accepted %d lines, want %d or %d (queue + the one in flight)", accepted, queueLines, queueLines+1)
	}

	close(sink.gate)
	if err := w.Close(fr5Ctx(t, 5*time.Second)); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := int64(len(fr5Lines(sink.String()))); got != accepted {
		t.Errorf("sink received %d lines, want the %d accepted ones", got, accepted)
	}
	if d := drops.Load(); d != int64(writes)-accepted {
		t.Errorf("onDrop calls changed after Close: %d", d)
	}
}

func TestFixRound5_AsyncWriterNilOnDropIsFine(t *testing.T) {
	defer guard(t)
	sink := &fr5Sink{gate: make(chan struct{})}
	w := NewAsyncWriter(sink, 1, nil)
	for i := 0; i < 100; i++ {
		_, _ = w.Write([]byte("x\n"))
	}
	close(sink.gate)
	if err := w.Close(fr5Ctx(t, 5*time.Second)); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// slog reuses its buffers: Write must copy p before returning.
func TestFixRound5_AsyncWriterCopiesItsArgument(t *testing.T) {
	defer guard(t)
	sink := &fr5Sink{gate: make(chan struct{})}
	w := NewAsyncWriter(sink, 8, nil)
	_, _ = w.Write([]byte("first\n"))
	// Make sure "first" is the line stuck in the sink, so the next ones sit in the queue.
	deadline := time.Now().Add(2 * time.Second)
	for sink.entered.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	buf := []byte("hello\n")
	_, _ = w.Write(buf)
	copy(buf, "XXXXX")
	buf2 := []byte("world\n")
	_, _ = w.Write(buf2)
	for i := range buf2 {
		buf2[i] = '#'
	}
	close(sink.gate)
	if err := w.Close(fr5Ctx(t, 5*time.Second)); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got, want := sink.String(), "first\nhello\nworld\n"; got != want {
		t.Errorf("sink = %q, want %q (Write must copy its argument)", got, want)
	}
}

func TestFixRound5_AsyncWriterCloseFlushesQueuedLines(t *testing.T) {
	defer guard(t)
	sink := &fr5Sink{gate: make(chan struct{})}
	w := NewAsyncWriter(sink, 100, nil)
	for i := 0; i < 50; i++ {
		_, _ = w.Write([]byte(fmt.Sprintf("q%02d\n", i)))
	}
	go func() { // the sink recovers shortly after Close starts waiting
		time.Sleep(50 * time.Millisecond)
		close(sink.gate)
	}()
	if err := w.Close(fr5Ctx(t, 5*time.Second)); err != nil {
		t.Fatalf("Close: %v, want nil after flushing", err)
	}
	if got := len(fr5Lines(sink.String())); got != 50 {
		t.Errorf("flushed %d lines, want 50", got)
	}
}

// Close bounded by ctx: with a sink that never returns, Close gives up at
// the deadline with ctx's error. The ONE goroutine stuck inside the sink's
// Write cannot be interrupted (io.Writer has no cancellation); that single
// goroutine is the documented, accepted leak, and it ends as soon as the sink
// returns. Nothing else may outlive Close.
func TestFixRound5_AsyncWriterCloseWithBlockedSinkHonoursContext(t *testing.T) {
	defer guard(t)
	before := runtime.NumGoroutine()
	sink := &fr5Sink{gate: make(chan struct{})}
	w := NewAsyncWriter(sink, 8, nil)
	for i := 0; i < 20; i++ {
		_, _ = w.Write([]byte("x\n"))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := w.Close(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Close = %v, want context.DeadlineExceeded", err)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Errorf("Close took %v with a 150ms deadline", el)
	}
	if n := runtime.NumGoroutine(); n > before+1 {
		t.Errorf("%d goroutines after Close, want at most %d (only the one stuck in the sink's Write)", n, before+1)
	}
	close(sink.gate)
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > before {
		t.Errorf("%d goroutines after the sink returned, want %d: the drain goroutine leaked", n, before)
	}
}

func TestFixRound5_AsyncWriterWriteAfterCloseIsDroppedNotPanic(t *testing.T) {
	defer guard(t)
	sink := &fr5Sink{}
	w := NewAsyncWriter(sink, 8, nil)
	_, _ = w.Write([]byte("before\n"))
	if err := w.Close(fr5Ctx(t, 5*time.Second)); err != nil {
		t.Fatalf("Close: %v", err)
	}
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Write after Close panicked: %v", r)
			}
		}()
		for i := 0; i < 20; i++ {
			_, _ = w.Write([]byte("after\n"))
		}
	}()
	time.Sleep(20 * time.Millisecond)
	if got := sink.String(); got != "before\n" {
		t.Errorf("sink = %q, want only the line written before Close", got)
	}
}

func TestFixRound5_AsyncWriterConcurrentWriters(t *testing.T) {
	defer guard(t)
	const writers, per = 8, 500
	sink := &fr5Sink{}
	var drops atomic.Int64
	w := NewAsyncWriter(sink, writers*per, func() { drops.Add(1) })
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				_, _ = w.Write([]byte(fmt.Sprintf("w%d-%04d-%s\n", g, i, strings.Repeat("z", 50))))
			}
		}()
	}
	wg.Wait()
	if err := w.Close(fr5Ctx(t, 5*time.Second)); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got := fr5Lines(sink.String())
	if int64(len(got))+drops.Load() != writers*per {
		t.Errorf("%d delivered + %d dropped != %d written", len(got), drops.Load(), writers*per)
	}
	if drops.Load() != 0 {
		t.Errorf("%d lines dropped although the queue could hold every line", drops.Load())
	}
	for _, ln := range got {
		if !strings.HasPrefix(ln, "w") || !strings.HasSuffix(ln, strings.Repeat("z", 50)) {
			t.Fatalf("corrupt or interleaved line %q", ln)
		}
	}
}

func TestFixRound5_AccessLogDroppedCounter(t *testing.T) {
	defer guard(t)
	reg := prometheus.NewRegistry()
	m := New(reg)
	m.AccessLogDropped()
	m.AccessLogDropped()
	m.AccessLogDropped()
	if v := value(t, reg, "tollgate_access_log_dropped_total", labels{}); v != 3 {
		t.Errorf("tollgate_access_log_dropped_total = %v, want 3", v)
	}
	if typ, _, _, ok := familyType(t, reg, "tollgate_access_log_dropped_total"); !ok || typ != "COUNTER" {
		t.Errorf("family type = %q (found=%v), want COUNTER", typ, ok)
	}
	var nilm *Metrics
	nilm.AccessLogDropped() // must not panic
}

// O3 unit level: ForgetRoute removes exactly one route's series from the four
// route-labelled families.
func fr5RouteSeries(t *testing.T, reg *prometheus.Registry, family, route string) int {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, mf := range mfs {
		if mf.GetName() != family {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "route" && lp.GetValue() == route {
					n++
				}
			}
		}
	}
	return n
}

var fr5RouteFamilies = []string{
	"tollgate_requests_total", "tollgate_request_duration_seconds",
	"tollgate_ratelimited_total", "tollgate_auth_failures_total",
}

func fr5Populate(m *Metrics, routes ...string) {
	for _, r := range routes {
		m.ObserveRequest(r, "u1", 200, time.Millisecond)
		m.ObserveRequest(r, "u1", 502, time.Millisecond)
		m.ObserveRequest(r, "u2", 200, time.Millisecond)
		m.RateLimited(r, "limit")
		m.RateLimited(r, "capacity")
		m.AuthFailed(r, "missing")
		m.AuthFailed(r, "invalid")
	}
}

func TestFixRound5_ForgetRouteRemovesExactlyThatRoute(t *testing.T) {
	defer guard(t)
	reg := prometheus.NewRegistry()
	m := New(reg)
	fr5Populate(m, "old", "keep")
	m.SetBackendUp("u1", "u1|http://b:1", true) // unrelated families stay untouched
	for _, f := range fr5RouteFamilies {
		if fr5RouteSeries(t, reg, f, "old") == 0 || fr5RouteSeries(t, reg, f, "keep") == 0 {
			t.Fatalf("fixture: %s lacks series", f)
		}
	}
	keepBefore := map[string]int{}
	for _, f := range fr5RouteFamilies {
		keepBefore[f] = fr5RouteSeries(t, reg, f, "keep")
	}

	m.ForgetRoute("old")

	for _, f := range fr5RouteFamilies {
		if n := fr5RouteSeries(t, reg, f, "old"); n != 0 {
			t.Errorf("%s still has %d series for the forgotten route", f, n)
		}
		if n := fr5RouteSeries(t, reg, f, "keep"); n != keepBefore[f] {
			t.Errorf("%s: %d series for the other route, want %d (untouched)", f, n, keepBefore[f])
		}
	}
	if v := value(t, reg, "tollgate_requests_total", labels{"route": "keep", "upstream": "u1", "code": "200"}); v != 1 {
		t.Errorf("kept counter = %v, want 1", v)
	}
	if !has(t, reg, "tollgate_backend_up", labels{"upstream": "u1", "backend": "u1|http://b:1"}) {
		t.Error("ForgetRoute removed a backend series")
	}
	// A forgotten route that traffic reaches again starts from zero.
	m.ObserveRequest("old", "u1", 200, time.Millisecond)
	if v := value(t, reg, "tollgate_requests_total", labels{"route": "old", "upstream": "u1", "code": "200"}); v != 1 {
		t.Errorf("re-observed route counter = %v, want a fresh 1", v)
	}
}

func TestFixRound5_ForgetRouteNoOps(t *testing.T) {
	defer guard(t)
	t.Run("nil receiver", func(t *testing.T) {
		defer guard(t)
		var m *Metrics
		m.ForgetRoute("anything")
	})
	t.Run("unknown route", func(t *testing.T) {
		defer guard(t)
		reg := prometheus.NewRegistry()
		m := New(reg)
		fr5Populate(m, "keep")
		before := map[string]int{}
		for _, f := range fr5RouteFamilies {
			before[f] = fr5RouteSeries(t, reg, f, "keep")
		}
		m.ForgetRoute("never-existed")
		m.ForgetRoute("")
		for _, f := range fr5RouteFamilies {
			if n := fr5RouteSeries(t, reg, f, "keep"); n != before[f] {
				t.Errorf("%s: %d series, want %d", f, n, before[f])
			}
		}
	})
}
