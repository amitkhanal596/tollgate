package controlplane

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// tickClock returns a clock that advances 1 ms per reading, so "least recently
// seen" is a strict order in tests.
func tickClock() func() time.Time {
	var mu sync.Mutex
	t := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	return func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		t = t.Add(time.Millisecond)
		return t
	}
}

func nodeIDs(ns []NodeStatus) []string {
	out := make([]string, len(ns))
	for i, n := range ns {
		out[i] = n.Node
	}
	return out
}

// ---- C2: nodes registry cap ---------------------------------------------------

func TestFixRound4_DefaultMaxNodes(t *testing.T) {
	defer failOnPanic(t)
	if DefaultMaxNodes != 4096 {
		t.Fatalf("DefaultMaxNodes = %d, want 4096", DefaultMaxNodes)
	}
	n := NewNodes(tickClock(), time.Hour) // keeps working, with the default cap
	for i := 0; i < DefaultMaxNodes+500; i++ {
		n.Report(fmt.Sprintf("n%d", i), StatusReport{Applied: 1})
	}
	if got := len(n.List()); got != DefaultMaxNodes {
		t.Fatalf("len(List) = %d after %d distinct reports, want exactly %d", got, DefaultMaxNodes+500, DefaultMaxNodes)
	}
}

func TestFixRound4_NodesNeverExceedMaxUnderManyReports(t *testing.T) {
	defer failOnPanic(t)
	const max, total = 100, 20_000
	n := NewNodesWithLimit(tickClock(), time.Hour, max)
	for i := 0; i < total; i++ {
		n.Report(fmt.Sprintf("n%d", i), StatusReport{Applied: int64(i)})
		if i%500 == 0 || i == total-1 {
			if got := len(n.List()); got > max {
				t.Fatalf("after %d reports the registry holds %d entries, cap %d", i+1, got, max)
			}
		}
	}
	// Disconnected entries are evicted least-recently-seen first, so what is
	// left is the newest max nodes.
	ids := nodeIDs(n.List())
	if len(ids) != max {
		t.Fatalf("len(List) = %d, want %d", len(ids), max)
	}
	got := map[string]bool{}
	for _, id := range ids {
		got[id] = true
	}
	for i := total - max; i < total; i++ {
		if !got[fmt.Sprintf("n%d", i)] {
			t.Fatalf("n%d (one of the %d most recent) was evicted; have %v", i, max, ids)
		}
	}
}

func TestFixRound4_NodesEvictLeastRecentlySeenDisconnected(t *testing.T) {
	defer failOnPanic(t)
	n := NewNodesWithLimit(tickClock(), time.Hour, 3)
	n.Report("a", StatusReport{})
	n.Report("b", StatusReport{})
	n.Report("c", StatusReport{})
	n.Seen("a")                   // a is now the most recently seen; b is the oldest
	n.Report("d", StatusReport{}) // evicts b
	if got := nodeIDs(n.List()); strings.Join(got, ",") != "a,c,d" {
		t.Fatalf("after adding d: %v, want [a c d]", got)
	}
	n.Report("c", StatusReport{Applied: 2}) // an existing node never evicts anyone
	n.Report("e", StatusReport{})           // evicts a (older than c and d)
	if got := nodeIDs(n.List()); strings.Join(got, ",") != "c,d,e" {
		t.Fatalf("after adding e: %v, want [c d e]", got)
	}
}

func TestFixRound4_ConnectedNodesAreNeverEvicted(t *testing.T) {
	defer failOnPanic(t)
	n := NewNodesWithLimit(tickClock(), time.Hour, 3)
	dc1, ok1 := n.TryConnect("c1", 0)
	dc2, ok2 := n.TryConnect("c2", 0)
	if !ok1 || !ok2 {
		t.Fatalf("TryConnect below the cap: ok = %v, %v", ok1, ok2)
	}
	defer dc1()
	defer dc2()
	for i := 0; i < 2000; i++ {
		n.Report(fmt.Sprintf("r%d", i), StatusReport{Applied: 1})
		if i%100 == 0 {
			l := n.List()
			if len(l) > 3 {
				t.Fatalf("registry holds %d entries, cap 3", len(l))
			}
			for _, id := range []string{"c1", "c2"} {
				if ns, ok := findNode(l, id); !ok || !ns.Connected {
					t.Fatalf("connected node %s evicted or marked disconnected after %d reports: %+v", id, i+1, l)
				}
			}
		}
	}
}

func TestFixRound4_ReportForUnadmittableNodeIsDropped(t *testing.T) {
	defer failOnPanic(t)
	n := NewNodesWithLimit(tickClock(), time.Hour, 2)
	dc1, _ := n.TryConnect("a", 4)
	dc2, _ := n.TryConnect("b", 5)
	defer dc1()
	defer dc2()
	n.Report("z", StatusReport{Applied: 9}) // full of connected nodes: dropped
	l := n.List()
	if _, ok := findNode(l, "z"); ok || len(l) != 2 {
		t.Fatalf("report for a new node over a full registry was admitted: %v", nodeIDs(l))
	}
	n.Report("a", StatusReport{Applied: 7}) // an existing connected node still updates
	if ns := mustNode(t, n, "a"); ns.AppliedVersion != 7 {
		t.Fatalf("report for a known node ignored: %+v", ns)
	}
}

func TestFixRound4_TryConnect(t *testing.T) {
	defer failOnPanic(t)
	n := NewNodesWithLimit(tickClock(), time.Hour, 2)
	da, ok := n.TryConnect("a", 3)
	if !ok || da == nil {
		t.Fatalf("TryConnect(a) = (%v, %v)", da != nil, ok)
	}
	if ns := mustNode(t, n, "a"); !ns.Connected || ns.AppliedVersion != 3 || ns.ConnectedAt == nil {
		t.Fatalf("after TryConnect: %+v", ns)
	}
	db, ok := n.TryConnect("b", 0)
	if !ok {
		t.Fatal("TryConnect(b) refused below the cap")
	}
	// Full of connected nodes: a new ID is refused and nothing changes.
	if dd, ok := n.TryConnect("c", 0); ok {
		dd()
		t.Fatal("TryConnect(c) admitted over a registry of connected nodes")
	}
	if got := nodeIDs(n.List()); strings.Join(got, ",") != "a,b" {
		t.Fatalf("registry after refused connect: %v", got)
	}
	// An already-registered node can open another (overlapping) connection.
	da2, ok := n.TryConnect("a", 3)
	if !ok {
		t.Fatal("second connection of a registered node refused")
	}
	da()
	if ns := mustNode(t, n, "a"); !ns.Connected {
		t.Fatal("a must stay connected while its second connection is open")
	}
	da2()
	da2() // idempotent
	// a is disconnected now, so it is the eviction candidate for c.
	dc, ok := n.TryConnect("c", 0)
	if !ok {
		t.Fatal("TryConnect(c) refused although a is disconnected")
	}
	defer dc()
	defer db()
	if got := nodeIDs(n.List()); strings.Join(got, ",") != "b,c" {
		t.Fatalf("registry = %v, want [b c] (a evicted)", got)
	}
}

func TestFixRound4_EvictedNodeStartsFresh(t *testing.T) {
	defer failOnPanic(t)
	n := NewNodesWithLimit(tickClock(), time.Hour, 2)
	d, ok := n.TryConnect("a", 5)
	if !ok {
		t.Fatal("TryConnect refused")
	}
	n.Offered("a", 9)
	n.Report("a", StatusReport{Applied: 7, LastRejected: 8, Error: "boom"})
	d() // disconnect: a is now evictable
	n.Report("b", StatusReport{})
	n.Report("c", StatusReport{}) // evicts a
	if _, ok := findNode(n.List(), "a"); ok {
		t.Fatal("a should have been evicted")
	}
	n.Report("a", StatusReport{Applied: 1}) // evicts b; a returns
	ns := mustNode(t, n, "a")
	if ns.AppliedVersion != 1 || ns.RejectedVersion != 0 || ns.LastError != "" || ns.LastOfferedVersion != 0 || ns.ConnectedAt != nil || ns.Connected {
		t.Fatalf("returning node kept stale state: %+v", ns)
	}
}

func TestFixRound4_NodesCapConcurrent(t *testing.T) {
	defer failOnPanic(t)
	const max = 16
	n := NewNodesWithLimit(nil, time.Hour, max)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { // a stub panic in a goroutine would kill the whole test binary
				if r := recover(); r != nil {
					t.Errorf("panic (stub not implemented?): %v", r)
				}
			}()
			for i := 0; i < 500; i++ {
				id := fmt.Sprintf("g%d-n%d", g, i)
				switch i % 3 {
				case 0:
					n.Report(id, StatusReport{Applied: int64(i)})
				case 1:
					if d, ok := n.TryConnect(id, 0); ok {
						n.Offered(id, 1)
						n.Seen(id)
						d()
					}
				default:
					if l := n.List(); len(l) > max {
						t.Errorf("List returned %d entries, cap %d", len(l), max)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	if l := n.List(); len(l) > max {
		t.Fatalf("registry holds %d entries, cap %d", len(l), max)
	}
}

// HTTP: with a cap of 2 and two open streams, a third stream is 503; once one
// disconnects a new stream is accepted. Status reports for unknown nodes over a
// full registry still answer their normal 204 but are not recorded.
func TestFixRound4_StreamRefusedWhenRegistryFullOfConnectedNodes(t *testing.T) {
	defer failOnPanic(t)
	se := newStreamEnv(t, func(o *Options) { o.Nodes = NewNodesWithLimit(nil, 0, 2) })
	se.seedStore(t, 1)
	a := se.open(t, "?node=a&since=1")
	b := se.open(t, "?node=b&since=1")
	waitFor(t, wait, "a and b registered", func() bool {
		n := 0
		for _, ns := range se.listNodes(t) {
			if ns.Connected {
				n++
			}
		}
		return n == 2
	})

	resp, cancel := se.dial(t, dpTok, "?node=c&since=1")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("third stream: status %d, want 503", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); strings.HasPrefix(ct, ContentTypeEventStream) {
		t.Fatalf("503 carries Content-Type %q", ct)
	}
	_ = resp.Body.Close()
	cancel()
	if _, ok := findNode(se.listNodes(t), "c"); ok {
		t.Fatal("refused node c is listed")
	}

	rec := do(t, se.h, "POST", "/v1/dataplanes/z/status", dpTok, `{"applied":3}`)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status report for an unadmittable node: %d, want 204", rec.Code)
	}
	if _, ok := findNode(se.listNodes(t), "z"); ok || len(se.listNodes(t)) != 2 {
		t.Fatalf("report for z was recorded over a full registry: %v", nodeIDs(se.listNodes(t)))
	}

	a.close()
	var accepted bool
	deadline := time.Now().Add(wait)
	for !accepted && time.Now().Before(deadline) {
		resp, cancel := se.dial(t, dpTok, "?node=d&since=1")
		accepted = resp.StatusCode == http.StatusOK
		if !accepted && resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("stream after a disconnect: status %d, want 200 or 503", resp.StatusCode)
		}
		_ = resp.Body.Close()
		cancel()
		if !accepted {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if !accepted {
		t.Fatalf("no new stream accepted within %v after a node disconnected", wait)
	}
	_ = b
}

// ---- C5: Hub.Max ----------------------------------------------------------------

func TestFixRound4_HubMax(t *testing.T) {
	defer failOnPanic(t)
	h := NewHub()
	defer h.Close()
	steps := []struct {
		name    string
		publish *int64
		want    int64
	}{
		{"empty hub", nil, 0},
		{"publish 3", i64(3), 3},
		{"lower version ignored", i64(2), 3},
		{"equal version ignored", i64(3), 3},
		{"higher version", i64(7), 7},
		{"zero ignored", i64(0), 7},
		{"nil config ignored", nil, 7},
	}
	for i, s := range steps {
		if s.publish != nil {
			h.Publish(cfgV(*s.publish))
		} else if i == len(steps)-1 {
			h.Publish(nil)
		}
		if got := h.Max(); got != s.want {
			t.Fatalf("%s: Max() = %d, want %d", s.name, got, s.want)
		}
	}
}

func TestFixRound4_HubMaxConcurrent(t *testing.T) {
	defer failOnPanic(t)
	h := NewHub()
	defer h.Close()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { // a stub panic in a goroutine would kill the whole test binary
				if r := recover(); r != nil {
					t.Errorf("panic (stub not implemented?): %v", r)
				}
			}()
			for v := int64(1 + g); v <= 400; v += 8 {
				h.Publish(cfgV(v))
				if m := h.Max(); m < v {
					t.Errorf("Max() = %d right after Publish(%d)", m, v)
					return
				}
			}
		}()
	}
	wg.Wait()
	if m := h.Max(); m != 400 {
		t.Fatalf("Max() = %d, want 400", m)
	}
}

// ---- C7 -------------------------------------------------------------------------

// A stream write that hangs must be cut off before the control plane's drain
// timeout (default 10 s in cmd/controlplane) expires, or shutdown reports a
// failed drain.
func TestFixRound4_StreamWriteTimeoutBelowDrainTimeout(t *testing.T) {
	const defaultDrain = 10 * time.Second // cmd/controlplane -drain-timeout default
	if DefaultStreamWriteTimeout != 5*time.Second {
		t.Fatalf("DefaultStreamWriteTimeout = %v, want 5s", DefaultStreamWriteTimeout)
	}
	if DefaultStreamWriteTimeout >= defaultDrain {
		t.Fatalf("DefaultStreamWriteTimeout %v is not below the default drain timeout %v", DefaultStreamWriteTimeout, defaultDrain)
	}
}
