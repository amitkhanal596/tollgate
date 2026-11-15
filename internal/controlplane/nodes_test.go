package controlplane

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeClock is a goroutine-safe injectable clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
}
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func mustNode(t *testing.T, n *Nodes, id string) NodeStatus {
	t.Helper()
	ns, ok := findNode(n.List(), id)
	if !ok {
		t.Fatalf("node %q not listed", id)
	}
	return ns
}

func TestNodes_NilClockAndZeroTTLAreUsable(t *testing.T) {
	defer failOnPanic(t)
	n := NewNodes(nil, 0)
	l := n.List()
	if l == nil || len(l) != 0 {
		t.Fatalf("empty List = %#v, want empty non-nil slice", l)
	}
	d := n.Connect("a", 0)
	if ns := mustNode(t, n, "a"); !ns.Connected || ns.LastSeen.IsZero() {
		t.Fatalf("%+v", ns)
	}
	d()
}

func TestNodes_ConnectDisconnect(t *testing.T) {
	defer failOnPanic(t)
	clk := newFakeClock()
	n := NewNodes(clk.Now, time.Hour)
	t0 := clk.Now()

	disc := n.Connect("gw-1", 7)
	ns := mustNode(t, n, "gw-1")
	if !ns.Connected || ns.ConnectedAt == nil || !ns.ConnectedAt.Equal(t0) || !ns.LastSeen.Equal(t0) {
		t.Fatalf("after Connect: %+v", ns)
	}
	if ns.AppliedVersion != 7 || ns.LastOfferedVersion != 0 || ns.RejectedVersion != 0 || ns.LastError != "" {
		t.Fatalf("after Connect: %+v", ns)
	}

	clk.Advance(time.Minute)
	disc()
	ns = mustNode(t, n, "gw-1")
	if ns.Connected {
		t.Fatalf("still Connected after disconnect")
	}
	if !ns.LastSeen.Equal(t0.Add(time.Minute)) {
		t.Fatalf("LastSeen = %v, want the disconnect time", ns.LastSeen)
	}
	if ns.ConnectedAt == nil || !ns.ConnectedAt.Equal(t0) {
		t.Fatalf("ConnectedAt = %v, want kept as %v (most recent connection)", ns.ConnectedAt, t0)
	}
	if ns.AppliedVersion != 7 {
		t.Fatalf("AppliedVersion = %d, want 7 kept", ns.AppliedVersion)
	}
}

func TestNodes_ConnectOverwritesAppliedNotMax(t *testing.T) {
	defer failOnPanic(t)
	n := NewNodes(newFakeClock().Now, 0)
	n.Connect("a", 9)()
	n.Connect("a", 0) // a restarted gateway goes back to 0
	if got := mustNode(t, n, "a").AppliedVersion; got != 0 {
		t.Fatalf("AppliedVersion = %d, want 0 (overwrite, not max)", got)
	}
}

func TestNodes_OverlappingConnectionsAreCounted(t *testing.T) {
	defer failOnPanic(t)
	clk := newFakeClock()
	n := NewNodes(clk.Now, 0)
	t0 := clk.Now()
	discOld := n.Connect("a", 1)
	clk.Advance(time.Second)
	discNew := n.Connect("a", 2) // reconnect overlaps the old connection

	if ns := mustNode(t, n, "a"); !ns.Connected || !ns.ConnectedAt.Equal(t0.Add(time.Second)) {
		t.Fatalf("%+v", ns)
	}
	discOld()
	discOld() // idempotent: must not decrement twice
	if ns := mustNode(t, n, "a"); !ns.Connected {
		t.Fatalf("old connection closing (twice) disconnected the node while the new one is open")
	}
	discNew()
	if ns := mustNode(t, n, "a"); ns.Connected {
		t.Fatalf("still connected after both connections closed")
	}
}

func TestNodes_Offered(t *testing.T) {
	defer failOnPanic(t)
	clk := newFakeClock()
	n := NewNodes(clk.Now, 0)
	n.Connect("a", 0)
	clk.Advance(time.Second)
	n.Offered("a", 5)
	n.Offered("a", 3) // max, not last
	ns := mustNode(t, n, "a")
	if ns.LastOfferedVersion != 5 || !ns.LastSeen.Equal(clk.Now()) {
		t.Fatalf("%+v, want offered 5 and LastSeen = now", ns)
	}
	// Unknown node: entry created, not connected, no ConnectedAt.
	n.Offered("ghost", 2)
	g := mustNode(t, n, "ghost")
	if g.LastOfferedVersion != 2 || g.Connected || g.ConnectedAt != nil {
		t.Fatalf("%+v", g)
	}
}

func TestNodes_Seen(t *testing.T) {
	defer failOnPanic(t)
	clk := newFakeClock()
	n := NewNodes(clk.Now, 0)
	n.Connect("a", 0)
	clk.Advance(time.Minute)
	n.Seen("a")
	if ns := mustNode(t, n, "a"); !ns.LastSeen.Equal(clk.Now()) {
		t.Fatalf("LastSeen = %v, want %v", ns.LastSeen, clk.Now())
	}
	clk.Advance(time.Minute)
	n.Seen("ghost")
	if g := mustNode(t, n, "ghost"); g.Connected || g.ConnectedAt != nil || !g.LastSeen.Equal(clk.Now()) {
		t.Fatalf("%+v", g)
	}
}

func TestNodes_Report(t *testing.T) {
	defer failOnPanic(t)
	clk := newFakeClock()
	n := NewNodes(clk.Now, 0)

	// A report before any stream connection creates the entry.
	n.Report("a", StatusReport{Applied: 4, LastRejected: 5, Error: "build: boom"})
	ns := mustNode(t, n, "a")
	if ns.Connected || ns.ConnectedAt != nil || ns.AppliedVersion != 4 || ns.RejectedVersion != 5 || ns.LastError != "build: boom" || !ns.LastSeen.Equal(clk.Now()) {
		t.Fatalf("%+v", ns)
	}

	// Report leaves connection state and offered version alone.
	disc := n.Connect("a", 4)
	n.Offered("a", 6)
	clk.Advance(time.Second)
	n.Report("a", StatusReport{Applied: 6, LastRejected: 5, Error: "build: boom"})
	ns = mustNode(t, n, "a")
	if !ns.Connected || ns.ConnectedAt == nil || ns.LastOfferedVersion != 6 || ns.AppliedVersion != 6 || !ns.LastSeen.Equal(clk.Now()) {
		t.Fatalf("%+v", ns)
	}

	// The latest report describes the whole state: it overwrites, even with zeros.
	n.Report("a", StatusReport{})
	ns = mustNode(t, n, "a")
	if ns.AppliedVersion != 0 || ns.RejectedVersion != 0 || ns.LastError != "" {
		t.Fatalf("report did not overwrite: %+v", ns)
	}
	disc()
}

func TestNodes_ReportKeepsLongErrorVerbatim(t *testing.T) {
	defer failOnPanic(t)
	// The 1024-byte cap (MaxReportErrorBytes) is enforced by the HTTP layer
	// (400), not here; a maximal error is stored whole.
	n := NewNodes(newFakeClock().Now, 0)
	msg := strings.Repeat("e", MaxReportErrorBytes)
	n.Report("a", StatusReport{Error: msg})
	if got := mustNode(t, n, "a").LastError; got != msg {
		t.Fatalf("LastError len %d, want %d", len(got), len(msg))
	}
}

func TestNodes_ListSortedAndCopied(t *testing.T) {
	defer failOnPanic(t)
	n := NewNodes(newFakeClock().Now, 0)
	ids := []string{"gw-b", "gw-a", "Zed", "gw-a2", "10", "9"}
	for _, id := range ids {
		n.Seen(id)
	}
	l := n.List()
	got := make([]string, len(l))
	for i, ns := range l {
		got[i] = ns.Node
	}
	want := append([]string(nil), ids...)
	sort.Strings(want)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("List order %v, want sorted by Node %v", got, want)
	}
	// Mutating the returned copy must not change the registry.
	l[0].AppliedVersion = 12345
	l[0].Node = "mutated"
	if ns := mustNode(t, n, want[0]); ns.AppliedVersion == 12345 {
		t.Fatalf("List returned live entries, not copies")
	}
}

func TestNodes_TTLEviction(t *testing.T) {
	defer failOnPanic(t)
	const ttl = 10 * time.Minute
	listed := func(n *Nodes, id string) bool { _, ok := findNode(n.List(), id); return ok }

	run(t, "disconnected node is evicted after ttl", func(t *testing.T) {
		defer failOnPanic(t)
		clk := newFakeClock()
		n := NewNodes(clk.Now, ttl)
		n.Connect("a", 0)()
		clk.Advance(ttl - time.Second)
		if !listed(n, "a") {
			t.Fatal("evicted before ttl")
		}
		clk.Advance(2 * time.Second)
		if listed(n, "a") {
			t.Fatal("still listed after ttl")
		}
	})
	run(t, "connected node is never evicted", func(t *testing.T) {
		defer failOnPanic(t)
		clk := newFakeClock()
		n := NewNodes(clk.Now, ttl)
		disc := n.Connect("a", 0)
		clk.Advance(100 * ttl)
		if !listed(n, "a") {
			t.Fatal("connected node evicted")
		}
		disc()
		if !listed(n, "a") { // LastSeen was just refreshed by disconnect
			t.Fatal("evicted right after disconnect")
		}
		clk.Advance(ttl + time.Second)
		if listed(n, "a") {
			t.Fatal("still listed ttl after disconnect")
		}
	})
	run(t, "Seen and Offered refresh the ttl", func(t *testing.T) {
		defer failOnPanic(t)
		clk := newFakeClock()
		n := NewNodes(clk.Now, ttl)
		n.Connect("a", 0)()
		clk.Advance(9 * time.Minute)
		n.Seen("a")
		clk.Advance(9 * time.Minute)
		if !listed(n, "a") {
			t.Fatal("evicted although seen 9 minutes ago")
		}
		n.Offered("a", 1)
		clk.Advance(9 * time.Minute)
		if !listed(n, "a") {
			t.Fatal("evicted although offered 9 minutes ago")
		}
	})
	run(t, "report-only node is evicted too", func(t *testing.T) {
		defer failOnPanic(t)
		clk := newFakeClock()
		n := NewNodes(clk.Now, ttl)
		n.Report("a", StatusReport{Applied: 3, LastRejected: 4, Error: "x"})
		clk.Advance(ttl + time.Second)
		if listed(n, "a") {
			t.Fatal("report-only node not evicted")
		}
		// It comes back fresh: nothing stale from the evicted entry.
		n.Connect("a", 1)
		ns := mustNode(t, n, "a")
		if ns.AppliedVersion != 1 || ns.RejectedVersion != 0 || ns.LastError != "" {
			t.Fatalf("stale state after eviction: %+v", ns)
		}
	})
	run(t, "only expired entries go", func(t *testing.T) {
		defer failOnPanic(t)
		clk := newFakeClock()
		n := NewNodes(clk.Now, ttl)
		n.Seen("old")
		clk.Advance(ttl)
		n.Seen("new")
		clk.Advance(time.Second)
		l := n.List()
		if len(l) != 1 || l[0].Node != "new" {
			t.Fatalf("List = %+v, want only 'new'", l)
		}
	})
}

func TestNodes_Concurrent(t *testing.T) {
	defer failOnPanic(t)
	const holders = 50
	clk := newFakeClock()
	n := NewNodes(clk.Now, time.Hour)

	var connected, release sync.WaitGroup
	connected.Add(holders)
	release.Add(1)
	var done sync.WaitGroup
	for i := 0; i < holders; i++ {
		done.Add(1)
		go func(i int) {
			defer done.Done()
			disc := n.Connect("shared", int64(i))
			n.Offered("shared", int64(i))
			n.Seen("shared")
			n.Report("shared", StatusReport{Applied: int64(i)})
			n.Report(fmt.Sprintf("own-%d", i%7), StatusReport{Applied: int64(i)})
			_ = n.List()
			connected.Done()
			release.Wait()
			disc()
			if i%2 == 0 {
				disc() // idempotent under concurrency too
			}
		}(i)
	}
	connected.Wait()
	if ns := mustNode(t, n, "shared"); !ns.Connected || ns.LastOfferedVersion != holders-1 {
		t.Fatalf("with %d connections open: %+v", holders, ns)
	}
	clk.Advance(time.Second)
	release.Done()
	done.Wait()
	if ns := mustNode(t, n, "shared"); ns.Connected {
		t.Fatalf("still Connected after every connection closed: %+v", ns)
	}
}
