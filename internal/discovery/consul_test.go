package discovery

import (
	"context"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// Rule 2: invalid input is the only synchronous error.
func TestWatch_EmptyServiceIsError(t *testing.T) {
	for _, q := range []Query{{}, {Tag: "v2"}} {
		t.Run("tag="+q.Tag, func(t *testing.T) {
			safely(t, func() {
				h := newHarness(t)
				ch, err := h.c.Watch(context.Background(), q)
				if err == nil {
					t.Fatal("want error for empty Service")
				}
				if ch != nil {
					t.Fatal("channel must be nil on error")
				}
				if n := len(h.fc.Reqs("")); n != 0 {
					t.Fatalf("no request expected, got %d", n)
				}
			})
		})
	}
}

// Rule 2: Consul unreachable is not an error; nothing is sent; backoff grows
// (ADR 0007 says only "back off exponentially": jitter is unspecified, so we
// pin non-decreasing, positive, capped at BackoffMax, and growing overall).
func TestWatch_UnreachableBacksOffThenRecovers(t *testing.T) {
	safely(t, func() {
		h := newHarness(t)
		h.fc.Set("web", mk("n1", "web-1", "10.1.1.1", 80))
		h.fc.Close() // address now refuses connections
		ch, _ := h.watch(Query{Service: "web"})

		poll(t, 3*time.Second, "6 backoff sleeps", func() bool { return len(h.sl.Durations()) >= 6 })
		expectNone(t, ch, 30*time.Millisecond)

		ds := h.sl.Durations()
		for i, d := range ds {
			if d <= 0 || d > testBackoffMax {
				t.Fatalf("sleep[%d]=%v outside (0, BackoffMax=%v]", i, d, testBackoffMax)
			}
			if i > 0 && d < ds[i-1] {
				t.Fatalf("backoff decreased: %v", ds)
			}
		}
		if ds[len(ds)-1] <= ds[0] {
			t.Fatalf("backoff did not grow: %v", ds)
		}

		h.fc.Restart()
		got := recv(t, ch)
		wantEndpoints(t, got, []Endpoint{{ID: "n1/web-1", Address: "10.1.1.1:80"}})
	})
}

// Rule 3: first value is the first successful result, even when empty.
func TestWatch_FirstValue(t *testing.T) {
	tests := []struct {
		name  string
		insts []fakeInst
		want  []Endpoint
	}{
		{"no instances registered", nil, nil},
		{"all instances failing", []fakeInst{{Node: "n1", NodeAddr: "10.0.0.1", ID: "a", Port: 80, Status: "critical"}}, nil},
		{"one passing", []fakeInst{mk("n1", "a", "10.1.1.1", 80)}, []Endpoint{{ID: "n1/a", Address: "10.1.1.1:80"}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			safely(t, func() {
				h := newHarness(t)
				h.fc.Set("web", tc.insts...)
				ch, _ := h.watch(Query{Service: "web"})
				wantEndpoints(t, recv(t, ch), tc.want)
			})
		})
	}
}

// Consul reporting zero passing instances later is trusted: an empty set is sent.
func TestWatch_BecomingEmptyIsSent(t *testing.T) {
	safely(t, func() {
		h := newHarness(t)
		h.fc.Set("web", mk("n1", "a", "10.1.1.1", 80))
		ch, _ := h.watch(Query{Service: "web"})
		if len(recv(t, ch)) != 1 {
			t.Fatal("want 1 endpoint")
		}
		h.fc.Set("web")
		if got := recv(t, ch); len(got) != 0 {
			t.Fatalf("want empty set, got %+v", got)
		}
	})
}

// Endpoint mapping.
func TestWatch_EndpointMapping(t *testing.T) {
	tests := []struct {
		name string
		in   fakeInst
		want Endpoint
	}{
		{"service address", fakeInst{Node: "n1", NodeAddr: "10.0.0.1", ID: "web-1", Addr: "10.1.1.1", Port: 8080}, Endpoint{"n1/web-1", "10.1.1.1:8080"}},
		{"falls back to node address", fakeInst{Node: "n1", NodeAddr: "10.0.0.1", ID: "web-1", Port: 8080}, Endpoint{"n1/web-1", "10.0.0.1:8080"}},
		{"ipv6 service address bracketed", fakeInst{Node: "n2", NodeAddr: "10.0.0.2", ID: "x", Addr: "fd00::1", Port: 8080}, Endpoint{"n2/x", "[fd00::1]:8080"}},
		{"ipv6 node fallback bracketed", fakeInst{Node: "n3", NodeAddr: "2001:db8::2", ID: "y", Port: 9000}, Endpoint{"n3/y", "[2001:db8::2]:9000"}},
		{"hostname", fakeInst{Node: "n4", NodeAddr: "10.0.0.4", ID: "z", Addr: "web.internal", Port: 80}, Endpoint{"n4/z", "web.internal:80"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			safely(t, func() {
				h := newHarness(t)
				h.fc.Set("web", tc.in)
				ch, _ := h.watch(Query{Service: "web"})
				wantEndpoints(t, recv(t, ch), []Endpoint{tc.want})
			})
		})
	}
}

// Health and tag filtering are requested from Consul and honored.
func TestWatch_PassingAndTagFilter(t *testing.T) {
	insts := []fakeInst{
		mk("n1", "a", "10.1.1.1", 80, "v1", "prod"),
		mk("n2", "b", "10.1.1.2", 80, "v2"),
		{Node: "n3", NodeAddr: "10.0.0.3", ID: "c", Addr: "10.1.1.3", Port: 80, Tags: []string{"v2"}, Status: "critical"},
		{Node: "n4", NodeAddr: "10.0.0.4", ID: "d", Addr: "10.1.1.4", Port: 80, Tags: []string{"v2"}, Status: "warning"},
		mk("n5", "e", "10.1.1.5", 80, "v2", "prod"),
	}
	tests := []struct {
		name    string
		tag     string
		wantIDs []string
	}{
		{"no tag", "", []string{"n1/a", "n2/b", "n5/e"}},
		{"tag v2", "v2", []string{"n2/b", "n5/e"}},
		{"tag prod", "prod", []string{"n1/a", "n5/e"}},
		{"tag none match", "nope", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			safely(t, func() {
				h := newHarness(t)
				h.fc.Set("web", insts...)
				ch, _ := h.watch(Query{Service: "web", Tag: tc.tag})
				got := recv(t, ch)
				var ids []string
				for _, e := range got {
					ids = append(ids, e.ID)
				}
				if len(ids) != len(tc.wantIDs) {
					t.Fatalf("ids = %v, want %v", ids, tc.wantIDs)
				}
				for i := range ids {
					if ids[i] != tc.wantIDs[i] {
						t.Fatalf("ids = %v, want %v", ids, tc.wantIDs)
					}
				}
				r := h.fc.Reqs("web")[0]
				if !r.Passing {
					t.Fatal("request must carry the passing param")
				}
				if r.Tag != tc.tag {
					t.Fatalf("tag param = %q, want %q", r.Tag, tc.tag)
				}
			})
		})
	}
}

// Rule 4: send only on change; sorted by ID.
func TestWatch_SendsOnlyOnChange(t *testing.T) {
	a := mk("n1", "a", "10.1.1.1", 80)
	b := mk("n2", "b", "10.1.1.2", 80)
	c := mk("n3", "c", "10.1.1.3", 80)
	bAddr := mk("n2", "b", "10.1.1.9", 80)
	bPort := mk("n2", "b", "10.1.1.2", 81)
	ea := Endpoint{"n1/a", "10.1.1.1:80"}
	eb := Endpoint{"n2/b", "10.1.1.2:80"}
	ec := Endpoint{"n3/c", "10.1.1.3:80"}

	tests := []struct {
		name string
		next []fakeInst
		want []Endpoint // nil = nothing must be sent
	}{
		{"identical list, index bump", []fakeInst{a, b}, nil},
		{"same instances, different order", []fakeInst{b, a}, nil},
		{"instance added", []fakeInst{a, b, c}, []Endpoint{ea, eb, ec}},
		{"instance removed", []fakeInst{a}, []Endpoint{ea}},
		{"address changed", []fakeInst{a, bAddr}, []Endpoint{ea, {"n2/b", "10.1.1.9:80"}}},
		{"port changed", []fakeInst{a, bPort}, []Endpoint{ea, {"n2/b", "10.1.1.2:81"}}},
		{"added in unsorted order", []fakeInst{c, b, a}, []Endpoint{ea, eb, ec}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			safely(t, func() {
				h := newHarness(t)
				h.fc.Set("web", a, b)
				ch, _ := h.watch(Query{Service: "web"})
				wantEndpoints(t, recv(t, ch), []Endpoint{ea, eb})
				h.fc.Set("web", tc.next...)
				if tc.want == nil {
					expectNone(t, ch, 250*time.Millisecond)
					return
				}
				wantEndpoints(t, recv(t, ch), tc.want)
				expectNone(t, ch, 100*time.Millisecond)
			})
		})
	}
}

func TestWatch_InitialSetIsSorted(t *testing.T) {
	safely(t, func() {
		h := newHarness(t)
		h.fc.Set("web", mk("nz", "z", "10.1.1.3", 80), mk("na", "a", "10.1.1.1", 80), mk("nm", "m", "10.1.1.2", 80))
		ch, _ := h.watch(Query{Service: "web"})
		got := recv(t, ch)
		wantEndpoints(t, got, []Endpoint{{"na/a", "10.1.1.1:80"}, {"nm/m", "10.1.1.2:80"}, {"nz/z", "10.1.1.3:80"}})
	})
}

// Architecture section 11 row 17: duplicate addresses, lowest ID wins.
func TestWatch_DedupesByAddressKeepingLowestID(t *testing.T) {
	safely(t, func() {
		h := newHarness(t)
		h.fc.Set("web",
			mk("nodeB", "web", "10.1.1.5", 8080),
			mk("nodeA", "web", "10.1.1.5", 8080),
			mk("nodeC", "web", "10.1.1.6", 8080),
		)
		ch, _ := h.watch(Query{Service: "web"})
		wantEndpoints(t, recv(t, ch), []Endpoint{{"nodeA/web", "10.1.1.5:8080"}, {"nodeC/web", "10.1.1.6:8080"}})
	})
}

// Blocking-query protocol.
func TestWatch_BlockingQueryProtocol(t *testing.T) {
	t.Run("index and wait are carried", func(t *testing.T) {
		safely(t, func() {
			h := newHarness(t)
			h.fc.Set("web", mk("n1", "a", "10.1.1.1", 80)) // index 2
			ch, _ := h.watch(Query{Service: "web"})
			recv(t, ch)
			poll(t, 2*time.Second, "second request", func() bool { return len(h.fc.Reqs("web")) >= 2 })
			reqs := h.fc.Reqs("web")
			if reqs[0].HasIndex && reqs[0].Index != "0" {
				t.Fatalf("first request index = %q, want none or 0", reqs[0].Index)
			}
			if reqs[1].Index != "2" {
				t.Fatalf("second request index = %q, want 2", reqs[1].Index)
			}
			if d, err := time.ParseDuration(reqs[1].Wait); err != nil || d != testWait {
				t.Fatalf("wait = %q, want %v", reqs[1].Wait, testWait)
			}
			h.fc.Set("web", mk("n1", "a", "10.1.1.1", 80), mk("n2", "b", "10.1.1.2", 80)) // index 3
			recv(t, ch)
			poll(t, 2*time.Second, "request with index 3", func() bool {
				for _, r := range h.fc.Reqs("web") {
					if r.Index == "3" {
						return true
					}
				}
				return false
			})
		})
	})

	t.Run("wait timeout is neither a value nor an error", func(t *testing.T) {
		safely(t, func() {
			h := newHarness(t)
			h.fc.Set("web", mk("n1", "a", "10.1.1.1", 80))
			ch, _ := h.watch(Query{Service: "web"})
			recv(t, ch)
			poll(t, 3*time.Second, "3 requests", func() bool { return len(h.fc.Reqs("web")) >= 3 })
			expectNone(t, ch, 50*time.Millisecond)
			if ds := h.sl.Durations(); len(ds) != 0 {
				t.Fatalf("no error backoff expected after wait timeouts, got %v", ds)
			}
		})
	})

	// ADR 0007: "If the index goes backwards, reset it to 0."
	t.Run("index going backwards resets to 0", func(t *testing.T) {
		safely(t, func() {
			h := newHarness(t)
			ins := mk("n1", "a", "10.1.1.1", 80)
			h.fc.Set("web", ins)
			h.fc.Set("web", ins)
			h.fc.Set("web", ins) // index 4
			ch, _ := h.watch(Query{Service: "web"})
			recv(t, ch)
			poll(t, 2*time.Second, "request with index 4", func() bool {
				rs := h.fc.Reqs("web")
				return len(rs) > 0 && rs[len(rs)-1].Index == "4"
			})
			n := len(h.fc.Reqs("web"))
			h.fc.Inject(fault{kind: faultIndex, index: 2})
			poll(t, 2*time.Second, "reset request", func() bool {
				rs := h.fc.Reqs("web")
				for _, r := range rs[n:] {
					if r.Index == "" || r.Index == "0" {
						return true
					}
				}
				return false
			})
			// After the reset the real index (4) is adopted again.
			poll(t, 2*time.Second, "index 4 again", func() bool {
				rs := h.fc.Reqs("web")
				return rs[len(rs)-1].Index == "4" && len(rs) > n+1
			})
			expectNone(t, ch, 50*time.Millisecond) // unchanged set: nothing sent
		})
	})

	// ADR 0007: "If it is 0 or less, use 1."
	t.Run("index 0 uses 1", func(t *testing.T) {
		safely(t, func() {
			h := newHarness(t)
			h.fc.Set("web", mk("n1", "a", "10.1.1.1", 80)) // index 2
			ch, _ := h.watch(Query{Service: "web"})
			recv(t, ch)
			poll(t, 2*time.Second, "blocked request", func() bool { return h.fc.Inflight() >= 1 })
			n := len(h.fc.Reqs("web"))
			h.fc.Inject(fault{kind: faultIndex, index: 0})
			poll(t, 2*time.Second, "request with index 1", func() bool {
				for _, r := range h.fc.Reqs("web")[n:] {
					if r.Index == "1" {
						return true
					}
				}
				return false
			})
			expectNone(t, ch, 50*time.Millisecond)
		})
	})
}

// Errors keep the last known set and never send an empty one.
func TestWatch_ErrorsKeepLastKnownSet(t *testing.T) {
	kinds := []struct {
		name string
		f    fault
	}{
		{"http 500", fault{kind: fault500}},
		{"malformed json", fault{kind: faultBadJSON}},
		{"dropped connection", fault{kind: faultClose}},
	}
	a := mk("n1", "a", "10.1.1.1", 80)
	b := mk("n2", "b", "10.1.1.2", 80)
	for _, k := range kinds {
		for _, changed := range []bool{false, true} {
			name := k.name + "/same set after recovery"
			if changed {
				name = k.name + "/new set after recovery"
			}
			t.Run(name, func(t *testing.T) {
				safely(t, func() {
					h := newHarness(t)
					h.fc.Set("web", a)
					ch, _ := h.watch(Query{Service: "web"})
					wantEndpoints(t, recv(t, ch), []Endpoint{{"n1/a", "10.1.1.1:80"}})
					poll(t, 2*time.Second, "blocked request", func() bool { return h.fc.Inflight() >= 1 })

					h.fc.Inject(k.f, k.f, k.f)
					poll(t, 3*time.Second, "backoff sleeps", func() bool { return len(h.sl.Durations()) >= 2 })
					expectNone(t, ch, 50*time.Millisecond)

					if !changed {
						h.fc.Touch("web")
						expectNone(t, ch, 250*time.Millisecond)
						return
					}
					h.fc.Set("web", a, b)
					wantEndpoints(t, recv(t, ch), []Endpoint{{"n1/a", "10.1.1.1:80"}, {"n2/b", "10.1.1.2:80"}})
				})
			})
		}
	}
}

func TestWatch_BackoffGrowsAndResetsAfterSuccess(t *testing.T) {
	safely(t, func() {
		h := newHarness(t)
		a := mk("n1", "a", "10.1.1.1", 80)
		h.fc.Set("web", a)
		ch, _ := h.watch(Query{Service: "web"})
		recv(t, ch)
		poll(t, 2*time.Second, "blocked request", func() bool { return h.fc.Inflight() >= 1 })

		h.fc.Inject(fault{kind: fault500}, fault{kind: fault500}, fault{kind: fault500}, fault{kind: fault500}, fault{kind: fault500})
		poll(t, 3*time.Second, "5 sleeps", func() bool { return len(h.sl.Durations()) >= 5 })
		first := h.sl.Durations()
		for i := 1; i < len(first); i++ {
			if first[i] < first[i-1] || first[i] > testBackoffMax {
				t.Fatalf("bad backoff sequence %v", first)
			}
		}
		last := first[len(first)-1]
		if last <= first[0] {
			t.Fatalf("backoff did not grow: %v", first)
		}

		// A success (a changed set arrives) must reset the backoff.
		h.fc.Set("web", a, mk("n2", "b", "10.1.1.2", 80))
		recv(t, ch)
		poll(t, 2*time.Second, "blocked request", func() bool { return h.fc.Inflight() >= 1 })
		n := len(h.sl.Durations())
		h.fc.Inject(fault{kind: fault500})
		poll(t, 3*time.Second, "sleep after reset", func() bool { return len(h.sl.Durations()) > n })
		after := h.sl.Durations()[n]
		if after >= last {
			t.Fatalf("backoff not reset after success: first-run last=%v, after=%v", last, after)
		}
	})
}

// Rule 5: capacity 1, latest wins.
func TestWatch_LatestWins(t *testing.T) {
	safely(t, func() {
		h := newHarness(t)
		ch, _ := h.watch(Query{Service: "web"}) // initial (empty) set goes unread
		poll(t, 2*time.Second, "first request answered", func() bool { return len(h.fc.Reqs("web")) >= 2 })

		var insts []fakeInst
		for i := 1; i <= 5; i++ {
			insts = append(insts, mk("n"+strconv.Itoa(i), "s", "10.1.1."+strconv.Itoa(i), 80))
			h.fc.Set("web", insts...)
			time.Sleep(5 * time.Millisecond)
		}
		final := strconv.FormatUint(h.fc.Index("web"), 10)
		poll(t, 3*time.Second, "watcher caught up", func() bool {
			rs := h.fc.Reqs("web")
			return rs[len(rs)-1].Index == final
		})
		got := recv(t, ch)
		if len(got) != 5 {
			t.Fatalf("want newest set (5 endpoints), got %+v", got)
		}
		expectNone(t, ch, 100*time.Millisecond)
	})
}

// Rule 1: independent watches.
func TestWatch_IndependentWatches(t *testing.T) {
	safely(t, func() {
		h := newHarness(t)
		h.fc.Set("a", mk("n1", "a1", "10.1.1.1", 80))
		h.fc.Set("b", mk("n2", "b1", "10.2.2.2", 80))
		ch1, cancel1 := h.watch(Query{Service: "a"})
		ch2, _ := h.watch(Query{Service: "a"})
		ch3, _ := h.watch(Query{Service: "b"})

		wantEndpoints(t, recv(t, ch1), []Endpoint{{"n1/a1", "10.1.1.1:80"}})
		wantEndpoints(t, recv(t, ch2), []Endpoint{{"n1/a1", "10.1.1.1:80"}})
		wantEndpoints(t, recv(t, ch3), []Endpoint{{"n2/b1", "10.2.2.2:80"}})

		h.fc.Set("a", mk("n1", "a1", "10.1.1.1", 80), mk("n3", "a2", "10.1.1.3", 80))
		if len(recv(t, ch1)) != 2 || len(recv(t, ch2)) != 2 {
			t.Fatal("both watches on a must see 2 endpoints")
		}
		expectNone(t, ch3, 50*time.Millisecond)

		cancel1()
		expectClosed(t, ch1, 2*time.Second)

		h.fc.Set("a", mk("n1", "a1", "10.1.1.1", 80))
		if len(recv(t, ch2)) != 1 {
			t.Fatal("surviving watch must keep working")
		}
		h.fc.Set("b")
		if len(recv(t, ch3)) != 0 {
			t.Fatal("watch on b must see the empty set")
		}
	})
}

// Rule 6: sent slices are immutable to both sides.
func TestWatch_ConsumerMutationDoesNotCorrupt(t *testing.T) {
	safely(t, func() {
		h := newHarness(t)
		a, b := mk("n1", "a", "10.1.1.1", 80), mk("n2", "b", "10.1.1.2", 80)
		h.fc.Set("web", a, b)
		ch, _ := h.watch(Query{Service: "web"})
		got := recv(t, ch)
		got[0].ID, got[0].Address = "zzz", "zzz"
		got[1] = Endpoint{}

		// An unchanged set must still be recognized as unchanged.
		h.fc.Touch("web")
		expectNone(t, ch, 250*time.Millisecond)

		c := mk("n3", "c", "10.1.1.3", 80)
		h.fc.Set("web", a, b, c)
		wantEndpoints(t, recv(t, ch), []Endpoint{{"n1/a", "10.1.1.1:80"}, {"n2/b", "10.1.1.2:80"}, {"n3/c", "10.1.1.3:80"}})
	})
}

// Rule 7: cancellation aborts the in-flight blocking query, then closes the channel.
func TestWatch_CancelAbortsQueryAndClosesChannel(t *testing.T) {
	safely(t, func() {
		h := newHarness(t, func(o *ConsulOptions) { o.WaitTime = 30 * time.Second })
		h.fc.Set("web", mk("n1", "a", "10.1.1.1", 80))
		ch, cancel := h.watch(Query{Service: "web"})
		recv(t, ch)
		poll(t, 2*time.Second, "query blocked at server", func() bool { return h.fc.Inflight() >= 1 })

		start := time.Now()
		cancel()
		expectClosed(t, ch, 2*time.Second)
		if d := time.Since(start); d > 2*time.Second {
			t.Fatalf("shutdown took %v", d)
		}
		poll(t, 2*time.Second, "server saw request context end", func() bool { return h.fc.Aborted() >= 1 })

		// Closed means closed: no further values.
		if v, ok := <-ch; ok {
			t.Fatalf("value after close: %+v", v)
		}
	})
}

func TestWatch_CancelDuringBackoffClosesChannel(t *testing.T) {
	safely(t, func() {
		h := newHarness(t)
		h.fc.Close()
		ch, cancel := h.watch(Query{Service: "web"})
		poll(t, 2*time.Second, "backoff", func() bool { return len(h.sl.Durations()) >= 2 })
		cancel()
		expectClosed(t, ch, 2*time.Second)
	})
}

func TestWatch_NoGoroutineLeak(t *testing.T) {
	safely(t, func() {
		h := newHarness(t, func(o *ConsulOptions) { o.WaitTime = 30 * time.Second })
		h.fc.Set("web", mk("n1", "a", "10.1.1.1", 80))

		run := func(n int) {
			var cancels []context.CancelFunc
			var chans []<-chan []Endpoint
			for i := 0; i < n; i++ {
				ch, cancel := h.watch(Query{Service: "web"})
				chans = append(chans, ch)
				cancels = append(cancels, cancel)
			}
			for _, ch := range chans {
				recv(t, ch)
			}
			for _, c := range cancels {
				c()
			}
			for _, ch := range chans {
				expectClosed(t, ch, 2*time.Second)
			}
		}
		run(1) // warm up pools and lazy goroutines
		h.fc.srv.CloseClientConnections()
		time.Sleep(50 * time.Millisecond)
		base := runtime.NumGoroutine()

		run(8)
		h.fc.srv.CloseClientConnections()
		poll(t, 3*time.Second, "goroutines to drain", func() bool { return runtime.NumGoroutine() <= base+3 })
	})
}
