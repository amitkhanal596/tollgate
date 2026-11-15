package controlplane

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

func TestHub_SubscribeOnFreshHubIsEmpty(t *testing.T) {
	defer failOnPanic(t)
	h := NewHub()
	defer h.Close()
	ch, cancel := h.Subscribe()
	defer cancel()
	if cap(ch) != 1 {
		t.Fatalf("cap(ch) = %d, want 1 (capacity-1 slot)", cap(ch))
	}
	expectNoRecv(t, ch, 30*time.Millisecond)
}

func TestHub_PublishThenSubscribeIsPrimed(t *testing.T) {
	defer failOnPanic(t)
	h := NewHub()
	defer h.Close()
	h.Publish(cfgV(1))
	h.Publish(cfgV(2))
	ch, cancel := h.Subscribe()
	defer cancel()
	if got := recvWithin(t, ch, time.Second); got.Version != 2 {
		t.Fatalf("primed with v%d, want the max (v2)", got.Version)
	}
	expectNoRecv(t, ch, 30*time.Millisecond)
}

func TestHub_SubscribersReceiveLaterPublishes(t *testing.T) {
	defer failOnPanic(t)
	h := NewHub()
	defer h.Close()
	a, cancelA := h.Subscribe()
	defer cancelA()
	b, cancelB := h.Subscribe()
	defer cancelB()
	cfg := cfgV(1)
	h.Publish(cfg)
	for name, ch := range map[string]<-chan *config.Config{"a": a, "b": b} {
		if got := recvWithin(t, ch, time.Second); got != cfg {
			t.Fatalf("%s got %p, want the very same *Config (shared, immutable)", name, got)
		}
	}
}

func TestHub_HighestVersionWins(t *testing.T) {
	defer failOnPanic(t)
	cases := []struct {
		name string
		seq  []int64
		want int64
	}{
		{"ascending, never read", []int64{1, 2, 3}, 3},
		{"out of order: v6 then v5 (row 5)", []int64{6, 5}, 6},
		{"duplicate ignored", []int64{3, 3}, 3},
		{"lower after higher", []int64{5, 6, 4, 2}, 6},
		{"single", []int64{9}, 9},
		{"long descending run", []int64{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}, 10},
	}
	for _, tc := range cases {
		run(t, tc.name, func(t *testing.T) {
			h := NewHub()
			defer h.Close()
			ch, cancel := h.Subscribe()
			defer cancel()
			for _, v := range tc.seq {
				h.Publish(cfgV(v))
			}
			got := recvWithin(t, ch, time.Second)
			if got.Version != tc.want {
				t.Fatalf("received v%d, want v%d", got.Version, tc.want)
			}
			// Nothing older (or anything else) may follow.
			expectNoRecv(t, ch, 30*time.Millisecond)
			// A late subscriber is primed with the same max.
			late, cancelLate := h.Subscribe()
			defer cancelLate()
			if got := recvWithin(t, late, time.Second); got.Version != tc.want {
				t.Fatalf("late subscriber primed with v%d, want v%d", got.Version, tc.want)
			}
		})
	}
}

func TestHub_InterleavedReadsNeverGoBackwards(t *testing.T) {
	defer failOnPanic(t)
	h := NewHub()
	defer h.Close()
	ch, cancel := h.Subscribe()
	defer cancel()
	h.Publish(cfgV(1))
	if got := recvWithin(t, ch, time.Second); got.Version != 1 {
		t.Fatalf("got v%d", got.Version)
	}
	h.Publish(cfgV(2))
	h.Publish(cfgV(1)) // late, must be ignored
	h.Publish(cfgV(2)) // equal, must be ignored
	if got := recvWithin(t, ch, time.Second); got.Version != 2 {
		t.Fatalf("got v%d, want v2", got.Version)
	}
	expectNoRecv(t, ch, 30*time.Millisecond)
}

func TestHub_PublishNilIsNoop(t *testing.T) {
	defer failOnPanic(t)
	h := NewHub()
	defer h.Close()
	ch, cancel := h.Subscribe()
	defer cancel()
	h.Publish(nil)
	expectNoRecv(t, ch, 30*time.Millisecond)
	h.Publish(cfgV(1))
	if got := recvWithin(t, ch, time.Second); got.Version != 1 {
		t.Fatalf("got v%d", got.Version)
	}
}

func TestHub_PublishNeverBlocksOnSlowSubscribers(t *testing.T) {
	defer failOnPanic(t)
	h := NewHub()
	defer h.Close()
	for i := 0; i < 10; i++ {
		_, cancel := h.Subscribe() // never read
		defer cancel()
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for v := int64(1); v <= 2000; v++ {
			h.Publish(cfgV(v))
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Publish blocked on non-reading subscribers")
	}
}

func TestHub_Cancel(t *testing.T) {
	defer failOnPanic(t)
	h := NewHub()
	defer h.Close()
	a, cancelA := h.Subscribe()
	b, cancelB := h.Subscribe()
	defer cancelB()

	cancelA()
	expectClosed(t, a, time.Second)
	cancelA() // idempotent
	cancelA()

	// Publishing after cancel must neither panic (send on closed channel) nor
	// disturb other subscribers.
	h.Publish(cfgV(1))
	if got := recvWithin(t, b, time.Second); got.Version != 1 {
		t.Fatalf("b got v%d", got.Version)
	}
	// A canceled subscriber receives nothing more.
	if c, ok := tryRecv(a); ok || c != nil {
		t.Fatalf("canceled subscriber received %v", c)
	}
}

func TestHub_Close(t *testing.T) {
	defer failOnPanic(t)
	h := NewHub()
	var chans []<-chan *config.Config
	var cancels []func()
	for i := 0; i < 5; i++ {
		ch, cancel := h.Subscribe()
		chans = append(chans, ch)
		cancels = append(cancels, cancel)
	}
	h.Publish(cfgV(1)) // leaves a pending value in every slot
	h.Close()
	for i, ch := range chans {
		expectClosed(t, ch, time.Second) // pending value may drain first, then closed
		_ = i
	}
	h.Close() // idempotent
	for _, c := range cancels {
		c() // cancel after Close is safe
	}

	// Documented behaviour after Close: Publish is a no-op, Subscribe returns
	// an already-closed channel and a no-op cancel.
	h.Publish(cfgV(2))
	ch, cancel := h.Subscribe()
	expectClosed(t, ch, time.Second)
	cancel()
	cancel()
}

func TestHub_ManySubscribers(t *testing.T) {
	defer failOnPanic(t)
	const subs, versions = 200, 50
	h := NewHub()
	defer h.Close()
	type result struct {
		last   int64
		sorted bool
	}
	results := make([]result, subs)
	var wg sync.WaitGroup
	for i := 0; i < subs; i++ {
		ch, cancel := h.Subscribe()
		defer cancel()
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := result{sorted: true}
			timeout := time.After(5 * time.Second)
			for r.last < versions {
				select {
				case c, ok := <-ch:
					if !ok {
						results[i] = r
						return
					}
					if c.Version <= r.last {
						r.sorted = false
					}
					r.last = c.Version
				case <-timeout:
					results[i] = r
					return
				}
			}
			results[i] = r
		}(i)
	}
	for v := int64(1); v <= versions; v++ {
		h.Publish(cfgV(v))
	}
	wg.Wait()
	for i, r := range results {
		if r.last != versions || !r.sorted {
			t.Fatalf("subscriber %d ended at v%d (strictly increasing: %v), want v%d", i, r.last, r.sorted, versions)
		}
	}
}

func TestHub_ConcurrentPublishSubscribeCancel(t *testing.T) {
	defer failOnPanic(t)
	const total, publishers, drainers, churners = 400, 8, 12, 8
	h := NewHub()
	defer h.Close()

	// Long-lived subscribers, created before any publish.
	type result struct {
		last int64
		bad  bool
	}
	results := make([]result, drainers)
	var drain sync.WaitGroup
	cancels := make([]func(), drainers)
	for i := 0; i < drainers; i++ {
		ch, cancel := h.Subscribe()
		cancels[i] = cancel
		drain.Add(1)
		go func(i int) {
			defer drain.Done()
			timeout := time.After(10 * time.Second)
			for results[i].last < total {
				select {
				case c, ok := <-ch:
					if !ok {
						return
					}
					if c.Version <= results[i].last {
						results[i].bad = true // went backwards or repeated
					}
					results[i].last = c.Version
				case <-timeout:
					return
				}
			}
		}(i)
	}

	// Publishers each publish a shuffled slice of 1..total.
	rng := rand.New(rand.NewSource(1))
	all := rng.Perm(total)
	var pub sync.WaitGroup
	for p := 0; p < publishers; p++ {
		pub.Add(1)
		go func(part []int) {
			defer pub.Done()
			for _, v := range part {
				h.Publish(cfgV(int64(v + 1)))
			}
		}(all[p*total/publishers : (p+1)*total/publishers])
	}
	// Churners subscribe, read a little and cancel, repeatedly.
	stop := make(chan struct{})
	var churn sync.WaitGroup
	for c := 0; c < churners; c++ {
		churn.Add(1)
		go func() {
			defer churn.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				ch, cancel := h.Subscribe()
				select {
				case <-ch:
				case <-time.After(time.Millisecond):
				}
				cancel()
				cancel()
			}
		}()
	}
	pub.Wait()
	drain.Wait()
	close(stop)
	churn.Wait()

	for i, r := range results {
		if r.last != total {
			t.Errorf("subscriber %d ended on v%d, want v%d", i, r.last, total)
		}
		if r.bad {
			t.Errorf("subscriber %d saw a non-increasing version", i)
		}
	}
	for _, c := range cancels {
		c()
	}
	// The max survived all the churn.
	ch, cancel := h.Subscribe()
	defer cancel()
	if got := recvWithin(t, ch, time.Second); got.Version != total {
		t.Fatalf("final prime v%d, want v%d", got.Version, total)
	}
}
