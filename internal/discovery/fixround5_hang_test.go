package discovery

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	consulapi "github.com/hashicorp/consul/api"
)

// hangingAgent accepts connections and reads the request but never answers.
type hangingAgent struct {
	srv     *httptest.Server
	release chan struct{}
	reqs    atomic.Int64
	once    sync.Once
}

func newHangingAgent(t *testing.T) *hangingAgent {
	t.Helper()
	a := &hangingAgent{release: make(chan struct{})}
	a.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.reqs.Add(1)
		select {
		case <-a.release:
		case <-r.Context().Done():
		}
	}))
	// LIFO: release the stuck handlers before Close waits for them.
	t.Cleanup(a.srv.Close)
	t.Cleanup(func() { a.once.Do(func() { close(a.release) }) })
	return a
}

// V1: a Consul agent that accepts the connection and never answers must not
// stall the watch forever. Each query gets its own deadline derived from
// WaitTime (+ WaitTime/16 + QueryMargin); a query that hits it is an error
// (backoff sleep), and the watcher retries.
func TestFixRound5_HungAgentIsAbandonedCountedAndRetried(t *testing.T) {
	safely(t, func() {
		a := newHangingAgent(t)
		client, err := consulapi.NewClient(&consulapi.Config{Address: a.srv.Listener.Addr().String()})
		if err != nil {
			t.Fatal(err)
		}
		sl := &sleepRec{}
		const wait = 100 * time.Millisecond
		c := NewConsul(client, ConsulOptions{
			WaitTime: wait, QueryMargin: 50 * time.Millisecond,
			BackoffMin: testBackoffMin, BackoffMax: testBackoffMax, MinInterval: testMinIv,
			Sleep: sl.Sleep,
		})
		start := time.Now()
		ch, err := c.Watch(t.Context(), Query{Service: "web"})
		if err != nil {
			t.Fatal(err)
		}

		poll(t, 2*time.Second, "the hung query to be abandoned and counted as an error (backoff sleep)",
			func() bool { return len(sl.Durations()) >= 1 })
		if el := time.Since(start); el > 2*time.Second {
			t.Errorf("hung query abandoned after %v, want within 2s", el)
		}
		// It must not give up on the service: a second query follows.
		poll(t, 2*time.Second, "the watcher to retry after abandoning the hung query",
			func() bool { return a.reqs.Load() >= 2 })
		expectNone(t, ch, 20*time.Millisecond) // an error never sends a set
	})
}
