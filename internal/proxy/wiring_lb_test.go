package proxy

import (
	"sync"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// L1: `algorithm: least_conn` must be honoured by the snapshot builder
// (ADR 0003), and round_robin must keep ignoring in-flight counts.
func TestWiringAlgorithmHonoured(t *testing.T) {
	tests := []struct {
		name  string
		algo  config.Algorithm
		quick int
		// check receives the /quick hit counts of A (busy), B, C.
		check func(t *testing.T, a, b, c int64)
	}{
		{
			name: "least_conn avoids the busy backend", algo: config.AlgorithmLeastConn, quick: 20,
			check: func(t *testing.T, a, b, c int64) {
				if a != 0 || b+c != 20 {
					t.Errorf("quick hits A=%d B=%d C=%d, want A=0 and B+C=20 (A holds a request open)", a, b, c)
				}
			},
		},
		{
			name: "round_robin still sends about a third to the busy backend", algo: config.AlgorithmRoundRobin, quick: 30,
			check: func(t *testing.T, a, b, c int64) {
				if a < 8 || a > 12 {
					t.Errorf("quick hits A=%d B=%d C=%d, want A near 10 of 30", a, b, c)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, b, c := newCtrlBackend(t, "A"), newCtrlBackend(t, "B"), newCtrlBackend(t, "C")
			release := make(chan struct{})
			a.hold.Store(&release) // only A blocks on /hold; B and C answer at once

			_, srv := startGatewayWith(t, Options{}, cfgOf(1,
				[]config.Route{rt("r", "", "/", "u")},
				upx("u", tc.algo, nil, nil, a.url(), b.url(), c.url())))
			// Cleanups run LIFO: release first, then wait for the client
			// goroutines, and only then the gateway server's Close (which
			// would otherwise block on the held requests).
			client := newClient(t)
			var wg sync.WaitGroup
			t.Cleanup(wg.Wait)
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(release) }) })

			// Fire /hold requests until one lands on A and blocks there.
			for i := 0; i < 15 && a.holding.Load() == 0; i++ {
				done := make(chan struct{})
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer close(done)
					resp, err := client.Get(srv.URL + "/hold")
					if err == nil {
						resp.Body.Close()
					}
				}()
				// Either B/C answered (done) or A is now holding it.
				waitFor(t, 2*time.Second, "hold request to finish or block on A", func() bool {
					select {
					case <-done:
						return true
					default:
						return a.holding.Load() > 0
					}
				})
			}
			if a.holding.Load() == 0 {
				t.Fatal("could not park a request on backend A")
			}

			for i := 0; i < tc.quick; i++ {
				if code := getCode(t, client, srv.URL+"/quick"); code != 200 {
					t.Fatalf("quick request %d: status %d", i, code)
				}
			}
			tc.check(t, a.quick.Load(), b.quick.Load(), c.quick.Load())
		})
	}
}
