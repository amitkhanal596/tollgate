package balancer

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkNextDone measures one pick plus its release under full
// parallelism, for each algorithm with 1 and 3 backends. It guards the
// claim that the pick is lock-free and allocation-free, and that its cost
// does not grow with the backend count in a way that matters (Phase 8).
//
//	go test -run '^$' -bench BenchmarkNextDone -benchmem ./internal/balancer/
func BenchmarkNextDone(b *testing.B) {
	algos := []struct {
		name string
		mk   func([]*Backend, func() time.Time) Balancer
	}{
		{"round_robin", NewRoundRobin},
		{"least_conn", NewLeastConn},
	}
	for _, a := range algos {
		for _, n := range []int{1, 3} {
			b.Run(fmt.Sprintf("%s/backends=%d", a.name, n), func(b *testing.B) {
				bs := make([]*Backend, n)
				for i := range bs {
					raw := fmt.Sprintf("http://10.0.0.%d:80", i+1)
					bs[i] = NewBackend("svc|"+raw, mustURL(b, raw))
				}
				lb := a.mk(bs, time.Now)
				b.ReportAllocs()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						be, err := lb.Next()
						if err != nil {
							b.Error(err)
							return
						}
						lb.Done(be)
					}
				})
			})
		}
	}
}
