package balancer

import (
	"math"
	"testing"
)

// D7: the tie-break must spread ties evenly among the backends that are
// actually tied, even when other backends are unavailable or busier. A bare
// rotating scan start sends all ties that fall "behind" an excluded backend
// to the one that follows it.

// pickAndRelease does n sequential Next+Done and counts picks per backend.
func pickAndRelease(t *testing.T, lb Balancer, n int) map[*Backend]int {
	t.Helper()
	got := map[*Backend]int{}
	for i := 0; i < n; i++ {
		b, err := lb.Next()
		if err != nil {
			t.Fatalf("Next #%d: %v", i, err)
		}
		got[b]++
		lb.Done(b)
	}
	return got
}

func within(got, want, tol int) bool { return int(math.Abs(float64(got-want))) <= tol }

func TestLeastConnTieBreakSpreadsEvenly(t *testing.T) {
	const picks = 3000
	tests := []struct {
		name  string
		setup func(bs []*Backend) // mutates state; returns nothing
		n     int                 // number of backends
		// tied lists the indexes that are tied at the minimum and available.
		tied []int
		// others must never be picked.
		others []int
	}{
		{
			name:  "first backend unavailable",
			n:     3,
			setup: func(bs []*Backend) { bs[0].SetActiveHealthy(false) },
			tied:  []int{1, 2}, others: []int{0},
		},
		{
			name:  "middle backend unavailable",
			n:     3,
			setup: func(bs []*Backend) { bs[1].SetActiveHealthy(false) },
			tied:  []int{0, 2}, others: []int{1},
		},
		{
			name:  "first backend busy with one long request",
			n:     3,
			setup: func(bs []*Backend) { bs[0].acquire() },
			tied:  []int{1, 2}, others: []int{0},
		},
		{
			name: "two of four tied at the minimum",
			n:    4,
			setup: func(bs []*Backend) {
				bs[0].acquire()
				bs[2].acquire()
			},
			tied: []int{1, 3}, others: []int{0, 2},
		},
		{
			name: "two of four tied, busy ones adjacent",
			n:    4,
			setup: func(bs []*Backend) {
				bs[1].acquire()
				bs[2].acquire()
			},
			tied: []int{0, 3}, others: []int{1, 2},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bs := makeBackends(t, tc.n)
			tc.setup(bs)
			lb := NewLeastConn(bs, (&fakeClock{t: t0}).Now)
			got := pickAndRelease(t, lb, picks)

			for _, i := range tc.others {
				if got[bs[i]] != 0 {
					t.Errorf("backend %d picked %d times, want 0", i, got[bs[i]])
				}
			}
			want := picks / len(tc.tied)
			for _, i := range tc.tied {
				if !within(got[bs[i]], want, 1) {
					t.Errorf("backend %d picked %d times, want %d (+-1); all picks: %v",
						i, got[bs[i]], want, summarize(bs, got))
				}
			}
		})
	}
}

func summarize(bs []*Backend, got map[*Backend]int) []int {
	out := make([]int, len(bs))
	for i, b := range bs {
		out[i] = got[b]
	}
	return out
}
