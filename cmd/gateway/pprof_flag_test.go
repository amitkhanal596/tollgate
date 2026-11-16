package main

import (
	"runtime"
	"testing"
	"time"
)

// -pprof is a debugging switch: off by default (the profiling endpoints
// must not exist on the admin port), and when on they are served on the
// admin listener only, never on the traffic port.
func TestPprofFlag(t *testing.T) {
	up := okUpstream(t, 0)
	tests := []struct {
		name        string
		extra       []string
		wantAdmin   int
		wantTraffic int
	}{
		{name: "off by default", wantAdmin: 404, wantTraffic: 404},
		{name: "on", extra: []string{"-pprof"}, wantAdmin: 200, wantTraffic: 404},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := startRun(t, writeCfg(t, up.URL), tc.extra...)
			r.waitReady(t)
			for _, p := range []string{"/debug/pprof/", "/debug/pprof/goroutine?debug=1"} {
				if code, _, err := get("http://" + r.admin + p); err != nil || code != tc.wantAdmin {
					t.Errorf("admin %s = %d %v, want %d", p, code, err, tc.wantAdmin)
				}
				if code, _, err := get("http://" + r.traffic + p); err != nil || code != tc.wantTraffic {
					t.Errorf("traffic %s = %d %v, want %d", p, code, err, tc.wantTraffic)
				}
			}
		})
	}
}

// -pprof turns on mutex sampling for the life of the process; on exit it must
// put back what was there before, not reset to 0 (an embedding process or a
// test that had its own rate would silently lose it). Not parallel: the
// profile rate is process-global.
func TestPprofRestoresMutexProfileFraction(t *testing.T) {
	prev := runtime.SetMutexProfileFraction(7) // returns the previous rate
	t.Cleanup(func() { runtime.SetMutexProfileFraction(prev) })

	up := okUpstream(t, 0)
	r := startRun(t, writeCfg(t, up.URL), "-pprof")
	r.waitReady(t)
	if got := runtime.SetMutexProfileFraction(-1); got == 7 {
		t.Fatalf("mutex profile fraction is still 7 while -pprof runs; the flag did not enable sampling")
	}
	r.cancel()
	r.wait(t, 10*time.Second)
	if got := runtime.SetMutexProfileFraction(-1); got != 7 {
		t.Errorf("mutex profile fraction after -pprof run = %d, want 7 (the value before the run)", got)
	}
}
