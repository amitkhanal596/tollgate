package main

import (
	"testing"
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
