package metrics

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// seriesCount returns the number of series in the metric family name.
func seriesCount(t *testing.T, reg *prometheus.Registry, name string) int {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return len(mf.GetMetric())
		}
	}
	return 0
}

// M1: the method label is bounded to the standard verbs; everything else is "other".
func TestFixRound4_ObserveAPIRequestBoundsMethodLabel(t *testing.T) {
	defer guard(t)
	tests := []struct {
		method string
		want   string
	}{
		{"GET", "GET"}, {"HEAD", "HEAD"}, {"POST", "POST"}, {"PUT", "PUT"},
		{"PATCH", "PATCH"}, {"DELETE", "DELETE"}, {"OPTIONS", "OPTIONS"},
		{"get", "other"}, {"Post", "other"},
		{"", "other"},
		{"TRACE", "other"}, {"CONNECT", "other"}, {"PROPFIND", "other"},
		{"GET ", "other"},
		{strings.Repeat("X", 2048), "other"},
	}
	for _, tc := range tests {
		name := tc.method
		if len(name) > 12 {
			name = fmt.Sprintf("junk-%d-bytes", len(name))
		}
		t.Run("method="+name, func(t *testing.T) {
			defer guard(t)
			reg := prometheus.NewRegistry()
			m := NewControlPlane(reg)
			m.ObserveAPIRequest(tc.method, "GET /v1/config", 200, time.Millisecond)
			if ok, v, _ := find(t, reg, "tollgate_cp_api_requests_total",
				labels{"method": tc.want, "route": "GET /v1/config", "code": "200"}); !ok || v != 1 {
				t.Errorf("requests_total{method=%q} found=%v value=%v, want found with 1", tc.want, ok, v)
			}
			if ok, _, n := find(t, reg, "tollgate_cp_api_request_duration_seconds",
				labels{"method": tc.want, "route": "GET /v1/config"}); !ok || n != 1 {
				t.Errorf("duration{method=%q} found=%v count=%d, want found with 1", tc.want, ok, n)
			}
		})
	}
}

func TestFixRound4_JunkMethodsDoNotGrowSeries(t *testing.T) {
	defer guard(t)
	reg := prometheus.NewRegistry()
	m := NewControlPlane(reg)
	m.ObserveAPIRequest("GET", "GET /healthz", 200, time.Millisecond)
	before := seriesCount(t, reg, "tollgate_cp_api_requests_total")
	if before != 1 {
		t.Fatalf("baseline series = %d, want 1", before)
	}
	for i := 0; i < 500; i++ {
		m.ObserveAPIRequest(fmt.Sprintf("JUNK%d-%s", i, strings.Repeat("x", 2048)), "unmatched", 400, time.Millisecond)
	}
	if got := seriesCount(t, reg, "tollgate_cp_api_requests_total"); got != before+1 {
		t.Errorf("series after 500 junk methods = %d, want %d (exactly one new series, method=other)", got, before+1)
	}
	if ok, v, _ := find(t, reg, "tollgate_cp_api_requests_total",
		labels{"method": "other", "route": "unmatched", "code": "400"}); !ok || v != 500 {
		t.Errorf("method=other counter found=%v value=%v, want 500", ok, v)
	}
}
