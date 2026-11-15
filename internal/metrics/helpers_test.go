package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// guard turns a panic (such as the Phase 6 stubs) into a test failure so the
// rest of the binary keeps running. Use as `defer guard(t)`.
func guard(t *testing.T) {
	t.Helper()
	if r := recover(); r != nil {
		t.Fatalf("panicked: %v", r)
	}
}

type labels map[string]string

// find returns the first metric of family name whose labels equal want exactly.
func find(t *testing.T, reg *prometheus.Registry, name string, want labels) (found bool, val float64, count uint64) {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			got := labels{}
			for _, lp := range m.GetLabel() {
				got[lp.GetName()] = lp.GetValue()
			}
			if len(got) != len(want) {
				continue
			}
			ok := true
			for k, v := range want {
				if got[k] != v {
					ok = false
				}
			}
			if !ok {
				continue
			}
			switch {
			case m.GetHistogram() != nil:
				return true, m.GetHistogram().GetSampleSum(), m.GetHistogram().GetSampleCount()
			case m.GetCounter() != nil:
				return true, m.GetCounter().GetValue(), 0
			case m.GetGauge() != nil:
				return true, m.GetGauge().GetValue(), 0
			}
		}
	}
	return false, 0, 0
}

// value fails the test if the series is absent.
func value(t *testing.T, reg *prometheus.Registry, name string, want labels) float64 {
	t.Helper()
	ok, v, _ := find(t, reg, name, want)
	if !ok {
		t.Fatalf("series %s%v not found", name, want)
	}
	return v
}

func has(t *testing.T, reg *prometheus.Registry, name string, want labels) bool {
	t.Helper()
	ok, _, _ := find(t, reg, name, want)
	return ok
}

// family returns the gathered family or nil.
func familyType(t *testing.T, reg *prometheus.Registry, name string) (typ string, help string, labelNames []string, ok bool) {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		if len(mf.GetMetric()) == 0 {
			return "", "", nil, false
		}
		for _, lp := range mf.GetMetric()[0].GetLabel() {
			labelNames = append(labelNames, lp.GetName())
		}
		return mf.GetType().String(), mf.GetHelp(), labelNames, true
	}
	return "", "", nil, false
}
