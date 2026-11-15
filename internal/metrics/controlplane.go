package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ControlPlaneMetrics holds the control-plane collectors.
type ControlPlaneMetrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
	head     prometheus.Gauge
	planes   prometheus.Gauge
}

// NewControlPlane registers the control-plane collectors on reg.
func NewControlPlane(reg prometheus.Registerer) *ControlPlaneMetrics {
	m := &ControlPlaneMetrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tollgate_cp_api_requests_total", Help: "Control-plane API requests by method, route pattern and status code.",
		}, []string{"method", "route", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "tollgate_cp_api_request_duration_seconds", Help: "Control-plane API latency in seconds.",
			Buckets: latencyBuckets,
		}, []string{"method", "route"}),
		head: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tollgate_cp_config_head_version", Help: "Latest config version stored by the control plane.",
		}),
		planes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tollgate_cp_dataplanes_connected", Help: "Data planes currently connected to the config stream.",
		}),
	}
	reg.MustRegister(m.requests, m.duration, m.head, m.planes)
	return m
}

// ObserveAPIRequest records one API request; pattern is the route pattern, never the raw path.
func (m *ControlPlaneMetrics) ObserveAPIRequest(method, pattern string, status int, d time.Duration) {
	if m == nil {
		return
	}
	m.requests.WithLabelValues(method, pattern, strconv.Itoa(status)).Inc()
	m.duration.WithLabelValues(method, pattern).Observe(d.Seconds())
}

// SetHeadVersion records the head config version.
func (m *ControlPlaneMetrics) SetHeadVersion(v int64) {
	if m == nil {
		return
	}
	m.head.Set(float64(v))
}

// SetConnectedDataPlanes records the number of connected data planes.
func (m *ControlPlaneMetrics) SetConnectedDataPlanes(n int) {
	if m == nil {
		return
	}
	m.planes.Set(float64(n))
}
