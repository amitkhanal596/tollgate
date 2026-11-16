// Package metrics defines Tollgate's Prometheus collectors and the access-log helper.
//
// Label-cardinality rule: labels are only route NAMES, upstream NAMES and
// backend IDs (all bounded by the loaded config), plus small closed sets such
// as status code and reason. Raw request paths, client IDs, IPs and query
// strings must never become label values, or the series count is unbounded.
//
// The registry is injected into New / NewControlPlane; this package has no
// package-level collectors or other global mutable state.
package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RouteNone labels requests that never matched a route/upstream (400/404), keeping cardinality bounded.
const RouteNone = "none"

// latencyBuckets span 0.5ms to 10s, suited to a proxy's request latencies.
var latencyBuckets = []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Metrics holds the data-plane collectors.
type Metrics struct {
	requests     *prometheus.CounterVec
	duration     *prometheus.HistogramVec
	rateLimited  *prometheus.CounterVec
	authFailures *prometheus.CounterVec
	backendUp    *prometheus.GaugeVec
	inflight     *prometheus.GaugeVec
	ejections    *prometheus.CounterVec
	configVer    prometheus.Gauge
	applyFail    prometheus.Counter
	streamUp     prometheus.Gauge
	logDropped   prometheus.Counter

	codes [600]string // status -> decimal string, built once so the hot path does not allocate
}

// New registers the gateway collectors on reg. It panics on duplicate registration.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tollgate_requests_total", Help: "Proxied requests by route, upstream and HTTP status code.",
		}, []string{"route", "upstream", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "tollgate_request_duration_seconds", Help: "Request latency in seconds by route and upstream.",
			Buckets: latencyBuckets,
		}, []string{"route", "upstream"}),
		rateLimited: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tollgate_ratelimited_total", Help: "Requests rejected by the rate limiter, by route and reason.",
		}, []string{"route", "reason"}),
		authFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tollgate_auth_failures_total", Help: "Requests rejected by authentication, by route and reason.",
		}, []string{"route", "reason"}),
		backendUp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tollgate_backend_up",
			Help: "1 if the backend is in rotation (its active health check passes and it is not passively ejected), else 0.",
		}, []string{"upstream", "backend"}),
		inflight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "tollgate_backend_inflight", Help: "Requests currently in flight to the backend.",
		}, []string{"upstream", "backend"}),
		ejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "tollgate_backend_ejections_total", Help: "Times the backend was ejected from rotation.",
		}, []string{"upstream", "backend"}),
		configVer: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tollgate_config_version", Help: "Version of the config snapshot currently applied.",
		}),
		applyFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tollgate_config_apply_failures_total", Help: "Config snapshots that failed to apply.",
		}),
		streamUp: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "tollgate_config_stream_connected", Help: "1 if the control-plane config stream is connected, else 0.",
		}),
		logDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "tollgate_access_log_dropped_total",
			Help: "Access-log lines dropped because the log sink could not keep up (the async buffer was full).",
		}),
	}
	for i := 100; i < len(m.codes); i++ {
		m.codes[i] = strconv.Itoa(i)
	}
	reg.MustRegister(m.requests, m.duration, m.rateLimited, m.authFailures, m.backendUp,
		m.inflight, m.ejections, m.configVer, m.applyFail, m.streamUp, m.logDropped)
	return m
}

func orNone(s string) string {
	if s == "" {
		return RouteNone
	}
	return s
}

// ObserveRequest records one completed request: it counts it in
// tollgate_requests_total and observes its latency. Empty route/upstream
// become RouteNone.
func (m *Metrics) ObserveRequest(route, upstream string, status int, d time.Duration) {
	if m == nil {
		return
	}
	route, upstream = orNone(route), orNone(upstream)
	m.requests.WithLabelValues(route, upstream, m.code(status)).Inc()
	m.duration.WithLabelValues(route, upstream).Observe(d.Seconds())
}

// CountRequest counts one request in tollgate_requests_total WITHOUT
// observing a latency. It is for upgraded connections (101 Switching
// Protocols): their duration is how long a socket stayed open, not a request
// latency, and would wreck the histogram's percentiles.
func (m *Metrics) CountRequest(route, upstream string, status int) {
	if m == nil {
		return
	}
	m.requests.WithLabelValues(orNone(route), orNone(upstream), m.code(status)).Inc()
}

// code returns the label value for status without allocating for 100-599.
func (m *Metrics) code(status int) string {
	if status >= 100 && status < len(m.codes) {
		return m.codes[status]
	}
	return strconv.Itoa(status) // out-of-range codes are rare; allocation is acceptable
}

// ForgetRoute deletes every series labelled route="<route>" from the four
// route-labelled families (requests, duration, rate-limited, auth
// failures). The gateway calls it for route names that a new config no
// longer has, so renamed or removed routes do not leave dead series behind.
// Unknown routes and "" are no-ops.
func (m *Metrics) ForgetRoute(route string) {
	if m == nil || route == "" {
		return
	}
	l := prometheus.Labels{"route": route}
	m.requests.DeletePartialMatch(l)
	m.duration.DeletePartialMatch(l)
	m.rateLimited.DeletePartialMatch(l)
	m.authFailures.DeletePartialMatch(l)
}

// AccessLogDropped counts one access-log line dropped by the AsyncWriter.
func (m *Metrics) AccessLogDropped() {
	if m == nil {
		return
	}
	m.logDropped.Inc()
}

// RateLimited counts a request rejected by the limiter.
func (m *Metrics) RateLimited(route, reason string) {
	if m == nil {
		return
	}
	m.rateLimited.WithLabelValues(orNone(route), reason).Inc()
}

// AuthFailed counts a request rejected by authentication.
func (m *Metrics) AuthFailed(route, reason string) {
	if m == nil {
		return
	}
	m.authFailures.WithLabelValues(orNone(route), reason).Inc()
}

// SetBackendUp sets the backend health gauge.
func (m *Metrics) SetBackendUp(upstream, backend string, up bool) {
	if m == nil {
		return
	}
	v := 0.0
	if up {
		v = 1
	}
	m.backendUp.WithLabelValues(upstream, backend).Set(v)
}

// SetBackendInFlight sets the backend in-flight gauge.
func (m *Metrics) SetBackendInFlight(upstream, backend string, n int64) {
	if m == nil {
		return
	}
	m.inflight.WithLabelValues(upstream, backend).Set(float64(n))
}

// BackendEjected counts one ejection of the backend.
func (m *Metrics) BackendEjected(upstream, backend string) {
	if m == nil {
		return
	}
	m.ejections.WithLabelValues(upstream, backend).Inc()
}

// ForgetBackend drops all series for a backend removed from config, so stale series do not linger.
func (m *Metrics) ForgetBackend(upstream, backend string) {
	if m == nil {
		return
	}
	m.backendUp.DeleteLabelValues(upstream, backend)
	m.inflight.DeleteLabelValues(upstream, backend)
	m.ejections.DeleteLabelValues(upstream, backend)
}

// SetConfigVersion records the applied config version.
func (m *Metrics) SetConfigVersion(v int64) {
	if m == nil {
		return
	}
	m.configVer.Set(float64(v))
}

// ConfigApplyFailed counts a failed config apply.
func (m *Metrics) ConfigApplyFailed() {
	if m == nil {
		return
	}
	m.applyFail.Inc()
}

// SetConfigStreamConnected records whether the config stream is connected.
func (m *Metrics) SetConfigStreamConnected(up bool) {
	if m == nil {
		return
	}
	v := 0.0
	if up {
		v = 1
	}
	m.streamUp.Set(v)
}

// Handler serves the metrics from g in the Prometheus text format.
func Handler(g prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(g, promhttp.HandlerOpts{})
}
