package proxy

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// Upstream connection-pool sizing, per pooled transport.
//
// The rule: the pool can keep every connection it is allowed to open, so a
// healthy keep-alive connection is only ever closed by IdleConnTimeout, never
// because the idle pool was full. When the idle cap was below the in-flight
// concurrency (64 per host before Phase 8), every burst above it opened extra
// connections and closed them again on return: one dial plus one TIME_WAIT
// socket per surplus request, every burst. In the benchmark a VM pause made
// k6 pile up to 2000 concurrent requests, and the gateway answered with up to
// ~1700 upstream dials a second and ~13k TIME_WAIT sockets, which slowed its
// recovery on an already saturated machine.
//
// So MaxConnsPerHost equals MaxIdleConnsPerHost. The tradeoff: beyond 1024
// concurrent requests to ONE backend, further requests wait inside the
// transport for a free connection (bounded by the request's context
// deadline) instead of dialling more. That is backpressure at a level no
// single HTTP/1.1 backend serves well anyway, and it stops a client burst
// from turning into a dial storm against the backend's accept queue.
//
// The cost of a full pool is about 1024 x (two goroutines + ~8 KiB of
// buffers) per backend, only after a burst that needed it, and released
// after IdleConnTimeout (90 s) without use. MaxIdleConns is 0 (no global
// cap): the per-host caps already bound the total, and a global cap smaller
// than their sum would evict, and so churn, connections whenever several
// backends are busy at once.
const (
	upstreamMaxConnsPerHost     = 1024
	upstreamMaxIdleConnsPerHost = upstreamMaxConnsPerHost
	upstreamMaxIdleConns        = 0
	upstreamIdleConnTimeout     = 90 * time.Second
	upstreamTCPKeepAlive        = 30 * time.Second
	upstreamExpectContinue      = 1 * time.Second
)

// transportKey identifies the transport settings that differ between
// routes. Request timeouts are enforced with a context deadline instead, so
// they do not need their own transport.
type transportKey struct {
	dial           time.Duration
	responseHeader time.Duration
}

// transportPool is the long-lived set of upstream transports, shared by
// every snapshot. Reusing the same *http.Transport across config swaps is
// what keeps warm keep-alive connections alive through a reload (ADR 0002).
//
// It is only touched while building or retiring snapshots (from the single
// reconcile goroutine) and never on the request path, so a plain mutex is
// simplest; it costs nothing measurable and keeps the pool safe even if a
// second caller appears later.
type transportPool struct {
	mu         sync.Mutex
	transports map[transportKey]*http.Transport
}

func newTransportPool() *transportPool {
	return &transportPool{transports: make(map[transportKey]*http.Transport)}
}

// get returns the transport for k, creating it on first use.
func (p *transportPool) get(k transportKey) *http.Transport {
	p.mu.Lock()
	defer p.mu.Unlock()
	if t, ok := p.transports[k]; ok {
		return t
	}
	t := newUpstreamTransport(k)
	p.transports[k] = t
	return t
}

// retain drops every transport whose key is not in keep and closes its idle
// connections, so a stream of configs with ever-changing timeouts cannot
// grow the pool without bound. An in-flight request on an older snapshot
// may still be using a dropped transport; that stays safe because
// CloseIdleConnections only touches idle connections, and any connection
// the request returns afterwards is closed by IdleConnTimeout.
func (p *transportPool) retain(keep map[transportKey]struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, t := range p.transports {
		if _, ok := keep[k]; !ok {
			t.CloseIdleConnections()
			delete(p.transports, k)
		}
	}
}

// closeIdle closes the idle connections of every pooled transport. Called
// when the reconcile loop exits so no connection goroutines outlive the
// gateway; transports remain usable if a straggling request still needs one.
func (p *transportPool) closeIdle() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, t := range p.transports {
		t.CloseIdleConnections()
	}
}

func newUpstreamTransport(k transportKey) *http.Transport {
	dialer := &net.Dialer{Timeout: k.dial, KeepAlive: upstreamTCPKeepAlive}
	return &http.Transport{
		// Never route upstream traffic through HTTP_PROXY from the
		// environment: backends are addressed directly.
		Proxy:       nil,
		DialContext: dialer.DialContext,
		// The configured dial timeout covers "TCP and TLS" (config docs).
		TLSHandshakeTimeout:   k.dial,
		ResponseHeaderTimeout: k.responseHeader,
		ExpectContinueTimeout: upstreamExpectContinue,
		MaxIdleConns:          upstreamMaxIdleConns,
		MaxIdleConnsPerHost:   upstreamMaxIdleConnsPerHost,
		MaxConnsPerHost:       upstreamMaxConnsPerHost,
		IdleConnTimeout:       upstreamIdleConnTimeout,
		// A proxy must be transparent: without this the transport would
		// add "Accept-Encoding: gzip" and silently decompress responses.
		DisableCompression: true,
		// HTTP/1.1 to upstreams (like nginx); ForceAttemptHTTP2 is left
		// off so the per-host pool limits above mean what they say.
	}
}

// bufferPool recycles the 32 KiB copy buffers httputil.ReverseProxy uses
// for response bodies; without one it allocates a fresh buffer per request.
// It holds no request state, so sharing it across snapshots is safe.
type bufferPool struct{ pool sync.Pool }

const copyBufferSize = 32 * 1024

func newBufferPool() *bufferPool {
	return &bufferPool{pool: sync.Pool{New: func() any {
		b := make([]byte, copyBufferSize)
		// Store a pointer: putting a slice header into an interface would
		// itself allocate on every Put.
		return &b
	}}}
}

func (p *bufferPool) Get() []byte { return *p.pool.Get().(*[]byte) }

func (p *bufferPool) Put(b []byte) {
	if cap(b) != copyBufferSize {
		return // not one of ours; let the GC have it
	}
	b = b[:copyBufferSize]
	p.pool.Put(&b)
}
