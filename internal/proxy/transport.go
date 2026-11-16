package proxy

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// Upstream connection-pool sizing. A pool exists per (transportKey, backend
// host:port), so every bound below is per backend PER transport key.
//
// The idle pool is what removes connection churn: a burst of up to 1024
// concurrent requests to one backend keeps every connection it opened, so a
// healthy keep-alive connection is closed only by IdleConnTimeout. Above
// 1024 the surplus is dialled and closed on return. With the old 64-per-host
// idle pool, the worst benchmark run (k6 piling up after a VM pause) made the
// gateway dial up to 2,505 times a second and hold 17.6k TIME_WAIT sockets.
//
// There is deliberately NO connection cap (MaxConnsPerHost 0). With a cap,
// SSE streams hold slots and further requests wait inside the transport with
// no bound but the route's request deadline (the dial and response-header
// timeouts do not cover that wait). The client then gets a 504 that passive
// health counts against a healthy backend, ejecting it, and every route that
// shares the transport key is starved. If a concurrency ceiling is ever
// wanted it belongs in admission (a per-backend max in-flight that answers
// 503 without blaming the backend), not in the transport.
//
// MaxIdleConns is 0: no global cap, since one smaller than the per-host caps'
// sum would evict, and so churn, connections when several backends are busy.
// So idle connections are bounded only at 1024 per backend per transport key.
// Each costs an fd, two goroutines and their read/write buffers until it has
// been idle for IdleConnTimeout (90 s). Known limit: with very many discovered
// backends (times transport keys) a full pool can press on the fd limit.
const (
	upstreamMaxIdleConnsPerHost = 1024
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
		// MaxConnsPerHost is left 0 (unlimited) on purpose: see the
		// comment on the pool constants.
		MaxIdleConns:        upstreamMaxIdleConns,
		MaxIdleConnsPerHost: upstreamMaxIdleConnsPerHost,
		IdleConnTimeout:     upstreamIdleConnTimeout,
		// A proxy must be transparent: without this the transport would
		// add "Accept-Encoding: gzip" and silently decompress responses.
		DisableCompression: true,
		// HTTP/1.1 to upstreams (like nginx); ForceAttemptHTTP2 is left
		// off so the per-host idle limit above means what it says.
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
