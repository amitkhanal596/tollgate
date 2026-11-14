package proxy

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// Upstream connection-pool sizing, per pooled transport. MaxIdleConnsPerHost
// matters most: the net/http default of 2 makes a busy proxy open and close
// a TCP connection for almost every request, which costs latency and can
// exhaust ephemeral ports through TIME_WAIT. IdleConnTimeout (90 s) bounds how
// long an unused warm connection (and its goroutines) is kept.
const (
	upstreamMaxIdleConns        = 1024
	upstreamMaxIdleConnsPerHost = 64
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
