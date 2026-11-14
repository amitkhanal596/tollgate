package balancer

import (
	"net/url"
	"sync"
)

// Registry is the long-lived owner of Backend identity. It returns the same
// *Backend for the same ID ("<upstream>|<base URL>") across config swaps, so
// health, ejection and in-flight state survive a reload (ADR 0002).
//
// It is safe for concurrent use. A plain mutex is fine here: the Registry is
// only touched by the single reconcile goroutine while building a snapshot
// (and by tests), never on the per-request path.
type Registry struct {
	mu       sync.Mutex
	backends map[string]*Backend
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{backends: make(map[string]*Backend)}
}

// BackendID returns the Registry key for base URL u in upstream.
func BackendID(upstream string, u *url.URL) string {
	return upstream + "|" + u.String()
}

// Get returns the Backend for (upstream, u), creating it on first use.
// Two calls with equal URL values return the same pointer even if the
// *url.URL pointers differ.
func (r *Registry) Get(upstream string, u *url.URL) *Backend {
	id := BackendID(upstream, u)

	r.mu.Lock()
	defer r.mu.Unlock()
	if b, ok := r.backends[id]; ok {
		return b
	}
	// Copy the URL so a caller that later reuses or mutates its *url.URL
	// cannot change where a long-lived, shared Backend forwards to.
	cp := *u
	b := NewBackend(id, &cp)
	r.backends[id] = b
	return b
}

// Prune drops every Backend whose ID is not in keep (a nil keep drops all).
// Requests still holding a dropped *Backend keep using it safely; a later
// Get for that ID creates a fresh Backend with reset state.
func (r *Registry) Prune(keep map[string]struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id := range r.backends {
		if _, ok := keep[id]; !ok {
			// Deleting during range is explicitly allowed by the Go spec.
			delete(r.backends, id)
		}
	}
}

// Len returns the number of Backends currently registered.
func (r *Registry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.backends)
}
