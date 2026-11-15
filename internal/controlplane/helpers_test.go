package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// Tokens are at least MinTokenBytes long and differ from each other.
const (
	adminTok = "admin-token-0123456789abcdef0123456789abcdef"
	dpTok    = "dplane-token-0123456789abcdef0123456789abcdef"
)

const seedBackend = "http://127.0.0.1:9001"

// seedJSON is a minimal valid full document: one upstream, one route.
const seedJSON = `{"version":0,"routes":[{"name":"api","path_prefix":"/api","upstream":"web"}],"upstreams":[{"name":"web","backends":["` + seedBackend + `"]}]}`

// failOnPanic turns a panic (the Phase 4 stubs panic) into a test failure so
// one run lists every failing test instead of aborting the binary.
// Use as: defer failOnPanic(t) at the top of a test or subtest.
func failOnPanic(t *testing.T) {
	if r := recover(); r != nil {
		t.Fatalf("panic (stub/placeholder not implemented?): %v", r)
	}
}

// run is t.Run with failOnPanic installed in the subtest.
func run(t *testing.T, name string, fn func(t *testing.T)) bool {
	t.Helper()
	return t.Run(name, func(t *testing.T) {
		defer failOnPanic(t)
		fn(t)
	})
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// ---- fixtures -------------------------------------------------------------

func testUpstream(name string) config.Upstream {
	return config.Upstream{Name: name, Backends: []string{seedBackend}}
}

func mutAddUpstream(name string) func(*config.Config) error {
	return func(c *config.Config) error {
		c.Upstreams = append(c.Upstreams, testUpstream(name))
		return nil
	}
}

func mutAddRoute(name, prefix, upstream string) func(*config.Config) error {
	return func(c *config.Config) error {
		c.Routes = append(c.Routes, config.Route{Name: name, PathPrefix: prefix, Upstream: upstream})
		return nil
	}
}

// mutSeed adds upstream "web" and route "api".
func mutSeed(c *config.Config) error {
	_ = mutAddUpstream("web")(c)
	return mutAddRoute("api", "/api", "web")(c)
}

// canonJSON renders a config for comparison: nil and empty slices are equal
// ("routes": null and [] are equivalent).
func canonJSON(c *config.Config) string {
	cp := *c
	if cp.Routes == nil {
		cp.Routes = []config.Route{}
	}
	if cp.Upstreams == nil {
		cp.Upstreams = []config.Upstream{}
	}
	b, err := json.Marshal(&cp)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// canonBody is canonJSON ignoring Version (to compare bodies of two versions).
func canonBody(c *config.Config) string {
	cp := *c
	cp.Version = 0
	return canonJSON(&cp)
}

func routeJSON(name, prefix, upstream string) string {
	return fmt.Sprintf(`{"name":%q,"path_prefix":%q,"upstream":%q}`, name, prefix, upstream)
}

func upstreamJSON(name string) string {
	return fmt.Sprintf(`{"name":%q,"backends":[%q]}`, name, seedBackend)
}

// ---- environment ----------------------------------------------------------

type env struct {
	h     http.Handler
	store Store
	hub   *Hub
	nodes *Nodes
}

func newEnv(t *testing.T, mods ...func(*Options)) *env {
	t.Helper()
	return newEnvWithStore(t, NewMemStore(), mods...)
}

func newEnvWithStore(t *testing.T, store Store, mods ...func(*Options)) *env {
	t.Helper()
	hub := NewHub()
	nodes := NewNodes(nil, 0)
	opts := Options{
		Store:          store,
		Hub:            hub,
		Nodes:          nodes,
		AdminToken:     adminTok,
		DataPlaneToken: dpTok,
		Heartbeat:      time.Hour, // no heartbeats unless a test asks
	}
	for _, m := range mods {
		m(&opts)
	}
	h, err := NewHandler(opts)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	t.Cleanup(hub.Close)
	return &env{h: h, store: store, hub: hub, nodes: nodes}
}

// do performs one request against h. kv are header name/value pairs. An empty
// token sends no Authorization header (pass it through kv for odd schemes).
func do(t *testing.T, h http.Handler, method, path, token, body string, kv ...string) *httptest.ResponseRecorder {
	t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if len(kv)%2 != 0 {
		t.Fatalf("do: odd number of header args")
	}
	for i := 0; i < len(kv); i += 2 {
		req.Header.Set(kv[i], kv[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func (e *env) admin(t *testing.T, method, path, body string, kv ...string) *httptest.ResponseRecorder {
	t.Helper()
	return do(t, e.h, method, path, adminTok, body, kv...)
}

// seed PUTs seedJSON and checks it became version 1.
func (e *env) seed(t *testing.T) {
	t.Helper()
	rec := e.admin(t, "PUT", "/v1/config", seedJSON)
	if rec.Code != 200 || rec.Header().Get("ETag") != `"1"` {
		t.Fatalf("seed: status %d etag %q body %s", rec.Code, rec.Header().Get("ETag"), rec.Body)
	}
}

// putUpstream creates/replaces upstream name through the API and returns the
// new version from the ETag.
func (e *env) putUpstream(t *testing.T, name string) int64 {
	t.Helper()
	rec := e.admin(t, "PUT", "/v1/upstreams/"+name, upstreamJSON(name))
	if rec.Code != 200 && rec.Code != 201 {
		t.Fatalf("PUT upstream %s: %d %s", name, rec.Code, rec.Body)
	}
	return etagVersion(t, rec)
}

// seedStore commits n versions directly in the store WITHOUT publishing to the
// hub (simulates a control plane that restarted with history in the database).
// Version i has exactly i upstreams, named u1..ui.
func (e *env) seedStore(t *testing.T, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		if _, err := e.store.Update(testCtx(t), UpdateRequest{}, mutAddUpstream(fmt.Sprintf("u%d", i))); err != nil {
			t.Fatalf("seedStore: %v", err)
		}
	}
}

func (e *env) headVersion(t *testing.T) int64 {
	t.Helper()
	c, err := e.store.Head(testCtx(t))
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	return c.Version
}

// ---- response decoding ----------------------------------------------------

// decodeStrict decodes body into v rejecting unknown fields and trailing data.
func decodeStrict(body []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("trailing data after JSON value")
	}
	return nil
}

func mustJSON(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json (status %d, body %q)", ct, rec.Code, rec.Body)
	}
	if err := decodeStrict(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("decode body: %v\nbody: %s", err, rec.Body)
	}
}

func decodeConfig(t *testing.T, rec *httptest.ResponseRecorder) *config.Config {
	t.Helper()
	var c config.Config
	mustJSON(t, rec, &c)
	return &c
}

// errBody asserts a JSON error response and returns its message.
func errBody(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var e ErrorBody
	mustJSON(t, rec, &e)
	if e.Error == "" {
		t.Fatalf("empty error message, body %s", rec.Body)
	}
	return e.Error
}

// etagVersion parses ETag: "N".
func etagVersion(t *testing.T, rec *httptest.ResponseRecorder) int64 {
	t.Helper()
	et := rec.Header().Get("ETag")
	if len(et) < 3 || et[0] != '"' || et[len(et)-1] != '"' {
		t.Fatalf("ETag = %q, want quoted decimal", et)
	}
	v, err := strconv.ParseInt(et[1:len(et)-1], 10, 64)
	if err != nil {
		t.Fatalf("ETag = %q: %v", et, err)
	}
	return v
}

func (e *env) listNodes(t *testing.T) []NodeStatus {
	t.Helper()
	rec := e.admin(t, "GET", "/v1/dataplanes", "")
	if rec.Code != 200 {
		t.Fatalf("GET /v1/dataplanes: %d %s", rec.Code, rec.Body)
	}
	var dl DataPlaneList
	mustJSON(t, rec, &dl)
	return dl.DataPlanes
}

func findNode(ns []NodeStatus, id string) (NodeStatus, bool) {
	for _, n := range ns {
		if n.Node == id {
			return n, true
		}
	}
	return NodeStatus{}, false
}

// ---- waiting --------------------------------------------------------------

// waitFor polls cond until it is true or d elapses.
func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", d, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---- hub helpers ----------------------------------------------------------

func tryRecv(ch <-chan *config.Config) (*config.Config, bool) {
	select {
	case c, ok := <-ch:
		return c, ok
	default:
		return nil, false
	}
}

func recvWithin(t *testing.T, ch <-chan *config.Config, d time.Duration) *config.Config {
	t.Helper()
	select {
	case c, ok := <-ch:
		if !ok {
			t.Fatalf("channel closed, wanted a config")
		}
		return c
	case <-time.After(d):
		t.Fatalf("no config within %v", d)
	}
	return nil
}

// expectNoRecv fails if anything (a config or a close) arrives within d.
func expectNoRecv(t *testing.T, ch <-chan *config.Config, d time.Duration) {
	t.Helper()
	select {
	case c, ok := <-ch:
		if !ok {
			t.Fatalf("channel unexpectedly closed")
		}
		t.Fatalf("unexpected config version %d", c.Version)
	case <-time.After(d):
	}
}

// expectClosed waits until ch is closed, discarding a pending buffered value.
func expectClosed(t *testing.T, ch <-chan *config.Config, d time.Duration) {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatalf("channel not closed within %v", d)
		}
	}
}

func cfgV(v int64) *config.Config { return &config.Config{Version: v} }

// ---- faulty store ---------------------------------------------------------

// faultyStore wraps a Store and injects failures.
type faultyStore struct {
	Store
	err    error                           // returned by Head/Get/List/Update/Rollback when set
	block  bool                            // Head/Get/List/Update/Rollback block until ctx is done
	pingFn func(ctx context.Context) error // overrides Ping when set
}

func (f *faultyStore) fault(ctx context.Context) error {
	if f.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.err
}

func (f *faultyStore) Head(ctx context.Context) (*config.Config, error) {
	if err := f.fault(ctx); err != nil {
		return nil, err
	}
	return f.Store.Head(ctx)
}

func (f *faultyStore) Get(ctx context.Context, v int64) (*config.Config, error) {
	if err := f.fault(ctx); err != nil {
		return nil, err
	}
	return f.Store.Get(ctx, v)
}

func (f *faultyStore) List(ctx context.Context, limit int, before int64) ([]VersionInfo, error) {
	if err := f.fault(ctx); err != nil {
		return nil, err
	}
	return f.Store.List(ctx, limit, before)
}

func (f *faultyStore) Update(ctx context.Context, req UpdateRequest, m func(*config.Config) error) (*config.Config, error) {
	if err := f.fault(ctx); err != nil {
		return nil, err
	}
	return f.Store.Update(ctx, req, m)
}

func (f *faultyStore) Rollback(ctx context.Context, to int64, req UpdateRequest) (*config.Config, error) {
	if err := f.fault(ctx); err != nil {
		return nil, err
	}
	return f.Store.Rollback(ctx, to, req)
}

func (f *faultyStore) Ping(ctx context.Context) error {
	if f.pingFn != nil {
		return f.pingFn(ctx)
	}
	return f.Store.Ping(ctx)
}

// TestFixturesAreValid guards the fixtures themselves (independent of Phase 4
// code): if config.Validate rejects them every other test would fail for the
// wrong reason.
func TestFixturesAreValid(t *testing.T) {
	var c config.Config
	if err := decodeStrict([]byte(seedJSON), &c); err != nil {
		t.Fatal(err)
	}
	c.Normalize()
	if err := c.Validate(); err != nil {
		t.Fatalf("seedJSON invalid: %v", err)
	}
	var m config.Config
	_ = mutSeed(&m)
	_ = mutAddUpstream("x")(&m)
	m.Normalize()
	if err := m.Validate(); err != nil {
		t.Fatalf("mutSeed invalid: %v", err)
	}
}

// after returns a channel that fires after n seconds.
func after(n int) <-chan time.Time { return time.After(time.Duration(n) * time.Second) }
