package discovery

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	consulapi "github.com/hashicorp/consul/api"
)

// ---- fake Consul agent ----------------------------------------------------

type fakeInst struct {
	Node, NodeAddr string
	ID             string // service ID
	Addr           string // service address ("" = fall back to node address)
	Port           int
	Tags           []string
	Status         string // check status; "" means passing
}

type fakeSvc struct {
	index uint64
	insts []fakeInst
}

type faultKind int

const (
	fault500 faultKind = iota
	faultClose
	faultBadJSON
	faultIndex // serve the normal body but with the given X-Consul-Index
)

type fault struct {
	kind  faultKind
	index uint64
}

type fakeReq struct {
	Service  string
	Index    string
	HasIndex bool
	Wait     string
	Passing  bool
	Tag      string
}

// fakeConsul implements GET /v1/health/service/{name} with real blocking-query
// semantics: it blocks while the service index <= the request index, until
// `wait` elapses or the request context ends.
type fakeConsul struct {
	t    *testing.T
	mu   sync.Mutex
	svcs map[string]*fakeSvc
	flts []fault
	reqs []fakeReq
	wake chan struct{}

	aborted  int // blocked requests whose context ended
	inflight int

	done chan struct{}
	srv  *httptest.Server
	addr string
}

func newFakeConsul(t *testing.T) *fakeConsul {
	f := &fakeConsul{t: t, svcs: map[string]*fakeSvc{}, wake: make(chan struct{}), done: make(chan struct{})}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	f.addr = f.srv.Listener.Addr().String()
	t.Cleanup(f.Close)
	return f
}

// Close stops the server (idempotent).
func (f *fakeConsul) Close() {
	f.mu.Lock()
	select {
	case <-f.done:
	default:
		close(f.done)
	}
	srv := f.srv
	f.srv = nil
	f.mu.Unlock()
	if srv != nil {
		srv.Close()
	}
}

// Restart brings the server back on the same address after Close.
func (f *fakeConsul) Restart() {
	f.t.Helper()
	l, err := net.Listen("tcp", f.addr)
	if err != nil {
		f.t.Skipf("cannot re-listen on %s: %v", f.addr, err)
	}
	f.mu.Lock()
	f.done = make(chan struct{})
	f.mu.Unlock()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(f.serve))
	srv.Listener.Close()
	srv.Listener = l
	srv.Start()
	f.mu.Lock()
	f.srv = srv
	f.mu.Unlock()
}

func (f *fakeConsul) client() *consulapi.Client {
	f.t.Helper()
	c, err := consulapi.NewClient(&consulapi.Config{Address: f.addr})
	if err != nil {
		f.t.Fatalf("consul client: %v", err)
	}
	return c
}

func (f *fakeConsul) broadcastLocked() {
	close(f.wake)
	f.wake = make(chan struct{})
}

func (f *fakeConsul) svcLocked(name string) *fakeSvc {
	s, ok := f.svcs[name]
	if !ok {
		s = &fakeSvc{index: 1}
		f.svcs[name] = s
	}
	return s
}

// Set replaces the instance list and bumps the index.
func (f *fakeConsul) Set(name string, insts ...fakeInst) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.svcLocked(name)
	s.insts = append([]fakeInst(nil), insts...)
	s.index++
	f.broadcastLocked()
}

// Touch bumps the index without changing the instances.
func (f *fakeConsul) Touch(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.svcLocked(name).index++
	f.broadcastLocked()
}

// Inject queues faults; each is consumed by one request (and wakes blocked ones).
func (f *fakeConsul) Inject(fs ...fault) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.flts = append(f.flts, fs...)
	f.broadcastLocked()
}

func (f *fakeConsul) Index(name string) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.svcLocked(name).index
}

func (f *fakeConsul) Reqs(name string) []fakeReq {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []fakeReq
	for _, r := range f.reqs {
		if r.Service == name {
			out = append(out, r)
		}
	}
	return out
}

func (f *fakeConsul) Aborted() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.aborted
}

func (f *fakeConsul) Inflight() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inflight
}

type jsonCheck struct {
	Node, CheckID, Name, Status, ServiceID string
}

type jsonEntry struct {
	Node struct {
		Node    string
		Address string
	}
	Service struct {
		ID      string
		Service string
		Address string
		Port    int
		Tags    []string
	}
	Checks []jsonCheck
}

func (f *fakeConsul) serve(w http.ResponseWriter, r *http.Request) {
	const prefix = "/v1/health/service/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		http.NotFound(w, r)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, prefix)
	q := r.URL.Query()
	idx, _ := strconv.ParseUint(q.Get("index"), 10, 64)
	wait := 5 * time.Minute
	if v := q.Get("wait"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			wait = d
		}
	}
	_, passing := q["passing"]
	_, hasIdx := q["index"]

	f.mu.Lock()
	f.reqs = append(f.reqs, fakeReq{Service: name, Index: q.Get("index"), HasIndex: hasIdx, Wait: q.Get("wait"), Passing: passing, Tag: q.Get("tag")})
	f.inflight++
	done := f.done
	timer := time.NewTimer(wait)
	defer timer.Stop()

block:
	for {
		s := f.svcLocked(name)
		if len(f.flts) > 0 || idx == 0 || s.index > idx {
			break
		}
		wake := f.wake
		f.mu.Unlock()
		select {
		case <-wake:
			f.mu.Lock()
		case <-timer.C:
			f.mu.Lock()
			break block
		case <-r.Context().Done():
			f.mu.Lock()
			f.aborted++
			f.inflight--
			f.mu.Unlock()
			return
		case <-done:
			f.mu.Lock()
			f.inflight--
			f.mu.Unlock()
			return
		}
	}

	s := f.svcLocked(name)
	index := s.index
	var flt *fault
	if len(f.flts) > 0 {
		x := f.flts[0]
		f.flts = f.flts[1:]
		flt = &x
	}
	var entries []jsonEntry
	tag := q.Get("tag")
	for _, in := range s.insts {
		status := in.Status
		if status == "" {
			status = "passing"
		}
		if passing && status != "passing" {
			continue
		}
		if tag != "" && !hasTag(in.Tags, tag) {
			continue
		}
		var e jsonEntry
		e.Node.Node, e.Node.Address = in.Node, in.NodeAddr
		e.Service.ID, e.Service.Service, e.Service.Address, e.Service.Port = in.ID, name, in.Addr, in.Port
		e.Service.Tags = in.Tags
		if e.Service.Tags == nil {
			e.Service.Tags = []string{}
		}
		e.Checks = []jsonCheck{
			{Node: in.Node, CheckID: "serfHealth", Name: "Serf Health Status", Status: "passing"},
			{Node: in.Node, CheckID: "service:" + in.ID, Name: "svc", Status: status, ServiceID: in.ID},
		}
		entries = append(entries, e)
	}
	f.inflight--
	f.mu.Unlock()
	if entries == nil {
		entries = []jsonEntry{}
	}

	hdr := w.Header()
	hdr.Set("Content-Type", "application/json")
	hdr.Set("X-Consul-LastContact", "0")
	hdr.Set("X-Consul-KnownLeader", "true")
	hdr.Set("X-Consul-Index", strconv.FormatUint(index, 10))
	if flt != nil {
		switch flt.kind {
		case fault500:
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		case faultClose:
			if hj, ok := w.(http.Hijacker); ok {
				if c, _, err := hj.Hijack(); err == nil {
					c.Close()
				}
			}
			return
		case faultBadJSON:
			_, _ = w.Write([]byte(`[{"Node": {"Node": `))
			return
		case faultIndex:
			hdr.Set("X-Consul-Index", strconv.FormatUint(flt.index, 10))
		}
	}
	_ = json.NewEncoder(w).Encode(entries)
}

func hasTag(tags []string, t string) bool {
	for _, x := range tags {
		if x == t {
			return true
		}
	}
	return false
}

// ---- harness & helpers ----------------------------------------------------

const (
	testWait       = 150 * time.Millisecond
	testBackoffMin = 10 * time.Millisecond
	testBackoffMax = 80 * time.Millisecond
	testMinIv      = time.Millisecond
)

// sleepRec is an injected Sleep: it records error-backoff durations (anything
// <= MinInterval is treated as the loop throttle and ignored) and returns
// after ~1ms so retry loops stay fast.
type sleepRec struct {
	mu sync.Mutex
	ds []time.Duration
}

func (s *sleepRec) Sleep(ctx context.Context, d time.Duration) error {
	if d > testMinIv {
		s.mu.Lock()
		s.ds = append(s.ds, d)
		s.mu.Unlock()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(time.Millisecond):
		return nil
	}
}

func (s *sleepRec) Durations() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Duration(nil), s.ds...)
}

type harness struct {
	t  *testing.T
	fc *fakeConsul
	c  *Consul
	sl *sleepRec
}

func newHarness(t *testing.T, mut ...func(*ConsulOptions)) *harness {
	t.Helper()
	h := &harness{t: t, fc: newFakeConsul(t), sl: &sleepRec{}}
	h.build(mut...)
	return h
}

func (h *harness) build(mut ...func(*ConsulOptions)) {
	opts := ConsulOptions{
		WaitTime: testWait, BackoffMin: testBackoffMin, BackoffMax: testBackoffMax,
		MinInterval: testMinIv, Sleep: h.sl.Sleep,
	}
	for _, m := range mut {
		m(&opts)
	}
	h.c = NewConsul(h.fc.client(), opts)
}

// watch starts a watch whose ctx is cancelled at test end.
func (h *harness) watch(q Query) (<-chan []Endpoint, context.CancelFunc) {
	h.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h.t.Cleanup(cancel)
	ch, err := h.c.Watch(ctx, q)
	if err != nil {
		h.t.Fatalf("Watch(%+v): %v", q, err)
	}
	return ch, cancel
}

// safely turns a stub/implementation panic into a test failure so one panic
// does not abort the whole test binary.
func safely(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic: %v", r)
		}
	}()
	fn()
}

func recvT(t *testing.T, ch <-chan []Endpoint, d time.Duration) []Endpoint {
	t.Helper()
	select {
	case v, ok := <-ch:
		if !ok {
			t.Fatal("channel closed, expected a value")
		}
		return v
	case <-time.After(d):
		t.Fatalf("no value within %v", d)
	}
	return nil
}

func recv(t *testing.T, ch <-chan []Endpoint) []Endpoint {
	t.Helper()
	return recvT(t, ch, 3*time.Second)
}

func expectNone(t *testing.T, ch <-chan []Endpoint, d time.Duration) {
	t.Helper()
	select {
	case v, ok := <-ch:
		t.Fatalf("unexpected receive (ok=%v): %+v", ok, v)
	case <-time.After(d):
	}
}

func expectClosed(t *testing.T, ch <-chan []Endpoint, d time.Duration) {
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

func poll(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", d, what)
}

func mk(node, id, addr string, port int, tags ...string) fakeInst {
	return fakeInst{Node: node, NodeAddr: "10.255.0.1", ID: id, Addr: addr, Port: port, Tags: tags}
}

func sameEndpoints(got, want []Endpoint) bool {
	if len(got) == 0 && len(want) == 0 {
		return true
	}
	return reflect.DeepEqual(got, want)
}

func wantEndpoints(t *testing.T, got, want []Endpoint) {
	t.Helper()
	if !sameEndpoints(got, want) {
		t.Fatalf("endpoints mismatch\n got: %+v\nwant: %+v", got, want)
	}
}
