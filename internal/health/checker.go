// Package health decides whether a backend may receive traffic, from two
// independent signals (ADR 0006):
//
//   - active: Checker probes GET base+path on every backend that has a
//     health_check and flips Backend.SetActiveHealthy with rise/fall
//     hysteresis;
//   - passive: ObservePassive looks at real proxied outcomes and ejects a
//     backend for a cooloff after N consecutive failures.
//
// Both write different fields of the shared *balancer.Backend, so they never
// coordinate and the balancer reads them lock-free. State transitions leave
// the package through a callback, so health never imports metrics.
package health

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amitkhanal596/tollgate/internal/balancer"
	"github.com/amitkhanal596/tollgate/internal/config"
)

// maxDrain bounds how much of a probe response body is read before closing
// it. Draining lets the transport reuse the connection; the bound keeps a
// misbehaving backend from making a probe read megabytes.
const maxDrain = 4 << 10

// userAgent identifies probes in backend access logs.
const userAgent = "tollgate-health-checker"

// Target is one backend to probe actively and its check settings.
type Target struct {
	Backend *balancer.Backend
	Check   config.HealthCheck
}

// Checker owns every active-probe goroutine: one per *Backend pointer.
//
// Lifecycle: build with NewChecker, call Run once in its own goroutine, and
// call Sync after every snapshot swap. Sync may be called before Run, from
// any goroutine, concurrently.
//
// Targets are diffed by *Backend POINTER identity plus Check settings, never
// by ID alone (docs/architecture.md §11 row 3):
//   - a new pointer starts a probe (so a backend that was pruned and re-added
//     under the same ID, which gets a new pointer from the Registry, is
//     probed, and the old pointer's probe stops);
//   - a pointer that is gone (target removed, or its upstream no longer has a
//     health_check and so is not passed to Sync) stops its probe and resets
//     that backend to up with SetActiveHealthy(true) (§11 row 7); otherwise a
//     backend that was down at that moment would stay down forever;
//   - a pointer whose settings changed restarts its probe and KEEPS the
//     current verdict; the new probe corrects it within rise/fall probes.
//
// Each probe: GET base URL + Check.Path, with a context deadline of
// Check.Timeout. A 2xx is a success; any other status, a transport error or
// a timeout is a failure. Redirects are never followed. The backend goes
// down after UnhealthyThreshold consecutive failures and comes up after
// HealthyThreshold consecutive successes. The first probe runs at a random
// offset within Interval (ADR 0006: avoids synchronised probe bursts), then
// one probe per Interval, measured from the end of the previous probe, so
// probes of one backend never overlap. Timeout > Interval is not rejected.
//
// onChange is called exactly once per transition of a backend's active
// verdict, from that backend's probe goroutine (so calls for different
// backends may be concurrent) and never while a lock is held. It should be
// quick: while it runs, that backend is not probed.
type Checker struct {
	client   *http.Client
	ownsTran *http.Transport // non-nil only if we built the transport; closed when Run exits
	// now is part of the §2 contract for time-stamping future events; probe
	// scheduling uses real timers because it must actually wait.
	now      func() time.Time
	onChange func(b *balancer.Backend, up bool)

	// desired holds the latest Sync argument. An atomic pointer (not a
	// mutex-guarded slice) keeps Sync lock-free and trivially non-blocking.
	desired atomic.Pointer[[]Target]
	// wake has capacity 1: Sync stores then does a non-blocking send, so any
	// number of Syncs coalesce into one pending wake-up and Run always loads
	// the latest desired set after the wake-up it receives (latest wins).
	wake chan struct{}

	running atomic.Bool // guards against two concurrent Runs sharing wake
}

// NewChecker builds a Checker.
//
// client is used for probes but never mutated: the Checker works on a
// shallow copy whose CheckRedirect returns the 3xx as-is and which carries
// no cookie jar. It shares the caller's Transport, so connections are pooled
// there. A nil client gets a private transport that Run closes on exit.
// nil now means time.Now; nil onChange means no callback.
func NewChecker(client *http.Client, now func() time.Time, onChange func(b *balancer.Backend, up bool)) *Checker {
	c := &Checker{now: now, onChange: onChange, wake: make(chan struct{}, 1)}
	if c.now == nil {
		c.now = time.Now
	}
	if c.onChange == nil {
		c.onChange = func(*balancer.Backend, bool) {}
	}

	var cp http.Client
	if client != nil {
		cp = *client // shallow copy: the caller's client is never modified
	} else {
		// Clone so we never mutate http.DefaultTransport; few idle conns per
		// host are enough for one sequential probe per backend.
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.MaxIdleConnsPerHost = 2
		c.ownsTran = tr
		cp.Transport = tr
	}
	// A 3xx must count as a failure, not be followed to some other page that
	// might return 200; this holds even if the caller's client follows.
	cp.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	// Probes are stateless; cookies set by a backend must not leak into them.
	cp.Jar = nil
	c.client = &cp
	return c
}

// Sync replaces the desired target set. It never blocks and the latest call
// wins: Run reconciles to whichever set was stored last. The slice is copied,
// so the caller may reuse it. Entries with a nil Backend are ignored; if the
// same pointer appears twice, the last entry wins.
func (c *Checker) Sync(targets []Target) {
	cp := append([]Target(nil), targets...)
	c.desired.Store(&cp)
	select {
	case c.wake <- struct{}{}:
	default: // a wake-up is already pending; Run will load our Store with it
	}
}

// Run reconciles probes to the latest Sync until ctx is done, then stops
// every probe and returns nil only after all probe goroutines have exited.
// Backends keep their last verdict on shutdown. Run must not be called
// concurrently with itself; a second concurrent call returns an error.
func (c *Checker) Run(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		return errors.New("health: Checker.Run is already running")
	}
	defer c.running.Store(false)

	// All of this state is owned by the Run goroutine alone, so no lock.
	r := &reconciler{
		c:       c,
		active:  make(map[*balancer.Backend]*probe),
		exiting: make(map[*balancer.Backend]<-chan struct{}),
	}
	defer func() {
		// Probe contexts derive from ctx, so they are all canceled by now;
		// wait for every goroutine, including ones stopped by earlier Syncs.
		r.wg.Wait()
		if c.ownsTran != nil {
			c.ownsTran.CloseIdleConnections() // no keep-alive goroutines outlive Run
		}
	}()

	r.apply(ctx, c.desired.Load()) // honour a Sync made before Run
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.wake:
			r.apply(ctx, c.desired.Load())
		}
	}
}

// probe is the Run goroutine's handle on one running probe goroutine.
type probe struct {
	check  config.HealthCheck
	cancel context.CancelFunc
	// reset tells the goroutine to SetActiveHealthy(true) as its last act.
	// Atomic because the goroutine may also be exiting due to Run's ctx and
	// read it while Run writes it.
	reset atomic.Bool
	done  chan struct{} // closed when the goroutine has fully exited
}

type reconciler struct {
	c      *Checker
	active map[*balancer.Backend]*probe
	// exiting maps a pointer to the done channel of its most recently stopped
	// probe that may still be running. A new probe for the same pointer waits
	// on it, so two probes never write one backend's verdict at once and a
	// late reset-to-up cannot overwrite the new probe's verdict.
	exiting map[*balancer.Backend]<-chan struct{}
	wg      sync.WaitGroup
}

func (r *reconciler) apply(ctx context.Context, desired *[]Target) {
	want := make(map[*balancer.Backend]config.HealthCheck)
	if desired != nil {
		for _, t := range *desired {
			if t.Backend != nil {
				want[t.Backend] = t.Check
			}
		}
	}

	// Forget stopped probes that have finished, so pruned Backends are not
	// kept alive by this map.
	for b, done := range r.exiting {
		select {
		case <-done:
			delete(r.exiting, b)
		default:
		}
	}

	for b, p := range r.active {
		chk, ok := want[b]
		if ok && chk == p.check {
			continue // unchanged: leave it running undisturbed
		}
		// Gone -> reset to up (§11 row 7); settings changed -> keep verdict.
		p.reset.Store(!ok)
		p.cancel() // aborts an in-flight probe request promptly
		r.exiting[b] = p.done
		delete(r.active, b)
	}

	for b, chk := range want {
		if _, ok := r.active[b]; ok {
			continue
		}
		pctx, cancel := context.WithCancel(ctx)
		p := &probe{check: chk, cancel: cancel, done: make(chan struct{})}
		r.active[b] = p
		r.wg.Add(1)
		go r.c.runProbe(pctx, b, p, r.exiting[b], &r.wg)
	}
}

// settings returns chk with defaults filled in for zero or negative values,
// so a config that skipped Normalize cannot panic a ticker or never probe.
func settings(chk config.HealthCheck) (interval, timeout time.Duration, rise, fall int) {
	interval, timeout = time.Duration(chk.Interval), time.Duration(chk.Timeout)
	rise, fall = chk.HealthyThreshold, chk.UnhealthyThreshold
	if interval <= 0 {
		interval = config.DefaultHealthInterval
	}
	if timeout <= 0 {
		timeout = config.DefaultHealthTimeout
	}
	if rise <= 0 {
		rise = config.DefaultHealthyThreshold
	}
	if fall <= 0 {
		fall = config.DefaultUnhealthyThreshold
	}
	return interval, timeout, rise, fall
}

// runProbe is one probe goroutine. It exits only when ctx is canceled
// (by Sync stopping it, or by Run's ctx).
func (c *Checker) runProbe(ctx context.Context, b *balancer.Backend, p *probe, prev <-chan struct{}, wg *sync.WaitGroup) {
	// Defers run LIFO: reset first, then close(done) so a successor that
	// waits on done starts after the reset, then wg.Done last so Run cannot
	// return before this goroutine is finished with everything.
	defer wg.Done()
	defer close(p.done)
	defer func() {
		if p.reset.Load() && b.SetActiveHealthy(true) {
			c.onChange(b, true)
		}
	}()

	if prev != nil {
		select {
		case <-prev: // predecessor for this pointer has exited
		case <-ctx.Done():
			return
		}
	}

	interval, timeout, rise, fall := settings(p.check)
	target := probeURL(b, p.check.Path)

	timer := time.NewTimer(rand.N(interval)) // random first offset (ADR 0006)
	defer timer.Stop()

	// Counters are local to this probe, so a restarted probe starts from zero
	// and needs a full rise/fall to change the verdict it inherited. The
	// verdict itself is read from the Backend, the single source of truth.
	var successes, failures int
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		ok := c.probeOnce(ctx, target, timeout)
		if ctx.Err() != nil {
			return // aborted by stop/shutdown: says nothing about the backend
		}
		if ok {
			failures = 0
			if successes < rise { // capped: only ">= rise" matters
				successes++
			}
			if successes >= rise && !b.ActiveHealthy() && b.SetActiveHealthy(true) {
				c.onChange(b, true)
			}
		} else {
			successes = 0
			if failures < fall {
				failures++
			}
			if failures >= fall && b.ActiveHealthy() && b.SetActiveHealthy(false) {
				c.onChange(b, false)
			}
		}
		// Interval is measured from the end of a probe, so a slow probe
		// (timeout > interval is allowed) never causes back-to-back overlap.
		timer.Reset(interval)
	}
}

// probeURL joins the backend base URL and the check path as strings, so a
// query string in path (e.g. "/health?full=1") is preserved.
func probeURL(b *balancer.Backend, path string) string {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return strings.TrimRight(b.URL.String(), "/") + path
}

// probeOnce sends one GET and reports whether it returned a 2xx in time.
func (c *Checker) probeOnce(ctx context.Context, target string, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return false // unparsable URL: report as a failing probe, not a panic
	}
	req.Header.Set("User-Agent", userAgent)
	resp, err := c.client.Do(req)
	if err != nil {
		return false
	}
	// Drain (bounded) before Close so the keep-alive connection is reused.
	_, _ = io.CopyN(io.Discard, resp.Body, maxDrain)
	_ = resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}
