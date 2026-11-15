package discovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"sort"
	"strconv"
	"time"

	consulapi "github.com/hashicorp/consul/api"
)

// Defaults follow ADR 0007 and Consul's blocking-query guidance.
const (
	defaultWaitTime    = 5 * time.Minute
	defaultBackoffMin  = 500 * time.Millisecond
	defaultBackoffMax  = 30 * time.Second
	defaultMinInterval = time.Second
)

// ConsulOptions configures NewConsul. Zero values select defaults.
type ConsulOptions struct {
	Logger     *slog.Logger // nil -> discard
	WaitTime   time.Duration
	BackoffMin time.Duration
	BackoffMax time.Duration
	// MinInterval is the floor between two consecutive queries (ADR 0007:
	// "sleep at least about 1 s between iterations"; 0 -> 1s). It is only
	// applied when the previous query returned sooner than MinInterval, so a
	// long blocking wait is never followed by an extra pause. It is not an
	// error backoff.
	MinInterval time.Duration
	// Sleep, if non-nil, is used for error backoff and the MinInterval
	// throttle. nil -> real timer honoring ctx.
	Sleep func(ctx context.Context, d time.Duration) error
}

// Consul is a Resolver backed by Consul blocking queries.
type Consul struct {
	client *consulapi.Client
	opts   ConsulOptions
}

var _ Resolver = (*Consul)(nil)

// NewConsul returns a Consul resolver.
func NewConsul(client *consulapi.Client, opts ConsulOptions) *Consul {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.WaitTime <= 0 {
		opts.WaitTime = defaultWaitTime
	}
	if opts.BackoffMin <= 0 {
		opts.BackoffMin = defaultBackoffMin
	}
	if opts.BackoffMax <= 0 {
		opts.BackoffMax = defaultBackoffMax
	}
	if opts.BackoffMax < opts.BackoffMin {
		opts.BackoffMax = opts.BackoffMin
	}
	if opts.MinInterval <= 0 {
		opts.MinInterval = defaultMinInterval
	}
	if opts.Sleep == nil {
		opts.Sleep = sleepCtx
	}
	return &Consul{client: client, opts: opts}
}

// sleepCtx waits for d or until ctx ends, whichever is first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Watch implements Resolver. Only an empty Service is a synchronous error.
func (c *Consul) Watch(ctx context.Context, q Query) (<-chan []Endpoint, error) {
	if q.Service == "" {
		return nil, errors.New("discovery: watch requires a non-empty service name")
	}
	// Capacity 1 + single sender = "latest wins" (see send).
	ch := make(chan []Endpoint, 1)
	go func() {
		defer close(ch) // only the watcher closes the channel
		c.run(ctx, q, ch)
	}()
	return ch, nil
}

// run is the watcher loop. It returns only when ctx is done.
func (c *Consul) run(ctx context.Context, q Query, ch chan []Endpoint) {
	log := c.opts.Logger.With("service", q.Service, "tag", q.Tag)

	var (
		idx     uint64        // next WaitIndex; 0 on the first request
		last    []Endpoint    // private copy of the last set SENT
		hasSent bool          // the first successful result is always sent
		backoff time.Duration // current error backoff step; 0 = no failures
		prevDur time.Duration // last backoff slept, keeps jitter non-decreasing
	)

	for ctx.Err() == nil {
		start := time.Now()
		qo := (&consulapi.QueryOptions{WaitIndex: idx, WaitTime: c.opts.WaitTime}).WithContext(ctx)
		entries, meta, err := c.client.Health().Service(q.Service, q.Tag, true, qo)
		if ctx.Err() != nil {
			return // canceled: the error (if any) is just the abort
		}
		if err != nil {
			// Never send anything on error: the consumer keeps the last set.
			log.Warn("consul query failed", "err", fmt.Errorf("health service %q: %w", q.Service, err))
			backoff = nextBackoff(backoff, c.opts.BackoffMin, c.opts.BackoffMax)
			d := jitter(backoff)
			if d < prevDur {
				d = prevDur // never decrease, even after hitting the cap
			}
			prevDur = d
			if c.opts.Sleep(ctx, d) != nil {
				return
			}
			continue
		}
		backoff, prevDur = 0, 0 // success resets the backoff

		idx = nextIndex(idx, meta.LastIndex)

		eps := toEndpoints(entries)
		if !hasSent || !equalEndpoints(last, eps) {
			last = append([]Endpoint(nil), eps...) // private copy for comparisons
			hasSent = true
			send(ch, eps)
		}

		// Throttle a misbehaving agent that answers immediately; a query that
		// blocked for a while already provided the spacing.
		if el := time.Since(start); el < c.opts.MinInterval {
			if c.opts.Sleep(ctx, c.opts.MinInterval-el) != nil {
				return
			}
		}
	}
}

// nextIndex applies ADR 0007's index rules to the returned X-Consul-Index.
func nextIndex(cur uint64, returned uint64) uint64 {
	// The Consul client exposes LastIndex as uint64, so "0 or less" is 0.
	switch {
	case returned == 0:
		return 1
	case returned < cur:
		return 0 // went backwards: start over
	default:
		return returned
	}
}

// nextBackoff doubles cur from min up to max.
func nextBackoff(cur, min, max time.Duration) time.Duration {
	if cur == 0 {
		return min
	}
	cur *= 2
	if cur > max || cur <= 0 {
		return max
	}
	return cur
}

// jitter returns a duration in [d/2, d]; successive steps' ranges only touch
// at the boundary, so the sequence is non-decreasing until the cap.
func jitter(d time.Duration) time.Duration {
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

// send delivers v with latest-wins semantics. The watcher goroutine is the
// only sender, so after draining the buffer (which the consumer may also have
// emptied) the buffer is empty and the send cannot block; the consumer can
// only ever make room, never fill it, so the newest value is never lost.
func send(ch chan []Endpoint, v []Endpoint) {
	select {
	case <-ch:
	default:
	}
	ch <- v
}

// toEndpoints maps entries to a de-duplicated (by Address, lowest ID wins,
// architecture §11 row 17) set sorted by ID. Always returns a fresh slice.
func toEndpoints(entries []*consulapi.ServiceEntry) []Endpoint {
	byAddr := make(map[string]Endpoint, len(entries))
	for _, e := range entries {
		if e == nil || e.Service == nil {
			continue
		}
		addr := e.Service.Address
		node := ""
		if e.Node != nil {
			node = e.Node.Node
			if addr == "" {
				addr = e.Node.Address // service registered without an address
			}
		}
		ep := Endpoint{
			ID:      node + "/" + e.Service.ID,
			Address: net.JoinHostPort(addr, strconv.Itoa(e.Service.Port)),
		}
		if old, ok := byAddr[ep.Address]; !ok || ep.ID < old.ID {
			byAddr[ep.Address] = ep
		}
	}
	out := make([]Endpoint, 0, len(byAddr))
	for _, ep := range byAddr {
		out = append(out, ep)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func equalEndpoints(a, b []Endpoint) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
