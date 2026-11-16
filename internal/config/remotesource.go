package config

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// Defaults for RemoteOptions (docs/architecture.md §5.1, ADR 0005).
const (
	DefaultRemoteBackoffMin    = 500 * time.Millisecond
	DefaultRemoteBackoffMax    = 30 * time.Second
	DefaultRemoteIdleTimeout   = 45 * time.Second // three missed 15 s heartbeats
	DefaultRemoteMaxEventBytes = 8 << 20
	DefaultRemoteStatusTimeout = 5 * time.Second
)

const (
	maxNodeIDLen = 128
	// maxStatusErrorBytes is the control plane's limit on StatusReport.Error.
	maxStatusErrorBytes = 1024
	// maxDiscardBytes bounds how much of an unwanted response body is read.
	maxDiscardBytes = 4 << 10
)

// RemoteOptions configures a RemoteSource (docs/architecture.md §5.1). Zero or
// negative durations and sizes take the defaults.
type RemoteOptions struct {
	// BaseURL is the control plane's absolute http(s) URL, e.g.
	// "http://controlplane:9000". No path other than "/", no query or fragment.
	BaseURL string
	// Token is TOLLGATE_DATAPLANE_TOKEN, sent as "Authorization: Bearer <Token>".
	Token string
	// NodeID identifies this gateway: 1..128 bytes of [A-Za-z0-9._-], the
	// first a letter or digit.
	NodeID string
	// LKGPath is the last-known-good file ("" disables it). It holds the
	// last config this process APPLIED, as JSON, written atomically with
	// mode 0600 (temp file in the same directory, fsync, rename).
	LKGPath string
	// BackoffMin and BackoffMax bound the reconnect backoff (default 500ms
	// and 30s); BackoffMin > BackoffMax after defaulting is an error.
	BackoffMin time.Duration
	BackoffMax time.Duration
	// IdleTimeout drops the stream when no byte arrives for this long
	// (default 45s, three heartbeats).
	IdleTimeout time.Duration
	// MaxEventBytes caps one event's data (default 8 MiB); one line may be
	// at most 64 bytes longer. Exceeding either drops the connection.
	MaxEventBytes int
	// StatusTimeout bounds each status POST (default 5s).
	StatusTimeout time.Duration
	// HTTPClient defaults to &http.Client{}. Its Timeout must be 0: a client
	// timeout would kill the long-lived stream.
	HTTPClient *http.Client
	// Logger defaults to discarding.
	Logger *slog.Logger
	// Sleep is the backoff wait (default: a timer or ctx.Done, returning
	// ctx.Err()). Tests inject a recording fake.
	Sleep func(ctx context.Context, d time.Duration) error
	// Rand is the jitter source in [0,1) (default math/rand/v2.Float64).
	Rand func() float64
}

// RemoteSource is the ConfigSource that follows the control plane's SSE config
// stream (ADR 0005). It delivers the on-disk last-known-good config first, if
// there is one, then every newer version the stream offers. It implements
// Acknowledger: only Applied advances the resume cursor and the LKG file.
//
// Goroutines (all started by Watch, all stopped by its ctx): the stream
// reader, the worker (the only place with LKG disk I/O and status POSTs),
// and a closer that closes the channel once both have exited.
type RemoteSource struct {
	streamURL     string // full stream URL up to and including "since="
	statusURL     string
	token         string
	nodeID        string
	lkgPath       string
	backoffMin    time.Duration
	backoffMax    time.Duration
	idleTimeout   time.Duration
	maxEventBytes int
	statusTimeout time.Duration
	client        *http.Client
	log           *slog.Logger
	sleep         func(context.Context, time.Duration) error
	rand          func() float64

	// connected is read by metrics from other goroutines and written only by
	// the reader; a lone flag needs no mutex.
	connected atomic.Bool

	// notify wakes the worker. Capacity 1 and non-blocking sends coalesce
	// any number of acks into one wake-up. It is never closed, so acks
	// after shutdown cannot panic.
	notify chan struct{}

	// mu guards the fields below. They form one invariant that three
	// goroutines (reader, reconcile loop via the ack methods, worker) update
	// together, e.g. Applied moves cursor, pending and lkgPending at once, so
	// a single short mutex is simpler and safer than several atomics. Every
	// critical section is O(len(pending)), which is at most a few entries,
	// and never does I/O.
	mu           sync.Mutex
	started      bool
	delivered    int64             // highest version sent on the channel (high-water mark)
	cursor       int64             // highest APPLIED version; sent as since=
	pending      map[int64]*Config // delivered and possibly received, not yet acknowledged
	lkgPending   *Config           // applied, not yet written to LKGPath
	lastRejected int64
	lastErr      string
	reportDirty  bool
	// workerDone is set by the worker as it exits. From then on nobody would
	// pick up lkgPending, so a late Applied writes the LKG file itself.
	workerDone bool

	// lkgMu serializes LKG writes and guards lkgWritten. Normally only the
	// worker writes, so it is uncontended; it exists for the shutdown overlap
	// where the worker's final flush and a late Applied could both write.
	// Holding it across the write (disk I/O) is the point: two renames racing
	// could otherwise leave the older version on disk.
	lkgMu      sync.Mutex
	lkgWritten int64 // highest version written to LKGPath by this process
}

// NewRemoteSource validates opts and returns a source; nothing starts until
// Watch.
func NewRemoteSource(opts RemoteOptions) (*RemoteSource, error) {
	base, err := checkBaseURL(opts.BaseURL)
	if err != nil {
		return nil, err
	}
	if opts.Token == "" {
		return nil, errors.New("config: remote source: Token is required")
	}
	if !validNodeID(opts.NodeID) {
		return nil, fmt.Errorf("config: remote source: NodeID %q must be 1-%d bytes of [A-Za-z0-9._-] starting with a letter or digit",
			opts.NodeID, maxNodeIDLen)
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{}
	}
	if client.Timeout != 0 {
		return nil, errors.New("config: remote source: HTTPClient.Timeout must be 0 (it would cut the stream)")
	}
	s := &RemoteSource{
		streamURL:     base + "/v1/config/stream?node=" + url.QueryEscape(opts.NodeID) + "&since=",
		statusURL:     base + "/v1/dataplanes/" + url.PathEscape(opts.NodeID) + "/status",
		token:         opts.Token,
		nodeID:        opts.NodeID,
		lkgPath:       opts.LKGPath,
		backoffMin:    orDefault(opts.BackoffMin, DefaultRemoteBackoffMin),
		backoffMax:    orDefault(opts.BackoffMax, DefaultRemoteBackoffMax),
		idleTimeout:   orDefault(opts.IdleTimeout, DefaultRemoteIdleTimeout),
		maxEventBytes: orDefault(opts.MaxEventBytes, DefaultRemoteMaxEventBytes),
		statusTimeout: orDefault(opts.StatusTimeout, DefaultRemoteStatusTimeout),
		client:        client,
		log:           opts.Logger,
		sleep:         opts.Sleep,
		rand:          opts.Rand,
		notify:        make(chan struct{}, 1),
		pending:       make(map[int64]*Config),
	}
	if s.backoffMin > s.backoffMax {
		return nil, fmt.Errorf("config: remote source: BackoffMin %v > BackoffMax %v", s.backoffMin, s.backoffMax)
	}
	if s.log == nil {
		s.log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if s.sleep == nil {
		s.sleep = sleepCtx
	}
	if s.rand == nil {
		s.rand = rand.Float64
	}
	s.log = s.log.With("component", "config.RemoteSource", "node", opts.NodeID)
	return s, nil
}

func orDefault[T time.Duration | int](v, def T) T {
	if v <= 0 {
		return def
	}
	return v
}

// checkBaseURL returns BaseURL as "scheme://host[:port]" or an error.
func checkBaseURL(raw string) (string, error) {
	if raw == "" {
		return "", errors.New("config: remote source: BaseURL is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("config: remote source: BaseURL: %w", err)
	}
	switch {
	case u.Scheme != "http" && u.Scheme != "https":
		return "", fmt.Errorf("config: remote source: BaseURL %q must be an absolute http or https URL", raw)
	case u.Host == "":
		return "", fmt.Errorf("config: remote source: BaseURL %q has no host", raw)
	case u.RawQuery != "" || u.ForceQuery || u.Fragment != "":
		return "", fmt.Errorf("config: remote source: BaseURL %q must not have a query or fragment", raw)
	case u.Path != "" && u.Path != "/":
		return "", fmt.Errorf("config: remote source: BaseURL %q must not have a path", raw)
	}
	return u.Scheme + "://" + u.Host, nil
}

// validNodeID is the node rule shared with the control plane (§6.4).
func validNodeID(s string) bool {
	if len(s) == 0 || len(s) > maxNodeIDLen || s[0] == '.' || s[0] == '_' || s[0] == '-' {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isNameByte(s[i]) {
			return false
		}
	}
	return true
}

// sleepCtx is the default Sleep: it waits for d or until ctx is done.
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

// Connected reports whether a stream is open and has delivered at least one
// frame (event or heartbeat) on the current connection.
func (s *RemoteSource) Connected() bool { return s.connected.Load() }

// Watch implements ConfigSource. It never fails for transient reasons; the
// only error is ErrAlreadyWatching.
func (s *RemoteSource) Watch(ctx context.Context) (<-chan *Config, error) {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return nil, ErrAlreadyWatching
	}
	s.started = true
	s.mu.Unlock()

	out := make(chan *Config, 1)
	s.removeStaleLKGTemps()
	if cfg := s.loadLKG(); cfg != nil {
		s.mu.Lock()
		s.delivered = cfg.Version
		s.pending[cfg.Version] = cfg
		s.mu.Unlock()
		out <- cfg // the buffer is empty and nobody else sends yet, so this cannot block
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); s.readLoop(ctx, out) }()
	go func() { defer wg.Done(); s.workLoop(ctx) }()
	// ConfigSource rule 7: close only after every goroutine that could
	// send (or still has work) has exited.
	go func() { wg.Wait(); close(out) }()
	return out, nil
}

// removeStaleLKGTemps deletes temp files a crash between CreateTemp and rename
// left behind. Nothing else writes them before Watch starts, so it is safe to
// do synchronously here. Failure only costs disk space, so it is logged.
func (s *RemoteSource) removeStaleLKGTemps() {
	if s.lkgPath == "" {
		return
	}
	removed, err := removeStaleLKGTemps(s.lkgPath)
	for _, name := range removed {
		s.log.Info("removed stale last-known-good temp file", "file", name)
	}
	if err != nil {
		s.log.Warn("removing stale last-known-good temp files", "path", s.lkgPath, "error", err)
	}
}

// loadLKG returns the last-known-good config, or nil if there is none or it
// is unusable (logged at Warn and left on disk).
func (s *RemoteSource) loadLKG() *Config {
	if s.lkgPath == "" {
		return nil
	}
	cfg, err := readLKG(s.lkgPath)
	if err != nil {
		s.log.Warn("ignoring unusable last-known-good config file; waiting for the control plane",
			"path", s.lkgPath, "error", err)
		return nil
	}
	if cfg == nil {
		s.log.Info("no last-known-good config file; waiting for the control plane", "path", s.lkgPath)
		return nil
	}
	s.log.Info("delivering last-known-good config", "path", s.lkgPath, "version", cfg.Version)
	return cfg
}

// ---- stream reader ---------------------------------------------------------

// readLoop connects, streams and reconnects with backoff until ctx is done.
func (s *RemoteSource) readLoop(ctx context.Context, out chan *Config) {
	attempt := 0
	for first := true; ; first = false {
		if !first { // the very first connect is immediate
			if err := s.sleep(ctx, s.backoff(attempt)); err != nil {
				return
			}
			attempt++
		}
		if ctx.Err() != nil {
			return
		}
		if s.stream(ctx, out) {
			attempt = 0 // a complete frame proves the path works: start the schedule over
		}
	}
}

// backoff is equal jitter: cap = min(BackoffMax, BackoffMin << attempt),
// d = cap/2 + Rand()*cap/2, so d is in [cap/2, cap).
func (s *RemoteSource) backoff(attempt int) time.Duration {
	c := s.backoffMin
	for i := 0; i < attempt && c < s.backoffMax; i++ {
		if c > s.backoffMax/2 { // doubling would pass the max (or overflow)
			c = s.backoffMax
		} else {
			c *= 2
		}
	}
	r := s.rand()
	if !(r >= 0 && r < 1) { // a broken injected source must not produce a negative or too-long wait
		r = 0
	}
	return c/2 + time.Duration(r*float64(c)/2)
}

// stream runs one connection until it ends. It reports whether at least one
// complete frame arrived (which resets the backoff).
func (s *RemoteSource) stream(ctx context.Context, out chan *Config) (healthy bool) {
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// One timer per connection: armed now (so a server that never sends
	// headers is also caught) and re-armed by activityReader on every read.
	// When it fires it cancels the request, which unblocks the body read.
	idle := time.AfterFunc(s.idleTimeout, cancel)
	defer idle.Stop()
	defer s.connected.Store(false)

	// The cursor may advance concurrently (Applied); either value is safe,
	// because anything re-offered is dropped by the high-water mark.
	s.mu.Lock()
	since := s.cursor
	s.mu.Unlock()

	req, err := http.NewRequestWithContext(connCtx, http.MethodGet, s.streamURL+strconv.FormatInt(since, 10), nil)
	if err != nil { // cannot happen: the URL was validated in the constructor
		s.log.Error("config stream: building request", "error", err)
		return false
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := s.client.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Warn("config stream: connect failed, retrying with backoff", "error", err)
		}
		return false
	}
	// Closing the body on every path releases the connection; on shutdown
	// connCtx is already canceled, so the transport tears it down.
	defer resp.Body.Close()

	if !isEventStream(resp) {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDiscardBytes))
		level := slog.LevelWarn
		if resp.StatusCode == http.StatusUnauthorized {
			level = slog.LevelError // a wrong data-plane token never fixes itself
		}
		s.log.Log(ctx, level, "config stream: unexpected response, retrying with backoff",
			"status", resp.StatusCode, "content_type", resp.Header.Get("Content-Type"))
		return false
	}
	s.log.Info("config stream: connected", "since", since)

	rd := newSSEReader(&activityReader{r: resp.Body, idle: idle, d: s.idleTimeout}, s.maxEventBytes)
	for {
		f, err := rd.next()
		if err != nil {
			switch {
			case ctx.Err() != nil: // shutdown: not worth a log line
			case errors.Is(err, errFrameTooLarge):
				s.log.Error("config stream: protocol error, reconnecting", "error", err, "max_event_bytes", s.maxEventBytes)
			case connCtx.Err() != nil:
				s.log.Warn("config stream: idle timeout, reconnecting", "idle_timeout", s.idleTimeout)
			case errors.Is(err, io.EOF):
				s.log.Info("config stream: ended by the control plane, reconnecting")
			default:
				s.log.Warn("config stream: read failed, reconnecting", "error", err)
			}
			return healthy
		}
		if !healthy {
			healthy = true
			s.connected.Store(true)
		}
		if !f.comment && f.event == "config" { // other event types (incl. the default "message") are ignored
			s.handleConfig(f, out)
		}
	}
}

// isEventStream accepts only 200 with media type text/event-stream.
func isEventStream(resp *http.Response) bool {
	if resp.StatusCode != http.StatusOK {
		return false
	}
	mt, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	return err == nil && mt == "text/event-stream"
}

// handleConfig applies §5.1 "Handling a config event" in order. Invalid events
// are dropped; the connection stays open.
func (s *RemoteSource) handleConfig(f sseFrame, out chan *Config) {
	cfg, err := ParseJSON(f.data)
	if err != nil {
		s.log.Error("config stream: invalid config from control plane, dropped", "id", f.id, "error", err)
		return
	}
	if f.hasID && f.id != strconv.FormatInt(cfg.Version, 10) {
		s.log.Warn("config stream: event id does not match config version, dropped", "id", f.id, "version", cfg.Version)
		return
	}
	s.mu.Lock()
	if cfg.Version <= s.delivered {
		delivered := s.delivered
		s.mu.Unlock()
		s.log.Debug("config stream: version already delivered, dropped", "version", cfg.Version, "delivered", delivered)
		return
	}
	s.delivered = cfg.Version
	s.pending[cfg.Version] = cfg
	s.mu.Unlock()

	// The reader is the only sender, so this never blocks. A config pushed
	// out of the buffer can never be received, hence never acknowledged:
	// forget it so pending stays small even under a fast stream.
	if stale := replaceLatest(out, cfg); stale != nil {
		s.mu.Lock()
		delete(s.pending, stale.Version)
		s.mu.Unlock()
	}
	s.log.Info("config stream: delivered config", "version", cfg.Version)
}

// ---- acknowledgements --------------------------------------------------------

// Applied implements Acknowledger: it advances the cursor in memory and hands
// the LKG write and the status report to the worker. While Watch is running it
// never blocks.
//
// Late acks are allowed by the Acknowledger contract: the reconcile loop can
// finish applying a config just as ctx is canceled. Once the worker has
// exited, nobody would ever pick up lkgPending, and a restart with the
// control plane down would come back one version behind. So on that path
// only, Applied writes the LKG file synchronously (one small file, fsync and
// rename). The status report is not sent: shutdown has canceled its context.
func (s *RemoteSource) Applied(version int64) {
	s.mu.Lock()
	cfg, ok := s.pending[version]
	if !ok {
		cursor := s.cursor
		s.mu.Unlock()
		s.log.Warn("config ack: Applied for a version that is not pending, ignored", "version", version, "applied", cursor)
		return
	}
	// Every pending entry is above the cursor (acks delete everything at or
	// below their version), so this only moves forwards.
	s.cursor = max(s.cursor, version)
	writeNow := s.lkgPath != "" && s.workerDone
	if s.lkgPath != "" && !s.workerDone {
		s.lkgPending = cfg
	}
	s.prunePendingLocked(version)
	s.reportDirty = true
	s.mu.Unlock()
	if writeNow {
		s.persist(cfg)
		return
	}
	s.signal()
}

// Rejected implements Acknowledger: it records the failure for the status
// report and leaves the cursor and the LKG file alone. The version stays at
// or below the delivered mark, so it is not delivered again in this process.
func (s *RemoteSource) Rejected(version int64, err error) {
	msg := "unknown error" // the contract says err is never nil; do not panic if it is
	if err != nil {
		msg = err.Error()
	}
	s.mu.Lock()
	if _, ok := s.pending[version]; !ok {
		cursor := s.cursor
		s.mu.Unlock()
		s.log.Warn("config ack: Rejected for a version that is not pending, ignored", "version", version, "applied", cursor)
		return
	}
	s.lastRejected = max(s.lastRejected, version)
	s.lastErr = truncateUTF8(msg, maxStatusErrorBytes)
	s.prunePendingLocked(version)
	s.reportDirty = true
	s.mu.Unlock()
	s.signal()
	s.log.Error("config rejected by the gateway; still serving the previous config", "version", version, "error", msg)
}

// prunePendingLocked forgets every pending version <= v: the consumer
// receives in order, so it will never acknowledge them. Caller holds s.mu.
func (s *RemoteSource) prunePendingLocked(v int64) {
	for k := range s.pending {
		if k <= v {
			delete(s.pending, k)
		}
	}
}

// signal wakes the worker without ever blocking. A full buffer already means
// "work pending", and the worker reads all state fresh under the mutex.
func (s *RemoteSource) signal() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// truncateUTF8 cuts s to at most n bytes without splitting a rune. Invalid
// UTF-8 is replaced first: JSON encoding would otherwise expand each bad byte
// to three, pushing the text over the server's byte limit.
func truncateUTF8(s string, n int) string {
	s = strings.ToValidUTF8(s, "�")
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// ---- worker --------------------------------------------------------------------

// statusReport has exactly the shape of controlplane.StatusReport (§6.6);
// config may not import controlplane, so it keeps its own copy.
type statusReport struct {
	Applied      int64  `json:"applied"`
	LastRejected int64  `json:"last_rejected"`
	Error        string `json:"error,omitempty"`
}

// workLoop owns all disk and status-network I/O, so Applied and Rejected
// never wait on it. It runs until ctx is done, then flushes a pending LKG
// write (disk only) and exits.
func (s *RemoteSource) workLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			// Taking lkgPending and setting workerDone in one critical
			// section means every Applied is handled exactly once: before
			// this point its config is in lkgPending (flushed below), after
			// it Applied writes the file itself.
			s.mu.Lock()
			lkg := s.lkgPending
			s.lkgPending = nil
			s.workerDone = true
			s.mu.Unlock()
			if lkg != nil {
				s.persist(lkg)
			}
			return
		case <-s.notify:
		}

		s.mu.Lock()
		lkg := s.lkgPending
		s.lkgPending = nil
		rep, send := statusReport{Applied: s.cursor, LastRejected: s.lastRejected, Error: s.lastErr}, s.reportDirty
		s.reportDirty = false
		s.mu.Unlock()

		// LKG first, so by the time the control plane shows "applied N" the
		// gateway can also restart into N.
		if lkg != nil {
			s.persist(lkg)
		}
		if send && ctx.Err() == nil {
			s.postStatus(ctx, rep)
		}
	}
}

// persist writes the LKG file. Failure is logged, not retried: the config is
// live in memory and the file only matters after a restart. The next Applied
// writes a fresh copy anyway. Writes are serialized and never go backwards,
// so the worker's final flush and a late Applied cannot leave an older
// version on disk.
func (s *RemoteSource) persist(cfg *Config) {
	s.lkgMu.Lock()
	defer s.lkgMu.Unlock()
	if cfg.Version <= s.lkgWritten {
		return
	}
	if err := writeLKG(s.lkgPath, cfg); err != nil {
		s.log.Error("writing last-known-good config file failed", "path", s.lkgPath, "version", cfg.Version, "error", err)
		return
	}
	s.lkgWritten = cfg.Version
	s.log.Debug("wrote last-known-good config file", "path", s.lkgPath, "version", cfg.Version)
}

// postStatus sends one best-effort report, bounded by StatusTimeout and by
// ctx (shutdown cancels a hung POST). It is not retried: the next ack sends
// fresher state anyway.
func (s *RemoteSource) postStatus(ctx context.Context, rep statusReport) {
	body, err := json.Marshal(rep)
	if err != nil { // cannot happen for this struct
		s.log.Error("status report: marshal", "error", err)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, s.statusTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.statusURL, bytes.NewReader(body))
	if err != nil {
		s.log.Error("status report: building request", "error", err)
		return
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		s.log.Warn("status report to the control plane failed", "error", err, "applied", rep.Applied)
		return
	}
	// Drain a little so the keep-alive connection can be reused.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxDiscardBytes))
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		s.log.Warn("status report rejected by the control plane", "status", resp.StatusCode, "applied", rep.Applied)
	}
}

var (
	_ ConfigSource = (*RemoteSource)(nil)
	_ Acknowledger = (*RemoteSource)(nil)
)
