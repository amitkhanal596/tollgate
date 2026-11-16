package config

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

// ---- 5. Applied: LKG file and status report --------------------------------

func TestRemoteAppliedWritesLKGAndReports(t *testing.T) {
	dir := t.TempDir()
	lkg := filepath.Join(dir, "lkg.json")
	h := newRS(t, func(o *RemoteOptions) { o.LKGPath = lkg })
	h.watch()
	c1 := h.cp.waitConn(t, 1)
	c1.send(rsFrame(5))
	cfg := h.expectVersion(5)

	if _, err := os.Stat(lkg); err == nil {
		t.Fatal("LKG must not be written on receipt, only on Applied")
	}
	h.src.Applied(5)
	rep := h.cp.waitReport(t, "applied 5", func(r rsStatusReq) bool { return r.Applied == 5 })
	if rep.LastRejected != 0 || rep.Error != "" {
		t.Errorf("report = %+v, want only applied=5", rep.rsReport)
	}
	if rep.method != "POST" || rep.path != "/v1/dataplanes/"+rsNode+"/status" ||
		rep.auth != "Bearer "+rsToken || !strings.HasPrefix(rep.ctype, "application/json") {
		t.Errorf("status request shape wrong: %+v", rep)
	}

	// The LKG write happens before the status POST in the worker, so it is visible now.
	b, err := os.ReadFile(lkg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ParseJSON(b)
	if err != nil {
		t.Fatalf("LKG does not parse back: %v", err)
	}
	if !reflect.DeepEqual(got, cfg) {
		t.Errorf("LKG content differs from the delivered config:\n got %+v\nwant %+v", got, cfg)
	}
	if fi, _ := os.Stat(lkg); fi.Mode().Perm() != 0o600 {
		t.Errorf("LKG mode = %v, want 0600", fi.Mode().Perm())
	}
	if names := rsDirNames(t, dir); !reflect.DeepEqual(names, []string{"lkg.json"}) {
		t.Errorf("directory = %v, want only lkg.json (no leftover temp files)", names)
	}
}

// A concurrent reader must always see a complete, valid file: temp file in
// the same directory plus rename, never an in-place truncate and write.
func TestRemoteLKGWriteIsAtomic(t *testing.T) {
	dir := t.TempDir()
	lkg := filepath.Join(dir, "lkg.json")
	h := newRS(t, func(o *RemoteOptions) { o.LKGPath = lkg })
	h.watch()
	c1 := h.cp.waitConn(t, 1)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var bad []string
	var reads int
	wg.Add(1)
	go func() {
		defer wg.Done()
		var last int64
		for {
			select {
			case <-stop:
				return
			default:
			}
			b, err := os.ReadFile(lkg)
			if errors.Is(err, os.ErrNotExist) {
				continue // before the first write
			}
			mu.Lock()
			reads++
			if err != nil {
				bad = append(bad, "read: "+err.Error())
			} else if c, err := ParseJSON(b); err != nil {
				bad = append(bad, "partial or invalid content: "+err.Error())
			} else if c.Version < last {
				bad = append(bad, "version went backwards")
			} else {
				last = c.Version
			}
			mu.Unlock()
		}
	}()

	const n = 25
	for v := int64(1); v <= n; v++ {
		c1.send(rsFrameData(v, rsBigCfgJSON(v, 300)))
		h.expectVersion(v)
		h.src.Applied(v)
	}
	rsWaitLKGVersion(t, lkg, n)
	h.cp.waitReport(t, "applied 25", func(r rsStatusReq) bool { return r.Applied == n })
	close(stop)
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if len(bad) > 0 {
		t.Fatalf("reader saw %d bad states after %d reads, first: %s", len(bad), reads, bad[0])
	}
	if names := rsDirNames(t, dir); !reflect.DeepEqual(names, []string{"lkg.json"}) {
		t.Errorf("directory = %v, want only lkg.json", names)
	}
}

// Shutdown flushes a pending LKG write (disk only).
//
// Fix round 4: the old version called Applied(5) and cancel back to back, so
// the worker's normal wake-up path usually wrote the file and deleting the
// final flush went unnoticed (30/30 passes). Now the worker is parked in a
// hung status POST when version 6 is applied, so the file provably holds
// version 5 until cancel. Cancel then frees the worker, which has two ready
// cases (ctx.Done and notify) and picks one at random; only the final flush
// guarantees version 6 in both orders, so the scenario repeats to make a
// missing flush fail with near certainty.
func TestRemoteFinalLKGWriteOnShutdown(t *testing.T) {
	for i := 0; i < 20; i++ {
		lkg := filepath.Join(t.TempDir(), "lkg.json")
		h := newRS(t, func(o *RemoteOptions) { o.LKGPath = lkg; o.StatusTimeout = 30 * time.Second })
		h.cp.statusHang.Store(true)
		ch := h.watch()
		c1 := h.cp.waitConn(t, 1)

		c1.send(rsFrame(5))
		h.expectVersion(5)
		h.src.Applied(5)
		rsWaitLKGVersion(t, lkg, 5)
		h.cp.waitReport(t, "applied 5 (POST now hangs)", func(r rsStatusReq) bool { return r.Applied == 5 })

		c1.send(rsFrame(6))
		h.expectVersion(6)
		h.src.Applied(6) // the worker is stuck in the POST, so this stays pending
		time.Sleep(20 * time.Millisecond)
		if v, err := rsReadLKG(lkg); err != nil || v != 5 {
			t.Fatalf("iteration %d: LKG before cancel = (%d, %v), want version 5 (worker should be parked)", i, v, err)
		}

		h.cancel()
		rsDrainClosed(t, ch, 2*time.Second)
		if v, err := rsReadLKG(lkg); err != nil || v != 6 {
			t.Fatalf("iteration %d: LKG after shutdown = (%d, %v), want version 6", i, v, err)
		}
	}
}

func TestRemoteNoLKGPathMeansNoPersistence(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir) // a stray relative write would land here
	h := newRS(t)
	h.watch()
	h.cp.waitConn(t, 1).send(rsFrame(5))
	h.expectVersion(5)
	h.src.Applied(5)
	h.cp.waitReport(t, "applied 5", func(r rsStatusReq) bool { return r.Applied == 5 })
	if names := rsDirNames(t, dir); len(names) != 0 {
		t.Fatalf("files written with LKGPath empty: %v", names)
	}
}

// A failed LKG write is logged, never fatal: the config is live in memory.
func TestRemoteLKGWriteFailureDoesNotBreakStream(t *testing.T) {
	logs := &rsLogBuf{}
	h := newRS(t, func(o *RemoteOptions) {
		o.LKGPath = filepath.Join(t.TempDir(), "missing-dir", "lkg.json")
		o.Logger = logs.logger()
	})
	h.watch()
	c1 := h.cp.waitConn(t, 1)
	c1.send(rsFrame(5))
	h.expectVersion(5)
	h.src.Applied(5)
	h.cp.waitReport(t, "applied 5 despite LKG failure", func(r rsStatusReq) bool { return r.Applied == 5 })
	c1.send(rsFrame(6))
	h.expectVersion(6)
	if !strings.Contains(logs.String(), "level=ERROR") {
		t.Errorf("a failed LKG write must be logged at Error:\n%s", logs.String())
	}
}

// Status reporting is best effort: it never blocks Applied, never blocks the
// stream, and a hung endpoint is cut off after StatusTimeout.
func TestRemoteStatusEndpointHangs(t *testing.T) {
	lkg := filepath.Join(t.TempDir(), "lkg.json")
	h := newRS(t, func(o *RemoteOptions) { o.LKGPath = lkg; o.StatusTimeout = 150 * time.Millisecond })
	h.cp.statusHang.Store(true)
	h.watch()
	c1 := h.cp.waitConn(t, 1)
	c1.send(rsFrame(5))
	h.expectVersion(5)

	start := time.Now()
	h.src.Applied(5)
	if d := time.Since(start); d > 250*time.Millisecond {
		t.Fatalf("Applied took %v; it must not wait for the status POST", d)
	}
	rsWaitLKGVersion(t, lkg, 5) // disk work is not held up either

	c1.send(rsFrame(6)) // the stream keeps flowing while a POST hangs
	h.expectVersion(6)
	h.src.Applied(6)
	waitFor(t, 3*time.Second, "hung POST to be cut off", func() bool {
		h.cp.mu.Lock()
		defer h.cp.mu.Unlock()
		return len(h.cp.hangElapsed) > 0
	})
	h.cp.mu.Lock()
	el := h.cp.hangElapsed[0]
	h.cp.mu.Unlock()
	if el < 100*time.Millisecond || el > 2*time.Second {
		t.Errorf("hung POST ended after %v, want about StatusTimeout (150ms)", el)
	}
	// Latest wins: the worker then sends the fresher state.
	h.cp.statusHang.Store(false)
	h.cp.waitReport(t, "applied 6 after the timeout", func(r rsStatusReq) bool { return r.Applied == 6 })
	rsWaitLKGVersion(t, lkg, 6)
}

func TestRemoteStatusEndpointErrors(t *testing.T) {
	for _, code := range []int{500, 401, 404} {
		t.Run(fmt.Sprintf("status %d", code), func(t *testing.T) {
			h := newRS(t)
			h.cp.statusCode.Store(int32(code))
			h.watch()
			c1 := h.cp.waitConn(t, 1)
			c1.send(rsFrame(5))
			h.expectVersion(5)
			h.src.Applied(5)
			h.cp.waitReport(t, "applied 5", func(r rsStatusReq) bool { return r.Applied == 5 })
			c1.send(rsFrame(6))
			h.expectVersion(6)
			if h.cp.connCount() != 1 {
				t.Fatal("a failing status endpoint must not disturb the stream")
			}
		})
	}
}

// §5.1 / ack.go rule 9: acks for versions the source did not deliver are
// logged and ignored (no LKG, no cursor move, no report).
func TestRemoteUnknownVersionAcksAreIgnored(t *testing.T) {
	dir := t.TempDir()
	lkg := filepath.Join(dir, "lkg.json")
	logs := &rsLogBuf{}
	h := newRS(t, func(o *RemoteOptions) { o.LKGPath = lkg; o.Logger = logs.logger() })
	h.watch()
	c1 := h.cp.waitConn(t, 1)
	c1.send(rsFrame(5))
	h.expectVersion(5)

	h.src.Applied(99)
	h.src.Rejected(98, errors.New("never delivered"))
	h.src.Applied(5) // barrier: the first report that arrives must be this one
	h.cp.waitReport(t, "applied 5", func(r rsStatusReq) bool { return r.Applied == 5 })
	for _, r := range h.cp.allReports() {
		if r.Applied == 99 || r.LastRejected == 98 {
			t.Errorf("report for an undelivered version: %+v", r.rsReport)
		}
	}
	if v, err := rsReadLKG(lkg); err != nil || v != 5 {
		t.Errorf("LKG = (%d, %v), want 5", v, err)
	}
	c1.abort()
	if since := h.cp.waitConn(t, 2).req.since; since != "5" {
		t.Errorf("since = %q, want 5", since)
	}
	if !strings.Contains(logs.String(), "level=WARN") {
		t.Errorf("ignored acks should be logged at Warn:\n%s", logs.String())
	}
}

// ---- 6. Rejected --------------------------------------------------------------

func TestRemoteRejected(t *testing.T) {
	dir := t.TempDir()
	lkg := filepath.Join(dir, "lkg.json")
	h := newRS(t, func(o *RemoteOptions) { o.LKGPath = lkg })
	h.watch()
	c1 := h.cp.waitConn(t, 1)
	c1.send(rsFrame(5))
	h.expectVersion(5)
	h.src.Applied(5)
	h.cp.waitReport(t, "applied 5", func(r rsStatusReq) bool { return r.Applied == 5 })
	before, err := os.ReadFile(lkg)
	if err != nil {
		t.Fatal(err)
	}

	c1.send(rsFrame(6))
	h.expectVersion(6)
	h.src.Rejected(6, errors.New("build: jwt public key: open /keys/jwt.pem: no such file"))
	rep := h.cp.waitReport(t, "rejected 6", func(r rsStatusReq) bool { return r.LastRejected == 6 })
	if rep.Applied != 5 || rep.Error != "build: jwt public key: open /keys/jwt.pem: no such file" {
		t.Errorf("report = %+v, want applied=5 last_rejected=6 and the error text", rep.rsReport)
	}
	after, _ := os.ReadFile(lkg)
	if string(after) != string(before) {
		t.Error("Rejected must not change the LKG file")
	}
	if names := rsDirNames(t, dir); !reflect.DeepEqual(names, []string{"lkg.json"}) {
		t.Errorf("directory = %v", names)
	}

	// The cursor stays at what is actually running, and the rejected version
	// is not re-delivered when the server offers it again after a reconnect.
	c1.abort()
	c2 := h.cp.waitConn(t, 2)
	if c2.req.since != "5" {
		t.Fatalf("since after Rejected(6) = %q, want 5", c2.req.since)
	}
	c2.send(rsFrame(6))
	h.quiet()
	c2.send(rsFrame(7))
	h.expectVersion(7) // a later version is delivered normally
	h.src.Applied(7)
	// last_rejected is sticky across a later Applied.
	rep = h.cp.waitReport(t, "applied 7", func(r rsStatusReq) bool { return r.Applied == 7 })
	if rep.LastRejected != 6 || rep.Error == "" {
		t.Errorf("report after Applied(7) = %+v, want last_rejected=6 and the old error kept", rep.rsReport)
	}
}

func TestRemoteRejectedWithoutPriorApplyKeepsCursorAtZero(t *testing.T) {
	h := newRS(t)
	h.watch()
	c1 := h.cp.waitConn(t, 1)
	c1.send(rsFrame(5))
	h.expectVersion(5)
	h.src.Rejected(5, errors.New("boom"))
	h.cp.waitReport(t, "rejected 5", func(r rsStatusReq) bool { return r.LastRejected == 5 && r.Applied == 0 })
	c1.abort()
	c2 := h.cp.waitConn(t, 2)
	if c2.req.since != "0" {
		t.Fatalf("since = %q, want 0", c2.req.since)
	}
	c2.send(rsFrame(5))
	h.quiet()
}

func TestRemoteRejectedErrorTruncation(t *testing.T) {
	tests := []struct {
		name    string
		err     string
		wantLen int // -1: unchanged
	}{
		{"short is unchanged", "short", -1},
		{"exactly 1024 is unchanged", strings.Repeat("a", 1024), -1},
		{"ascii over the limit is cut at 1024", strings.Repeat("a", 5000), 1024},
		{"multi-byte rune is not split", "a" + strings.Repeat("é", 700), 1023},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newRS(t)
			h.watch()
			c1 := h.cp.waitConn(t, 1)
			c1.send(rsFrame(5))
			h.expectVersion(5)
			h.src.Rejected(5, errors.New(tc.err))
			rep := h.cp.waitReport(t, "rejected 5", func(r rsStatusReq) bool { return r.LastRejected == 5 })
			if !utf8.ValidString(rep.Error) {
				t.Fatal("truncated error is not valid UTF-8")
			}
			if !strings.HasPrefix(tc.err, rep.Error) {
				t.Fatal("truncated error is not a prefix of the original")
			}
			want := tc.wantLen
			if want == -1 {
				want = len(tc.err)
			}
			if len(rep.Error) != want {
				t.Fatalf("error length = %d, want %d", len(rep.Error), want)
			}
		})
	}
}

// ---- 7. LKG at startup ----------------------------------------------------------

func TestRemoteLKGDeliveredFirstWithControlPlaneDown(t *testing.T) {
	lkg := filepath.Join(t.TempDir(), "lkg.json")
	want := rsWriteLKG(t, lkg, 3)
	h := newRS(t, func(o *RemoteOptions) { o.LKGPath = lkg })
	h.cp.close() // control plane is down
	h.watch()
	got := h.expectVersion(3)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("LKG config = %+v, want %+v", got, want)
	}
	h.quiet()
}

func TestRemoteLKGThenStream(t *testing.T) {
	lkg := filepath.Join(t.TempDir(), "lkg.json")
	rsWriteLKG(t, lkg, 3)
	h := newRS(t, func(o *RemoteOptions) { o.LKGPath = lkg })
	h.watch()
	h.expectVersion(3)
	c1 := h.cp.waitConn(t, 1)
	if c1.req.since != "0" {
		t.Errorf("since = %q: the cursor is the applied version, and nothing was applied yet", c1.req.since)
	}
	c1.send(rsFrame(3)) // head equals the LKG version: already delivered
	c1.send(rsFrame(2))
	h.quiet()
	c1.send(rsFrame(4))
	h.expectVersion(4)

	// The LKG version can be acknowledged like any delivered config.
	h.src.Applied(3)
	h.cp.waitReport(t, "applied 3", func(r rsStatusReq) bool { return r.Applied == 3 })
	c1.abort()
	if since := h.cp.waitConn(t, 2).req.since; since != "3" {
		t.Errorf("since after Applied(3) = %q, want 3", since)
	}
}

func TestRemoteLKGIgnoredWhenUnusable(t *testing.T) {
	good := func(v int64) string { return rsCfgJSON(v) }
	tests := []struct {
		name    string
		content string
		isDir   bool
		warns   bool // §5.1: unreadable, unparseable or invalid is logged at Warn
	}{
		{"corrupt json", "{not json", false, true},
		{"truncated json", good(3)[:40], false, true},
		{"empty file", "", false, true},
		{"unknown field", `{"version":3,"bogus":true,"routes":[],"upstreams":[]}`, false, true},
		{"fails Validate", `{"version":3,"routes":[{"name":"r","path_prefix":"/","upstream":"ghost"}],"upstreams":[]}`, false, true},
		{"trailing garbage", good(3) + " {}", false, true},
		{"path is a directory", "", true, true},
		{"version zero", good(0), false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			lkg := filepath.Join(t.TempDir(), "lkg.json")
			if tc.isDir {
				if err := os.Mkdir(lkg, 0o755); err != nil {
					t.Fatal(err)
				}
			} else {
				writeFile(t, lkg, tc.content)
			}
			logs := &rsLogBuf{}
			h := newRS(t, func(o *RemoteOptions) { o.LKGPath = lkg; o.Logger = logs.logger() })
			h.watch() // no error from Watch
			c1 := h.cp.waitConn(t, 1)
			h.quiet() // nothing delivered from the file
			if c1.req.since != "0" {
				t.Errorf("since = %q, want 0", c1.req.since)
			}
			c1.send(rsFrame(1)) // the stream still works, even below the ignored file's version
			h.expectVersion(1)
			if tc.warns && !strings.Contains(logs.String(), "level=WARN") {
				t.Errorf("expected a Warn log line:\n%s", logs.String())
			}
			if !tc.isDir {
				if b, _ := os.ReadFile(lkg); string(b) != tc.content {
					t.Error("an unusable LKG file must be left alone, not deleted or rewritten")
				}
			}
		})
	}
}

func TestRemoteMissingLKGIsSilent(t *testing.T) {
	logs := &rsLogBuf{}
	h := newRS(t, func(o *RemoteOptions) {
		o.LKGPath = filepath.Join(t.TempDir(), "lkg.json")
		o.Logger = logs.logger()
	})
	h.watch()
	h.cp.waitConn(t, 1).send(rsFrame(1))
	h.expectVersion(1)
	if s := logs.String(); strings.Contains(s, "level=WARN") || strings.Contains(s, "level=ERROR") {
		t.Errorf("a missing LKG file is normal on first start, got logs:\n%s", s)
	}
}

// After a restart the LKG version is delivered first, with the control plane down.
func TestRemoteLKGPersistsAcrossRestart(t *testing.T) {
	lkg := filepath.Join(t.TempDir(), "lkg.json")
	h := newRS(t, func(o *RemoteOptions) { o.LKGPath = lkg })
	ch := h.watch()
	h.cp.waitConn(t, 1).send(rsFrame(5))
	h.expectVersion(5)
	h.src.Applied(5)
	h.cancel()
	rsDrainClosed(t, ch, 2*time.Second)

	h.cp.close()
	src2 := rsNew(t, h.opts)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch2, err := src2.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c := recv(t, ch2); c.Version != 5 {
		t.Fatalf("restart delivered version %d, want the LKG version 5", c.Version)
	}
}
