package config

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

// G2 (review #7): an Applied that arrives after the source has shut down (the
// reconcile loop can finish applying a config just as ctx is canceled) must
// still reach the LKG file. Today the worker is gone, so the write is lost.
func TestFixRound4_AppliedAfterShutdownStillWritesLKG(t *testing.T) {
	lkg := filepath.Join(t.TempDir(), "lkg.json")
	h := newRS(t, func(o *RemoteOptions) { o.LKGPath = lkg })
	ch := h.watch()
	c1 := h.cp.waitConn(t, 1)

	c1.send(rsFrame(1))
	h.expectVersion(1)
	h.src.Applied(1)
	rsWaitLKGVersion(t, lkg, 1)

	c1.send(rsFrame(2))
	h.expectVersion(2) // delivered and received, not yet acknowledged

	h.cancel()
	rsDrainClosed(t, ch, 2*time.Second) // reader, worker and closer are all gone

	h.src.Applied(2) // late ack, after every goroutine has exited
	waitFor(t, time.Second, "LKG file at version 2 after a post-shutdown Applied", func() bool {
		v, err := rsReadLKG(lkg)
		return err == nil && v == 2
	})
	if names := rsDirNames(t, filepath.Dir(lkg)); !reflect.DeepEqual(names, []string{"lkg.json"}) {
		t.Errorf("directory = %v, want only lkg.json", names)
	}
}

// G3 (review #8): a crash between CreateTemp and rename leaves
// "<lkg>.tmp-*" files behind forever. Watch removes the ones that belong to
// this LKG path and nothing else.
func TestFixRound4_StaleLKGTempFilesRemovedAtWatch(t *testing.T) {
	dir := t.TempDir()
	lkg := filepath.Join(dir, "lkg.json")
	rsWriteLKG(t, lkg, 3)

	stale := []string{"lkg.json.tmp-123456", "lkg.json.tmp-abc", "lkg.json.tmp-"}
	unrelated := []string{"other.json", "other.json.tmp-999", "lkg.json.bak", "notes.txt"}
	for _, n := range append(append([]string{}, stale...), unrelated...) {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	h := newRS(t, func(o *RemoteOptions) { o.LKGPath = lkg })
	h.watch()
	h.expectVersion(3) // the LKG itself is still loaded and delivered

	want := append([]string{"lkg.json"}, unrelated...)
	sort.Strings(want)
	waitFor(t, time.Second, "stale temp files to be removed", func() bool {
		return reflect.DeepEqual(rsDirNames(t, dir), want)
	})
	if got := rsDirNames(t, dir); !reflect.DeepEqual(got, want) {
		t.Errorf("directory = %v, want %v", got, want)
	}
	if v, err := rsReadLKG(lkg); err != nil || v != 3 {
		t.Errorf("LKG = (%d, %v), want version 3 untouched", v, err)
	}
}

// With no LKG file at all (first start) stale temp files are still cleaned.
func TestFixRound4_StaleLKGTempFilesRemovedWithoutLKGFile(t *testing.T) {
	dir := t.TempDir()
	lkg := filepath.Join(dir, "lkg.json")
	if err := os.WriteFile(filepath.Join(dir, "lkg.json.tmp-1"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	h := newRS(t, func(o *RemoteOptions) { o.LKGPath = lkg })
	h.watch()
	waitFor(t, time.Second, "stale temp file to be removed", func() bool {
		return len(rsDirNames(t, dir)) == 0
	})
}
