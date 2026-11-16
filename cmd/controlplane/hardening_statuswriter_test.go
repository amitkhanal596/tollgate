package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// flushErrWriter is a ResponseWriter with only FlushError (no Flush), the shape
// net/http's real writers have, so a wrapper that only forwards Flush() loses it.
type flushErrWriter struct {
	http.ResponseWriter
	err     error
	flushed int
}

func (f *flushErrWriter) FlushError() error { f.flushed++; return f.err }

func TestHardening_StatusWriterFlushError(t *testing.T) {
	boom := errors.New("flush boom")
	tests := []struct {
		name    string
		under   func() (http.ResponseWriter, func() int)
		wantErr error
	}{
		{"propagates underlying error", func() (http.ResponseWriter, func() int) {
			u := &flushErrWriter{ResponseWriter: httptest.NewRecorder(), err: boom}
			return u, func() int { return u.flushed }
		}, boom},
		{"nil error passes through", func() (http.ResponseWriter, func() int) {
			u := &flushErrWriter{ResponseWriter: httptest.NewRecorder()}
			return u, func() int { return u.flushed }
		}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			under, flushes := tc.under()
			sw := &statusWriter{ResponseWriter: under, status: http.StatusOK}

			// The wrapper itself must implement FlushError (not rely on Unwrap).
			fe, ok := http.ResponseWriter(sw).(interface{ FlushError() error })
			if !ok {
				t.Fatalf("statusWriter has no FlushError() error method")
			}
			if err := fe.FlushError(); !errors.Is(err, tc.wantErr) {
				t.Errorf("FlushError() = %v, want %v", err, tc.wantErr)
			}
			if err := http.NewResponseController(sw).Flush(); !errors.Is(err, tc.wantErr) {
				t.Errorf("ResponseController.Flush() = %v, want %v", err, tc.wantErr)
			}
			if got := flushes(); got != 2 {
				t.Errorf("underlying flushed %d times, want 2", got)
			}
			if !sw.wroteHeader {
				t.Errorf("flush must mark the header as written (implicit 200)")
			}
		})
	}
}

func TestHardening_StatusWriterFlushUnsupported(t *testing.T) {
	// Embedding the interface hides the recorder's Flush, so nothing can flush.
	under := struct{ http.ResponseWriter }{httptest.NewRecorder()}
	sw := &statusWriter{ResponseWriter: under, status: http.StatusOK}
	fe, ok := http.ResponseWriter(sw).(interface{ FlushError() error })
	if !ok {
		t.Fatalf("statusWriter has no FlushError() error method")
	}
	if err := fe.FlushError(); !errors.Is(err, http.ErrNotSupported) {
		t.Errorf("FlushError() = %v, want http.ErrNotSupported", err)
	}
	if err := http.NewResponseController(sw).Flush(); !errors.Is(err, http.ErrNotSupported) {
		t.Errorf("ResponseController.Flush() = %v, want http.ErrNotSupported", err)
	}
}

func TestHardening_StatusWriterKeepsUnwrap(t *testing.T) {
	rec := httptest.NewRecorder()
	sw := &statusWriter{ResponseWriter: rec, status: http.StatusOK}
	if got := sw.Unwrap(); got != http.ResponseWriter(rec) {
		t.Errorf("Unwrap() = %v, want the underlying writer", got)
	}
}
