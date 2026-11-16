package metrics

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
)

// maxPooledLine bounds the buffers kept for reuse, so one huge line does not
// pin a large allocation in the pool forever.
const maxPooledLine = 64 << 10

// AsyncWriter decouples log writers from a slow or blocked sink. Each Write
// (one log line from slog) is copied into a bounded queue and written to the
// sink by a single drain goroutine. When the queue is full the line is
// dropped and onDrop is called: under a stuck stdout the caller keeps going
// and loses (counted) log lines instead of stalling.
//
// Write is safe for concurrent use and never blocks. Lines from concurrent
// writers are never interleaved (each Write is queued whole), and lines from
// one goroutine reach the sink in order.
//
// Shutdown: Close stops accepting, flushes what is queued (bounded by its
// ctx) and stops the drain goroutine. A Write racing with Close may be lost
// without being counted; Writes after Close are dropped (and counted).
type AsyncWriter struct {
	w      io.Writer
	queue  chan *[]byte
	onDrop func()
	// bufs recycles line buffers, so a Write costs no allocation in steady
	// state (slog reuses its own buffer, so p must be copied).
	bufs sync.Pool

	// closed is read on every Write; an atomic keeps that a plain load with
	// no lock. The queue is never closed (a send on a closed channel would
	// panic in a racing Write), so Close signals through stop instead.
	closed    atomic.Bool
	closeOnce sync.Once
	stop      chan struct{} // closed by Close: flush the queue and exit
	abortOnce sync.Once
	abort     chan struct{} // closed when Close gives up: exit without flushing further
	done      chan struct{} // closed by the drain goroutine when it exits
}

// NewAsyncWriter returns an AsyncWriter that queues up to lines lines (at
// least 1) for w and starts its drain goroutine. onDrop may be nil; it is
// called on the writing goroutine, once per dropped line, so it must be
// cheap and must not log through this writer.
func NewAsyncWriter(w io.Writer, lines int, onDrop func()) *AsyncWriter {
	if lines < 1 {
		lines = 1
	}
	if onDrop == nil {
		onDrop = func() {}
	}
	a := &AsyncWriter{
		w:      w,
		queue:  make(chan *[]byte, lines),
		onDrop: onDrop,
		stop:   make(chan struct{}),
		abort:  make(chan struct{}),
		done:   make(chan struct{}),
	}
	a.bufs.New = func() any {
		b := make([]byte, 0, 512)
		return &b
	}
	go a.drain()
	return a
}

// Write queues a copy of p and returns len(p), nil, also when the line is
// dropped: a logger must never turn a full buffer into an error path.
func (a *AsyncWriter) Write(p []byte) (int, error) {
	if a.closed.Load() {
		a.onDrop()
		return len(p), nil
	}
	bp := a.bufs.Get().(*[]byte)
	*bp = append((*bp)[:0], p...)
	select {
	case a.queue <- bp:
	default:
		// Full: the sink is not keeping up. Dropping is the point; blocking
		// here would stall every request that logs.
		a.recycle(bp)
		a.onDrop()
	}
	return len(p), nil
}

// Close stops accepting lines, writes what is queued and stops the drain
// goroutine. It returns nil once everything queued has been written, or
// ctx's error if ctx ends first. In that case the drain goroutine is told to
// stop, but if it is blocked inside the sink's Write it cannot be
// interrupted (io.Writer has no cancellation): that one goroutine stays
// until the sink returns, then exits without writing anything more. Close
// may be called more than once.
func (a *AsyncWriter) Close(ctx context.Context) error {
	a.closeOnce.Do(func() {
		a.closed.Store(true)
		close(a.stop)
	})
	select {
	case <-a.done:
		return nil
	case <-ctx.Done():
		a.abortOnce.Do(func() { close(a.abort) })
		return ctx.Err()
	}
}

// drain is the only goroutine that writes to the sink.
func (a *AsyncWriter) drain() {
	defer close(a.done)
	for {
		select {
		case bp := <-a.queue:
			a.write(bp)
		case <-a.stop:
			a.flush()
			return
		}
	}
}

// flush writes what is still queued, unless Close has given up meanwhile.
func (a *AsyncWriter) flush() {
	for {
		select {
		case <-a.abort:
			return
		default:
		}
		select {
		case bp := <-a.queue:
			a.write(bp)
		default:
			return // empty: everything accepted before Close is written
		}
	}
}

func (a *AsyncWriter) write(bp *[]byte) {
	// A failed write cannot be reported anywhere useful (this IS the log);
	// the line is simply lost, like a dropped one.
	_, _ = a.w.Write(*bp)
	a.recycle(bp)
}

func (a *AsyncWriter) recycle(bp *[]byte) {
	if cap(*bp) <= maxPooledLine {
		a.bufs.Put(bp)
	}
}
