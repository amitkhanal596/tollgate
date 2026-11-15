package config

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"time"
)

// errFrameTooLarge means the stream broke the size limits of §5.1. There is
// no safe way to resynchronise in the middle of an event, so the caller drops
// the connection.
var errFrameTooLarge = errors.New("config: SSE line or event data exceeds MaxEventBytes")

// sseLineSlack is how much longer than MaxEventBytes one line may be: room for
// the "data: " field name in front of a maximal event.
const sseLineSlack = 64

// sseFrame is one complete frame: either a comment line (heartbeat) or a
// dispatched event. data is only valid until the next call to next().
type sseFrame struct {
	comment bool
	event   string
	data    []byte
	id      string
	hasID   bool
}

// sseReader is a hand-written Server-Sent Events parser with bounded memory:
// no line may exceed maxLine bytes and no event's data may exceed maxData, so
// a hostile or broken server cannot make it buffer without limit.
type sseReader struct {
	br      *bufio.Reader
	maxLine int
	maxData int
	line    []byte // reused line buffer

	// The event being built; it survives across next() calls because a
	// comment inside an event is returned as its own frame.
	inEvent bool
	typ     string
	data    []byte
	hasData bool
	id      string
	hasID   bool
}

func newSSEReader(r io.Reader, maxEventBytes int) *sseReader {
	return &sseReader{
		br:      bufio.NewReaderSize(r, 32<<10),
		maxLine: maxEventBytes + sseLineSlack,
		maxData: maxEventBytes,
	}
}

// next returns the next complete frame. Any error (EOF included) ends the
// connection; a partial event at EOF is never dispatched.
func (r *sseReader) next() (sseFrame, error) {
	for {
		line, err := r.readLine()
		if err != nil {
			return sseFrame{}, err
		}
		if len(line) == 0 {
			if !r.inEvent {
				continue // stray blank line: nothing to dispatch
			}
			f := sseFrame{event: r.typ, data: r.data, id: r.id, hasID: r.hasID}
			r.inEvent, r.typ, r.data, r.hasData, r.id, r.hasID = false, "", r.data[:0], false, "", false
			return f, nil
		}
		if line[0] == ':' {
			return sseFrame{comment: true}, nil
		}
		// No ':' means the whole line is the field name with an empty value.
		name, value, _ := bytes.Cut(line, []byte(":"))
		value = bytes.TrimPrefix(value, []byte(" ")) // exactly one leading space
		r.inEvent = true
		switch string(name) {
		case "event":
			r.typ = string(value)
		case "id":
			r.id, r.hasID = string(value), true // last one wins
		case "data":
			n := len(value)
			if r.hasData {
				n++ // the joining newline counts towards the limit too
			}
			if len(r.data)+n > r.maxData {
				return sseFrame{}, errFrameTooLarge
			}
			if r.hasData {
				r.data = append(r.data, '\n')
			}
			r.data, r.hasData = append(r.data, value...), true
		default: // "retry" and unknown fields are ignored
		}
	}
}

// readLine returns one line without its "\n" or "\r\n" terminator, or
// errFrameTooLarge as soon as the line passes maxLine, without reading the
// rest of it.
func (r *sseReader) readLine() ([]byte, error) {
	r.line = r.line[:0]
	for {
		chunk, err := r.br.ReadSlice('\n')
		// +2 leaves room for the "\r\n" terminator, which is not part of the line.
		if len(r.line)+len(chunk) > r.maxLine+2 {
			return nil, errFrameTooLarge
		}
		r.line = append(r.line, chunk...)
		if err == nil {
			break
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return nil, err // includes io.EOF in the middle of a line
		}
	}
	line := bytes.TrimSuffix(r.line[:len(r.line)-1], []byte("\r"))
	if len(line) > r.maxLine {
		return nil, errFrameTooLarge
	}
	return line, nil
}

// activityReader re-arms the connection's idle timer whenever bytes arrive.
// Re-arming one timer per connection (instead of creating one per frame or
// per read) keeps idle detection allocation-free on the hot path.
type activityReader struct {
	r    io.Reader
	idle *time.Timer
	d    time.Duration
}

func (a *activityReader) Read(p []byte) (int, error) {
	n, err := a.r.Read(p)
	if n > 0 {
		a.idle.Reset(a.d)
	}
	return n, err
}
