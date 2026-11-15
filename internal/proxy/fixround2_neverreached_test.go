package proxy

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/amitkhanal596/tollgate/internal/config"
)

// rawDial opens a TCP connection to the test server, closed on cleanup.
func rawDial(t *testing.T, srv *httptest.Server) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// rawStatus writes payload as-is and returns the status of the first response.
func rawStatus(t *testing.T, srv *httptest.Server, payload string) int {
	t.Helper()
	conn := rawDial(t, srv)
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(conn, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	return readStatus(t, conn)
}

func readStatus(t *testing.T, conn net.Conn) int {
	t.Helper()
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Errorf("read response: %v", err)
		return 0
	}
	resp.Body.Close()
	return resp.StatusCode
}

func passive3() *config.PassiveHealth {
	return &config.PassiveHealth{ConsecutiveFailures: 3, Cooloff: config.Duration(5 * time.Second)}
}

// D1: requests that never reach the backend (the gateway refuses them, or
// the client's own bytes are broken) are not backend failures: no 502, no
// passive-health strike, no leaked in-flight slot.
func TestRequestsNeverReachingBackendAreNotBackendFailures(t *testing.T) {
	tests := []struct {
		name       string
		payload    string
		want       int
		wantNoHits bool // the backend must not even see the request
	}{
		{"CONNECT with origin-form target",
			"CONNECT /x HTTP/1.1\r\nHost: gw\r\n\r\n", http.StatusMethodNotAllowed, true},
		{"Upgrade header with a non-printable byte",
			"GET / HTTP/1.1\r\nHost: gw\r\nConnection: Upgrade\r\nUpgrade: \xff\r\n\r\n", http.StatusBadRequest, true},
		{"malformed chunk in a chunked upload",
			"POST / HTTP/1.1\r\nHost: gw\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n", http.StatusBadRequest, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newCtrlBackend(t, "B")
			h, srv := startGatewayWith(t, Options{}, cfgOf(1, []config.Route{rt("r", "", "/", "u")},
				upx("u", config.AlgorithmRoundRobin, nil, passive3(), b.url())))

			for i := 0; i < 5; i++ {
				if got := rawStatus(t, srv, tc.payload); got != tc.want {
					t.Errorf("bad request %d: status %d, want %d", i+1, got, tc.want)
				}
			}
			if tc.wantNoHits {
				if got := b.hits.Load(); got != 0 {
					t.Errorf("backend saw %d of the refused requests, want 0", got)
				}
			}
			waitFor(t, 3*time.Second, "in-flight to return to 0", func() bool {
				return inFlight(t, h.g, "u", b.url()) == 0
			})
			if code := getCode(t, newClient(t), srv.URL+"/x"); code != http.StatusOK {
				t.Fatalf("normal GET after 5 bad requests: %d, want 200 (the only backend was ejected)", code)
			}
		})
	}
}

// bodyBackend is an upstream that reads the whole request body before it
// answers 200, optionally after a delay (a slow backend).
type bodyBackend struct {
	srv   *httptest.Server
	hits  atomic.Int64
	delay atomic.Int64 // ns
}

func newBodyBackend(t *testing.T) *bodyBackend {
	t.Helper()
	b := &bodyBackend{}
	b.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.hits.Add(1)
		if d := b.delay.Load(); d > 0 {
			select {
			case <-time.After(time.Duration(d)):
			case <-r.Context().Done():
				return
			}
		}
		_, _ = io.Copy(io.Discard, r.Body)
		_, _ = w.Write([]byte("ok"))
	}))
	t.Cleanup(b.srv.Close)
	return b
}

// D2: a client that stalls its upload is the client's problem. When the
// gateway's deadline fires while the request body is still incomplete, it
// answers 408 and does not strike the backend.
func TestStalledUploadIs408AndNotAPassiveFailure(t *testing.T) {
	b := newBodyBackend(t)
	r := rt("r", "", "/", "u")
	r.Timeouts = config.Timeouts{Request: ms(300)}
	h, srv := startGatewayWith(t, Options{}, cfgOf(1, []config.Route{r},
		upx("u", config.AlgorithmRoundRobin, nil, passive3(), b.srv.URL)))

	const stalled = 3 // == consecutive_failures
	codes := make([]int, stalled)
	var wg sync.WaitGroup
	for i := 0; i < stalled; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := rawDial(t, srv)
			_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
			// Promise 100 body bytes, send 2, then go quiet.
			_, _ = io.WriteString(conn, "POST /up HTTP/1.1\r\nHost: gw\r\nContent-Length: 100\r\n\r\nab")
			codes[i] = readStatus(t, conn)
		}()
	}
	wg.Wait()
	for i, c := range codes {
		if c != http.StatusRequestTimeout {
			t.Errorf("stalled upload %d: status %d, want 408", i, c)
		}
	}
	waitFor(t, 3*time.Second, "in-flight to return to 0", func() bool {
		return inFlight(t, h.g, "u", b.srv.URL) == 0
	})
	if code := getCode(t, newClient(t), srv.URL+"/x"); code != http.StatusOK {
		t.Fatalf("normal GET after %d stalled uploads: %d, want 200 (backend was ejected)", stalled, code)
	}
}

// Contrast to D2: the client sent everything and the BACKEND is slow. That
// is a real backend failure: 504, and three in a row eject it.
func TestSlowBackendAfterCompleteUploadStillCountsAs504(t *testing.T) {
	b := newBodyBackend(t)
	b.delay.Store(int64(2 * time.Second))
	r := rt("r", "", "/", "u")
	r.Timeouts = config.Timeouts{Request: ms(100)}
	_, srv := startGatewayWith(t, Options{}, cfgOf(1, []config.Route{r},
		upx("u", config.AlgorithmRoundRobin, nil, passive3(), b.srv.URL)))
	c := newClient(t)

	for i := 0; i < 3; i++ {
		resp, err := c.Post(srv.URL+"/up", "text/plain", strings.NewReader("hello"))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusGatewayTimeout {
			t.Fatalf("slow-backend request %d: %d, want 504", i+1, resp.StatusCode)
		}
	}
	if code := getCode(t, c, srv.URL+"/x"); code != http.StatusServiceUnavailable {
		t.Fatalf("after 3 timeouts the backend answers %d, want 503 (ejected)", code)
	}
}
