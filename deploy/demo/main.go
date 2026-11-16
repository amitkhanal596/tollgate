// Command demo is a tiny upstream service for the Compose and Kubernetes
// demos. It is NOT part of Tollgate itself.
//
//   - any path returns {"service","instance","path"} as JSON
//   - /healthz is the health-check target (Consul and the gateway probe it)
//   - it registers itself in Consul at startup and deregisters on SIGTERM
//   - POST /admin/fail?mode=500|slow|unhealthy|off injects faults for the chaos
//     script. DEMO ONLY: it is unauthenticated, never expose it.
//   - "-probe URL[,URL...]" makes the binary an HTTP health probe and exits, so
//     distroless containers (no shell, no curl) can have a healthcheck.
//
// Only the standard library is used; Consul is spoken to over its HTTP API.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

// Fault modes for POST /admin/fail.
const (
	modeOff       = "off"
	mode500       = "500"
	modeSlow      = "slow"
	modeUnhealthy = "unhealthy"
)

const slowDelay = 2 * time.Second

type server struct {
	name     string
	instance string
	mode     atomic.Value // string
	sleep    func(context.Context, time.Duration)
}

func newServer(name, instance string) *server {
	s := &server{name: name, instance: instance, sleep: sleepCtx}
	s.mode.Store(modeOff)
	return s
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

func (s *server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		// Only "unhealthy" fails the health check; "500" fails real traffic
		// but stays "healthy", which is what exercises passive ejection.
		if s.mode.Load() == modeUnhealthy {
			http.Error(w, "unhealthy", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/admin/fail", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "use POST", http.StatusMethodNotAllowed)
			return
		}
		m := r.URL.Query().Get("mode")
		switch m {
		case modeOff, mode500, modeSlow, modeUnhealthy:
			s.mode.Store(m)
			writeJSON(w, http.StatusOK, map[string]string{"service": s.name, "instance": s.instance, "mode": m})
		default:
			http.Error(w, "mode must be 500, slow, unhealthy or off", http.StatusBadRequest)
		}
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		switch s.mode.Load() {
		case mode500:
			http.Error(w, "injected failure", http.StatusInternalServerError)
			return
		case modeSlow:
			s.sleep(r.Context(), slowDelay)
		}
		writeJSON(w, http.StatusOK, map[string]string{
			"service": s.name, "instance": s.instance, "path": r.URL.Path,
		})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() { os.Exit(run(os.Args[1:])) }

func run(args []string) int {
	host, _ := os.Hostname()
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	name := fs.String("name", getenv("DEMO_NAME", host), "instance name shown in responses")
	listen := fs.String("listen", getenv("DEMO_LISTEN", ":8080"), "listen address")
	consul := fs.String("consul-addr", getenv("DEMO_CONSUL_ADDR", ""), "host:port of Consul; empty disables registration")
	svc := fs.String("service", getenv("DEMO_SERVICE", "demo"), "Consul service name")
	advertise := fs.String("advertise-addr", getenv("DEMO_ADVERTISE_ADDR", ""), "address Consul and the gateway use to reach this instance (default: first non-loopback IPv4)")
	probe := fs.String("probe", "", "comma-separated URLs: GET each, exit 0 only if all return 2xx (container healthcheck mode)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *probe != "" {
		return runProbe(strings.Split(*probe, ","))
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		logger.Error("listen failed", "err", err)
		return 1
	}
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	s := newServer(*name, host)
	srv := &http.Server{Handler: s.handler(), ReadHeaderTimeout: 5 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	logger.Info("demo started", "name", *name, "listen", ln.Addr().String())

	var reg *registration
	regDone := make(chan struct{})
	if *consul != "" {
		addr := *advertise
		if addr == "" {
			addr = firstIPv4()
		}
		var port int
		_, _ = fmt.Sscanf(portStr, "%d", &port)
		reg = &registration{
			consul: *consul, id: *svc + "-" + *name, service: *svc,
			addr: addr, port: port, client: &http.Client{Timeout: 3 * time.Second}, logger: logger,
		}
		go func() { defer close(regDone); reg.registerWithRetry(ctx) }()
	} else {
		close(regDone)
	}

	code := 0
	select {
	case <-ctx.Done():
	case err := <-serveErr:
		logger.Error("server failed", "err", err)
		code = 1
	}
	stop()
	<-regDone
	if reg != nil {
		// Fresh context: ctx is already canceled.
		dctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		reg.deregister(dctx)
		cancel()
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
	return code
}

func runProbe(urls []string) int {
	c := &http.Client{Timeout: 3 * time.Second}
	for _, u := range urls {
		resp, err := c.Get(strings.TrimSpace(u))
		if err != nil {
			fmt.Fprintln(os.Stderr, "probe:", err)
			return 1
		}
		_ = resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			fmt.Fprintln(os.Stderr, "probe:", u, resp.Status)
			return 1
		}
	}
	return 0
}

func firstIPv4() string {
	addrs, _ := net.InterfaceAddrs()
	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok && !ipn.IP.IsLoopback() && ipn.IP.To4() != nil {
			return ipn.IP.String()
		}
	}
	return "127.0.0.1"
}

// registration talks to Consul's agent HTTP API directly (stdlib only).
type registration struct {
	consul, id, service, addr string
	port                      int
	client                    *http.Client
	logger                    *slog.Logger
}

func (r *registration) body() []byte {
	b, _ := json.Marshal(map[string]any{
		"ID": r.id, "Name": r.service, "Address": r.addr, "Port": r.port,
		"Check": map[string]any{
			"HTTP":     fmt.Sprintf("http://%s/healthz", net.JoinHostPort(r.addr, fmt.Sprint(r.port))),
			"Interval": "2s", "Timeout": "1s",
			// Consul removes the instance itself if it crashes without deregistering.
			"DeregisterCriticalServiceAfter": "1m",
		},
	})
	return b
}

// registerWithRetry loops until Consul accepts the registration or ctx ends,
// because in Compose Consul may still be starting.
func (r *registration) registerWithRetry(ctx context.Context) {
	for {
		err := r.put(ctx, "/v1/agent/service/register?replace-existing-checks=true", r.body())
		if err == nil {
			r.logger.Info("registered in consul", "id", r.id, "addr", r.addr, "port", r.port)
			return
		}
		r.logger.Warn("consul registration failed, retrying", "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func (r *registration) deregister(ctx context.Context) {
	if err := r.put(ctx, "/v1/agent/service/deregister/"+r.id, nil); err != nil {
		r.logger.Warn("consul deregistration failed", "err", err)
		return
	}
	r.logger.Info("deregistered from consul", "id", r.id)
}

func (r *registration) put(ctx context.Context, path string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, "http://"+r.consul+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return fmt.Errorf("consul %s: %s", resp.Status, b)
	}
	return nil
}
