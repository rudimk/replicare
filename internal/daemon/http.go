package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"

	"github.com/rudimk/replicare/internal/observability/status"
)

// HTTPServers holds the running operator HTTP endpoints so the caller can shut
// them down. StatusAddr / MetricsAddr are the actually-bound addresses (useful
// when the config requests an ephemeral :0 port).
type HTTPServers struct {
	StatusAddr  string
	MetricsAddr string
	stops       []func(context.Context) error
}

// Shutdown gracefully stops every started HTTP server.
func (h *HTTPServers) Shutdown(ctx context.Context) error {
	var firstErr error
	for _, stop := range h.stops {
		if err := stop(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// StartHTTP binds the operator surface from the config's observability block
// (CLAUDE.md §10): the status API (/status + /healthz, and /metrics) on
// status_addr, and — when metrics_addr is set to a different address — a bare
// Prometheus /metrics there too (the conventional split). Either address may be
// empty to disable it. Servers begin serving immediately; the bound addresses are
// returned for callers that requested an ephemeral port.
func (d *Daemon) StartHTTP() (*HTTPServers, error) {
	hs := &HTTPServers{}
	obs := d.cfg.Observability

	if obs.StatusAddr != "" {
		srv := status.NewServer(status.NewReporter(d.store), d.metrics.Handler(), d.syncNames)
		addr, stop, err := listenAndServe(obs.StatusAddr, srv.Handler())
		if err != nil {
			return nil, fmt.Errorf("daemon: bind status_addr %q: %w", obs.StatusAddr, err)
		}
		hs.StatusAddr = addr
		hs.stops = append(hs.stops, stop)
	}

	if obs.MetricsAddr != "" && obs.MetricsAddr != obs.StatusAddr {
		mux := http.NewServeMux()
		mux.Handle("/metrics", d.metrics.Handler())
		mux.HandleFunc("/healthz", healthz)
		addr, stop, err := listenAndServe(obs.MetricsAddr, mux)
		if err != nil {
			_ = hs.Shutdown(context.Background())
			return nil, fmt.Errorf("daemon: bind metrics_addr %q: %w", obs.MetricsAddr, err)
		}
		hs.MetricsAddr = addr
		hs.stops = append(hs.stops, stop)
	}
	return hs, nil
}

// listenAndServe binds addr eagerly (so a bind error surfaces synchronously) and
// serves h in the background, returning the bound address and a Shutdown stopper.
func listenAndServe(addr string, h http.Handler) (string, func(context.Context) error, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", nil, err
	}
	srv := &http.Server{Handler: h}
	go func() { _ = srv.Serve(ln) }()
	return ln.Addr().String(), srv.Shutdown, nil
}

func healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// syncNames lists the configured sync names for the status API.
func (d *Daemon) syncNames(_ context.Context) ([]string, error) {
	names := make([]string, len(d.cfg.Syncs))
	for i, s := range d.cfg.Syncs {
		names[i] = s.Name
	}
	return names, nil
}
