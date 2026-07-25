package status

import (
	"context"
	"encoding/json"
	"net/http"
)

// SyncLister enumerates the sync names to report (the daemon passes
// StateStore.ListSyncs-derived names; a one-shot CLI passes its config's syncs).
type SyncLister func(ctx context.Context) ([]string, error)

// Server bundles the operator HTTP surface: Prometheus /metrics, the JSON
// /status API, and a /healthz liveness probe (CLAUDE.md §10). It is transport
// only — all state comes from the injected Reporter and metrics Handler — so the
// daemon (M7) just wires it to a listen address.
type Server struct {
	reporter *Reporter
	metrics  http.Handler
	syncs    SyncLister
}

// NewServer builds the surface. metrics may be nil (then /metrics 503s); syncs
// may be nil (then /status reports nothing).
func NewServer(reporter *Reporter, metrics http.Handler, syncs SyncLister) *Server {
	return &Server{reporter: reporter, metrics: metrics, syncs: syncs}
}

// Handler returns the routed mux for the operator surface.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/status", s.handleStatus)
	if s.metrics != nil {
		mux.Handle("/metrics", s.metrics)
	} else {
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "metrics registry not configured", http.StatusServiceUnavailable)
		})
	}
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleStatus reports one sync (?sync=name) or all known syncs as a JSON array.
func (s *Server) handleStatus(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	names, err := s.syncNames(ctx, req.URL.Query().Get("sync"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	reports := make([]Report, 0, len(names))
	for _, name := range names {
		rep, err := s.reporter.Report(ctx, name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		reports = append(reports, rep)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(reports)
}

func (s *Server) syncNames(ctx context.Context, requested string) ([]string, error) {
	if requested != "" {
		return []string{requested}, nil
	}
	if s.syncs == nil {
		return nil, nil
	}
	return s.syncs(ctx)
}
