package daemon

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/observability"
)

// freePort reserves and releases an ephemeral port, returning a concrete
// 127.0.0.1:<port> address so two distinct listen addresses can be configured.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

const httpTestConfigTmpl = `
observability: { status_addr: "%s", metrics_addr: "%s" }
state_store:
  engine: postgres
  postgres: { host: localhost, port: 5432, database: s, user: u, password: p, sslmode: disable }
sources:
  src:
    engine: postgres
    postgres: { host: localhost, port: 5432, database: a, user: u, password: p, sslmode: disable }
targets:
  dst:
    engine: postgres
    postgres: { host: localhost, port: 5432, database: b, user: u, password: p, sslmode: disable }
syncs:
  - name: s1
    source: src
    targets: [dst]
    include: ["public.*"]
`

// TestStartHTTPServesMetricsAndHealth binds the operator surface on ephemeral
// ports and asserts /healthz and the Prometheus /metrics endpoint answer — no
// database needed (the metrics registry and liveness probe are independent of
// the StateStore). This is the M6-deferred server binding, now wired in the daemon.
func TestStartHTTPServesMetricsAndHealth(t *testing.T) {
	statusAddr, metricsAddr := freePort(t), freePort(t)
	cfg, err := config.Load(writeConfig(t, fmt.Sprintf(httpTestConfigTmpl, statusAddr, metricsAddr)))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	d, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}
	// Emit one series so the registry has something to expose (a fresh registry
	// outputs nothing until a labeled child is observed).
	d.Metrics().Gauge(observability.MetricTargetUp).WithLabelValues("s1", "dst").Set(1)

	hs, err := d.StartHTTP()
	if err != nil {
		t.Fatalf("StartHTTP: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = hs.Shutdown(ctx)
	}()

	if hs.StatusAddr == "" || hs.MetricsAddr == "" {
		t.Fatalf("expected both status and metrics bound, got status=%q metrics=%q", hs.StatusAddr, hs.MetricsAddr)
	}

	// /healthz on the status server.
	if body, code := get(t, "http://"+hs.StatusAddr+"/healthz"); code != 200 || !strings.Contains(body, "ok") {
		t.Errorf("healthz = %d %q", code, body)
	}
	// /metrics on the status server exposes the contract's series.
	if body, code := get(t, "http://"+hs.StatusAddr+"/metrics"); code != 200 || !strings.Contains(body, "replicare_") {
		t.Errorf("status /metrics = %d, missing replicare_ series", code)
	}
	// bare /metrics on the dedicated metrics address.
	if body, code := get(t, "http://"+hs.MetricsAddr+"/metrics"); code != 200 || !strings.Contains(body, "replicare_") {
		t.Errorf("metrics /metrics = %d %q", code, body[:min(40, len(body))])
	}
}

func get(t *testing.T, url string) (string, int) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b), resp.StatusCode
}
