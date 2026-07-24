package pipeline

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/rudimk/replicare/internal/engine"
	_ "github.com/rudimk/replicare/internal/engine/postgres" // register the engine
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/observability/prom"
	"github.com/rudimk/replicare/internal/observability/telemetry"
	"github.com/rudimk/replicare/internal/observability/tracing"
	"github.com/rudimk/replicare/internal/state"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func srcCfg() engine.ConnConfig {
	return engine.ConnConfig{Host: env("RC_SRC_HOST", "127.0.0.1"), Port: 5440, Database: env("RC_SRC_DB", "replicare_src"), User: env("RC_USER", "postgres"), Password: env("RC_PASSWORD", "postgres"), TLS: engine.TLSDisable}
}
func tgtCfg() engine.ConnConfig {
	return engine.ConnConfig{Host: env("RC_DST_HOST", "127.0.0.1"), Port: 5441, Database: env("RC_DST_DB", "replicare_dst"), User: env("RC_USER", "postgres"), Password: env("RC_PASSWORD", "postgres"), TLS: engine.TLSDisable}
}

// captureSink records durable events for assertions.
type captureSink struct{ events []state.Event }

func (c *captureSink) RecordEvent(_ context.Context, e state.Event) error {
	c.events = append(c.events, e)
	return nil
}

func gaugeValue(t *testing.T, reg *prom.Registry, name string, want map[string]string) (float64, bool) {
	t.Helper()
	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range fams {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.Metric {
			got := map[string]string{}
			for _, l := range m.Label {
				got[l.GetName()] = l.GetValue()
			}
			if len(got) != len(want) {
				continue
			}
			ok := true
			for k, v := range want {
				if got[k] != v {
					ok = false
					break
				}
			}
			if ok {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// TestDrainAgainstDownedTargetAllChannels is the M6 headline acceptance: a real
// drain pass against a downed target surfaces across all three channels — an
// errored span with backlog attributes, an ERROR target.unreachable log, and the
// reachability gauge (0) plus the backlog series.
func TestDrainAgainstDownedTargetAllChannels(t *testing.T) {
	if os.Getenv("REPLICARE_INTEGRATION") != "1" {
		t.Skip("integration test; set REPLICARE_INTEGRATION=1 and run `task harness:up`")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	rawSrc, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable",
		srcCfg().User, srcCfg().Password, srcCfg().Host, srcCfg().Port, srcCfg().Database))
	if err != nil {
		t.Fatalf("raw src: %v", err)
	}
	defer rawSrc.Close(context.Background())
	_, _ = rawSrc.Exec(ctx, "DROP SCHEMA IF EXISTS rc_it CASCADE")
	_, _ = rawSrc.Exec(ctx, "DROP SCHEMA IF EXISTS replicare CASCADE")
	_, _ = rawSrc.Exec(ctx, "CREATE SCHEMA rc_it")
	_, _ = rawSrc.Exec(ctx, "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)")
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = rawSrc.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = rawSrc.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
	})

	eng, err := engine.Get("postgres")
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	src, err := eng.NewSource(srcCfg())
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	if err := src.Connect(ctx); err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer src.Close(context.Background())

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install capture: %v", err)
	}
	// Queue an unconsumed backlog so the drain has work and the backlog is visible.
	if _, err := rawSrc.Exec(ctx, "INSERT INTO rc_it.orders SELECT g, 'v'||g FROM generate_series(1,7) g"); err != nil {
		t.Fatalf("seed backlog: %v", err)
	}

	// A sink connected to the real target, then CLOSED to simulate the target
	// going down: the drain's apply and the reachability probe both fail.
	sink, err := eng.NewSink(tgtCfg())
	if err != nil {
		t.Fatalf("new sink: %v", err)
	}
	if err := sink.Connect(ctx); err != nil {
		t.Fatalf("connect sink: %v", err)
	}
	if err := sink.Close(ctx); err != nil {
		t.Fatalf("close sink (simulate down): %v", err)
	}

	// Wire all channels.
	reg := prom.New()
	rec := tracetest.NewSpanRecorder()
	tracer := tracing.New(tracing.NewProvider(rec))
	var logBuf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	sink0 := &captureSink{}
	tm := telemetry.New(reg, tracer, log, sink0)

	n, err := DrainTable(ctx, tm, src, sink, "s1", "dst", ref, 100, engine.RetentionPolicy{MaxAgeSeconds: 3600})
	if err == nil {
		t.Fatalf("expected the drain to fail against a downed target")
	}
	if n != 0 {
		t.Errorf("consumed = %d, want 0 on failure", n)
	}

	// Trace channel: an errored drain span.
	ended := rec.Ended()
	if len(ended) != 1 || ended[0].Status().Code != codes.Error {
		t.Fatalf("expected one errored drain span, got %+v", ended)
	}

	// Metrics channel: reachability down + backlog published.
	if v, ok := gaugeValue(t, reg, observability.MetricTargetUp,
		map[string]string{"sync": "s1", "target": "dst"}); !ok || v != 0 {
		t.Errorf("target_up = %v (present=%v), want 0", v, ok)
	}
	if v, ok := gaugeValue(t, reg, observability.MetricDeltaBacklog,
		map[string]string{"sync": "s1", "target": "dst", "table": "rc_it.orders"}); !ok || v != 7 {
		t.Errorf("delta_backlog = %v (present=%v), want 7", v, ok)
	}

	// Log channel: ERROR target.unreachable.
	logs := logBuf.String()
	if !strings.Contains(logs, `"event":"`+observability.EventTargetUnreachable+`"`) || !strings.Contains(logs, `"level":"ERROR"`) {
		t.Errorf("expected ERROR target.unreachable log, got %s", logs)
	}

	// Durable channel: the event was recorded.
	if len(sink0.events) != 1 || sink0.events[0].Event != observability.EventTargetUnreachable {
		t.Errorf("durable events = %+v", sink0.events)
	}
}
