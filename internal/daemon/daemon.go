// Package daemon assembles and runs replicare from a validated config (CLAUDE.md
// §8): it constructs the connected engine objects for each sync, manages several
// named syncs concurrently under single-active ownership, and drives each one
// through its lifecycle (initial copy → cutover → continuous streaming). It is
// the process layer over the engine-neutral pipeline.Syncer.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"golang.org/x/sync/errgroup"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability/prom"
	"github.com/rudimk/replicare/internal/observability/telemetry"
	"github.com/rudimk/replicare/internal/state"
	statepg "github.com/rudimk/replicare/internal/state/postgres"
)

// Daemon runs the syncs of a config until its context is cancelled.
type Daemon struct {
	cfg     *config.Config
	store   state.StateStore
	metrics *prom.Registry
	tel     *telemetry.Telemetry
	log     *slog.Logger
}

// New builds a Daemon from a validated config. It wires the shared infrastructure
// (StateStore, Prometheus registry, cross-channel telemetry with the StateStore
// as the durable event sink) but does not connect anything until Run. v1's
// StateStore backend is Postgres (CLAUDE.md §9).
func New(cfg *config.Config, log *slog.Logger) (*Daemon, error) {
	if log == nil {
		log = slog.Default()
	}
	if cfg.StateStore == nil {
		return nil, fmt.Errorf("daemon: no state_store configured")
	}
	store := statepg.New(cfg.StateStore.Conn.ConnConfig())
	metrics := prom.New()
	tel := telemetry.New(metrics, nil, log, store) // no-op tracer until OTLP is wired
	return &Daemon{cfg: cfg, store: store, metrics: metrics, tel: tel, log: log}, nil
}

// Metrics exposes the Prometheus registry so the caller can serve /metrics.
func (d *Daemon) Metrics() *prom.Registry { return d.metrics }

// Store exposes the StateStore so the caller can serve the status API.
func (d *Daemon) Store() state.StateStore { return d.store }

// Run opens the StateStore and runs every sync concurrently, each under its own
// single-active ownership lock, until ctx is cancelled. A sync already owned by
// another daemon is skipped with a warning (single active daemon per sync, v1;
// CLAUDE.md §9). Graceful shutdown: cancelling ctx propagates to each Syncer's
// streaming loop, which finishes its in-flight pass (checkpointed) and returns —
// so Run returns nil on a clean, cancellation-driven stop.
func (d *Daemon) Run(ctx context.Context) error {
	if err := d.store.Open(ctx); err != nil {
		return fmt.Errorf("daemon: open state store: %w", err)
	}
	defer func() { _ = d.store.Close(context.Background()) }()

	g, gctx := errgroup.WithContext(ctx)
	owned := 0
	for _, sync := range d.cfg.Syncs {
		held, release, err := d.store.Acquire(gctx, sync.Name)
		if err != nil {
			return fmt.Errorf("daemon: acquire ownership for %q: %w", sync.Name, err)
		}
		if !held {
			d.log.Warn("sync owned by another daemon; skipping", slog.String("sync", sync.Name))
			continue
		}
		owned++
		sync := sync
		g.Go(func() error {
			defer func() { _ = release() }()
			return d.runSync(gctx, sync)
		})
	}
	d.log.Info("daemon started", slog.Int("syncs_owned", owned))

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// runSync persists the sync definition, then runs each of its targets
// concurrently through bring-up and streaming (fan-out = one Syncer per target
// in v1). A per-target failure cancels the sync's targets via the errgroup.
func (d *Daemon) runSync(ctx context.Context, sync *config.Sync) error {
	targets := make([]engine.TargetID, len(sync.Targets))
	for i, t := range sync.Targets {
		targets[i] = engine.TargetID(t)
	}
	if err := d.store.PutSync(ctx, state.SyncDef{Name: sync.Name, Source: sync.Source, Targets: targets}); err != nil {
		return fmt.Errorf("daemon: persist sync %q: %w", sync.Name, err)
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, target := range sync.Targets {
		target := target
		g.Go(func() error {
			syncer, cleanup, err := d.buildSyncer(gctx, sync, target)
			if err != nil {
				return fmt.Errorf("daemon: build sync %q target %q: %w", sync.Name, target, err)
			}
			defer cleanup()
			if err := syncer.Bringup(gctx); err != nil {
				return fmt.Errorf("daemon: bringup %q/%q: %w", sync.Name, target, err)
			}
			d.log.Info("sync streaming", slog.String("sync", sync.Name), slog.String("target", target))
			return syncer.Stream(gctx)
		})
	}
	return g.Wait()
}
