package main

import (
	"context"
	"fmt"
	"io"
	"os/signal"
	"syscall"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/pipeline"
	"github.com/rudimk/replicare/internal/state"
	statepg "github.com/rudimk/replicare/internal/state/postgres"
)

// runIndexes handles `replicare indexes restore <config> [--sync s] [--target t]`:
// build again, right now, every target index replicare took down for an initial copy
// and has not built yet. The daemon does this by itself once a sync streams; this is
// for a sync that will not run again (abandoned mid-copy, or about to have its state
// store wiped), so a target is never left without its indexes.
func runIndexes(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 || args[0] != "restore" {
		fmt.Fprintln(stderr, "usage: replicare indexes restore <config.yml> [--sync <name>] [--target <name>]")
		return 2
	}
	var cfgPath, onlySync, onlyTarget string
	rest := args[1:]
	for i := 0; i < len(rest); i++ {
		switch a := rest[i]; {
		case a == "--sync" || a == "--target":
			if i+1 >= len(rest) {
				fmt.Fprintf(stderr, "indexes restore: %s needs a value\n", a)
				return 2
			}
			i++
			if a == "--sync" {
				onlySync = rest[i]
			} else {
				onlyTarget = rest[i]
			}
		case cfgPath == "":
			cfgPath = a
		default:
			fmt.Fprintf(stderr, "indexes restore: unexpected argument %q\n", a)
			return 2
		}
	}
	if cfgPath == "" {
		fmt.Fprintln(stderr, "usage: replicare indexes restore <config.yml> [--sync <name>] [--target <name>]")
		return 2
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		fmt.Fprintf(stderr, "indexes restore: %v\n", err)
		return 2
	}
	if cfg.StateStore == nil {
		fmt.Fprintln(stderr, "indexes restore: no state_store configured")
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store := statepg.New(cfg.StateStore.Conn.ConnConfig())
	openCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	err = store.Open(openCtx)
	cancel()
	if err != nil {
		fmt.Fprintf(stderr, "indexes restore: open state store: %v\n", err)
		return 1
	}
	defer func() { _ = store.Close(context.Background()) }()

	found, failed := 0, 0
	for _, sync := range cfg.Syncs {
		if onlySync != "" && sync.Name != onlySync {
			continue
		}
		saved, err := store.ListIndexes(ctx, sync.Name)
		if err != nil {
			fmt.Fprintf(stderr, "indexes restore: %s: %v\n", sync.Name, err)
			return 1
		}
		targets := pendingTargets(saved, onlyTarget)
		if len(targets) == 0 {
			continue
		}
		// Two builders of the same index would collide: refuse while a daemon owns the sync.
		held, release, err := store.Acquire(ctx, sync.Name)
		if err != nil {
			fmt.Fprintf(stderr, "indexes restore: %s: %v\n", sync.Name, err)
			return 1
		}
		if !held {
			fmt.Fprintf(stderr, "indexes restore: sync %q is running in a daemon, which builds these indexes itself while it streams. "+
				"To build them from here instead, stop the daemon first (for example, scale it to 0 replicas).\n", sync.Name)
			failed++
			continue
		}
		for _, target := range targets {
			found++
			left, err := restoreTarget(ctx, cfg, sync, target, store, stdout)
			if err != nil || left > 0 {
				failed++
				fmt.Fprintf(stderr, "indexes restore: %s / %s: %d still not built: %v\n", sync.Name, target, left, err)
			}
		}
		_ = release()
	}
	if found == 0 && failed == 0 {
		fmt.Fprintln(stdout, "no indexes to build: every index replicare took down has been built again")
		return 0
	}
	if failed > 0 {
		return 1
	}
	return 0
}

// pendingTargets lists the targets with saved indexes not yet built, in order.
func pendingTargets(saved []state.SavedIndex, only string) []engine.TargetID {
	var out []engine.TargetID
	seen := map[engine.TargetID]bool{}
	for _, si := range saved {
		if si.Built || seen[si.Target] || (only != "" && string(si.Target) != only) {
			continue
		}
		seen[si.Target] = true
		out = append(out, si.Target)
	}
	return out
}

func restoreTarget(ctx context.Context, cfg *config.Config, sync *config.Sync, target engine.TargetID,
	store state.IndexStore, stdout io.Writer) (int, error) {
	ep := cfg.Targets[string(target)]
	if ep == nil {
		return -1, fmt.Errorf("target %q is not in this config", target)
	}
	eng, err := engine.Get(ep.Engine)
	if err != nil {
		return -1, err
	}
	r := &pipeline.IndexRebuild{
		Sync: sync.Name, Target: target, Store: store,
		Options: engine.IndexBuildOptions{
			MaintenanceWorkMem: sync.DeferIndexes.MaintenanceWorkMem,
			ParallelWorkers:    sync.DeferIndexes.ParallelWorkers,
		},
		Open: func(ctx context.Context) (engine.Sink, error) {
			s, err := eng.NewSink(ep.Conn.ConnConfig())
			if err != nil {
				return nil, err
			}
			cctx, cancel := context.WithTimeout(ctx, connectTimeout)
			defer cancel()
			if err := s.Connect(cctx); err != nil {
				return nil, err
			}
			return s, nil
		},
		Event: func(_ context.Context, e state.Event) {
			where := e.Target
			if e.Table.Name != "" {
				where += " " + e.Table.String()
			}
			fmt.Fprintf(stdout, "[%s] %s: %s\n", e.Level, where, e.Message)
		},
	}
	return r.Run(ctx)
}
