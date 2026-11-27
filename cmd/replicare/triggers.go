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

// runTriggers handles `replicare triggers restore <config> [--sync s] [--target t]`:
// switch back on, right now, every target trigger replicare switched off so it would not
// fire on its writes, each in the mode it had before. The daemon does this by itself for
// a sync that is paused; this is for doing it at a moment of the operator's choosing
// (a cutover), or for a sync that will not run again. It refuses for a sync a daemon is
// running: that daemon would switch them off again on its next start, and meanwhile they
// would fire on its writes.
func runTriggers(args []string, stdout, stderr io.Writer) int {
	const usage = "usage: replicare triggers restore <config.yml> [--sync <name>] [--target <name>]"
	if len(args) == 0 || args[0] != "restore" {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	var cfgPath, onlySync, onlyTarget string
	rest := args[1:]
	for i := 0; i < len(rest); i++ {
		switch a := rest[i]; {
		case a == "--sync" || a == "--target":
			if i+1 >= len(rest) {
				fmt.Fprintf(stderr, "triggers restore: %s needs a value\n", a)
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
			fmt.Fprintf(stderr, "triggers restore: unexpected argument %q\n", a)
			return 2
		}
	}
	if cfgPath == "" {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		fmt.Fprintf(stderr, "triggers restore: %v\n", err)
		return 2
	}
	if cfg.StateStore == nil {
		fmt.Fprintln(stderr, "triggers restore: no state_store configured")
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store := statepg.New(cfg.StateStore.Conn.ConnConfig())
	openCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	err = store.Open(openCtx)
	cancel()
	if err != nil {
		fmt.Fprintf(stderr, "triggers restore: open state store: %v\n", err)
		return 1
	}
	defer func() { _ = store.Close(context.Background()) }()

	restored, failed := 0, 0
	for _, sync := range cfg.Syncs {
		if onlySync != "" && sync.Name != onlySync {
			continue
		}
		saved, err := store.ListTriggers(ctx, sync.Name)
		if err != nil {
			fmt.Fprintf(stderr, "triggers restore: %s: %v\n", sync.Name, err)
			return 1
		}
		targets := savedTriggerTargets(saved, onlyTarget)
		if len(targets) == 0 {
			continue
		}
		held, release, err := store.Acquire(ctx, sync.Name)
		if err != nil {
			fmt.Fprintf(stderr, "triggers restore: %s: %v\n", sync.Name, err)
			return 1
		}
		if !held {
			fmt.Fprintf(stderr, "triggers restore: sync %q is running in a daemon, which needs these triggers off while it writes. "+
				"Pause the sync (enabled: false) or stop the daemon first; a paused sync's daemon switches them back on by itself.\n", sync.Name)
			failed++
			continue
		}
		for _, target := range targets {
			n, err := restoreTriggersOn(ctx, cfg, sync.Name, target, store, stdout)
			restored += n
			if err != nil {
				failed++
				fmt.Fprintf(stderr, "triggers restore: %s / %s: %v\n", sync.Name, target, err)
			}
		}
		_ = release()
	}
	if restored == 0 && failed == 0 {
		fmt.Fprintln(stdout, "no triggers to switch back on: replicare has none switched off")
		return 0
	}
	if failed > 0 {
		return 1
	}
	fmt.Fprintf(stdout, "switched %d triggers back on\n", restored)
	return 0
}

// savedTriggerTargets lists the targets with saved triggers, in order.
func savedTriggerTargets(saved []state.SavedTrigger, only string) []engine.TargetID {
	var out []engine.TargetID
	seen := map[engine.TargetID]bool{}
	for _, st := range saved {
		if seen[st.Target] || (only != "" && string(st.Target) != only) {
			continue
		}
		seen[st.Target] = true
		out = append(out, st.Target)
	}
	return out
}

func restoreTriggersOn(ctx context.Context, cfg *config.Config, sync string, target engine.TargetID,
	store state.TriggerStore, stdout io.Writer) (int, error) {
	ep := cfg.Targets[string(target)]
	if ep == nil {
		return 0, fmt.Errorf("target %q is not in this config", target)
	}
	eng, err := engine.Get(ep.Engine)
	if err != nil {
		return 0, err
	}
	sk, err := eng.NewSink(ep.Conn.ConnConfig())
	if err != nil {
		return 0, err
	}
	connCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	err = sk.Connect(connCtx)
	cancel()
	if err != nil {
		return 0, fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = sk.Close(context.Background()) }()
	g, ok := sk.(engine.TriggerGuard)
	if !ok {
		return 0, fmt.Errorf("the %s engine does not switch triggers", ep.Engine)
	}
	return pipeline.RestoreTargetTriggers(ctx, store, g, sync, target, func(_ context.Context, e state.Event) {
		fmt.Fprintf(stdout, "%s / %s: %s\n", sync, target, e.Message)
	})
}
