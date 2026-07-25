package main

import (
	"context"
	"fmt"
	"io"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
	"github.com/rudimk/replicare/internal/state"
	statepg "github.com/rudimk/replicare/internal/state/postgres"
)

// runReseed implements `replicare reseed <config> --sync <name> --target <t>`: it
// flags the target's cursors needs_reseed in the StateStore so the running daemon
// re-copies that target from current source state on its next pass (CLAUDE.md
// §3.4). It signals the owning daemon rather than reseeding directly, honoring
// single-active ownership. Exit codes: 0 ok, 1 error, 2 usage/config.
func runReseed(args []string, stdout, stderr io.Writer) int {
	var cfgPath, syncName, target string
	for i := 0; i < len(args); i++ {
		switch a := args[i]; a {
		case "--sync":
			if i+1 >= len(args) {
				fmt.Fprintln(stderr, "reseed: --sync needs a value")
				return 2
			}
			i++
			syncName = args[i]
		case "--target":
			if i+1 >= len(args) {
				fmt.Fprintln(stderr, "reseed: --target needs a value")
				return 2
			}
			i++
			target = args[i]
		default:
			if cfgPath == "" {
				cfgPath = a
			} else {
				fmt.Fprintf(stderr, "reseed: unexpected argument %q\n", a)
				return 2
			}
		}
	}
	if cfgPath == "" || syncName == "" || target == "" {
		fmt.Fprintln(stderr, "usage: replicare reseed <config.yml> --sync <name> --target <name>")
		return 2
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		fmt.Fprintf(stderr, "reseed: %v\n", err)
		return 2
	}
	if cfg.StateStore == nil {
		fmt.Fprintln(stderr, "reseed: no state_store configured")
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	store := statepg.New(cfg.StateStore.Conn.ConnConfig())
	if err := store.Open(ctx); err != nil {
		fmt.Fprintf(stderr, "reseed: open state store: %v\n", err)
		return 1
	}
	defer func() { _ = store.Close(context.Background()) }()

	cursors, err := store.ListCursors(ctx, syncName)
	if err != nil {
		fmt.Fprintf(stderr, "reseed: %v\n", err)
		return 1
	}
	flagged := 0
	for _, c := range cursors {
		if c.Target != engine.TargetID(target) {
			continue
		}
		c.NeedsReseed = true
		if err := store.SaveCursor(ctx, syncName, c); err != nil {
			fmt.Fprintf(stderr, "reseed: flag %s: %v\n", c.Table, err)
			return 1
		}
		flagged++
	}

	if flagged == 0 {
		fmt.Fprintf(stdout, "reseed: no cursors for sync %q target %q yet; it will full-copy on first run\n", syncName, target)
		return 0
	}
	_ = store.RecordEvent(ctx, state.Event{
		Sync: syncName, Target: target, Level: "WARN", Event: observability.EventTargetNeedsReseed,
		Message: fmt.Sprintf("operator forced reseed of %d table(s)", flagged),
	})
	fmt.Fprintf(stdout, "reseed: flagged %d table(s) for sync %q target %q; the running daemon will re-copy on its next pass\n", flagged, syncName, target)
	return 0
}
