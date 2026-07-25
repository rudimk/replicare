package main

import (
	"context"
	"fmt"
	"io"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"
)

// runCapture implements `replicare capture install|remove <config> [--sync <name>]`:
// it installs or removes the trigger-based CDC machinery on each sync's source
// (CLAUDE.md §3.1, §12). Install needs only CREATE-on-schema + TRIGGER + SELECT;
// remove additionally needs table ownership to drop the triggers (documented
// asymmetry, §12) — a failure there is reported, not hidden. Exit codes: 0 ok,
// 1 error, 2 usage/config.
func runCapture(args []string, stdout, stderr io.Writer) int {
	if len(args) < 1 {
		return captureUsage(stderr)
	}
	action := args[0]
	if action != "install" && action != "remove" {
		fmt.Fprintf(stderr, "capture: unknown action %q\n", action)
		return captureUsage(stderr)
	}

	var cfgPath, only string
	for i := 1; i < len(args); i++ {
		switch a := args[i]; {
		case a == "--sync":
			if i+1 >= len(args) {
				fmt.Fprintln(stderr, "capture: --sync needs a value")
				return 2
			}
			i++
			only = args[i]
		case cfgPath == "":
			cfgPath = a
		default:
			fmt.Fprintf(stderr, "capture: unexpected argument %q\n", a)
			return 2
		}
	}
	if cfgPath == "" {
		return captureUsage(stderr)
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		fmt.Fprintf(stderr, "capture: %v\n", err)
		return 2
	}

	failed := false
	for _, sync := range cfg.Syncs {
		if only != "" && sync.Name != only {
			continue
		}
		if err := captureOne(stdout, cfg, sync, action); err != nil {
			fmt.Fprintf(stderr, "capture %s: sync %q: %v\n", action, sync.Name, err)
			failed = true
		}
	}
	if failed {
		return 1
	}
	return 0
}

// captureOne installs or removes capture for a single sync's source over its
// selected tables.
func captureOne(stdout io.Writer, cfg *config.Config, sync *config.Sync, action string) error {
	src := cfg.Sources[sync.Source]
	if src == nil {
		return fmt.Errorf("unknown source %q", sync.Source)
	}
	eng, err := engine.Get(src.Engine)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	source, err := eng.NewSource(src.Conn.ConnConfig())
	if err != nil {
		return err
	}
	if err := source.Connect(ctx); err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer func() { _ = source.Close(ctx) }()

	schema, err := source.Introspect(ctx, engine.Selection{Include: sync.Include, Exclude: sync.Exclude})
	if err != nil {
		return fmt.Errorf("introspect: %w", err)
	}
	refs := make([]engine.TableRef, 0, len(schema.Tables))
	for _, t := range schema.Tables {
		refs = append(refs, t.Ref)
	}

	switch action {
	case "install":
		if err := source.InstallCapture(ctx, refs); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "sync %q: capture installed on %d table(s) of source %q\n", sync.Name, len(refs), sync.Source)
	case "remove":
		if err := source.RemoveCapture(ctx, refs); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "sync %q: capture removed from source %q\n", sync.Name, sync.Source)
	}
	return nil
}

func captureUsage(w io.Writer) int {
	fmt.Fprintln(w, "usage: replicare capture install|remove <config.yml> [--sync <name>]")
	return 2
}
