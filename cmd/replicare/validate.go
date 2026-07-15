package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability"
)

// connectTimeout bounds each connect+introspect step so validate fails fast
// against an unreachable endpoint rather than hanging.
const connectTimeout = 30 * time.Second

// runValidate implements `replicare validate <config>`: it loads the config,
// runs pre-flight for every (sync, target) pair, renders the classified report,
// and returns a process exit code — 0 when nothing blocks, 1 when any pre-flight
// blocks or a step errors, 2 for usage/config errors.
func runValidate(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: replicare validate <config.yml>")
		return 2
	}
	cfg, err := config.Load(args[0])
	if err != nil {
		fmt.Fprintf(stderr, "replicare validate: %v\n", err)
		return 2
	}

	log := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	anyBlocked := false
	anyError := false
	for _, sync := range cfg.Syncs {
		blocked, failed := validateSync(stdout, log, cfg, sync)
		anyBlocked = anyBlocked || blocked
		anyError = anyError || failed
	}

	fmt.Fprintln(stdout)
	switch {
	case anyError:
		fmt.Fprintln(stdout, "validate: FAILED (errors during pre-flight)")
		return 1
	case anyBlocked:
		fmt.Fprintln(stdout, "validate: BLOCKED (fix the BLOCK findings above before starting)")
		return 1
	default:
		fmt.Fprintln(stdout, "validate: OK")
		return 0
	}
}

// validateSync runs pre-flight for one sync across all its targets, rendering
// each report. It returns whether any target blocked and whether any step errored.
func validateSync(stdout io.Writer, log *slog.Logger, cfg *config.Config, sync *config.Sync) (blocked, failed bool) {
	src := cfg.Sources[sync.Source]
	eng, err := engine.Get(src.Engine)
	if err != nil {
		fmt.Fprintf(stdout, "sync %q: %v\n", sync.Name, err)
		return false, true
	}

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	source, err := eng.NewSource(src.Conn.ConnConfig())
	if err != nil {
		fmt.Fprintf(stdout, "sync %q: new source: %v\n", sync.Name, err)
		return false, true
	}
	if err := source.Connect(ctx); err != nil {
		fmt.Fprintf(stdout, "sync %q: connect source %q: %v\n", sync.Name, sync.Source, err)
		return false, true
	}
	defer func() { _ = source.Close(ctx) }()

	sel := engine.Selection{Include: sync.Include, Exclude: sync.Exclude}
	srcSchema, err := source.Introspect(ctx, sel)
	if err != nil {
		fmt.Fprintf(stdout, "sync %q: introspect source: %v\n", sync.Name, err)
		return false, true
	}
	srcVer, err := source.ServerVersion(ctx)
	if err != nil {
		fmt.Fprintf(stdout, "sync %q: source version: %v\n", sync.Name, err)
		return false, true
	}

	for _, tname := range sync.Targets {
		b, f := validateTarget(stdout, log, cfg, eng, sync, tname, sel, srcSchema, srcVer)
		blocked = blocked || b
		failed = failed || f
	}
	return blocked, failed
}

// validateTarget runs and renders pre-flight for one (sync, target) pair.
func validateTarget(stdout io.Writer, log *slog.Logger, cfg *config.Config, eng engine.Engine,
	sync *config.Sync, tname string, sel engine.Selection, srcSchema *engine.Schema, srcVer int) (blocked, failed bool) {

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	tep := cfg.Targets[tname]
	sink, err := eng.NewSink(tep.Conn.ConnConfig())
	if err != nil {
		fmt.Fprintf(stdout, "sync %q target %q: new sink: %v\n", sync.Name, tname, err)
		return false, true
	}
	if err := sink.Connect(ctx); err != nil {
		fmt.Fprintf(stdout, "sync %q target %q: connect: %v\n", sync.Name, tname, err)
		return false, true
	}
	defer func() { _ = sink.Close(ctx) }()

	tgtSchema, err := sink.Introspect(ctx, sel)
	if err != nil {
		fmt.Fprintf(stdout, "sync %q target %q: introspect: %v\n", sync.Name, tname, err)
		return false, true
	}
	tgtVer, err := sink.ServerVersion(ctx)
	if err != nil {
		fmt.Fprintf(stdout, "sync %q target %q: version: %v\n", sync.Name, tname, err)
		return false, true
	}

	report := eng.Preflight(sync.Name, srcVer, tgtVer, srcSchema, tgtSchema)
	renderReport(stdout, eng.Name(), sync.Source, tname, report)

	if report.Blocked() {
		// F2 log channel: the pre-flight-blocked event (CLAUDE.md §10).
		log.Error("pre-flight blocked",
			slog.String("event", observability.EventPreflightBlocked),
			slog.String(observability.AttrSync, sync.Name),
			slog.String(observability.AttrTarget, tname))
		return true, false
	}
	return false, false
}

// renderReport prints a human-readable pre-flight report.
func renderReport(w io.Writer, engineName, sourceName, targetName string, r *engine.PreflightReport) {
	fmt.Fprintf(w, "\n== sync %q: source %q -> target %q [%s] ==\n", r.Sync, sourceName, targetName, engineName)
	fmt.Fprintf(w, "server versions: source=%d target=%d\n", r.SourceVersion, r.TargetVersion)

	fmt.Fprintf(w, "\nreplicable tables (%d):\n", len(r.Replicable))
	for _, t := range r.Replicable {
		fmt.Fprintf(w, "  %s\n", t)
	}
	if len(r.Skipped) > 0 {
		fmt.Fprintf(w, "\nskipped (%d):\n", len(r.Skipped))
		for _, s := range r.Skipped {
			fmt.Fprintf(w, "  %s  (%s)\n", s.Table, s.Reason)
		}
	}
	if len(r.Components) > 0 {
		fmt.Fprintf(w, "\nFK components (%d):\n", len(r.Components))
		for _, comp := range r.Components {
			names := make([]string, len(comp))
			for i, c := range comp {
				names[i] = c.String()
			}
			fmt.Fprintf(w, "  [%s]\n", strings.Join(names, ", "))
		}
	}

	if len(r.Findings) > 0 {
		fmt.Fprintf(w, "\nfindings (%d):\n", len(r.Findings))
		for _, f := range r.Findings {
			table := f.Table.String()
			if table == "" {
				table = "-"
			}
			fmt.Fprintf(w, "  %-5s %-24s %-14s %s\n", f.Severity, table, f.Category, f.Message)
		}
	}

	info, warn, block := r.Counts()
	fmt.Fprintf(w, "\nsummary: %d replicable, %d skipped, %d components; findings %d info / %d warn / %d block\n",
		len(r.Replicable), len(r.Skipped), len(r.Components), info, warn, block)
}
