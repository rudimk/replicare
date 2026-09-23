package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/signal"
	"syscall"
	"time"

	"text/tabwriter"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/observability/live"
	"github.com/rudimk/replicare/internal/observability/status"
	statepg "github.com/rudimk/replicare/internal/state/postgres"
)

// runStatus implements
//
//	replicare status <config> [--json] [--sync <name>] [--no-live] [--watch <dur>]
//
// It reads the StateStore for persisted phase/lag/copy/reseed state (works whether
// or not a daemon is running) and, LIVE BY DEFAULT, connects to the sync's source
// and targets to add live row/key counts and per-target delta backlog — the
// visibility an operator needs without Grafana (CLAUDE.md §10). --no-live drops the
// live signals (state-store only); --watch <dur> re-renders on an interval until
// SIGINT. Exit codes: 0 ok, 1 error, 2 usage/config.
func runStatus(args []string, stdout, stderr io.Writer) int {
	var cfgPath, only string
	asJSON := false
	liveMode := true
	var watch time.Duration
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "--json":
			asJSON = true
		case a == "--no-live":
			liveMode = false
		case a == "--live":
			liveMode = true
		case a == "--sync":
			if i+1 >= len(args) {
				fmt.Fprintln(stderr, "status: --sync needs a value")
				return 2
			}
			i++
			only = args[i]
		case a == "--watch":
			if i+1 >= len(args) {
				fmt.Fprintln(stderr, "status: --watch needs a duration (e.g. 5s)")
				return 2
			}
			i++
			d, err := time.ParseDuration(args[i])
			if err != nil || d <= 0 {
				fmt.Fprintf(stderr, "status: invalid --watch duration %q\n", args[i])
				return 2
			}
			watch = d
		case cfgPath == "":
			cfgPath = a
		default:
			fmt.Fprintf(stderr, "status: unexpected argument %q\n", a)
			return 2
		}
	}
	if cfgPath == "" {
		fmt.Fprintln(stderr, "usage: replicare status <config.yml> [--json] [--sync <name>] [--no-live] [--watch <dur>]")
		return 2
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		fmt.Fprintf(stderr, "status: %v\n", err)
		return 2
	}
	if cfg.StateStore == nil {
		fmt.Fprintln(stderr, "status: no state_store configured")
		return 2
	}

	store := statepg.New(cfg.StateStore.Conn.ConnConfig())
	openCtx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	err = store.Open(openCtx)
	cancel()
	if err != nil {
		fmt.Fprintf(stderr, "status: open state store: %v\n", err)
		return 1
	}
	defer func() { _ = store.Close(context.Background()) }()

	names := syncNames(cfg, only)
	reporter := status.NewReporter(store)
	var enricher *live.Enricher
	if liveMode {
		enricher = live.New(cfg, connectTimeout)
	}

	// collect builds the current report set (state store + optional live enrichment).
	collect := func(ctx context.Context) ([]status.Report, error) {
		reports := make([]status.Report, 0, len(names))
		for _, name := range names {
			rep, err := reporter.Report(ctx, name)
			if err != nil {
				return nil, fmt.Errorf("report %s: %w", name, err)
			}
			if enricher != nil {
				rep = enricher.Enrich(ctx, name, rep)
			}
			reports = append(reports, rep)
		}
		return reports, nil
	}

	if watch == 0 {
		ctx, c := context.WithTimeout(context.Background(), connectTimeout)
		defer c()
		reports, err := collect(ctx)
		if err != nil {
			fmt.Fprintf(stderr, "status: %v\n", err)
			return 1
		}
		if asJSON {
			return emitJSON(stdout, stderr, reports)
		}
		renderReports(stdout, reports, liveMode)
		return 0
	}

	// Watch mode: re-render every `watch` until SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ticker := time.NewTicker(watch)
	defer ticker.Stop()
	for {
		stepCtx, c := context.WithTimeout(ctx, connectTimeout)
		reports, err := collect(stepCtx)
		c()
		if err != nil {
			if ctx.Err() != nil {
				return 0
			}
			fmt.Fprintf(stderr, "status: %v\n", err)
			return 1
		}
		if asJSON {
			if code := emitJSON(stdout, stderr, reports); code != 0 {
				return code
			}
		} else {
			fmt.Fprint(stdout, clearScreen)
			fmt.Fprintf(stdout, "replicare status @ %s  (every %s, Ctrl-C to stop)\n\n",
				time.Now().Format(time.RFC3339), watch)
			renderReports(stdout, reports, liveMode)
		}
		select {
		case <-ctx.Done():
			return 0
		case <-ticker.C:
		}
	}
}

// clearScreen is the ANSI home+clear sequence used between watch frames.
const clearScreen = "\033[H\033[2J"

// emitJSON encodes the reports as indented JSON.
func emitJSON(stdout, stderr io.Writer, reports []status.Report) int {
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(reports); err != nil {
		fmt.Fprintf(stderr, "status: encode: %v\n", err)
		return 1
	}
	return 0
}

// syncNames returns the syncs to report: just `only` if set, else every sync in
// the config.
func syncNames(cfg *config.Config, only string) []string {
	if only != "" {
		return []string{only}
	}
	names := make([]string, 0, len(cfg.Syncs))
	for _, s := range cfg.Syncs {
		names = append(names, s.Name)
	}
	return names
}

// renderReports prints a human-readable status table per sync. When live is true it
// includes the live SRC_ROWS / TGT_ROWS / BACKLOG columns.
func renderReports(w io.Writer, reports []status.Report, live bool) {
	for _, rep := range reports {
		fmt.Fprintf(w, "sync: %s\n", rep.Sync)
		if rep.LiveError != "" {
			fmt.Fprintf(w, "  live: partial (%s)\n", rep.LiveError)
		}
		if len(rep.Tables) == 0 {
			fmt.Fprintln(w, "  (no tables tracked yet)")
		}
		tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
		if live {
			fmt.Fprintln(tw, "  TABLE\tCOPY\tSRC_ROWS\tTARGET\tPHASE\tLAG\tTGT_ROWS\tBACKLOG\tLAST_DELTA\tRESEED")
		} else {
			fmt.Fprintln(tw, "  TABLE\tCOPY\tTARGET\tPHASE\tLAG\tLAST_DELTA\tRESEED")
		}
		for _, tbl := range rep.Tables {
			cp := "pending"
			if tbl.CopyDone {
				cp = "done"
			}
			src := countCell(tbl.SourceRows)
			if len(tbl.Targets) == 0 {
				if live {
					fmt.Fprintf(tw, "  %s\t%s\t%s\t-\t-\t-\t-\t-\t-\t-\n", tbl.Table, cp, src)
				} else {
					fmt.Fprintf(tw, "  %s\t%s\t-\t-\t-\t-\t-\n", tbl.Table, cp)
				}
				continue
			}
			for _, tg := range tbl.Targets {
				reseed := "-"
				if tg.NeedsReseed {
					reseed = "NEEDS-RESEED"
				}
				if live {
					fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%s\n",
						tbl.Table, cp, src, tg.Target, tg.Phase, humanAge(tg.CursorAgeSeconds),
						countCell(tg.TargetRows), backlogCell(tg.Backlog), tg.LastDelta, reseed)
				} else {
					fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%s\t%d\t%s\n",
						tbl.Table, cp, tg.Target, tg.Phase, humanAge(tg.CursorAgeSeconds), tg.LastDelta, reseed)
				}
			}
		}
		_ = tw.Flush()

		if len(rep.Events) > 0 {
			fmt.Fprintln(w, "  recent events:")
			for _, ev := range rep.Events {
				fmt.Fprintf(w, "    [%s] %s %s %s %s\n",
					ev.Level, ev.Event, ev.Target, ev.Table, ev.Message)
			}
		}
		fmt.Fprintln(w)
	}
}

// countCell renders a live count, "-" when the signal is absent (endpoint
// unreachable or the engine has no verifier).
func countCell(n *int64) string {
	if n == nil {
		return "-"
	}
	return fmt.Sprintf("%d", *n)
}

// backlogCell renders a target's delta backlog as "rows (oldest-age)", or "0" when
// caught up, or "-" when the signal is absent.
func backlogCell(b *status.Backlog) string {
	if b == nil {
		return "-"
	}
	if b.Rows == 0 {
		return "0"
	}
	return fmt.Sprintf("%d (%s)", b.Rows, humanAge(b.OldestAgeSeconds))
}

// humanAge renders a cursor age (seconds) compactly.
func humanAge(sec float64) string {
	if sec <= 0 {
		return "-"
	}
	d := time.Duration(sec * float64(time.Second))
	return d.Round(time.Second).String()
}
