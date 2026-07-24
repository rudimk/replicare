package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"text/tabwriter"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/observability/status"
	statepg "github.com/rudimk/replicare/internal/state/postgres"
)

// runStatus implements `replicare status <config> [--json] [--sync <name>]`: it
// reads the StateStore and renders each sync's phase, lag, initial-copy progress,
// and recent events (CLAUDE.md §10). It reads persisted checkpoints, so it works
// whether or not a daemon is currently running. Exit codes: 0 ok, 1 error, 2
// usage/config.
func runStatus(args []string, stdout, stderr io.Writer) int {
	var cfgPath, only string
	asJSON := false
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "--json":
			asJSON = true
		case a == "--sync":
			if i+1 >= len(args) {
				fmt.Fprintln(stderr, "status: --sync needs a value")
				return 2
			}
			i++
			only = args[i]
		case cfgPath == "":
			cfgPath = a
		default:
			fmt.Fprintf(stderr, "status: unexpected argument %q\n", a)
			return 2
		}
	}
	if cfgPath == "" {
		fmt.Fprintln(stderr, "usage: replicare status <config.yml> [--json] [--sync <name>]")
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

	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()

	store := statepg.New(cfg.StateStore.Conn.ConnConfig())
	if err := store.Open(ctx); err != nil {
		fmt.Fprintf(stderr, "status: open state store: %v\n", err)
		return 1
	}
	defer func() { _ = store.Close(context.Background()) }()

	names := syncNames(cfg, only)
	reporter := status.NewReporter(store)
	reports := make([]status.Report, 0, len(names))
	for _, name := range names {
		rep, err := reporter.Report(ctx, name)
		if err != nil {
			fmt.Fprintf(stderr, "status: report %s: %v\n", name, err)
			return 1
		}
		reports = append(reports, rep)
	}

	if asJSON {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(reports); err != nil {
			fmt.Fprintf(stderr, "status: encode: %v\n", err)
			return 1
		}
		return 0
	}
	renderReports(stdout, reports)
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

// renderReports prints a human-readable status table per sync.
func renderReports(w io.Writer, reports []status.Report) {
	for _, rep := range reports {
		fmt.Fprintf(w, "sync: %s\n", rep.Sync)
		if len(rep.Tables) == 0 {
			fmt.Fprintln(w, "  (no tables tracked yet)")
		}
		tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
		fmt.Fprintln(tw, "  TABLE\tCOPY\tTARGET\tPHASE\tLAG\tLAST_DELTA\tRESEED")
		for _, tbl := range rep.Tables {
			cp := "pending"
			if tbl.CopyDone {
				cp = "done"
			}
			if len(tbl.Targets) == 0 {
				fmt.Fprintf(tw, "  %s\t%s\t-\t-\t-\t-\t-\n", tbl.Table, cp)
				continue
			}
			for _, tg := range tbl.Targets {
				reseed := "-"
				if tg.NeedsReseed {
					reseed = "NEEDS-RESEED"
				}
				fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%s\t%d\t%s\n",
					tbl.Table, cp, tg.Target, tg.Phase, humanAge(tg.CursorAgeSeconds), tg.LastDelta, reseed)
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

// humanAge renders a cursor age (seconds) compactly.
func humanAge(sec float64) string {
	if sec <= 0 {
		return "-"
	}
	d := time.Duration(sec * float64(time.Second))
	return d.Round(time.Second).String()
}
