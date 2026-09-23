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
)

// runVerify implements
//
//	replicare verify <config> [--json] [--sync <name>] [--watch <dur>]
//
// It is a READ-ONLY source↔target convergence spot-check: for every replicated unit
// it counts and content-fingerprints the source and each target and compares
// (CLAUDE.md §1.7 faithful transport — verify never writes). It installs nothing and
// is safe against a source we may not own. Because a sync is single-engine (§6), the
// two ends fingerprint identically and their checksums are directly comparable.
//
// Exit codes: 0 = all selected syncs converged; 1 = drift or an error reaching an
// endpoint; 2 = usage/config. In --watch mode it re-checks on an interval until
// SIGINT and the exit code reflects the last completed pass.
func runVerify(args []string, stdout, stderr io.Writer) int {
	var cfgPath, only string
	asJSON := false
	var watch time.Duration
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "--json":
			asJSON = true
		case a == "--sync":
			if i+1 >= len(args) {
				fmt.Fprintln(stderr, "verify: --sync needs a value")
				return 2
			}
			i++
			only = args[i]
		case a == "--watch":
			if i+1 >= len(args) {
				fmt.Fprintln(stderr, "verify: --watch needs a duration (e.g. 5s)")
				return 2
			}
			i++
			d, err := time.ParseDuration(args[i])
			if err != nil || d <= 0 {
				fmt.Fprintf(stderr, "verify: invalid --watch duration %q\n", args[i])
				return 2
			}
			watch = d
		case cfgPath == "":
			cfgPath = a
		default:
			fmt.Fprintf(stderr, "verify: unexpected argument %q\n", a)
			return 2
		}
	}
	if cfgPath == "" {
		fmt.Fprintln(stderr, "usage: replicare verify <config.yml> [--json] [--sync <name>] [--watch <dur>]")
		return 2
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		fmt.Fprintf(stderr, "verify: %v\n", err)
		return 2
	}
	names := syncNames(cfg, only)
	if len(names) == 0 {
		fmt.Fprintln(stderr, "verify: no syncs in config")
		return 2
	}
	enricher := live.New(cfg, connectTimeout)

	runOnce := func(ctx context.Context) ([]live.VerifyReport, bool) {
		reports := make([]live.VerifyReport, 0, len(names))
		converged := true
		for _, name := range names {
			rep := enricher.Verify(ctx, name)
			if rep.Error != "" || !rep.Converged {
				converged = false
			}
			reports = append(reports, rep)
		}
		return reports, converged
	}

	if watch == 0 {
		ctx, c := context.WithTimeout(context.Background(), connectTimeout*time.Duration(len(names)+1))
		defer c()
		reports, converged := runOnce(ctx)
		if asJSON {
			if code := emitVerifyJSON(stdout, stderr, reports); code != 0 {
				return code
			}
		} else {
			renderVerify(stdout, reports)
		}
		if converged {
			return 0
		}
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ticker := time.NewTicker(watch)
	defer ticker.Stop()
	var lastConverged bool // set every iteration before the ctx.Done() read below
	for {
		stepCtx, c := context.WithTimeout(ctx, connectTimeout*time.Duration(len(names)+1))
		reports, converged := runOnce(stepCtx)
		c()
		lastConverged = converged
		if asJSON {
			_ = emitVerifyJSON(stdout, stderr, reports)
		} else {
			fmt.Fprint(stdout, clearScreen)
			fmt.Fprintf(stdout, "replicare verify @ %s  (every %s, Ctrl-C to stop)\n\n",
				time.Now().Format(time.RFC3339), watch)
			renderVerify(stdout, reports)
		}
		select {
		case <-ctx.Done():
			if lastConverged {
				return 0
			}
			return 1
		case <-ticker.C:
		}
	}
}

func emitVerifyJSON(stdout, stderr io.Writer, reports []live.VerifyReport) int {
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(reports); err != nil {
		fmt.Fprintf(stderr, "verify: encode: %v\n", err)
		return 1
	}
	return 0
}

// renderVerify prints a per-table convergence table per (sync, target).
func renderVerify(w io.Writer, reports []live.VerifyReport) {
	for _, rep := range reports {
		summary := "CONVERGED"
		if rep.Error != "" || !rep.Converged {
			summary = "DIVERGED"
		}
		fmt.Fprintf(w, "sync: %s  [%s]\n", rep.Sync, summary)
		if rep.Error != "" {
			fmt.Fprintf(w, "  error: %s\n\n", rep.Error)
			continue
		}
		for _, tg := range rep.Targets {
			fmt.Fprintf(w, "  target: %s\n", tg.Target)
			if tg.Error != "" {
				fmt.Fprintf(w, "    error: %s\n", tg.Error)
				continue
			}
			tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
			fmt.Fprintln(tw, "    TABLE\tSOURCE\tTARGET\tSTATUS")
			for _, t := range tg.Tables {
				st := t.Status
				if t.Error != "" {
					st = "error: " + t.Error
				}
				fmt.Fprintf(tw, "    %s\t%d\t%d\t%s\n", t.Table, t.SourceRows, t.TargetRows, st)
			}
			_ = tw.Flush()
		}
		fmt.Fprintln(w)
	}
}
