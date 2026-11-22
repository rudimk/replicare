package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"text/tabwriter"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/observability/live"
)

// runVerify implements
//
//	replicare verify <config> [--json] [--sync <name>] [--table <schema.table>]...
//	                 [--sample <n>] [--chunk-rows <n>] [--pause <dur>]
//	                 [--recheck-after <dur>] [--watch <dur>]
//
// It is a READ-ONLY source↔target convergence check: for every replicated unit it
// content-fingerprints the source and each target and compares. Engines with chunked
// checks (Postgres) compare one key range at a time, with a pause between ranges, and
// narrow any difference to the exact rows; --sample checks a few random ranges only.
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
	var opts live.VerifyOptions
	value := func(i int, flag string) (string, bool) {
		if i+1 >= len(args) {
			fmt.Fprintf(stderr, "verify: %s needs a value\n", flag)
			return "", false
		}
		return args[i+1], true
	}
	for i := 0; i < len(args); i++ {
		switch a := args[i]; {
		case a == "--json":
			asJSON = true
		case a == "--table":
			v, ok := value(i, a)
			if !ok {
				return 2
			}
			i++
			opts.Tables = append(opts.Tables, v)
		case a == "--sample" || a == "--chunk-rows":
			v, ok := value(i, a)
			if !ok {
				return 2
			}
			i++
			n, err := strconv.Atoi(v)
			if err != nil || n < 1 {
				fmt.Fprintf(stderr, "verify: %s needs a whole number above 0, got %q\n", a, v)
				return 2
			}
			if a == "--sample" {
				opts.Sample = n
			} else {
				opts.ChunkRows = n
			}
		case a == "--pause" || a == "--recheck-after":
			v, ok := value(i, a)
			if !ok {
				return 2
			}
			i++
			d, err := time.ParseDuration(v)
			if err != nil || d < 0 {
				fmt.Fprintf(stderr, "verify: invalid %s duration %q\n", a, v)
				return 2
			}
			if d == 0 {
				d = -1 // explicit zero: no pause / re-check straight away
			}
			if a == "--pause" {
				opts.Pause = d
			} else {
				opts.RecheckAfter = d
			}
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
		fmt.Fprintln(stderr, "usage: replicare verify <config.yml> [--json] [--sync <name>] [--table <schema.table>]... [--sample <n>] [--chunk-rows <n>] [--pause <dur>] [--recheck-after <dur>] [--watch <dur>]")
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
	if len(opts.Tables) > 0 && len(names) > 1 {
		fmt.Fprintln(stderr, "verify: --table needs --sync, to say which sync the tables belong to")
		return 2
	}
	paused := pausedSyncs(cfg)
	enricher := live.New(cfg, connectTimeout)
	if !asJSON {
		opts.Progress = progressPrinter(stderr)
	}

	runOnce := func(ctx context.Context) ([]live.VerifyReport, bool) {
		reports := make([]live.VerifyReport, 0, len(names))
		converged := true
		for _, name := range names {
			rep := enricher.VerifyWith(ctx, name, opts)
			rep.Paused = paused[name]
			if rep.Error != "" || !rep.Converged {
				converged = false
			}
			reports = append(reports, rep)
		}
		return reports, converged
	}

	if watch == 0 {
		// No overall deadline: a full check of a large table takes as long as it takes
		// (each query has its own time limit). Ctrl-C stops it.
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
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
		reports, converged := runOnce(ctx)
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

// progressPrinter reports a long check's progress on stderr, at most every 30 seconds
// per table, so a full check of a large table is visibly moving.
func progressPrinter(w io.Writer) func(string, int64) {
	last := map[string]time.Time{}
	return func(table string, rows int64) {
		now := time.Now()
		if t, ok := last[table]; ok && now.Sub(t) < 30*time.Second {
			return
		}
		if _, ok := last[table]; !ok {
			last[table] = now // the first call starts the clock; report from the second interval on
			return
		}
		last[table] = now
		fmt.Fprintf(w, "verify: %s: %d rows checked so far\n", table, rows)
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
		if rep.Paused {
			// A paused sync isn't replicating, so convergence here is just a static
			// source-vs-target snapshot, not evidence the pipeline is live. Flag it so
			// "[CONVERGED]" on a paused sync isn't read as "still working".
			fmt.Fprintf(w, "sync: %s  [PAUSED] [%s]\n", rep.Sync, summary)
		} else {
			fmt.Fprintf(w, "sync: %s  [%s]\n", rep.Sync, summary)
		}
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
			fmt.Fprintln(tw, "    TABLE\tMODE\tSOURCE\tTARGET\tSTATUS")
			for _, t := range tg.Tables {
				st := t.Status
				if t.Error != "" {
					st = "error: " + t.Error
				}
				if t.Missing+t.Extra+t.Different > 0 {
					st += fmt.Sprintf(" (missing %d, extra %d, different %d)", t.Missing, t.Extra, t.Different)
				}
				if t.Truncated {
					st += " (more differences than were tracked)"
				}
				fmt.Fprintf(tw, "    %s\t%s\t%d\t%d\t%s\n", t.Table, t.Mode, t.SourceRows, t.TargetRows, st)
			}
			_ = tw.Flush()
			for _, t := range tg.Tables {
				for _, k := range t.Keys {
					fmt.Fprintf(w, "      %s key %s: %s\n", t.Table, k.Key, k.Problem)
				}
				if shown := len(t.Keys); shown > 0 && shown < t.Missing+t.Extra+t.Different {
					fmt.Fprintf(w, "      %s: %d more not listed (--json lists the same keys)\n", t.Table, t.Missing+t.Extra+t.Different-shown)
				}
			}
		}
		fmt.Fprintln(w)
	}
}
