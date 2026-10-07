package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/signal"
	"strings"
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
	paused := pausedSyncs(cfg)
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
			rep.Paused = paused[name]
			// Live enrichment still runs for a paused sync: its persisted cursors and the
			// growing source-side delta backlog (how much has queued while paused) are
			// exactly what an operator wants to see. The PAUSED banner marks it as not
			// running so the last-known phase isn't mistaken for live.
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
		renderReports(stdout, reports, liveMode, time.Now())
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
			renderReports(stdout, reports, liveMode, time.Now())
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

// pausedSyncs maps each sync name to whether it is paused (enabled: false), so the
// status view can flag a disabled sync as PAUSED rather than showing its stale
// last-known phase as if it were live.
func pausedSyncs(cfg *config.Config) map[string]bool {
	m := make(map[string]bool, len(cfg.Syncs))
	for _, s := range cfg.Syncs {
		m[s.Name] = !s.IsEnabled()
	}
	return m
}

// renderReports prints a human-readable status per sync: a one-line health headline
// followed by a per-(table,target) grid. When live is true it includes the live
// SRC_ROWS / TGT_ROWS / BACKLOG columns. now is the reference time for relative ages
// (injected so the output is deterministic in tests). Columns:
//   - SEEN: time since the last healthy streaming pass (liveness heartbeat — small when
//     the daemon is running, regardless of whether data moved).
//   - LAST_SYNC / ROWS: time since the last pass that actually APPLIED rows, and how many
//     it applied ("-" until the first data-moving pass).
func renderReports(w io.Writer, reports []status.Report, live bool, now time.Time) {
	for _, rep := range reports {
		fmt.Fprintf(w, "sync: %s\n", rep.Sync)
		if rep.Paused {
			fmt.Fprintln(w, "  [PAUSED] disabled (enabled: false) — not running; source capture still queues deltas. Set enabled: true and restart to resume.")
		} else {
			fmt.Fprintf(w, "  %s\n", summarizeSync(rep, live, now))
		}
		if rep.LiveError != "" {
			fmt.Fprintf(w, "  live: partial (%s)\n", rep.LiveError)
		}
		if len(rep.Tables) == 0 {
			fmt.Fprintln(w, "  (no tables tracked yet)")
		}
		tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
		if live {
			fmt.Fprintln(tw, "  TABLE\tCOPY\tSRC_ROWS\tTARGET\tPHASE\tSEEN\tTGT_ROWS\tBACKLOG\tLAST_SYNC\tROWS\tRESEED")
		} else {
			fmt.Fprintln(tw, "  TABLE\tCOPY\tTARGET\tPHASE\tSEEN\tLAST_SYNC\tROWS\tRESEED")
		}
		for _, tbl := range rep.Tables {
			cp := "pending"
			if tbl.CopyDone {
				cp = "done"
			}
			src := countCell(tbl.SourceRows)
			if len(tbl.Targets) == 0 {
				if live {
					fmt.Fprintf(tw, "  %s\t%s\t%s\t-\t-\t-\t-\t-\t-\t-\t-\n", tbl.Table, cp, src)
				} else {
					fmt.Fprintf(tw, "  %s\t%s\t-\t-\t-\t-\t-\t-\n", tbl.Table, cp)
				}
				continue
			}
			for _, tg := range tbl.Targets {
				reseed := "-"
				if tg.NeedsReseed {
					reseed = "NEEDS-RESEED"
				}
				if live {
					fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
						tbl.Table, cp, src, tg.Target, tg.Phase, humanAge(tg.CursorAgeSeconds),
						countCell(tg.TargetRows), backlogCell(tg.Backlog),
						lastSyncCell(tg.LastAppliedAt, now), rowsCell(tg), reseed)
				} else {
					fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
						tbl.Table, cp, tg.Target, tg.Phase, humanAge(tg.CursorAgeSeconds),
						lastSyncCell(tg.LastAppliedAt, now), rowsCell(tg), reseed)
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

// lastSyncCell renders the time since the last data-moving pass, or "-" when no rows
// have been applied to this (target, table) yet.
func lastSyncCell(at *time.Time, now time.Time) string {
	if at == nil {
		return "-"
	}
	return humanAge(now.Sub(*at).Seconds()) + " ago"
}

// rowsCell renders the rows applied in the last data-moving pass, or "-" when none yet.
func rowsCell(tg status.TargetStatus) string {
	if tg.LastAppliedAt == nil {
		return "-"
	}
	return fmt.Sprintf("%d", tg.LastAppliedRows)
}

// summarizeSync builds the one-line health headline for a sync: phase, liveness (time
// since the most recent healthy pass across its targets), last data movement (most recent
// applied pass + total rows applied across tables), and — in live mode — the backlog
// rollup (caught up / N rows / unknown). It aggregates over every (table, target).
func summarizeSync(rep status.Report, live bool, now time.Time) string {
	var (
		anyTarget    bool
		anyInitial   bool
		anyStreaming bool
		minSeen      = -1.0      // smallest cursor age = most recent healthy pass
		lastApplied  time.Time   // most recent data-moving pass
		totalApplied int64       // rows applied across tables' last data-moving passes
		backlogRows  int64
		backlogKnown bool        // at least one target reported a backlog
		backlogAll0  = true      // every reported backlog is 0
		oldestAge    float64
	)
	for _, tbl := range rep.Tables {
		for _, tg := range tbl.Targets {
			anyTarget = true
			switch tg.Phase {
			case "initial_copy":
				anyInitial = true
			case "streaming":
				anyStreaming = true
			}
			if minSeen < 0 || tg.CursorAgeSeconds < minSeen {
				minSeen = tg.CursorAgeSeconds
			}
			if tg.LastAppliedAt != nil {
				if tg.LastAppliedAt.After(lastApplied) {
					lastApplied = *tg.LastAppliedAt
				}
				totalApplied += tg.LastAppliedRows
			}
			if live && tg.Backlog != nil {
				backlogKnown = true
				backlogRows += tg.Backlog.Rows
				if tg.Backlog.Rows > 0 {
					backlogAll0 = false
				}
				if tg.Backlog.OldestAgeSeconds > oldestAge {
					oldestAge = tg.Backlog.OldestAgeSeconds
				}
			}
		}
	}

	if !anyTarget {
		return "no targets tracked yet"
	}

	phase := "pending"
	switch {
	case anyInitial:
		phase = "initial-copy"
	case anyStreaming:
		phase = "streaming"
	}
	parts := []string{phase}
	if minSeen >= 0 {
		parts = append(parts, "last pass "+humanAge(minSeen)+" ago")
	}
	if lastApplied.IsZero() {
		parts = append(parts, "no data applied yet")
	} else {
		parts = append(parts, fmt.Sprintf("last applied %s ago (%d rows total)",
			humanAge(now.Sub(lastApplied).Seconds()), totalApplied))
	}
	if live {
		switch {
		case !backlogKnown:
			parts = append(parts, "backlog unknown")
		case backlogAll0:
			parts = append(parts, "backlog 0 (caught up)")
		default:
			parts = append(parts, fmt.Sprintf("backlog %d (oldest %s)", backlogRows, humanAge(oldestAge)))
		}
	}
	return strings.Join(parts, " · ")
}
