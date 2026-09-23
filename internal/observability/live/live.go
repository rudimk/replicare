// Package live is the operator-facing LIVE surface behind the `replicare status`
// (live mode) and `replicare verify` CLI commands. Where the state-store Reporter
// (internal/observability/status) reports only persisted checkpoints — and so needs
// no database beyond the state store — this package connects to the sync's actual
// source and targets to read signals that live only in the databases:
//
//   - live source/target row (or key) counts, so an operator can see initial-copy
//     progress and streaming convergence as concrete numbers;
//   - per-target delta backlog (unconsumed rows/bytes + oldest-unconsumed age), the
//     headline "how far behind / is the source footprint healthy?" signal (§3.4);
//   - a source↔target convergence spot-check (count + content fingerprint).
//
// It is engine-neutral: counts and fingerprints come through the optional
// engine.Verifier capability, delta backlog through the Source. All reads are
// read-only and install nothing, so this is safe against a source we may not own
// and against live targets. It is best-effort: an unreachable endpoint degrades to
// a note (status) or a per-target error (verify), never a crash.
package live

import (
	"context"
	"fmt"
	"time"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/observability/status"
)

// Enricher connects to a sync's source and targets on demand to collect live
// signals. It holds no long-lived connections; every call opens and closes its own,
// so it is safe to invoke repeatedly (e.g. a --watch loop).
type Enricher struct {
	cfg     *config.Config
	timeout time.Duration
}

// New builds an Enricher over a validated config. timeout bounds each individual
// connect/introspect/query so one unreachable endpoint cannot hang the command.
func New(cfg *config.Config, timeout time.Duration) *Enricher {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Enricher{cfg: cfg, timeout: timeout}
}

// findSync resolves a sync by name.
func (e *Enricher) findSync(name string) *config.Sync {
	for _, s := range e.cfg.Syncs {
		if s.Name == name {
			return s
		}
	}
	return nil
}

// replicableTable pairs a table ref with the name-matched, insert-eligible column
// projection used for fingerprinting.
type replicableTable struct {
	ref  engine.TableRef
	cols []string
}

// replicableTables introspects the source for the selection and returns the tables
// with a usable key (the ones actually replicated, §3.1) and their projections.
func replicableTables(schema *engine.Schema) []replicableTable {
	out := make([]replicableTable, 0, len(schema.Tables))
	for _, t := range schema.Tables {
		if !t.HasUsableKey() {
			continue
		}
		cols := make([]string, 0, len(t.Columns))
		for _, c := range t.Columns {
			if c.Generated {
				continue
			}
			cols = append(cols, c.Name)
		}
		out = append(out, replicableTable{ref: t.Ref, cols: cols})
	}
	return out
}

// openSource opens and connects an engine Source for an endpoint, introspecting it
// for the selection (which also primes engine-specific selection state, e.g. Redis
// key-globs). The returned close func releases the connection.
func openSource(ctx context.Context, ep *config.Endpoint, sel engine.Selection) (engine.Source, *engine.Schema, func(), error) {
	eng, err := engine.Get(ep.Engine)
	if err != nil {
		return nil, nil, func() {}, err
	}
	src, err := eng.NewSource(ep.Conn.ConnConfig())
	if err != nil {
		return nil, nil, func() {}, err
	}
	if err := src.Connect(ctx); err != nil {
		return nil, nil, func() {}, err
	}
	closeFn := func() { _ = src.Close(context.Background()) }
	schema, err := src.Introspect(ctx, sel)
	if err != nil {
		closeFn()
		return nil, nil, func() {}, fmt.Errorf("introspect source: %w", err)
	}
	return src, schema, closeFn, nil
}

// openSink opens and connects an engine Sink for an endpoint and introspects it for
// the selection (priming engine-specific selection state).
func openSink(ctx context.Context, ep *config.Endpoint, sel engine.Selection) (engine.Sink, func(), error) {
	eng, err := engine.Get(ep.Engine)
	if err != nil {
		return nil, func() {}, err
	}
	sink, err := eng.NewSink(ep.Conn.ConnConfig())
	if err != nil {
		return nil, func() {}, err
	}
	if err := sink.Connect(ctx); err != nil {
		return nil, func() {}, err
	}
	closeFn := func() { _ = sink.Close(context.Background()) }
	if _, err := sink.Introspect(ctx, sel); err != nil {
		closeFn()
		return nil, func() {}, fmt.Errorf("introspect target: %w", err)
	}
	return sink, closeFn, nil
}

// Enrich augments a state-store Report (base, for sync `name`) with live signals:
// per-table source row count, and per (target, table) target row count + delta
// backlog. It is best-effort — a source/target that cannot be reached leaves the
// live fields nil and records base.LiveError, so the state-store view still renders.
func (e *Enricher) Enrich(ctx context.Context, name string, base status.Report) status.Report {
	sync := e.findSync(name)
	if sync == nil {
		base.LiveError = fmt.Sprintf("sync %q not found in config (live signals unavailable)", name)
		return base
	}
	sel := engine.Selection{Include: sync.Include, Exclude: sync.Exclude}

	srcCtx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	srcEp := e.cfg.Sources[sync.Source]
	if srcEp == nil {
		base.LiveError = fmt.Sprintf("source %q not found in config", sync.Source)
		return base
	}
	src, schema, closeSrc, err := openSource(srcCtx, srcEp, sel)
	if err != nil {
		base.LiveError = fmt.Sprintf("source %q unreachable: %v", sync.Source, err)
		return base
	}
	defer closeSrc()

	tables := replicableTables(schema)
	// Index the base report's tables so live signals fold onto the right row, and
	// add any replicable table missing from the state store (e.g. before its first
	// checkpoint) so live counts still show it.
	idx := map[string]int{}
	for i := range base.Tables {
		idx[base.Tables[i].Table] = i
	}
	ensure := func(ref engine.TableRef) *status.TableStatus {
		key := ref.String()
		if i, ok := idx[key]; ok {
			return &base.Tables[i]
		}
		base.Tables = append(base.Tables, status.TableStatus{Table: key})
		idx[key] = len(base.Tables) - 1
		return &base.Tables[len(base.Tables)-1]
	}

	var notes []string
	srcVerifier, srcCanCount := src.(engine.Verifier)

	for _, rt := range tables {
		ts := ensure(rt.ref)
		if srcCanCount {
			qctx, qcancel := context.WithTimeout(ctx, e.timeout)
			if n, err := srcVerifier.CountRows(qctx, rt.ref); err == nil {
				v := n
				ts.SourceRows = &v
			} else {
				notes = appendNote(notes, fmt.Sprintf("source count %s: %v", rt.ref, err))
			}
			qcancel()
		}
	}

	// Per target: target row count + delta backlog (read from the source).
	for _, tgtName := range sync.Targets {
		tgtEp := e.cfg.Targets[tgtName]
		if tgtEp == nil {
			notes = appendNote(notes, fmt.Sprintf("target %q not found in config", tgtName))
			continue
		}
		tctx, tcancel := context.WithTimeout(ctx, e.timeout)
		sink, closeSink, err := openSink(tctx, tgtEp, sel)
		tcancel()
		if err != nil {
			notes = appendNote(notes, fmt.Sprintf("target %q unreachable: %v", tgtName, err))
			continue
		}
		sinkVerifier, sinkCanCount := sink.(engine.Verifier)
		for _, rt := range tables {
			ts := ensure(rt.ref)
			tt := ensureTarget(ts, tgtName)
			if sinkCanCount {
				qctx, qcancel := context.WithTimeout(ctx, e.timeout)
				if n, err := sinkVerifier.CountRows(qctx, rt.ref); err == nil {
					v := n
					tt.TargetRows = &v
				}
				qcancel()
			}
			qctx, qcancel := context.WithTimeout(ctx, e.timeout)
			if b, err := src.DeltaBacklog(qctx, rt.ref, engine.TargetID(tgtName)); err == nil {
				tt.Backlog = &status.Backlog{
					Rows: b.Rows, Bytes: b.Bytes, OldestAgeSeconds: b.OldestAge.Seconds(),
				}
			}
			qcancel()
		}
		closeSink()
	}

	if len(notes) > 0 {
		base.LiveError = joinNotes(notes)
	}
	return base
}

// ensureTarget returns the TargetStatus for target within a TableStatus, creating a
// bare one if the state store has no cursor row for it yet.
func ensureTarget(ts *status.TableStatus, target string) *status.TargetStatus {
	for i := range ts.Targets {
		if ts.Targets[i].Target == target {
			return &ts.Targets[i]
		}
	}
	ts.Targets = append(ts.Targets, status.TargetStatus{Target: target})
	return &ts.Targets[len(ts.Targets)-1]
}

func appendNote(notes []string, n string) []string {
	const maxNotes = 8
	if len(notes) >= maxNotes {
		return notes
	}
	return append(notes, n)
}

func joinNotes(notes []string) string {
	out := ""
	for i, n := range notes {
		if i > 0 {
			out += "; "
		}
		out += n
	}
	return out
}
