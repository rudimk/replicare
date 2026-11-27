package daemon

import (
	"context"
	"fmt"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"
)

// consumer is one job (a sync target or a cluster edge) that reads changes from a
// source database.
type consumer struct {
	job     string // sync name, or the cluster edge's name
	target  engine.TargetID
	sel     engine.Selection
	enabled bool
}

// consumersOf lists every job in the config that reads from the database at src: each
// target of every sync whose source is that database, and every cluster edge whose
// source node is. Paused ones are included; the caller decides whether they count.
func consumersOf(cfg *config.Config, src *config.Endpoint) []consumer {
	id := config.ConnIdentity(src)
	if id == "" {
		return nil
	}
	var out []consumer
	for _, s := range cfg.Syncs {
		if config.ConnIdentity(cfg.Sources[s.Source]) != id {
			continue
		}
		for _, tg := range s.Targets {
			out = append(out, consumer{job: s.Name, target: engine.TargetID(tg),
				sel: engine.Selection{Include: s.Include, Exclude: s.Exclude}, enabled: s.IsEnabled()})
		}
	}
	for _, e := range clusterEdges(cfg.Clusters) {
		if config.ConnIdentity(cfg.Nodes[e.srcNode]) != id {
			continue
		}
		out = append(out, consumer{job: e.name(), target: engine.TargetID(e.dstNode),
			sel: engine.Selection{Include: e.cluster.Include, Exclude: e.cluster.Exclude}, enabled: e.cluster.IsEnabled()})
	}
	return out
}

// purgeTargets works out, per table, every target that reads that table's changes from
// the source the syncer reads: the targets of every running job on that database, and
// of paused jobs that have started (state in the store). A paused job that has never
// run has nothing queued that it needs, so it does not hold changes back. Each job's
// tables come from introspecting the shared source with that job's own selection.
func (d *Daemon) purgeTargets(ctx context.Context, src engine.Source, srcEp *config.Endpoint) (map[engine.TableRef][]engine.TargetID, error) {
	out := map[engine.TableRef][]engine.TargetID{}
	seen := map[engine.TableRef]map[engine.TargetID]bool{}
	for _, c := range consumersOf(d.cfg, srcEp) {
		if !c.enabled {
			started, err := d.jobStarted(ctx, c.job)
			if err != nil {
				return nil, err
			}
			if !started {
				continue
			}
		}
		schema, err := src.Introspect(ctx, c.sel)
		if err != nil {
			return nil, fmt.Errorf("tables of %s: %w", c.job, err)
		}
		for _, t := range schema.Tables {
			if !t.HasUsableKey() {
				continue
			}
			if seen[t.Ref] == nil {
				seen[t.Ref] = map[engine.TargetID]bool{}
			}
			if !seen[t.Ref][c.target] {
				seen[t.Ref][c.target] = true
				out[t.Ref] = append(out[t.Ref], c.target)
			}
		}
	}
	return out, nil
}

// jobStarted reports whether a job has any copy progress or streaming cursor in the
// state store, i.e. has run before.
func (d *Daemon) jobStarted(ctx context.Context, job string) (bool, error) {
	cur, err := d.store.ListCursors(ctx, job)
	if err != nil {
		return false, fmt.Errorf("state of %s: %w", job, err)
	}
	if len(cur) > 0 {
		return true, nil
	}
	prog, err := d.store.ListCopyProgress(ctx, job)
	if err != nil {
		return false, fmt.Errorf("state of %s: %w", job, err)
	}
	return len(prog) > 0, nil
}
