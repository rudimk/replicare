package daemon

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

// clusterEdge is one directed replication edge of an active-active cluster: changes
// captured on srcNode are applied to dstNode. A full mesh over N members is the set
// of all ordered pairs (srcNode != dstNode), so every member's writes reach every
// other member directly (docs/multi-master.md §5.2). Each edge runs as an
// independent single-active job in cluster mode (loop-suppressing capture + marked
// apply), so an inbound change is not re-captured and echoed back around the mesh.
type clusterEdge struct {
	cluster *config.Cluster
	srcNode string // key into Config.Nodes
	dstNode string // key into Config.Nodes
}

// name is the edge's stable job identity, used for the ownership lock and the stored
// sync definition. It is unique per (cluster, src, dst).
func (e clusterEdge) name() string {
	return fmt.Sprintf("%s::%s->%s", e.cluster.Name, e.srcNode, e.dstNode)
}

// clusterEdges expands every cluster into its directed mesh edges. v1 supports only
// the full-mesh topology (validated by the config layer), so this is every ordered
// pair of distinct members. The order is deterministic (member declaration order) so
// ownership and logs are stable across restarts.
func clusterEdges(clusters []*config.Cluster) []clusterEdge {
	var edges []clusterEdge
	for _, cl := range clusters {
		for i, src := range cl.Members {
			for j, dst := range cl.Members {
				if i == j {
					continue
				}
				edges = append(edges, clusterEdge{cluster: cl, srcNode: src, dstNode: dst})
			}
		}
	}
	return edges
}

// runClusterEdge persists the edge's sync definition, then brings it up and streams
// it, exactly like a one-way sync target but in cluster mode. A build/bringup failure
// returns an error that cancels the daemon's errgroup (same contract as runSync).
func (d *Daemon) runClusterEdge(ctx context.Context, e clusterEdge) error {
	if err := d.store.PutSync(ctx, state.SyncDef{
		Name:    e.name(),
		Source:  e.srcNode,
		Targets: []engine.TargetID{engine.TargetID(e.dstNode)},
	}); err != nil {
		return fmt.Errorf("daemon: persist cluster edge %q: %w", e.name(), err)
	}

	syncer, cleanup, err := d.buildClusterEdge(ctx, e)
	if err != nil {
		return fmt.Errorf("daemon: build cluster edge %q: %w", e.name(), err)
	}
	defer cleanup()
	if err := syncer.Bringup(ctx); err != nil {
		return fmt.Errorf("daemon: bringup cluster edge %q: %w", e.name(), err)
	}
	d.beat.Register(healthKey(e.name(), e.dstNode))
	d.log.Info("cluster edge streaming",
		slog.String("cluster", e.cluster.Name),
		slog.String("src", e.srcNode), slog.String("dst", e.dstNode))
	return syncer.Stream(ctx)
}
