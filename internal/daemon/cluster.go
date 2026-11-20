package daemon

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/reseed"
	"github.com/rudimk/replicare/internal/state"
)

// clusterEdge is one directed replication edge of an active-active cluster: changes
// captured on srcNode are applied to dstNode. A full mesh over N members is the set
// of all ordered pairs (srcNode != dstNode), so every member's writes reach every
// other member directly (.sisyphus/multi-master-design.md §5.2). Each edge runs as an
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

// consumers is every reader of this edge's source-node capture: one consumer per
// peer of the source node (each outgoing edge src→peer shares src's delta tables),
// keyed by that edge's job name, where its cursors live. Derived from config, not
// from which edges this daemon owns, so the purge also waits on edges another daemon
// runs. Tombstone GC relies on this too: it treats "the delete delta is gone" as
// "every peer applied the delete".
func (e clusterEdge) consumers() []reseed.Consumer {
	var out []reseed.Consumer
	for _, peer := range e.cluster.Members {
		if peer == e.srcNode {
			continue
		}
		peerEdge := clusterEdge{cluster: e.cluster, srcNode: e.srcNode, dstNode: peer}
		out = append(out, reseed.Consumer{Sync: peerEdge.name(), Target: engine.TargetID(peer)})
	}
	return out
}

// runClusterEdge persists the edge's sync definition, then brings it up and streams
// it, exactly like a one-way sync target but in cluster mode. A build/bringup failure
// returns an error that cancels the daemon's errgroup (same contract as runSync).
func (d *Daemon) runClusterEdge(ctx context.Context, e clusterEdge) error {
	// Persist this edge's definition AND those of its sibling edges (every edge out of
	// the same source node). A sibling's retention pass may flag this edge's target
	// needs-reseed, and vice versa; cursor rows reference their sync definition, so the
	// flag can only be written once that definition exists — even for an edge another
	// daemon owns or that has not started yet. Each is an idempotent upsert from config.
	consumers := e.consumers()
	for _, c := range consumers {
		if err := d.store.PutSync(ctx, state.SyncDef{
			Name:    c.Sync,
			Source:  e.srcNode,
			Targets: []engine.TargetID{c.Target},
		}); err != nil {
			return fmt.Errorf("daemon: persist cluster edge %q: %w", c.Sync, err)
		}
	}

	syncer, cleanup, err := d.buildClusterEdge(ctx, e)
	if err != nil {
		return fmt.Errorf("daemon: build cluster edge %q: %w", e.name(), err)
	}
	defer cleanup()
	syncer.Consumers = consumers
	if err := syncer.Bringup(ctx); err != nil {
		return fmt.Errorf("daemon: bringup cluster edge %q: %w", e.name(), err)
	}
	d.beat.Register(healthKey(e.name(), e.dstNode))
	d.log.Info("cluster edge streaming",
		slog.String("cluster", e.cluster.Name),
		slog.String("src", e.srcNode), slog.String("dst", e.dstNode))
	return syncer.Stream(ctx)
}
