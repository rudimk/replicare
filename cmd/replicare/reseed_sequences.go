package main

import (
	"context"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/rudimk/replicare/internal/config"
	"github.com/rudimk/replicare/internal/engine"
)

// runReseedSequences implements:
//
//	replicare reseed-sequences <config> --sync <name> [--target <name>] [--dry-run]
//
// It advances each replicated table's identity/serial/AUTO_INCREMENT counter on a
// PROMOTED one-way (DR) target to MAX(id)+1, so after a failover cutover the node never
// re-issues an id already present in the replicated data — the counter is schema-object
// state the data-only path never carries (CLAUDE.md §7;
// `.sisyphus/sequence-reseed-plan.md`). Run it BEFORE opening the promoted node to
// application writes.
//
// PASSIVE / one-way / DR ONLY: it REFUSES a target that is an active-active cluster
// member, where the same operation is silent data loss (plan §5) — there the answer is
// globally-unique keys (UUID/ULID), not reseeding.
//
// Exit codes: 0 ok; 1 runtime error (endpoint unreachable, missing grant, engine without
// the capability); 2 usage/config, or a refused precondition (target is a mesh member).
func runReseedSequences(args []string, stdout, stderr io.Writer) int {
	var cfgPath, syncName, onlyTarget string
	dryRun := false
	for i := 0; i < len(args); i++ {
		switch a := args[i]; a {
		case "--sync":
			if i+1 >= len(args) {
				fmt.Fprintln(stderr, "reseed-sequences: --sync needs a value")
				return 2
			}
			i++
			syncName = args[i]
		case "--target":
			if i+1 >= len(args) {
				fmt.Fprintln(stderr, "reseed-sequences: --target needs a value")
				return 2
			}
			i++
			onlyTarget = args[i]
		case "--dry-run":
			dryRun = true
		default:
			if cfgPath == "" {
				cfgPath = a
			} else {
				fmt.Fprintf(stderr, "reseed-sequences: unexpected argument %q\n", a)
				return 2
			}
		}
	}
	if cfgPath == "" || syncName == "" {
		fmt.Fprintln(stderr, "usage: replicare reseed-sequences <config.yml> --sync <name> [--target <name>] [--dry-run]")
		return 2
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		fmt.Fprintf(stderr, "reseed-sequences: %v\n", err)
		return 2
	}

	var sync *config.Sync
	for _, s := range cfg.Syncs {
		if s.Name == syncName {
			sync = s
			break
		}
	}
	if sync == nil {
		fmt.Fprintf(stderr, "reseed-sequences: no one-way sync named %q — reseed-sequences is for passive/DR targets; active-active clusters are not reseedable (see .sisyphus/sequence-reseed-plan.md §5/§6, use UUID/ULID keys there)\n", syncName)
		return 2
	}

	targets := sync.Targets
	if onlyTarget != "" {
		found := false
		for _, t := range targets {
			if t == onlyTarget {
				found = true
			}
		}
		if !found {
			fmt.Fprintf(stderr, "reseed-sequences: target %q is not a target of sync %q\n", onlyTarget, syncName)
			return 2
		}
		targets = []string{onlyTarget}
	}

	members := clusterMemberIdentities(cfg)
	sel := engine.Selection{Include: sync.Include, Exclude: sync.Exclude}

	mode := "reseed"
	if dryRun {
		mode = "dry-run (no writes)"
	}
	fmt.Fprintf(stdout, "reseed-sequences: sync %q — %s\n", syncName, mode)

	rc := 0
	for _, tgtName := range targets {
		ep := cfg.Targets[tgtName]
		if ep == nil {
			fmt.Fprintf(stderr, "reseed-sequences: target %q not found in config\n", tgtName)
			rc = 2
			continue
		}
		// MESH GUARD (plan §5): refuse a target that points at the same database as any
		// active-active cluster member — reseeding a mesh node collapses every node onto
		// the same counter and silently loses rows via LWW.
		if id, ok := matchingClusterMember(ep, members); ok {
			fmt.Fprintf(stderr, "reseed-sequences: REFUSING target %q — it is an active-active cluster member (%s). Reseeding a mesh node is silent data loss (plan §5); use globally-unique keys (UUID/ULID) for active-active id allocation.\n", tgtName, id)
			rc = 2
			continue
		}
		if code := reseedOneTarget(context.Background(), ep, tgtName, sel, dryRun, stdout, stderr); code != 0 {
			rc = code
		}
	}
	return rc
}

// reseedOneTarget opens the target Sink, introspects the selection, and reseeds every
// replicated table's owned counter (or reports dry-run values). Returns a CLI exit code.
func reseedOneTarget(parent context.Context, ep *config.Endpoint, name string, sel engine.Selection, dryRun bool, stdout, stderr io.Writer) int {
	ctx, cancel := context.WithTimeout(parent, connectTimeout)
	defer cancel()

	eng, err := engine.Get(ep.Engine)
	if err != nil {
		fmt.Fprintf(stderr, "reseed-sequences: target %q: %v\n", name, err)
		return 1
	}
	sink, err := eng.NewSink(ep.Conn.ConnConfig())
	if err != nil {
		fmt.Fprintf(stderr, "reseed-sequences: target %q: %v\n", name, err)
		return 1
	}
	if err := sink.Connect(ctx); err != nil {
		fmt.Fprintf(stderr, "reseed-sequences: target %q unreachable: %v\n", name, err)
		return 1
	}
	defer func() { _ = sink.Close(context.Background()) }()

	schema, err := sink.Introspect(ctx, sel)
	if err != nil {
		fmt.Fprintf(stderr, "reseed-sequences: target %q introspect: %v\n", name, err)
		return 1
	}
	reseeder, ok := sink.(engine.SequenceReseeder)
	if !ok {
		fmt.Fprintf(stderr, "reseed-sequences: engine %q does not support sequence reseed\n", ep.Engine)
		return 1
	}

	fmt.Fprintf(stdout, "  target: %s\n", name)
	tw := tabwriter.NewWriter(stdout, 0, 2, 2, ' ', 0)
	reseeded, skipped := 0, 0
	verb := "set to"
	if dryRun {
		verb = "would set to"
	}
	for _, t := range schema.Tables {
		if !t.HasUsableKey() {
			continue // not replicated (no key) — nothing to converge, nothing to reseed
		}
		r, err := reseeder.ReseedSequences(ctx, t.Ref, dryRun)
		if err != nil {
			_ = tw.Flush()
			fmt.Fprintf(stderr, "reseed-sequences: target %q: %s: %v\n", name, t.Ref, err)
			return 1
		}
		if len(r.Columns) == 0 {
			fmt.Fprintf(tw, "    %s\t—\t(no identity/sequence; skipped)\n", t.Ref)
			skipped++
			continue
		}
		for _, c := range r.Columns {
			fmt.Fprintf(tw, "    %s.%s\tmax=%d\t%s %d\n", t.Ref, c.Column, c.Max, verb, c.SetTo)
			reseeded++
		}
	}
	_ = tw.Flush()
	fmt.Fprintf(stdout, "  %d counter(s) %s, %d table(s) skipped\n", reseeded,
		map[bool]string{true: "to set (dry-run)", false: "reseeded"}[dryRun], skipped)
	return 0
}

// connIdentity is the host/port/database tuple that decides whether two endpoints point
// at the same database — the mesh-guard comparison.
type connIdentity struct {
	host string
	port int
	db   string
}

func (c connIdentity) String() string { return fmt.Sprintf("%s:%d/%s", c.host, c.port, c.db) }

func identityOf(ep *config.Endpoint) connIdentity {
	cc := ep.Conn.ConnConfig()
	return connIdentity{host: cc.Host, port: cc.Port, db: cc.Database}
}

// clusterMemberIdentities returns the connection identity of every active-active cluster
// member node in the config (empty when there are no clusters).
func clusterMemberIdentities(cfg *config.Config) []connIdentity {
	var out []connIdentity
	for _, cl := range cfg.Clusters {
		for _, m := range cl.Members {
			if n := cfg.Nodes[m]; n != nil && n.Conn != nil {
				out = append(out, identityOf(n))
			}
		}
	}
	return out
}

// matchingClusterMember reports whether ep points at the same database as any cluster
// member — the mesh-guard check. Pure (no I/O), so it is unit-tested directly.
func matchingClusterMember(ep *config.Endpoint, members []connIdentity) (connIdentity, bool) {
	if ep == nil || ep.Conn == nil {
		return connIdentity{}, false
	}
	id := identityOf(ep)
	for _, m := range members {
		if m == id {
			return m, true
		}
	}
	return connIdentity{}, false
}
