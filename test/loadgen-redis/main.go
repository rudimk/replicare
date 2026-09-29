// Command loadgen-redis is a manual, idempotent Redis load + verification harness
// for exercising replicare's Redis engine against realistic, broad, multi-type
// data. It is the Redis sibling of test/loadgen (the Postgres harness) and mirrors
// its ergonomics.
//
// It is NOT part of the shipped binary -- run it from a checkout:
//
//	go run ./test/loadgen-redis run    --dsn "$SOURCE"                 # seed if empty, else churn
//	go run ./test/loadgen-redis verify --source "$SOURCE" --target "$TARGET" --wait 60s
//	go run ./test/loadgen-redis reset  --dsn "$SOURCE" --yes           # DEL every lg:*/lgskip:* key
//
// Typical loop: `run` against the source to seed, start replicare source->target,
// then `run` again to generate change (inserts, value mutations, RENAMEs, DELs,
// TTLs) and `verify` that the target converges. See test/loadgen-redis/README.md.
//
// Unlike the Postgres harness there is no `ddl` step: Redis has no schema, and
// replicare RESTOREs keys straight into the target keyspace, which need not
// pre-exist.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

type logf func(format string, args ...any)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	log := logf(func(format string, args ...any) { fmt.Printf(format+"\n", args...) })

	ctx := context.Background()
	var err error
	switch os.Args[1] {
	case "run":
		err = cmdRun(ctx, os.Args[2:], log)
	case "verify":
		err = cmdVerify(ctx, os.Args[2:], log)
	case "reset":
		err = cmdReset(ctx, os.Args[2:], log)
	case "-h", "--help", "help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "loadgen-redis: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `loadgen-redis -- Redis load + verification harness for replicare

Subcommands:
  run     Seed an empty keyspace, or churn an already-seeded one
  verify  Compare source vs target keyspace (key set + per-key content + TTL presence)
  reset   DEL every lg:* and lgskip:* key (requires --yes)

Run 'loadgen-redis <subcommand> -h' for flags.

Connection specs (--dsn / --source / --target):
  standalone: a redis:// URL, e.g. redis://:pw@localhost:6390/0 (empty = redis://localhost:6379/0)
  cluster:    pass --cluster and a comma-separated seed list, e.g.
              --dsn "node1:6379,node2:6379,node3:6379" (--password for AUTH)

Keys are laid out as lg:{<bucket>}:<type>:<n>. The {bucket} hash tag is literal
bytes on a standalone server and spreads keys across slots/shards on a cluster
(while keeping a RENAME's old+new key in one slot). lgskip:* keys are written to
the source only and must be excluded by replicare's selection (include: ["lg:*"]).
`)
}

// rdb wraps a standalone or cluster client. uc routes every single-key command;
// cluster is non-nil only in cluster mode, for the per-master SCAN fan-out that a
// full-keyspace sweep (seed-check, verify, reset) needs.
type rdb struct {
	uc      goredis.UniversalClient
	cluster *goredis.ClusterClient
	// node is this writer's index for active-active (0 = single-writer / active-passive).
	// It namespaces the keys this client generates so several nodes write disjoint
	// slices of the keyspace and the mesh converges to their union. See schema.go.
	node int
}

// openRDB builds a client from a connection spec. With cluster=false, spec is a
// redis:// URL (empty -> redis://localhost:6379/0). With cluster=true, spec is a
// comma-separated host:port seed list (an optional redis:// prefix is stripped),
// and password applies AUTH. node is the writer index for active-active (0 for the
// default single-writer behaviour).
func openRDB(ctx context.Context, spec string, cluster bool, password string, node int) (*rdb, error) {
	if cluster {
		addrs := parseAddrs(spec)
		if len(addrs) == 0 {
			return nil, fmt.Errorf("cluster mode needs a seed list (--dsn host:port,host:port)")
		}
		cc := goredis.NewClusterClient(&goredis.ClusterOptions{Addrs: addrs, Password: password})
		r := &rdb{uc: cc, cluster: cc, node: node}
		if err := r.uc.Ping(ctx).Err(); err != nil {
			_ = cc.Close()
			return nil, fmt.Errorf("connect cluster %v: %w", addrs, err)
		}
		return r, nil
	}
	if spec == "" {
		spec = "redis://localhost:6379/0"
	}
	opt, err := goredis.ParseURL(spec)
	if err != nil {
		return nil, fmt.Errorf("parse dsn %q: %w", spec, err)
	}
	if password != "" {
		opt.Password = password
	}
	cl := goredis.NewClient(opt)
	if err := cl.Ping(ctx).Err(); err != nil {
		_ = cl.Close()
		return nil, fmt.Errorf("connect %q: %w", spec, err)
	}
	return &rdb{uc: cl, node: node}, nil
}

// parseAddrs turns "redis://a:1,b:2" or "a:1, b:2" into ["a:1","b:2"].
func parseAddrs(spec string) []string {
	spec = strings.TrimPrefix(spec, "redis://")
	spec = strings.TrimPrefix(spec, "rediss://")
	var out []string
	for _, a := range strings.Split(spec, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}

func (r *rdb) close() { _ = r.uc.Close() }

// forEachMaster runs fn against every shard master's command surface (each master
// in cluster mode, concurrently via ForEachMaster; the single client otherwise).
// It is how a full-keyspace SCAN reaches every key regardless of topology. fn must
// be safe for concurrent use.
func (r *rdb) forEachMaster(ctx context.Context, fn func(ctx context.Context, c goredis.Cmdable) error) error {
	if r.cluster != nil {
		return r.cluster.ForEachMaster(ctx, func(ctx context.Context, c *goredis.Client) error {
			return fn(ctx, c)
		})
	}
	return fn(ctx, r.uc)
}

// connFlags is the shared connection flag set for run/verify/reset.
type connFlags struct {
	cluster  bool
	password string
}

func addConnFlags(fs *flag.FlagSet) *connFlags {
	c := &connFlags{}
	fs.BoolVar(&c.cluster, "cluster", false, "connect in cluster mode (--dsn/--source/--target is a comma-separated seed list)")
	fs.StringVar(&c.password, "password", "", "AUTH password (cluster mode; standalone can put it in the redis:// URL)")
	return c
}

func cmdRun(ctx context.Context, args []string, log logf) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	dsn := fs.String("dsn", "", "source connection spec (see -h)")
	scaleF := fs.Float64("scale", 1.0, "multiply default key counts (e.g. 0.1 for a quick run, 2 for ~2x)")
	ops := fs.Int("ops", 200, "number of churn statements when the keyspace is already seeded")
	seedVal := fs.Int64("seed", 1, "RNG seed for reproducible op selection and generated values")
	nodeID := fs.Int("node-id", 0, "ACTIVE-ACTIVE: this writer node's index (0 = the default single-writer / active-passive behaviour). Each node writes a disjoint key namespace (node 0: lg:{...}; node k>0: lg:nk:{...}), all under lg:* so replicare replicates them, so several nodes can seed/churn a mesh concurrently and converge to the union. Run the SAME node-id for every `run` against a given node.")
	cf := addConnFlags(fs)
	_ = fs.Parse(args)

	if *nodeID < 0 {
		return fmt.Errorf("--node-id must be >= 0")
	}

	r, err := openRDB(ctx, *dsn, cf.cluster, cf.password, *nodeID)
	if err != nil {
		return err
	}
	defer r.close()

	// Seeded iff THIS node's own key slice is populated — checking all of lg:* would
	// wrongly report "seeded" once a peer's keys have replicated in.
	seeded, err := r.anyKey(ctx, r.ownDataPattern())
	if err != nil {
		return fmt.Errorf("check seeded: %w", err)
	}
	rng := rand.New(rand.NewSource(*seedVal))

	if !seeded {
		sc := defaultScale().mul(*scaleF)
		if *nodeID == 0 {
			log("empty source detected -> seeding (~%d keys, scale %.2f, seed %d)", sc.total(), *scaleF, *seedVal)
		} else {
			log("empty node %d slice detected -> seeding (~%d keys, scale %.2f, seed %d, namespace lg:n%d:)", *nodeID, sc.total(), *scaleF, *seedVal, *nodeID)
		}
		start := time.Now()
		if err := seed(ctx, r, sc, rng, log); err != nil {
			return err
		}
		log("seed complete in %s", time.Since(start).Round(time.Millisecond))
		return nil
	}

	log("populated node %d detected -> churning %d ops (seed %d)", *nodeID, *ops, *seedVal)
	start := time.Now()
	stats, err := churn(ctx, r, *ops, rng, log)
	if err != nil {
		return err
	}
	log("churn complete in %s:", time.Since(start).Round(time.Millisecond))
	for _, line := range summaryLines(stats) {
		log("%s", line)
	}
	return nil
}

func cmdVerify(ctx context.Context, args []string, log logf) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	src := fs.String("source", "", "source connection spec")
	dst := fs.String("target", "", "target connection spec")
	wait := fs.Duration("wait", 0, "keep re-checking until converged or this timeout elapses (0 = single pass)")
	interval := fs.Duration("interval", 2*time.Second, "poll interval when --wait is set")
	nodeID := fs.Int("node-id", 0, "ACTIVE-ACTIVE: the SOURCE's node index (0 for single-writer). Scopes the skip-leak check to the source's own lgskip:n<node>:* namespace, since in a mesh every node has its own source-only lgskip keys. Data (lg:*) is compared across the whole keyspace either way (a converged mesh holds the union on every node).")
	cf := addConnFlags(fs)
	_ = fs.Parse(args)

	if *dst == "" {
		return fmt.Errorf("--target is required")
	}
	if *nodeID < 0 {
		return fmt.Errorf("--node-id must be >= 0")
	}
	srcR, err := openRDB(ctx, *src, cf.cluster, cf.password, *nodeID)
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}
	defer srcR.close()
	dstR, err := openRDB(ctx, *dst, cf.cluster, cf.password, 0)
	if err != nil {
		return fmt.Errorf("target: %w", err)
	}
	defer dstR.close()

	deadline := time.Now().Add(*wait)
	pass := 0
	for {
		pass++
		res, err := verifyOnce(ctx, srcR, dstR, log)
		if err != nil {
			return err
		}
		if res.converged() {
			res.report(log)
			log("CONVERGED: %d replicated keys match, no skip leak", res.srcKeys)
			return nil
		}
		if *wait == 0 || time.Now().After(deadline) {
			res.report(log)
			return fmt.Errorf("NOT CONVERGED: %d missing, %d extra, %d content-drift, %d skip-leak (target may still be catching up)",
				len(res.missing), len(res.extra), len(res.mismatch), res.skipLeak)
		}
		// Not converged yet and still within --wait: log this pass so the loop
		// visibly ticks instead of looking hung, then back off.
		log("pass %d: %d src keys, %d missing, %d extra, %d drift, %d skip-leak — retrying in %s",
			pass, res.srcKeys, len(res.missing), len(res.extra), len(res.mismatch), res.skipLeak, *interval)
		time.Sleep(*interval)
	}
}

func cmdReset(ctx context.Context, args []string, log logf) error {
	fs := flag.NewFlagSet("reset", flag.ExitOnError)
	dsn := fs.String("dsn", "", "connection spec to clear lg:*/lgskip:* keys from")
	yes := fs.Bool("yes", false, "required: confirm DEL of every lg:* and lgskip:* key")
	cf := addConnFlags(fs)
	_ = fs.Parse(args)

	if !*yes {
		return fmt.Errorf("refusing to delete without --yes (this DELs every lg:* and lgskip:* key)")
	}
	// reset clears the whole harness keyspace (all nodes' lg:*/lgskip:*), so node 0.
	r, err := openRDB(ctx, *dsn, cf.cluster, cf.password, 0)
	if err != nil {
		return err
	}
	defer r.close()

	var deleted int64
	for _, pat := range []string{keyPrefix + "*", skipPrefix + "*"} {
		n, err := deleteMatching(ctx, r, pat)
		if err != nil {
			return err
		}
		deleted += n
	}
	deleted += r.uc.Del(ctx, metaKey).Val() // the churn count marker (lgmeta:*)
	log("deleted %d keys (lg:* + lgskip:* + meta)", deleted)
	return nil
}
