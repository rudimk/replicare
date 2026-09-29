// Command loadgen-mysql is a manual, idempotent MySQL load + verification harness
// for exercising replicare's MySQL engine against realistic, broad, high-volume
// data. It is the MySQL sibling of test/loadgen (Postgres) and mirrors its
// ergonomics and schema, adapted to the MySQL dialect and to MySQL 5.7 as the
// oldest supported source (no generate_series, no CTEs, no window functions).
//
// It is NOT part of the shipped binary -- run it from a checkout:
//
//	go run ./test/loadgen-mysql ddl    --dsn "$TARGET"          # create the schema on the target
//	go run ./test/loadgen-mysql run    --dsn "$SOURCE"          # seed if empty, else churn
//	go run ./test/loadgen-mysql verify --source "$SOURCE" --target "$TARGET" --wait 60s
//	go run ./test/loadgen-mysql reset  --dsn "$SOURCE" --yes    # DROP DATABASE loadgen
//
// Typical loop: `ddl` the target once, `run` against the source to seed, start
// replicare source->target, then `run` again at random intervals to generate
// change and `verify` that the target converges. See test/loadgen-mysql/README.md.
//
// All harness tables live in a dedicated `loadgen` database (created by ddl/run) so
// replicare can select them wholesale (include: ["loadgen.*"]) and the harness never
// collides with anything else on the server. Every statement is schema-qualified
// (loadgen.<table>), so the --dsn's default database is irrelevant.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/go-sql-driver/mysql"
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
	case "ddl":
		err = cmdDDL(ctx, os.Args[2:], log)
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
		fmt.Fprintf(os.Stderr, "loadgen-mysql: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `loadgen-mysql -- MySQL load + verification harness for replicare

Subcommands:
  run     Ensure schema, then seed-if-empty or churn an already-seeded source
  ddl     Apply the schema only (use to pre-create the tables on the target)
  verify  Compare source vs target per-table (counts + content checksums)
  reset   DROP DATABASE loadgen (requires --yes)

Run 'loadgen-mysql <subcommand> -h' for flags. --dsn is a go-sql-driver DSN, e.g.
  root:replicare@tcp(127.0.0.1:3340)/     (source, harness-mysql)
  root:replicare@tcp(127.0.0.1:3341)/     (target, harness-mysql)
All tables live in a dedicated `+"`loadgen`"+` database, so the DSN's default database
does not matter (and need not exist).
`)
}

// connect opens a single-session *sql.DB (MaxOpenConns=1 so session variables and the
// seeded RAND() sequence persist across statements). It pins time_zone='+00:00' via a
// DSN param so every (re)connection is deterministic UTC, matching the session
// canonicalization replicare's own MySQL transport applies (internal/engine/mysql).
func connect(dsn string) (*sql.DB, error) {
	if dsn == "" {
		return nil, fmt.Errorf("--dsn is required (e.g. root:replicare@tcp(127.0.0.1:3340)/)")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if cfg.Params == nil {
		cfg.Params = map[string]string{}
	}
	// Deterministic UTC rendering of NOW()/DATETIME, applied on every connection.
	cfg.Params["time_zone"] = "'+00:00'"
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	// One session for the whole run: session vars and the seeded RAND() state are
	// per-connection, so a pool would scatter them.
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(0)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect: %w", err)
	}
	return db, nil
}

// applyDDL creates the loadgen database and its tables idempotently. With cyclic=true
// it also adds the optional nullable FK cycles (see cyclicFKs / applyCyclic).
func applyDDL(ctx context.Context, db *sql.DB, cyclic bool, log logf) error {
	for _, stmt := range ddlStatements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ddl: %w", err)
		}
	}
	if cyclic {
		if err := applyCyclic(ctx, db); err != nil {
			return err
		}
	}
	log("database loadgen ensured (%d statements%s)", len(ddlStatements), cyclicSuffix(cyclic))
	return nil
}

func cyclicSuffix(cyclic bool) string {
	if cyclic {
		return ", incl. FK cycles"
	}
	return ""
}

// setSeed makes server-side RAND() reproducible for this session: RAND(seed) seeds the
// generator, and subsequent bare RAND() calls continue that deterministic sequence on
// the same (single) connection. This is the MySQL analog of Postgres setseed(); it is
// best-effort (MySQL does not guarantee cross-version RAND() identity), which is why
// op SELECTION uses the client rng instead (see churn).
func setSeed(ctx context.Context, db *sql.DB, seed int64) error {
	// Map into a stable non-negative seed value.
	s := seed % 2_000_000_000
	if s < 0 {
		s = -s
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("SELECT RAND(%d)", s)); err != nil {
		return fmt.Errorf("seed rand: %w", err)
	}
	return nil
}

func cmdRun(ctx context.Context, args []string, log logf) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	dsn := fs.String("dsn", "", "source DSN (go-sql-driver, e.g. root:pw@tcp(127.0.0.1:3340)/)")
	scaleF := fs.Float64("scale", 1.0, "multiply default row counts (e.g. 0.1 for a quick run, 2 for ~2M events)")
	ops := fs.Int("ops", 200, "number of churn statements when the DB is already seeded (per burst under --duration)")
	seedVal := fs.Int64("seed", 1, "RNG seed (server RAND + client op selection) for reproducibility")
	cyclic := fs.Bool("cyclic", false, "also add the optional FK cycles (exercises the cyclic-copy null-then-fill path; streaming under churn is limited for non-DEFERRABLE FKs)")
	duration := fs.Duration("duration", 0, "run churn CONTINUOUSLY for this long (repeated --ops bursts). Run this WHILE replicare does its initial copy to exercise the live-source skew path — new parent rows (tenants/users) inserted mid-copy are referenced by children copied from a later snapshot (transient FK).")
	nodeID := fs.Int("node-id", 0, "ACTIVE-ACTIVE: this writer node's index (0 = the default single-writer / active-passive behaviour). Each node claims a disjoint key slice [node-id*1e9+1, +1e9), so several nodes can seed/churn concurrently against a mesh and converge to the clean union. Run the SAME node-id for every `run` against a given node's DB.")
	_ = fs.Parse(args)

	if *nodeID < 0 {
		return fmt.Errorf("--node-id must be >= 0")
	}
	base := baseFor(*nodeID)

	db, err := connect(*dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	if err := applyDDL(ctx, db, *cyclic, log); err != nil {
		return err
	}
	if err := setSeed(ctx, db, *seedVal); err != nil {
		return err
	}

	// This node is seeded iff ITS slice holds tenants — checking the whole table would
	// wrongly report "seeded" once a peer's rows have replicated in.
	var seeded int64
	if err := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT COUNT(*) FROM loadgen.tenants WHERE %s", partition("id", base))).Scan(&seeded); err != nil {
		return fmt.Errorf("check seeded: %w", err)
	}

	if seeded == 0 {
		sc := defaultScale().mul(*scaleF)
		// A node's seeded row count must stay well inside its 1e9 slice so churn ids and
		// the next node's base never collide.
		if int64(sc.total()) >= nodeStride/2 {
			return fmt.Errorf("--scale too large for active-active partitioning (%d rows approaches the %d per-node key slice); lower --scale", sc.total(), nodeStride)
		}
		if *nodeID == 0 {
			log("empty source detected -> seeding (~%d rows, scale %.2f, seed %d)", sc.total(), *scaleF, *seedVal)
		} else {
			log("empty node %d slice detected -> seeding (~%d rows, scale %.2f, seed %d, key base %d)", *nodeID, sc.total(), *scaleF, *seedVal, base)
		}
		start := time.Now()
		if err := seed(ctx, db, sc, base, log); err != nil {
			return err
		}
		log("seed complete in %s", time.Since(start).Round(time.Millisecond))
		return nil
	}

	rng := rand.New(rand.NewSource(*seedVal))
	if *duration > 0 {
		return churnFor(ctx, db, *ops, base, *duration, rng, log)
	}

	log("populated node %d detected (%d owned tenants) -> churning %d ops (seed %d)", *nodeID, seeded, *ops, *seedVal)
	start := time.Now()
	stats, err := churn(ctx, db, *ops, base, rng, log)
	if err != nil {
		return err
	}
	log("churn complete in %s:", time.Since(start).Round(time.Millisecond))
	for _, line := range summaryLines(stats) {
		log("%s", line)
	}
	return nil
}

// churnFor runs repeated churn bursts until the duration elapses (or the context is
// cancelled), so the source keeps changing while replicare copies. Run it in parallel
// with the daemon's initial copy to exercise the live-source skew path.
func churnFor(ctx context.Context, db *sql.DB, ops int, base int64, d time.Duration, rng *rand.Rand, log logf) error {
	log("continuous churn for %s (%d ops/burst) -> run this WHILE replicare does its initial copy", d, ops)
	deadline := time.Now().Add(d)
	bursts, total := 0, map[string]churnStat{}
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			break
		}
		stats, err := churn(ctx, db, ops, base, rng, log)
		if err != nil {
			return err
		}
		bursts++
		for k, v := range stats {
			cur := total[k]
			cur.calls += v.calls
			cur.rows += v.rows
			total[k] = cur
		}
	}
	log("continuous churn done: %d bursts", bursts)
	for _, line := range summaryLines(total) {
		log("%s", line)
	}
	return nil
}

func cmdDDL(ctx context.Context, args []string, log logf) error {
	fs := flag.NewFlagSet("ddl", flag.ExitOnError)
	dsn := fs.String("dsn", "", "DSN to apply the schema to")
	cyclic := fs.Bool("cyclic", false, "also add the optional FK cycles (must match the source; see run --cyclic)")
	_ = fs.Parse(args)

	db, err := connect(*dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	return applyDDL(ctx, db, *cyclic, log)
}

func cmdVerify(ctx context.Context, args []string, log logf) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	src := fs.String("source", "", "source DSN")
	dst := fs.String("target", "", "target DSN")
	wait := fs.Duration("wait", 0, "keep re-checking until converged or this timeout elapses (0 = single pass)")
	interval := fs.Duration("interval", 2*time.Second, "poll interval when --wait is set")
	_ = fs.Parse(args)

	if *dst == "" {
		return fmt.Errorf("--target is required")
	}
	srcDB, err := connect(*src)
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}
	defer func() { _ = srcDB.Close() }()
	dstDB, err := connect(*dst)
	if err != nil {
		return fmt.Errorf("target: %w", err)
	}
	defer func() { _ = dstDB.Close() }()

	// Column render lists come from the SOURCE schema (authoritative); the target has
	// the same schema via ddl, so the same per-row rendering applies on both ends.
	cols, err := loadColumns(ctx, srcDB)
	if err != nil {
		return fmt.Errorf("introspect source columns: %w", err)
	}

	deadline := time.Now().Add(*wait)
	for {
		diffs, err := verifyOnce(ctx, srcDB, dstDB, cols)
		if err != nil {
			return err
		}
		if allConverged(diffs) {
			reportDiffs(diffs, log)
			log("CONVERGED: all %d replicated tables match", len(diffs))
			return nil
		}
		if *wait == 0 || time.Now().After(deadline) {
			drift := reportDiffs(diffs, log)
			return fmt.Errorf("NOT CONVERGED: %d/%d tables drifted (target may still be catching up)", drift, len(diffs))
		}
		time.Sleep(*interval)
	}
}

func cmdReset(ctx context.Context, args []string, log logf) error {
	fs := flag.NewFlagSet("reset", flag.ExitOnError)
	dsn := fs.String("dsn", "", "DSN to drop the loadgen database from")
	yes := fs.Bool("yes", false, "required: confirm DROP DATABASE loadgen")
	_ = fs.Parse(args)

	if !*yes {
		return fmt.Errorf("refusing to drop without --yes (this runs DROP DATABASE loadgen)")
	}
	db, err := connect(*dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if _, err := db.ExecContext(ctx, "DROP DATABASE IF EXISTS loadgen"); err != nil {
		return fmt.Errorf("drop database: %w", err)
	}
	log("dropped database loadgen")
	return nil
}
