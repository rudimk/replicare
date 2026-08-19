// Command loadgen is a manual, idempotent Postgres load + verification harness
// for exercising replicare against realistic, broad, high-volume data.
//
// It is NOT part of the shipped binary -- run it from a checkout:
//
//	go run ./test/loadgen ddl    --dsn "$TARGET"          # create the schema on the target
//	go run ./test/loadgen run    --dsn "$SOURCE"          # seed if empty, else churn
//	go run ./test/loadgen verify --source "$SOURCE" --target "$TARGET" --wait 60s
//	go run ./test/loadgen reset  --dsn "$SOURCE" --yes    # DROP SCHEMA loadgen CASCADE
//
// Typical loop: `ddl` the target once, `run` against the source to seed, start
// replicare source->target, then `run` again at random intervals to generate
// change and `verify` that the target converges. See test/loadgen/README.md.
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
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
		fmt.Fprintf(os.Stderr, "loadgen: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `loadgen -- Postgres load + verification harness for replicare

Subcommands:
  run     Ensure schema, then seed-if-empty or churn an already-seeded source
  ddl     Apply the schema only (use to pre-create the tables on the target)
  verify  Compare source vs target per-table (counts + content checksums)
  reset   DROP SCHEMA loadgen CASCADE (requires --yes)

Run 'loadgen <subcommand> -h' for flags. --dsn accepts a libpq/pgx URL or
keyword string; if empty, standard PG* environment variables are used.
`)
}

// connect opens a single pgx connection. An empty dsn lets pgx read PG* env vars.
func connect(ctx context.Context, dsn string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return conn, nil
}

// applyDDL runs the idempotent schema statements. With cyclic=true it also adds
// the optional FK cycles (see cyclicDDL).
func applyDDL(ctx context.Context, conn *pgx.Conn, cyclic bool, log logf) error {
	stmts := ddlStatements
	if cyclic {
		stmts = append(append([]string{}, ddlStatements...), cyclicDDL...)
	}
	for _, stmt := range stmts {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("ddl: %w", err)
		}
	}
	log("schema loadgen ensured (%d statements%s)", len(stmts), cyclicSuffix(cyclic))
	return nil
}

func cyclicSuffix(cyclic bool) string {
	if cyclic {
		return ", incl. FK cycles"
	}
	return ""
}

// setSeed makes server-side random() reproducible for this session. seed maps
// into setseed()'s [-1,1] domain.
func setSeed(ctx context.Context, conn *pgx.Conn, seed int64) error {
	f := float64(seed%2_000_000)/1_000_000.0 - 1.0
	if _, err := conn.Exec(ctx, "SELECT setseed($1)", f); err != nil {
		return fmt.Errorf("setseed: %w", err)
	}
	return nil
}

func cmdRun(ctx context.Context, args []string, log logf) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	dsn := fs.String("dsn", "", "source DSN (empty = PG* env)")
	scaleF := fs.Float64("scale", 1.0, "multiply default row counts (e.g. 0.1 for a quick run, 2 for ~2M events)")
	ops := fs.Int("ops", 200, "number of churn statements when the DB is already seeded")
	seedVal := fs.Int64("seed", 1, "RNG seed (server setseed + client op selection) for reproducibility")
	cyclic := fs.Bool("cyclic", false, "also add the optional FK cycles (exercises replicare's cyclic-copy path; does not converge today)")
	_ = fs.Parse(args)

	conn, err := connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if err := applyDDL(ctx, conn, *cyclic, log); err != nil {
		return err
	}
	if err := setSeed(ctx, conn, *seedVal); err != nil {
		return err
	}

	var seeded int64
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM loadgen.tenants").Scan(&seeded); err != nil {
		return fmt.Errorf("check seeded: %w", err)
	}

	if seeded == 0 {
		sc := defaultScale().mul(*scaleF)
		log("empty source detected -> seeding (~%d rows, scale %.2f, seed %d)", sc.total(), *scaleF, *seedVal)
		start := time.Now()
		if err := seed(ctx, conn, sc, log); err != nil {
			return err
		}
		log("seed complete in %s", time.Since(start).Round(time.Millisecond))
		return nil
	}

	log("populated source detected (%d tenants) -> churning %d ops (seed %d)", seeded, *ops, *seedVal)
	rng := rand.New(rand.NewSource(*seedVal))
	start := time.Now()
	stats, err := churn(ctx, conn, *ops, rng, log)
	if err != nil {
		return err
	}
	log("churn complete in %s:", time.Since(start).Round(time.Millisecond))
	for _, line := range summaryLines(stats) {
		log("%s", line)
	}
	return nil
}

func cmdDDL(ctx context.Context, args []string, log logf) error {
	fs := flag.NewFlagSet("ddl", flag.ExitOnError)
	dsn := fs.String("dsn", "", "DSN to apply the schema to (empty = PG* env)")
	cyclic := fs.Bool("cyclic", false, "also add the optional FK cycles (must match the source; see run --cyclic)")
	_ = fs.Parse(args)

	conn, err := connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	return applyDDL(ctx, conn, *cyclic, log)
}

func cmdVerify(ctx context.Context, args []string, log logf) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	src := fs.String("source", "", "source DSN (empty = PG* env)")
	dst := fs.String("target", "", "target DSN")
	wait := fs.Duration("wait", 0, "keep re-checking until converged or this timeout elapses (0 = single pass)")
	interval := fs.Duration("interval", 2*time.Second, "poll interval when --wait is set")
	_ = fs.Parse(args)

	if *dst == "" {
		return fmt.Errorf("--target is required")
	}
	srcConn, err := connect(ctx, *src)
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}
	defer srcConn.Close(ctx)
	dstConn, err := connect(ctx, *dst)
	if err != nil {
		return fmt.Errorf("target: %w", err)
	}
	defer dstConn.Close(ctx)

	if err := pinGUCs(ctx, srcConn); err != nil {
		return err
	}
	if err := pinGUCs(ctx, dstConn); err != nil {
		return err
	}

	deadline := time.Now().Add(*wait)
	for {
		diffs, err := verifyOnce(ctx, srcConn, dstConn)
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
	dsn := fs.String("dsn", "", "DSN to drop the loadgen schema from (empty = PG* env)")
	yes := fs.Bool("yes", false, "required: confirm DROP SCHEMA loadgen CASCADE")
	_ = fs.Parse(args)

	if !*yes {
		return fmt.Errorf("refusing to drop without --yes (this runs DROP SCHEMA loadgen CASCADE)")
	}
	conn, err := connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "DROP SCHEMA IF EXISTS loadgen CASCADE"); err != nil {
		return fmt.Errorf("drop schema: %w", err)
	}
	log("dropped schema loadgen")
	return nil
}
