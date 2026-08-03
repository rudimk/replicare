package daemon

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/rudimk/replicare/internal/config"
	_ "github.com/rudimk/replicare/internal/engine/redis" // register the Redis engine
)

// RM9 verifies Redis syncs run under the REAL neutral daemon and that all three
// engines coexist in one process. The daemon dispatches purely through engine.Get()
// + the neutral Source/Sink interfaces, so nothing here is Redis-specific in the
// daemon itself. Gated on REPLICARE_REDIS=1 (plus a Postgres state store + the Redis
// pair), so they SKIP in the Postgres-only CI integration job; run with
// `task test:integration:tri`.

func redisDaemon(t *testing.T) bool {
	t.Helper()
	if os.Getenv("REPLICARE_INTEGRATION") != "1" || os.Getenv("REPLICARE_REDIS") != "1" {
		t.Skip("tri-harness test; run `task test:integration:tri`")
		return false
	}
	return true
}

func rDial(t *testing.T, host, port string) *goredis.Client {
	t.Helper()
	c := goredis.NewClient(&goredis.Options{Addr: host + ":" + port})
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping redis %s:%s: %v", host, port, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// rExists returns the number of the given keys present on the client.
func rExists(t *testing.T, ctx context.Context, c *goredis.Client, keys ...string) int64 {
	t.Helper()
	n, err := c.Exists(ctx, keys...).Result()
	if err != nil {
		t.Fatalf("redis exists: %v", err)
	}
	return n
}

func rDBSize(t *testing.T, ctx context.Context, c *goredis.Client) int64 {
	t.Helper()
	n, err := c.DBSize(ctx).Result()
	if err != nil {
		return -1
	}
	return n
}

// redisDaemonConfigYAML points a Redis source (6.2) + target (7.4) at the standalone
// harness and the state store at the Postgres target (v1's only StateStore backend).
func redisDaemonConfigYAML(syncs string) string {
	return fmt.Sprintf(`
logging: { level: warn, format: text }
state_store:
  engine: postgres
  postgres: { host: %s, port: %s, database: %s, user: %s, password: %s, sslmode: disable }
sources:
  rsrc:
    engine: redis
    redis: { host: %s, port: %s }
targets:
  rdst:
    engine: redis
    redis: { host: %s, port: %s }
%s`,
		envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"), envd("RC_USER", "postgres"), envd("RC_PASSWORD", "postgres"),
		envd("RC_REDIS_SRC_HOST", "127.0.0.1"), envd("RC_REDIS_SRC_PORT", "6390"),
		envd("RC_REDIS_DST_HOST", "127.0.0.1"), envd("RC_REDIS_DST_PORT", "6391"),
		syncs)
}

// TestDaemonRedisConvergesAndStopsCleanly runs a Redis sync under the real daemon:
// the SCAN initial copy converges, live SET/DEL mutations stream through (upsert via
// reconciliation, delete via the sweep), and a cancellation stops it cleanly.
func TestDaemonRedisConvergesAndStopsCleanly(t *testing.T) {
	if !redisDaemon(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	src := rDial(t, envd("RC_REDIS_SRC_HOST", "127.0.0.1"), envd("RC_REDIS_SRC_PORT", "6390"))
	tgt := rDial(t, envd("RC_REDIS_DST_HOST", "127.0.0.1"), envd("RC_REDIS_DST_PORT", "6391"))
	if err := src.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush src: %v", err)
	}
	if err := tgt.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush tgt: %v", err)
	}
	clearPGState(t, ctx)

	// Seed the source (all keys under the selection prefix).
	for i := 0; i < 30; i++ {
		if err := src.Set(ctx, fmt.Sprintf("rc:%d", i), fmt.Sprintf("v%d", i), 0).Err(); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}

	syncBlock := `
syncs:
  - name: s1
    source: rsrc
    targets: [rdst]
    include: ["rc:*"]
    tuning: { drain_interval: 100ms }
`
	cfg, err := config.Load(writeConfig(t, redisDaemonConfigYAML(syncBlock)))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	d, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Run(runCtx) }()

	if !pollUntil(t, 40*time.Second, func() bool { return rDBSize(t, ctx, tgt) == 30 }) {
		t.Fatalf("initial copy did not converge: target has %d keys", rDBSize(t, ctx, tgt))
	}

	// Live mutations: overwrite, new key, delete.
	if err := src.Set(ctx, "rc:1", "v1-updated", 0).Err(); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := src.Set(ctx, "rc:99", "new", 0).Err(); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := src.Del(ctx, "rc:2").Err(); err != nil {
		t.Fatalf("delete: %v", err)
	}
	converged := pollUntil(t, 40*time.Second, func() bool {
		got, _ := tgt.Get(ctx, "rc:1").Result()
		return got == "v1-updated" &&
			rExists(t, ctx, tgt, "rc:99") == 1 &&
			rExists(t, ctx, tgt, "rc:2") == 0
	})
	if !converged {
		t.Fatalf("streaming did not converge on mutation (upsert+delete)")
	}

	stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("daemon Run returned %v, want nil on graceful shutdown", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("daemon did not stop within 20s of cancellation")
	}
}

// TestDaemonRedisResumesAfterRestart: a second daemon started on the same state
// store resumes from the checkpointed copy (it does not re-copy from scratch) and
// streams a mutation made while it was down — the crash/restart contract.
func TestDaemonRedisResumesAfterRestart(t *testing.T) {
	if !redisDaemon(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	src := rDial(t, envd("RC_REDIS_SRC_HOST", "127.0.0.1"), envd("RC_REDIS_SRC_PORT", "6390"))
	tgt := rDial(t, envd("RC_REDIS_DST_HOST", "127.0.0.1"), envd("RC_REDIS_DST_PORT", "6391"))
	must0(t, src.FlushDB(ctx).Err())
	must0(t, tgt.FlushDB(ctx).Err())
	clearPGState(t, ctx)
	for i := 0; i < 20; i++ {
		must0(t, src.Set(ctx, fmt.Sprintf("rc:%d", i), "v", 0).Err())
	}

	syncBlock := `
syncs:
  - name: s1
    source: rsrc
    targets: [rdst]
    include: ["rc:*"]
    tuning: { drain_interval: 100ms }
`
	cfgText := redisDaemonConfigYAML(syncBlock)

	// First daemon: bring up + converge the initial copy, then stop.
	cfg1, err := config.Load(writeConfig(t, cfgText))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	d1, err := New(cfg1, nil)
	if err != nil {
		t.Fatalf("new daemon 1: %v", err)
	}
	run1, stop1 := context.WithCancel(ctx)
	done1 := make(chan error, 1)
	go func() { done1 <- d1.Run(run1) }()
	if !pollUntil(t, 40*time.Second, func() bool { return rDBSize(t, ctx, tgt) == 20 }) {
		t.Fatalf("first daemon copy did not converge: %d keys", rDBSize(t, ctx, tgt))
	}
	stop1()
	<-done1

	// While down: a mutation the restarted daemon must pick up.
	must0(t, src.Set(ctx, "rc:50", "new", 0).Err())
	must0(t, src.Del(ctx, "rc:0").Err())

	// Second daemon on the SAME state store resumes (copy already Done → straight to
	// streaming) and converges the mutation.
	cfg2, err := config.Load(writeConfig(t, cfgText))
	if err != nil {
		t.Fatalf("load config 2: %v", err)
	}
	d2, err := New(cfg2, nil)
	if err != nil {
		t.Fatalf("new daemon 2: %v", err)
	}
	run2, stop2 := context.WithCancel(ctx)
	done2 := make(chan error, 1)
	go func() { done2 <- d2.Run(run2) }()
	if !pollUntil(t, 40*time.Second, func() bool {
		return rExists(t, ctx, tgt, "rc:50") == 1 && rExists(t, ctx, tgt, "rc:0") == 0
	}) {
		t.Fatalf("restarted daemon did not converge the while-down mutation")
	}
	stop2()
	<-done2
}

// TestDaemonTwoConcurrentRedisSyncs: one daemon runs two independent Redis syncs at
// once (distinct key prefixes), each converging its own selection.
func TestDaemonTwoConcurrentRedisSyncs(t *testing.T) {
	if !redisDaemon(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	src := rDial(t, envd("RC_REDIS_SRC_HOST", "127.0.0.1"), envd("RC_REDIS_SRC_PORT", "6390"))
	tgt := rDial(t, envd("RC_REDIS_DST_HOST", "127.0.0.1"), envd("RC_REDIS_DST_PORT", "6391"))
	if err := src.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush src: %v", err)
	}
	if err := tgt.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush tgt: %v", err)
	}
	clearPGState(t, ctx)
	for i := 0; i < 10; i++ {
		must0(t, src.Set(ctx, fmt.Sprintf("a:%d", i), "x", 0).Err())
		must0(t, src.Set(ctx, fmt.Sprintf("b:%d", i), "x", 0).Err())
	}

	syncBlock := `
syncs:
  - { name: sa, source: rsrc, targets: [rdst], include: ["a:*"], tuning: { drain_interval: 100ms } }
  - { name: sb, source: rsrc, targets: [rdst], include: ["b:*"], tuning: { drain_interval: 100ms } }
`
	cfg, err := config.Load(writeConfig(t, redisDaemonConfigYAML(syncBlock)))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	d, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Run(runCtx) }()

	if !pollUntil(t, 60*time.Second, func() bool {
		return rExists(t, ctx, tgt, "a:0", "a:9") == 2 && rExists(t, ctx, tgt, "b:0", "b:9") == 2
	}) {
		t.Fatalf("two Redis syncs did not both converge (dbsize=%d)", rDBSize(t, ctx, tgt))
	}
	stop()
	<-done
}

func must0(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
}

// TestDaemonTriEngines is the RM9 headline acceptance: a Redis sync, a MySQL sync,
// and a Postgres sync run concurrently in ONE daemon on the tri-harness, each within
// its own engine (the single-engine-per-sync rule holds), all three converging both
// the initial copy and a live mutation. Gated on REPLICARE_MYSQL too (needs the
// MySQL pair); run with `task test:integration:tri`.
func TestDaemonTriEngines(t *testing.T) {
	if !redisDaemon(t) {
		return
	}
	if os.Getenv("REPLICARE_MYSQL") != "1" {
		t.Skip("tri-engine test also needs the MySQL harness; run `task test:integration:tri`")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Second)
	defer cancel()

	// Redis side.
	rsrc := rDial(t, envd("RC_REDIS_SRC_HOST", "127.0.0.1"), envd("RC_REDIS_SRC_PORT", "6390"))
	rtgt := rDial(t, envd("RC_REDIS_DST_HOST", "127.0.0.1"), envd("RC_REDIS_DST_PORT", "6391"))
	must0(t, rsrc.FlushDB(ctx).Err())
	must0(t, rtgt.FlushDB(ctx).Err())
	for i := 0; i < 20; i++ {
		must0(t, rsrc.Set(ctx, fmt.Sprintf("rc:%d", i), "x", 0).Err())
	}

	// MySQL side (helpers from daemon_mysql_test.go).
	mysrc, mytgt := resetMySQLPair(t, ctx, "rc_my", "CREATE TABLE rc_my.t (id INT PRIMARY KEY, v VARCHAR(50)) ENGINE=InnoDB")
	myExec(t, ctx, mysrc, "INSERT INTO rc_my.t SELECT n, CONCAT('m', n) FROM "+seq(1, 20))

	// Postgres side + the shared state store.
	pgSrc := dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	defer pgSrc.Close(context.Background())
	pgTgt := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	defer pgTgt.Close(context.Background())
	pgDDL := "CREATE TABLE rc_pg.t (id int PRIMARY KEY, v text)"
	mustExec(t, ctx, pgSrc, "DROP SCHEMA IF EXISTS rc_pg CASCADE")
	mustExec(t, ctx, pgSrc, "CREATE SCHEMA rc_pg")
	mustExec(t, ctx, pgSrc, pgDDL)
	mustExec(t, ctx, pgTgt, "DROP SCHEMA IF EXISTS rc_pg CASCADE")
	mustExec(t, ctx, pgTgt, "CREATE SCHEMA rc_pg")
	mustExec(t, ctx, pgTgt, pgDDL)
	mustExec(t, ctx, pgSrc, "DROP SCHEMA IF EXISTS replicare CASCADE")
	mustExec(t, ctx, pgTgt, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	mustExec(t, ctx, pgSrc, "INSERT INTO rc_pg.t SELECT g, 'p'||g FROM generate_series(1,20) g")
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = pgSrc.Exec(bg, "DROP SCHEMA IF EXISTS rc_pg CASCADE")
		_, _ = pgSrc.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = pgTgt.Exec(bg, "DROP SCHEMA IF EXISTS rc_pg CASCADE")
		_, _ = pgTgt.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	})

	body := fmt.Sprintf(`
logging: { level: warn, format: text }
state_store:
  engine: postgres
  postgres: { host: %s, port: %s, database: %s, user: %s, password: %s, sslmode: disable }
sources:
  pgsrc:
    engine: postgres
    postgres: { host: %s, port: %s, database: %s, user: %s, password: %s, sslmode: disable }
  mysrc:
    engine: mysql
    mysql: { host: %s, port: %s, database: replicare_src, user: root, password: replicare, tls: disable, local_infile: true }
  rsrc:
    engine: redis
    redis: { host: %s, port: %s }
targets:
  pgdst:
    engine: postgres
    postgres: { host: %s, port: %s, database: %s, user: %s, password: %s, sslmode: disable }
  mydst:
    engine: mysql
    mysql: { host: %s, port: %s, database: replicare_dst, user: root, password: replicare, tls: disable, local_infile: true }
  rdst:
    engine: redis
    redis: { host: %s, port: %s }
syncs:
  - { name: pg, source: pgsrc, targets: [pgdst], include: ["rc_pg.*"], tuning: { drain_interval: 100ms } }
  - { name: my, source: mysrc, targets: [mydst], include: ["rc_my.*"], tuning: { drain_interval: 100ms } }
  - { name: rd, source: rsrc, targets: [rdst], include: ["rc:*"], tuning: { drain_interval: 100ms } }
`,
		envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"), envd("RC_USER", "postgres"), envd("RC_PASSWORD", "postgres"),
		envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"), envd("RC_USER", "postgres"), envd("RC_PASSWORD", "postgres"),
		myHost(), envd("RC_MYSQL_SRC_PORT", "3340"),
		envd("RC_REDIS_SRC_HOST", "127.0.0.1"), envd("RC_REDIS_SRC_PORT", "6390"),
		envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"), envd("RC_USER", "postgres"), envd("RC_PASSWORD", "postgres"),
		myTgtHost(), envd("RC_MYSQL_DST_PORT", "3341"),
		envd("RC_REDIS_DST_HOST", "127.0.0.1"), envd("RC_REDIS_DST_PORT", "6391"))

	cfg, err := config.Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("load tri config: %v", err)
	}
	d, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Run(runCtx) }()

	// All three engines converge concurrently in one daemon.
	if !pollUntil(t, 90*time.Second, func() bool {
		return rDBSize(t, ctx, rtgt) == 20 &&
			myCount(t, ctx, mytgt, "rc_my.t") == 20 &&
			count(t, ctx, pgTgt, "rc_pg.t") == 20
	}) {
		t.Fatalf("tri-engine syncs did not all converge: redis=%d mysql=%d postgres=%d",
			rDBSize(t, ctx, rtgt), myCount(t, ctx, mytgt, "rc_my.t"), count(t, ctx, pgTgt, "rc_pg.t"))
	}

	// A live mutation on each engine also streams through side by side.
	must0(t, rsrc.Set(ctx, "rc:99", "new", 0).Err())
	myExec(t, ctx, mysrc, "INSERT INTO rc_my.t VALUES (21, 'm21')")
	mustExec(t, ctx, pgSrc, "INSERT INTO rc_pg.t VALUES (21, 'p21')")
	if !pollUntil(t, 60*time.Second, func() bool {
		return rExists(t, ctx, rtgt, "rc:99") == 1 &&
			myCount(t, ctx, mytgt, "rc_my.t WHERE id = 21") == 1 &&
			count(t, ctx, pgTgt, "rc_pg.t WHERE id = 21") == 1
	}) {
		t.Fatalf("tri-engine streaming did not converge on all three engines")
	}

	stop()
	<-done
}
