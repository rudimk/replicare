package daemon

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/config"
)

// fanoutHarness sets up one source and two target databases on the harness (dst1 is
// the usual target database, which also holds the state store; dst2 a second database
// on the same server), with rc_it.orders and rc_it.items on all three and 30 orders on
// the source.
type fanoutHarness struct {
	src, tgt1, tgt2 *pgx.Conn
	dst2DB          string
}

func newFanoutHarness(t *testing.T, ctx context.Context) *fanoutHarness {
	t.Helper()
	h := &fanoutHarness{dst2DB: "replicare_dst2"}
	h.src = dial(t, ctx, envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"))
	h.tgt1 = dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"))
	mustExec(t, ctx, h.tgt1, "DROP DATABASE IF EXISTS "+h.dst2DB+" WITH (FORCE)")
	mustExec(t, ctx, h.tgt1, "CREATE DATABASE "+h.dst2DB)
	h.tgt2 = dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), h.dst2DB)
	for _, c := range []*pgx.Conn{h.src, h.tgt1, h.tgt2} {
		mustExec(t, ctx, c, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		mustExec(t, ctx, c, "CREATE SCHEMA rc_it")
		mustExec(t, ctx, c, "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)")
		mustExec(t, ctx, c, "CREATE TABLE rc_it.items (id int PRIMARY KEY, note text)")
	}
	mustExec(t, ctx, h.src, "DROP SCHEMA IF EXISTS replicare CASCADE")
	mustExec(t, ctx, h.tgt1, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
	mustExec(t, ctx, h.src, "INSERT INTO rc_it.orders SELECT g, 'v'||g FROM generate_series(1,30) g")
	t.Cleanup(func() {
		bg := context.Background()
		_, _ = h.src.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = h.src.Exec(bg, "DROP SCHEMA IF EXISTS replicare CASCADE")
		_, _ = h.tgt1.Exec(bg, "DROP SCHEMA IF EXISTS rc_it CASCADE")
		_, _ = h.tgt1.Exec(bg, "DROP SCHEMA IF EXISTS replicare_state CASCADE")
		_ = h.tgt2.Close(bg)
		_, _ = h.tgt1.Exec(bg, "DROP DATABASE IF EXISTS "+h.dst2DB+" WITH (FORCE)")
		_ = h.src.Close(bg)
		_ = h.tgt1.Close(bg)
	})
	return h
}

// config builds a config with the harness's source and both targets plus the given
// syncs block.
func (h *fanoutHarness) config(t *testing.T, syncs string) *config.Config {
	t.Helper()
	body := fmt.Sprintf(`
logging: { level: warn, format: text }
state_store:
  engine: postgres
  postgres: { host: %[1]s, port: %[2]s, database: %[3]s, user: %[4]s, password: %[5]s, sslmode: disable }
sources:
  src:
    engine: postgres
    postgres: { host: %[6]s, port: %[7]s, database: %[8]s, user: %[4]s, password: %[5]s, sslmode: disable }
targets:
  dst1:
    engine: postgres
    postgres: { host: %[1]s, port: %[2]s, database: %[3]s, user: %[4]s, password: %[5]s, sslmode: disable }
  dst2:
    engine: postgres
    postgres: { host: %[1]s, port: %[2]s, database: %[9]s, user: %[4]s, password: %[5]s, sslmode: disable }
%[10]s`,
		envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), envd("RC_DST_DB", "replicare_dst"),
		envd("RC_USER", "postgres"), envd("RC_PASSWORD", "postgres"),
		envd("RC_SRC_HOST", "127.0.0.1"), envd("RC_SRC_PORT", "5440"), envd("RC_SRC_DB", "replicare_src"),
		h.dst2DB, syncs)
	cfg, err := config.Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	return cfg
}

// run starts a daemon over cfg and returns a func that stops it and waits.
func runDaemon(t *testing.T, ctx context.Context, cfg *config.Config) (stop func()) {
	t.Helper()
	d, err := New(cfg, nil)
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Run(runCtx) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("daemon Run returned %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("daemon did not stop within 15s")
		}
	}
}

// deltaRows counts the changes still queued on the source for one table.
func deltaRows(t *testing.T, ctx context.Context, src *pgx.Conn, table string) int {
	t.Helper()
	var relID int
	if err := src.QueryRow(ctx, "SELECT rel_id FROM replicare.captured WHERE schema_name = 'rc_it' AND table_name = $1", table).Scan(&relID); err != nil {
		t.Fatalf("captured rel_id of %s: %v", table, err)
	}
	return count(t, ctx, src, fmt.Sprintf("replicare.delta_%d", relID))
}

// TestFanOutSlowTargetKeepsChanges: with one sync fanning out to two targets, a target
// held up mid-pass must still receive every change the other target took meanwhile.
// Before the fix, each target's job deleted a queued change as soon as its own target
// had it, so the held-up target silently lost them (9 of 10 rows in this test).
func TestFanOutSlowTargetKeepsChanges(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), daemonTestBudget)
	defer cancel()
	h := newFanoutHarness(t, ctx)
	cfg := h.config(t, `
syncs:
  - name: fan
    source: src
    targets: [dst1, dst2]
    include: ["rc_it.*"]
    tuning: { drain_interval: 100ms }
`)
	stop := runDaemon(t, ctx, cfg)
	defer func() {
		if stop != nil {
			stop()
		}
	}()
	if !pollUntil(t, copyTimeout, func() bool {
		return count(t, ctx, h.tgt1, "rc_it.orders") == 30 && count(t, ctx, h.tgt2, "rc_it.orders") == 30
	}) {
		t.Fatal("initial copy did not converge on both targets")
	}

	// Hold dst2 up: lock its table, so its next pass reads row 40 and then waits.
	lockConn := dial(t, ctx, envd("RC_DST_HOST", "127.0.0.1"), envd("RC_DST_PORT", "5441"), h.dst2DB)
	defer lockConn.Close(context.Background())
	tx, err := lockConn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, ctx, h.src, "INSERT INTO rc_it.orders VALUES (40, 'v40')")
	if _, err := tx.Exec(ctx, "LOCK TABLE rc_it.orders IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	// dst1 takes 41..49 while dst2 is stuck, and its passes purge what it has applied.
	mustExec(t, ctx, h.src, "INSERT INTO rc_it.orders SELECT g, 'v'||g FROM generate_series(41,49) g")
	if !pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, h.tgt1, "rc_it.orders WHERE id BETWEEN 40 AND 49") == 10
	}) {
		t.Fatal("dst1 did not get rows 40..49")
	}
	time.Sleep(2 * time.Second)
	if n := deltaRows(t, ctx, h.src, "orders"); n < 9 {
		t.Errorf("only %d changes still queued while dst2 is held up, want at least 9", n)
	}
	_ = tx.Rollback(ctx)

	if !pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, h.tgt2, "rc_it.orders WHERE id BETWEEN 40 AND 49") == 10
	}) {
		t.Fatalf("dst2 has %d of rows 40..49, want all 10", count(t, ctx, h.tgt2, "rc_it.orders WHERE id BETWEEN 40 AND 49"))
	}
	// Once both have them, the queue empties.
	if !pollUntil(t, convergeTimeout, func() bool { return deltaRows(t, ctx, h.src, "orders") == 0 }) {
		t.Errorf("changes still queued after both targets had them: %d", deltaRows(t, ctx, h.src, "orders"))
	}
}

// TestPausedSyncKeepsItsChanges: two syncs read the same source. While one is paused,
// the other keeps running and must not delete the changes the paused one has yet to
// take; once it runs again, it catches up with nothing lost.
func TestPausedSyncKeepsItsChanges(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), daemonTestBudget)
	defer cancel()
	h := newFanoutHarness(t, ctx)
	syncs := func(bEnabled bool) string {
		return fmt.Sprintf(`
syncs:
  - name: a
    source: src
    targets: [dst1]
    include: ["rc_it.*"]
    tuning: { drain_interval: 100ms }
  - name: b
    source: src
    targets: [dst2]
    include: ["rc_it.*"]
    enabled: %t
    tuning: { drain_interval: 100ms }
`, bEnabled)
	}

	stop := runDaemon(t, ctx, h.config(t, syncs(true)))
	if !pollUntil(t, copyTimeout, func() bool {
		return count(t, ctx, h.tgt1, "rc_it.orders") == 30 && count(t, ctx, h.tgt2, "rc_it.orders") == 30
	}) {
		stop()
		t.Fatal("initial copy did not converge on both targets")
	}
	stop()

	// Pause b; a keeps running and takes new changes.
	stop = runDaemon(t, ctx, h.config(t, syncs(false)))
	mustExec(t, ctx, h.src, "INSERT INTO rc_it.orders SELECT g, 'v'||g FROM generate_series(31,40) g")
	mustExec(t, ctx, h.src, "DELETE FROM rc_it.orders WHERE id = 5")
	if !pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, h.tgt1, "rc_it.orders") == 39
	}) {
		stop()
		t.Fatal("a did not stream while b was paused")
	}
	time.Sleep(2 * time.Second) // several of a's passes, each purging what it may
	stop()
	if n := deltaRows(t, ctx, h.src, "orders"); n != 11 {
		t.Errorf("changes queued for paused b = %d, want 11", n)
	}

	// Unpause b: it catches up.
	stop = runDaemon(t, ctx, h.config(t, syncs(true)))
	defer stop()
	if !pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, h.tgt2, "rc_it.orders") == 39 && count(t, ctx, h.tgt2, "rc_it.orders WHERE id = 5") == 0
	}) {
		t.Fatalf("b did not catch up after its pause: %d rows", count(t, ctx, h.tgt2, "rc_it.orders"))
	}
}

// TestNarrowSyncDoesNotHoldOtherTables: a second sync from the same source that
// replicates only some of the tables waits only for those; the queue of a table it
// does not replicate empties as soon as the other sync has its changes.
func TestNarrowSyncDoesNotHoldOtherTables(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), daemonTestBudget)
	defer cancel()
	h := newFanoutHarness(t, ctx)
	stop := runDaemon(t, ctx, h.config(t, `
syncs:
  - name: wide
    source: src
    targets: [dst1]
    include: ["rc_it.*"]
    tuning: { drain_interval: 100ms }
  - name: narrow
    source: src
    targets: [dst2]
    include: ["rc_it.orders"]
    tuning: { drain_interval: 100ms }
`))
	defer stop()
	if !pollUntil(t, copyTimeout, func() bool {
		return count(t, ctx, h.tgt1, "rc_it.orders") == 30 && count(t, ctx, h.tgt2, "rc_it.orders") == 30
	}) {
		t.Fatal("initial copy did not converge")
	}
	mustExec(t, ctx, h.src, "INSERT INTO rc_it.items SELECT g, 'i'||g FROM generate_series(1,5) g")
	mustExec(t, ctx, h.src, "INSERT INTO rc_it.orders VALUES (31, 'v31')")
	if !pollUntil(t, convergeTimeout, func() bool {
		return count(t, ctx, h.tgt1, "rc_it.items") == 5 && count(t, ctx, h.tgt2, "rc_it.orders WHERE id = 31") == 1
	}) {
		t.Fatal("changes did not stream")
	}
	if !pollUntil(t, convergeTimeout, func() bool { return deltaRows(t, ctx, h.src, "items") == 0 }) {
		t.Errorf("items changes still queued (%d): the narrow sync, which doesn't replicate items, is holding them",
			deltaRows(t, ctx, h.src, "items"))
	}
	if count(t, ctx, h.tgt2, "rc_it.items") != 0 {
		t.Error("the narrow sync copied items, which it doesn't include")
	}
}
