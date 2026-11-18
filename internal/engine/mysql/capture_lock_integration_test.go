package mysql

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// MySQL twin of the Postgres capture-lock incident: trigger DDL needs an exclusive
// metadata lock on the user's table, waits behind any open transaction that touched it
// (default lock_wait_timeout is a year), and queues every new query on the table behind
// it. These hold an open application transaction on the table from a second session.

// holdOpenTxn opens a transaction on another pooled connection that writes the table and
// stays open (the "idle in transaction" app session). The returned func ends it.
func holdOpenTxn(t *testing.T, ctx context.Context, s *Source, table string) func() {
	t.Helper()
	// A separate Source: the daemon's own pool holds a single connection.
	other := &Source{cfg: srcCfg()}
	if err := other.Connect(ctx); err != nil {
		t.Fatalf("second session: %v", err)
	}
	conn, err := other.db.Conn(ctx)
	if err != nil {
		t.Fatalf("second session: %v", err)
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := tx.ExecContext(ctx, "UPDATE "+table+" SET note = note WHERE id = -1"); err != nil {
		t.Fatalf("touch table: %v", err)
	}
	done := false
	end := func() {
		if done {
			return
		}
		done = true
		_ = tx.Rollback()
		_ = conn.Close()
		_ = other.Close(context.Background())
	}
	t.Cleanup(end)
	return end
}

func TestReinstallCaptureTakesNoLockOnUserTable(t *testing.T) {
	if !integration(t) {
		return
	}
	for _, origin := range []bool{false, true} {
		name := map[bool]string{false: "one-way", true: "cluster"}[origin]
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
			defer cancel()
			s := connectSource(t, ctx)
			t.Cleanup(func() { _, _ = s.db.Exec("DROP DATABASE IF EXISTS replicare") })
			mustExec(t, ctx, s.db, "CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB")
			ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
			install := func() error { return s.InstallCapture(ctx, []engine.TableRef{ref}) }
			if origin {
				install = func() error { return s.InstallOriginCapture(ctx, []engine.TableRef{ref}, "node-a") }
			}
			if err := install(); err != nil {
				t.Fatalf("first install: %v", err)
			}

			end := holdOpenTxn(t, ctx, s, "rc_it.orders")
			start := time.Now()
			if err := install(); err != nil {
				t.Fatalf("re-install with an open transaction on the table must not need its lock: %v", err)
			}
			if d := time.Since(start); d > captureLockWaitSeconds*time.Second {
				t.Errorf("re-install took %s; it must not wait on the user table's lock", d)
			}
			end()

			mustExec(t, ctx, s.db, "INSERT INTO rc_it.orders VALUES (1, 'a')")
			if got := deltaRows(t, ctx, s, ref); len(got) != 1 || got[0] != "I|1" {
				t.Errorf("trigger stopped capturing after re-install: %v", got)
			}
		})
	}
}

func TestInstallCaptureGivesUpOnBusyTable(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s := connectSource(t, ctx)
	t.Cleanup(func() { _, _ = s.db.Exec("DROP DATABASE IF EXISTS replicare") })
	mustExec(t, ctx, s.db, "CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}

	oldAttempts, oldBackoff := captureLockAttempts, captureLockBackoff
	captureLockAttempts, captureLockBackoff = 2, func(int) time.Duration { return 10 * time.Millisecond }
	t.Cleanup(func() { captureLockAttempts, captureLockBackoff = oldAttempts, oldBackoff })

	end := holdOpenTxn(t, ctx, s, "rc_it.orders")
	start := time.Now()
	err := s.InstallCapture(ctx, []engine.TableRef{ref})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("install must not succeed while an open transaction holds the table")
	}
	if !strings.Contains(err.Error(), "could not get a lock") || !isLockWaitTimeout(err) {
		t.Errorf("want a loud lock-timeout error, got: %v", err)
	}
	if max := time.Duration(captureLockAttempts*captureLockWaitSeconds)*time.Second + 3*time.Second; elapsed > max {
		t.Errorf("gave up after %s; each attempt must wait at most %ds", elapsed, captureLockWaitSeconds)
	}

	end()
	if err := s.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install after the transaction ended: %v", err)
	}
	// The pooled connection that ran the DDL must not keep the short lock wait.
	var v int
	if err := s.db.QueryRowContext(ctx, "SELECT @@SESSION.lock_wait_timeout").Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v == captureLockWaitSeconds {
		t.Errorf("session lock_wait_timeout leaked back into the pool (%d)", v)
	}
}

func TestReinstallCaptureReplacesAChangedTrigger(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	s := connectSource(t, ctx)
	t.Cleanup(func() { _, _ = s.db.Exec("DROP DATABASE IF EXISTS replicare") })
	mustExec(t, ctx, s.db, "CREATE TABLE rc_it.orders (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := s.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install: %v", err)
	}
	relID, _, _, _ := s.lookupRegistry(ctx, ref)
	// Swap the insert trigger for one that captures nothing: the skip must notice.
	mustExec(t, ctx, s.db,
		dropTriggerDDL(relID, "rc_it", 'I'),
		"CREATE TRIGGER rc_it."+bq(triggerName(relID, 'I'))+" AFTER INSERT ON rc_it.orders FOR EACH ROW BEGIN END")

	if err := s.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("re-install: %v", err)
	}
	mustExec(t, ctx, s.db, "INSERT INTO rc_it.orders VALUES (1, 'a')")
	if got := deltaRows(t, ctx, s, ref); len(got) != 1 || got[0] != "I|1" {
		t.Errorf("changed trigger was not replaced by re-install: %v", got)
	}
}
