package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// These recreate a production incident: replicare restarted while a manual
// VACUUM held "Message"; its DROP TRIGGER waited on the VACUUM's lock, and because
// Postgres queues new lock requests behind a waiting one, every application query on
// "Message" stalled behind replicare for minutes.

// holdLock opens a second source session and holds mode on table in an open
// transaction (SHARE UPDATE EXCLUSIVE is what VACUUM holds). The returned func releases it.
func holdLock(t *testing.T, ctx context.Context, table, mode string) func() {
	t.Helper()
	other := &Source{cfg: harnessConn(t, "source")}
	if err := other.Connect(ctx); err != nil {
		t.Fatalf("connect second session: %v", err)
	}
	mustExec(t, ctx, other.conn, "BEGIN")
	mustExec(t, ctx, other.conn, "LOCK TABLE "+table+" IN "+mode+" MODE")
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		_, _ = other.conn.Exec(context.Background(), "ROLLBACK")
		_ = other.Close(context.Background())
	}
	t.Cleanup(release)
	return release
}

// TestReinstallCaptureTakesNoLockOnUserTable: the restart case. Capture is already in
// place, so a re-install must not touch the user's table at all — it completes at once
// even while a VACUUM-strength lock is held, and the trigger keeps firing.
func TestReinstallCaptureTakesNoLockOnUserTable(t *testing.T) {
	for _, origin := range []bool{false, true} {
		name := map[bool]string{false: "one-way", true: "cluster"}[origin]
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			src := captureSource(t, ctx)
			setupSourceTables(t, ctx, src, `CREATE TABLE rc_it."Message" (id int PRIMARY KEY, note text)`)
			ref := engine.TableRef{Schema: "rc_it", Name: "Message"}
			install := func() error { return src.InstallCapture(ctx, []engine.TableRef{ref}) }
			if origin {
				install = func() error { return src.InstallOriginCapture(ctx, []engine.TableRef{ref}, "node-a") }
			}
			if err := install(); err != nil {
				t.Fatalf("first install: %v", err)
			}

			release := holdLock(t, ctx, `rc_it."Message"`, "SHARE UPDATE EXCLUSIVE")
			start := time.Now()
			if err := install(); err != nil {
				t.Fatalf("re-install while the table is locked must not need the lock: %v", err)
			}
			if d := time.Since(start); d > captureLockWait {
				t.Errorf("re-install took %s; it must not wait on the user table's lock", d)
			}
			release()

			relID, _, _ := lookupRegistry(ctx, src.conn, ref)
			mustExec(t, ctx, src.conn, `INSERT INTO rc_it."Message" VALUES (1, 'x')`)
			if got := deltaCount(t, ctx, src, relID); got != 1 {
				t.Errorf("trigger stopped capturing after re-install: %d deltas, want 1", got)
			}
		})
	}
}

// TestInstallCaptureGivesUpOnBusyTable: a first install genuinely needs the lock. It
// must wait at most captureLockWait per attempt, retry, and fail loud — never park a
// lock request indefinitely in front of the application.
func TestInstallCaptureGivesUpOnBusyTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, `CREATE TABLE rc_it."Message" (id int PRIMARY KEY, note text)`)
	ref := engine.TableRef{Schema: "rc_it", Name: "Message"}

	oldAttempts, oldBackoff := captureLockAttempts, captureLockBackoff
	captureLockAttempts, captureLockBackoff = 2, func(int) time.Duration { return 10 * time.Millisecond }
	t.Cleanup(func() { captureLockAttempts, captureLockBackoff = oldAttempts, oldBackoff })

	release := holdLock(t, ctx, `rc_it."Message"`, "SHARE UPDATE EXCLUSIVE")

	start := time.Now()
	err := src.InstallCapture(ctx, []engine.TableRef{ref})
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("install must not succeed while the table lock is held")
	}
	if !strings.Contains(err.Error(), "could not get a lock") || !isLockWaitTimeout(err) {
		t.Errorf("want a loud lock-timeout error, got: %v", err)
	}
	if max := time.Duration(captureLockAttempts)*captureLockWait + 3*time.Second; elapsed > max {
		t.Errorf("gave up after %s; each attempt must wait at most %s", elapsed, captureLockWait)
	}

	// Once the lock is gone, the next install succeeds normally.
	release()
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install after the lock was released: %v", err)
	}
}

// TestReinstallCaptureRepairsAChangedTrigger: the skip must not be too lax — a capture
// trigger someone disabled is not "current" and gets re-created.
func TestReinstallCaptureRepairsAChangedTrigger(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	setupSourceTables(t, ctx, src, "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}
	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install: %v", err)
	}
	relID, _, _ := lookupRegistry(ctx, src.conn, ref)
	mustExec(t, ctx, src.conn, "ALTER TABLE rc_it.orders DISABLE TRIGGER "+quoteIdentifier(triggerName(relID)))

	if err := src.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("re-install: %v", err)
	}
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.orders VALUES (1, 'x')")
	if got := deltaCount(t, ctx, src, relID); got != 1 {
		t.Errorf("disabled trigger was not repaired by re-install: %d deltas, want 1", got)
	}
}
