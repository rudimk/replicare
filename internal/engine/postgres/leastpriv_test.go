package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// roleConn returns a copy of the source harness ConnConfig authenticating as a
// specific role.
func roleConn(t *testing.T, user, password string) engine.ConnConfig {
	cc := harnessConn(t, "source")
	cc.User = user
	cc.Password = password
	return cc
}

// TestLeastPrivilegeCapture proves capture works under a non-superuser role with
// exactly the documented grants (CLAUDE.md §12), and that the SECURITY DEFINER
// trigger lets an unprivileged application writer — with no access to the
// replicare schema — still enqueue deltas.
func TestLeastPrivilegeCapture(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	// Superuser connection for setup/teardown.
	su, err := connect(ctx, harnessConn(t, "source"))
	if err != nil {
		t.Fatalf("connect superuser: %v", err)
	}
	defer su.Close(context.Background())

	cleanup := func(conn *pgx.Conn) {
		for _, stmt := range []string{
			"DROP SCHEMA IF EXISTS replicare CASCADE",
			"DROP SCHEMA IF EXISTS rc_it CASCADE",
			"DROP OWNED BY replicare_role",
			"DROP OWNED BY app_writer",
			"DROP ROLE IF EXISTS replicare_role",
			"DROP ROLE IF EXISTS app_writer",
		} {
			_, _ = conn.Exec(context.Background(), stmt)
		}
	}
	cleanup(su) // start clean
	t.Cleanup(func() { cleanup(su) })

	// Setup as superuser: two non-superuser roles, an app schema/table owned by
	// the app, and EXACTLY the documented least-privilege grants for the daemon
	// role. The replicare schema is pre-created and owned by the daemon role
	// (the "pre-created schema we own" option in §12), so no database-level
	// CREATE grant is needed.
	setup := []string{
		"CREATE ROLE replicare_role LOGIN PASSWORD 'rc'",
		"CREATE ROLE app_writer LOGIN PASSWORD 'aw'",
		"CREATE SCHEMA rc_it",
		"CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text)",
		// Pre-created capture schema owned by the daemon role.
		"CREATE SCHEMA replicare AUTHORIZATION replicare_role",
		// Daemon role: USAGE on the app schema, SELECT + TRIGGER on the table.
		"GRANT USAGE ON SCHEMA rc_it TO replicare_role",
		"GRANT SELECT, TRIGGER ON rc_it.orders TO replicare_role",
		// App writer: ordinary DML, and NO access to the replicare schema.
		"GRANT USAGE ON SCHEMA rc_it TO app_writer",
		"GRANT SELECT, INSERT, UPDATE, DELETE ON rc_it.orders TO app_writer",
	}
	for _, stmt := range setup {
		if _, err := su.Exec(ctx, stmt); err != nil {
			t.Fatalf("setup failed: %v\nSQL: %s", err, stmt)
		}
	}

	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}

	// Install capture AS THE DAEMON ROLE (non-superuser).
	daemon := &Source{cfg: roleConn(t, "replicare_role", "rc")}
	if err := daemon.Connect(ctx); err != nil {
		t.Fatalf("connect as replicare_role: %v", err)
	}
	defer daemon.Close(context.Background())
	if err := daemon.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("InstallCapture as least-privilege role: %v", err)
	}

	// Write AS THE APP WRITER, which has no privileges on the replicare schema.
	app, err := connect(ctx, roleConn(t, "app_writer", "aw"))
	if err != nil {
		t.Fatalf("connect as app_writer: %v", err)
	}
	defer app.Close(context.Background())
	if _, err := app.Exec(ctx, "INSERT INTO rc_it.orders VALUES (1, 'x'), (2, 'y')"); err != nil {
		t.Fatalf("app writer INSERT: %v", err)
	}
	if _, err := app.Exec(ctx, "UPDATE rc_it.orders SET note='z' WHERE id=1"); err != nil {
		t.Fatalf("app writer UPDATE: %v", err)
	}

	// The SECURITY DEFINER trigger enqueued the deltas despite the app writer
	// having no access to the replicare schema. The daemon role consumes them.
	keys, err := daemon.ReadDirtyKeys(ctx, ref, "dst", 0)
	if err != nil {
		t.Fatalf("ReadDirtyKeys as daemon role: %v", err)
	}
	if len(keys) != 3 { // 2 inserts + 1 update
		t.Fatalf("expected 3 deltas from app-writer changes, got %d", len(keys))
	}
	if err := daemon.ConfirmConsumed(ctx, ref, "dst", dirtyIDs(keys)); err != nil {
		t.Fatalf("ConfirmConsumed as daemon role: %v", err)
	}

	// Sanity: the app writer genuinely cannot touch the replicare schema.
	if _, err := app.Exec(ctx, "SELECT count(*) FROM replicare.captured"); err == nil {
		t.Error("app writer should NOT be able to read the replicare schema")
	}

	// RemoveCapture drops the trigger, which Postgres permits only to the table
	// owner (TRIGGER privilege allows CREATE TRIGGER but not DROP TRIGGER). So a
	// least-privilege daemon role can install and run capture, but uninstalling
	// the trigger requires table ownership (documented in §12). Here the app
	// owns the table, so removal runs as the owner.
	owner := &Source{cfg: harnessConn(t, "source")}
	if err := owner.Connect(ctx); err != nil {
		t.Fatalf("connect owner: %v", err)
	}
	defer owner.Close(context.Background())
	if err := owner.RemoveCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("RemoveCapture as table owner: %v", err)
	}
}
