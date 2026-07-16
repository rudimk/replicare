package postgres

import (
	"strings"
	"testing"
)

func contains(t *testing.T, s, sub string) {
	t.Helper()
	if !strings.Contains(s, sub) {
		t.Errorf("expected SQL to contain %q\n---\n%s", sub, s)
	}
}

func TestDeltaColumns(t *testing.T) {
	got := deltaColumns(3)
	if len(got) != 3 || got[0] != "k1" || got[2] != "k3" {
		t.Errorf("deltaColumns(3) = %v", got)
	}
}

func TestObjectNames(t *testing.T) {
	if deltaTableName(5) != "delta_5" || trackTableName(5) != "track_5" ||
		triggerFnName(5) != "tf_5" || triggerName(5) != "replicare_trg_5" {
		t.Error("unexpected object name")
	}
	if qualifiedCapture("delta_5") != `"replicare"."delta_5"` {
		t.Errorf("qualifiedCapture = %q", qualifiedCapture("delta_5"))
	}
}

func TestDeltaTableDDL(t *testing.T) {
	sql := deltaTableDDL(7, []captureCol{{"id", "integer"}, {"sku", "text"}})
	contains(t, sql, `"replicare"."delta_7"`)
	contains(t, sql, "delta_id bigserial PRIMARY KEY")
	contains(t, sql, "rc_op char(1) NOT NULL")
	contains(t, sql, "k1 integer")
	contains(t, sql, "k2 text")
	contains(t, sql, "rc_txid bigint NOT NULL DEFAULT txid_current()")
	contains(t, sql, "nextval('replicare.delta_seq')")
	contains(t, sql, "rc_at timestamptz NOT NULL DEFAULT clock_timestamp()")
}

func TestTrackTableDDL(t *testing.T) {
	sql := trackTableDDL(7)
	contains(t, sql, `"replicare"."track_7"`)
	contains(t, sql, "PRIMARY KEY (target, delta_id)")
}

func TestAutovacuumDDL(t *testing.T) {
	sql := deltaAutovacuumDDL(7)
	contains(t, sql, `"replicare"."delta_7"`)
	contains(t, sql, "autovacuum_vacuum_scale_factor = 0")
	contains(t, sql, "autovacuum_vacuum_cost_delay = 0")
}

func TestTriggerFunctionDDLSinglePK(t *testing.T) {
	sql := triggerFunctionDDL(7, []captureCol{{"id", "integer"}})
	contains(t, sql, `CREATE OR REPLACE FUNCTION "replicare"."tf_7"() RETURNS trigger`)
	contains(t, sql, "SECURITY DEFINER SET search_path = pg_catalog")
	// INSERT / UPDATE / DELETE branches.
	contains(t, sql, "IF TG_OP = 'INSERT' THEN")
	contains(t, sql, "ELSIF TG_OP = 'UPDATE' THEN")
	contains(t, sql, "ELSIF TG_OP = 'DELETE' THEN")
	// Insert into the delta table with the op and the key.
	contains(t, sql, `INSERT INTO "replicare"."delta_7" (rc_op, k1) VALUES ('I', NEW."id");`)
	// PK-change: dual enqueue (old as delete, new as upsert).
	contains(t, sql, `NEW."id" IS DISTINCT FROM OLD."id"`)
	contains(t, sql, `VALUES ('D', OLD."id");`)
	contains(t, sql, `VALUES ('U', NEW."id");`)
	// DELETE enqueues the old key.
	contains(t, sql, "RETURN NULL;")
}

func TestTriggerFunctionDDLCompositePK(t *testing.T) {
	sql := triggerFunctionDDL(9, []captureCol{{"order_id", "bigint"}, {"sku", "text"}})
	// Both key columns in the insert list and values.
	contains(t, sql, `(rc_op, k1, k2)`)
	contains(t, sql, `NEW."order_id", NEW."sku"`)
	contains(t, sql, `OLD."order_id", OLD."sku"`)
	// PK-change condition covers both columns joined by OR.
	contains(t, sql, `NEW."order_id" IS DISTINCT FROM OLD."order_id" OR NEW."sku" IS DISTINCT FROM OLD."sku"`)
}

func TestCreateAndDropTriggerDDL(t *testing.T) {
	create := createTriggerDDL(7, `"public"."orders"`)
	contains(t, create, `CREATE TRIGGER "replicare_trg_7"`)
	contains(t, create, `AFTER INSERT OR UPDATE OR DELETE ON "public"."orders"`)
	// EXECUTE PROCEDURE (not EXECUTE FUNCTION) for old-server portability.
	contains(t, create, `EXECUTE PROCEDURE "replicare"."tf_7"()`)

	drop := dropTriggerDDL(7, `"public"."orders"`)
	contains(t, drop, `DROP TRIGGER IF EXISTS "replicare_trg_7" ON "public"."orders"`)
}

func TestTriggerFunctionQuotesReservedColumn(t *testing.T) {
	// A column named like a keyword must be quoted in NEW/OLD references.
	sql := triggerFunctionDDL(1, []captureCol{{"select", "integer"}})
	contains(t, sql, `NEW."select"`)
}
