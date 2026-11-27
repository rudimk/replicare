package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// TestRequeueKeys: requeued keys land in the table's delta queue as changes, so the next
// streaming pass re-reads each from the source, whether the row exists or not.
func TestRequeueKeys(t *testing.T) {
	if !integration(t) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	s := connectSource(t, ctx)
	t.Cleanup(func() { _, _ = s.db.Exec("DROP DATABASE IF EXISTS replicare") })
	mustExec(t, ctx, s.db, "DROP TABLE IF EXISTS rc_it.rq", "CREATE TABLE rc_it.rq (id INT PRIMARY KEY, note VARCHAR(50)) ENGINE=InnoDB")
	t.Cleanup(func() { _, _ = s.db.Exec("DROP TABLE IF EXISTS rc_it.rq") })
	ref := engine.TableRef{Schema: "rc_it", Name: "rq"}
	if err := s.InstallCapture(ctx, []engine.TableRef{ref}); err != nil {
		t.Fatalf("install capture: %v", err)
	}
	if err := s.RequeueKeys(ctx, ref, []engine.KeyValues{{"7"}, {"9"}}); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	got := deltaRows(t, ctx, s, ref)
	if len(got) != 2 || got[0] != "U|7" || got[1] != "U|9" {
		t.Fatalf("delta rows = %v, want [U|7 U|9]", got)
	}
	dk, err := s.ReadDirtyKeys(ctx, ref, "dst", 0)
	if err != nil || len(dk) != 2 {
		t.Fatalf("dirty keys = %d (%v), want 2", len(dk), err)
	}
}
