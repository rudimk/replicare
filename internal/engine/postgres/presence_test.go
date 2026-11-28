package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// TablesWithChanges tells queued from empty change queues, in several statements when
// there are more tables than one statement checks, and answers true for a table it does
// not capture; and Purge of a table it calls empty deletes nothing (skipping it is safe).
func TestTablesWithChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	src := captureSource(t, ctx)
	var ddl []string
	var refs []engine.TableRef
	for i := 0; i < 5; i++ {
		ddl = append(ddl, fmt.Sprintf("CREATE TABLE rc_it.t%d (id int PRIMARY KEY, v int)", i))
		refs = append(refs, engine.TableRef{Schema: "rc_it", Name: fmt.Sprintf("t%d", i)})
	}
	setupSourceTables(t, ctx, src, ddl...)
	if err := src.InstallCapture(ctx, refs); err != nil {
		t.Fatal(err)
	}
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.t1 VALUES (1, 1)")
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.t4 VALUES (1, 1), (2, 2)")
	notCaptured := engine.TableRef{Schema: "rc_it", Name: "nope"}

	defer func(n int) { presencePerStatement = n }(presencePerStatement)
	presencePerStatement = 2 // 5 tables: three statements
	got, err := src.TablesWithChanges(ctx, append(append([]engine.TableRef{}, refs...), notCaptured))
	if err != nil {
		t.Fatal(err)
	}
	want := map[engine.TableRef]bool{refs[0]: false, refs[1]: true, refs[2]: false, refs[3]: false, refs[4]: true, notCaptured: true}
	for ref, w := range want {
		if got[ref] != w {
			t.Errorf("%v: has changes = %v, want %v (all: %v)", ref, got[ref], w, got)
		}
	}

	// An empty queue: Purge finds nothing to delete and no target over a cap, so the
	// pass skipping it changes nothing.
	st, err := src.Purge(ctx, refs[0], []engine.TargetID{"dst"}, engine.RetentionPolicy{MaxAgeSeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	if st.DeltasPurged != 0 || len(st.TargetsReseeded) != 0 {
		t.Fatalf("purge of an empty queue = %+v, want nothing", st)
	}
	bl, err := src.DeltaBacklog(ctx, refs[0], "dst")
	if err != nil {
		t.Fatal(err)
	}
	if bl != (engine.DeltaBacklog{}) {
		t.Fatalf("backlog of an empty queue = %+v, want zero (what the monitor uses when it skips it)", bl)
	}
}
