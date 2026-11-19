package postgres

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// Integration tests (REPLICARE_INTEGRATION=1) for a self-referential table whose self-link
// column sits in a unique index where empty values collide — the production failure:
//
//	cyclic null-fill pass 1 (public.Account): upsert public.Account: ERROR: duplicate key
//	value violates unique constraint "Account_email_parent_unique" (SQLSTATE 23505)
//
// NULLS NOT DISTINCT needs Postgres 15+, so it is only ever declared on the TARGET (the
// harness source is old); the source carries an ordinary unique, which is all the data
// needs. That is also realistic: the collision happens in the target's index.

// acctSrc*DDL are the source table: a self-referencing account tree where the same email
// appears under different parents (legal: (email, parent) is unique). The source carries
// an ordinary unique, declared either as a constraint or as a bare unique index — the
// latter matters because a bare index is invisible to pg_constraint, so detection must
// come from the indexes themselves.
var (
	acctSrcConstraintDDL = []string{`CREATE TABLE rc_it.acct (
	id int PRIMARY KEY, email text NOT NULL, parent int REFERENCES rc_it.acct(id), name text,
	CONSTRAINT acct_email_parent_unique UNIQUE (email, parent))`}
	acctSrcIndexDDL = []string{
		`CREATE TABLE rc_it.acct (id int PRIMARY KEY, email text NOT NULL, parent int REFERENCES rc_it.acct(id), name text)`,
		`CREATE UNIQUE INDEX acct_email_parent_unique ON rc_it.acct (email, parent)`,
	}
)

// acctTgtConstraintDDL / acctTgtIndexDDL are the two ways a target can carry the
// colliding unique: a declared constraint, or a bare unique index (which Postgres reports
// with the same "violates unique constraint" wording and which pg_constraint never lists).
var (
	acctTgtConstraintDDL = []string{`CREATE TABLE rc_it.acct (
	id int PRIMARY KEY, email text NOT NULL, parent int REFERENCES rc_it.acct(id), name text,
	CONSTRAINT acct_email_parent_unique UNIQUE NULLS NOT DISTINCT (email, parent))`}
	acctTgtIndexDDL = []string{
		`CREATE TABLE rc_it.acct (id int PRIMARY KEY, email text NOT NULL, parent int REFERENCES rc_it.acct(id), name text)`,
		`CREATE UNIQUE INDEX acct_email_parent_unique ON rc_it.acct (email, parent) NULLS NOT DISTINCT`,
	}
)

// seedAcct fills the source with 100 roots, 1900 children and 100 grandchildren in one
// statement. Roots 1-3 use emails e1-e3, which the children reuse under other parents, so
// emptying `parent` on the children collapses many of them onto (eN, NULL). order "DESC"
// stores children before their parents, so a copy stream emits children first (the case
// a single-statement load must survive); "ASC" stores roots first.
func seedAcct(t *testing.T, ctx context.Context, src *Source, order string) {
	t.Helper()
	mustExec(t, ctx, src.conn, `INSERT INTO rc_it.acct (id, email, parent, name)
	SELECT id, email, parent, 'n'||id FROM (
	  SELECT g AS id, CASE WHEN g <= 3 THEN 'e'||g ELSE 'r'||g END AS email, NULL::int AS parent FROM generate_series(1,100) g
	  UNION ALL SELECT g, 'e'||(g/100), (g % 100) + 1 FROM generate_series(101,2000) g
	  UNION ALL SELECT g, 'g'||g, g - 1900 FROM generate_series(2001,2100) g
	) v ORDER BY id `+order)
}

const acctDump = "SELECT id::text, email, coalesce(parent::text,'<n>'), name FROM rc_it.acct ORDER BY id"

func setupAcct(t *testing.T, ctx context.Context, srcDDL, tgtDDL []string, order string) (*Source, *Sink) {
	t.Helper()
	src := captureSource(t, ctx)
	sink := sinkTarget(t, ctx)
	setupSourceTables(t, ctx, src, srcDDL...)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	for _, s := range tgtDDL {
		mustExecTarget(t, ctx, sink, s)
	}
	seedAcct(t, ctx, src, order)
	return src, sink
}

func requireConverged(t *testing.T, ctx context.Context, src *Source, sink *Sink) {
	t.Helper()
	s, d := dumpText(t, ctx, src.conn, acctDump), dumpText(t, ctx, sink.conn, acctDump)
	if !eqLines(s, d) {
		t.Fatalf("target does not match source: %d source rows, %d target rows", len(s), len(d))
	}
}

// TestCyclicCopySelfRefNullCollidingUnique: the old two-pass load reproduces the
// production duplicate-key error, and the component copy now loads the table intact —
// for both a declared constraint and a bare unique index on the target.
func TestCyclicCopySelfRefNullCollidingUnique(t *testing.T) {
	for _, tc := range []struct {
		name   string
		srcDDL []string
		ddl    []string
	}{
		{"constraint", acctSrcConstraintDDL, acctTgtConstraintDDL},
		{"bare-index", acctSrcIndexDDL, acctTgtIndexDDL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			src, sink := setupAcct(t, ctx, tc.srcDDL, tc.ddl, "DESC")
			acct := ref("rc_it.acct")

			// Precondition: emptying `parent` really does collide on this schema.
			err := LoadCyclicNullFill(ctx, src, sink, acct)
			if err == nil || !strings.Contains(err.Error(), "23505") {
				t.Fatalf("precondition: two-pass load should fail with a duplicate key (23505), got %v", err)
			}
			mustExecTarget(t, ctx, sink, "TRUNCATE rc_it.acct")

			var copied int64
			if err := sink.CopyCyclicComponent(ctx, src, []engine.TableRef{acct}, func(_ engine.TableRef, n int64) { copied += n }); err != nil {
				t.Fatalf("CopyCyclicComponent: %v", err)
			}
			requireConverged(t, ctx, src, sink)
			// The whole-table, one-statement load reports its rows once it commits.
			if want := tgtCount(t, ctx, sink, "SELECT count(*) FROM rc_it.acct"); copied != int64(want) {
				t.Errorf("reported rows copied = %d, want %d", copied, want)
			}
		})
	}
}

// TestCyclicCopySelfRefRetryOverLeftovers is the operator's next step after the failure:
// re-run the copy over a target holding what the failed two-pass attempt left behind —
// children that landed with `parent` empty, while the roots sharing their email never
// landed. Rows are staged roots-first (ASC), so without applying the leftovers' fixes
// first, inserting root (e1, NULL) would collide with leftover child (e1, NULL).
func TestCyclicCopySelfRefRetryOverLeftovers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	src, sink := setupAcct(t, ctx, acctSrcConstraintDDL, acctTgtConstraintDDL, "ASC")
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.acct (id, email, name) VALUES (101,'e1','n101'), (201,'e2','n201'), (301,'e3','n301')")

	if err := sink.CopyCyclicComponent(ctx, src, []engine.TableRef{ref("rc_it.acct")}, nil); err != nil {
		t.Fatalf("CopyCyclicComponent over leftovers: %v", err)
	}
	requireConverged(t, ctx, src, sink)
}

// TestStreamingSelfRefKeepsLinkReal: the streaming apply must not empty the self-link
// either. A NEW child whose email matches an existing parentless row — (a, 2) next to
// root (a, NULL) — would land as (a, NULL) and collide forever if `parent` were left out.
func TestStreamingSelfRefKeepsLinkReal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sink := sinkTarget(t, ctx)
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	for _, s := range acctTgtConstraintDDL {
		mustExecTarget(t, ctx, sink, s)
	}
	mustExecTarget(t, ctx, sink, "INSERT INTO rc_it.acct VALUES (1,'a',NULL,'root'), (2,'a',1,'child')")
	acct := ref("rc_it.acct")

	cc, err := sink.CyclicCols(ctx, []engine.TableRef{acct})
	if err != nil {
		t.Fatalf("CyclicCols: %v", err)
	}
	if len(cc[acct]) != 0 {
		t.Fatalf("streaming would empty %v on acct; the self-link must be kept real", cc[acct])
	}

	tx, err := sink.BeginApply(ctx, true, []engine.TableRef{acct})
	if err != nil {
		t.Fatalf("BeginApply: %v", err)
	}
	cols := []string{"id", "email", "parent", "name"}
	if err := tx.StageUpsert(ctx, acct, cols, strings.NewReader("3\ta\t2\tgrandchild\n2\ta\t1\tchild renamed\n")); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("StageUpsert of a new child sharing an email with a parentless row: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	got := dumpText(t, ctx, sink.conn, "SELECT id::text, email, coalesce(parent::text,'<n>'), name FROM rc_it.acct ORDER BY id")
	want := []string{"1|a|<n>|root", "2|a|1|child renamed", "3|a|2|grandchild"}
	if !eqLines(got, want) {
		t.Fatalf("target after apply = %v, want %v", got, want)
	}
}

// TestIntrospectNullCollidingCols pins the detection: which unique-index shapes make a
// column null-colliding. NULLS NOT DISTINCT shapes run on the target (PG15+); the
// partial and expression shapes also run on the source, so the catalog query is proven on
// whatever (possibly very old) version the source is.
func TestIntrospectNullCollidingCols(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	sink := sinkTarget(t, ctx)
	src := captureSource(t, ctx)

	oldShapes := []string{
		// ordinary unique: emptied values never clash -> not listed
		`CREATE TABLE rc_it.plain (id int PRIMARY KEY, email text, parent int REFERENCES rc_it.plain(id), CONSTRAINT plain_u UNIQUE (email, parent))`,
		// partial unique whose filter mentions parent -> listed (all rows match once emptied)
		`CREATE TABLE rc_it.partial (id int PRIMARY KEY, email text, parent int REFERENCES rc_it.partial(id))`,
		`CREATE UNIQUE INDEX partial_u ON rc_it.partial (email) WHERE parent IS NULL`,
		// expression unique on parent -> listed (every emptied parent maps to 0)
		`CREATE TABLE rc_it.expr (id int PRIMARY KEY, email text, parent int REFERENCES rc_it.expr(id))`,
		`CREATE UNIQUE INDEX expr_u ON rc_it.expr (email, COALESCE(parent, 0))`,
	}
	want := map[string][]string{
		"rc_it.plain":   nil,
		"rc_it.partial": {"email", "parent"},
		"rc_it.expr":    {"email", "parent"},
	}

	// Source (may be an old major): the version-gated query must run and catch the
	// partial / expression shapes.
	setupSourceTables(t, ctx, src, oldShapes...)
	srcSchema, err := src.Introspect(ctx, engine.Selection{Include: []string{"rc_it.*"}})
	if err != nil {
		t.Fatalf("introspect source: %v", err)
	}
	checkNullColliding(t, "source", srcSchema, want)

	// Target (PG15+): additionally the NULLS NOT DISTINCT constraint and bare index.
	mustExecTarget(t, ctx, sink, "CREATE SCHEMA rc_it")
	for _, s := range oldShapes {
		mustExecTarget(t, ctx, sink, s)
	}
	mustExecTarget(t, ctx, sink, `CREATE TABLE rc_it.c_nnd (id int PRIMARY KEY, email text, parent int REFERENCES rc_it.c_nnd(id), CONSTRAINT c_nnd_u UNIQUE NULLS NOT DISTINCT (email, parent))`)
	mustExecTarget(t, ctx, sink, `CREATE TABLE rc_it.i_nnd (id int PRIMARY KEY, email text, parent int REFERENCES rc_it.i_nnd(id))`)
	mustExecTarget(t, ctx, sink, `CREATE UNIQUE INDEX i_nnd_u ON rc_it.i_nnd (email, parent) NULLS NOT DISTINCT`)
	want["rc_it.c_nnd"] = []string{"email", "parent"}
	want["rc_it.i_nnd"] = []string{"email", "parent"}
	tgtSchema, err := sink.Introspect(ctx, engine.Selection{Include: []string{"rc_it.*"}})
	if err != nil {
		t.Fatalf("introspect target: %v", err)
	}
	checkNullColliding(t, "target", tgtSchema, want)
}

func checkNullColliding(t *testing.T, side string, s *engine.Schema, want map[string][]string) {
	t.Helper()
	got := map[string][]string{}
	for _, tb := range s.Tables {
		got[tb.Ref.String()] = tb.NullCollidingCols
	}
	for tbl, w := range want {
		if !reflect.DeepEqual(got[tbl], w) {
			t.Errorf("%s %s NullCollidingCols = %v, want %v", side, tbl, got[tbl], w)
		}
	}
}

// TestCyclicCopySelfRefOverStaleTarget is the production failure after a pause: the target
// holds an earlier, unfinished copy, and the source has changed since. An account was
// deleted and re-created under a new id with the same (email, parent), and two accounts
// swapped emails. The stale target rows hold those unique values under other ids, which an
// upsert keyed on the id can neither match nor get past:
//
//	cyclic whole-load (public.Account): upsert: ERROR: duplicate key value violates
//	unique constraint "Account_email_parent_unique" (SQLSTATE 23505)
//
// A cyclic component's copy restarts from scratch anyway, so the copy clears the
// component's tables on the target first and loads into empty tables.
func TestCyclicCopySelfRefOverStaleTarget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	src, sink := setupAcct(t, ctx, acctSrcConstraintDDL, acctTgtConstraintDDL, "ASC")
	acct := ref("rc_it.acct")
	if err := sink.CopyCyclicComponent(ctx, src, []engine.TableRef{acct}, nil); err != nil {
		t.Fatalf("first copy: %v", err)
	}
	requireConverged(t, ctx, src, sink)

	// Overnight on the source. 250 (e2, parent 51) has no children.
	mustExec(t, ctx, src.conn, "DELETE FROM rc_it.acct WHERE id = 250")
	mustExec(t, ctx, src.conn, "INSERT INTO rc_it.acct VALUES (5000, 'e2', 51, 'recreated')")
	// 300 (e3, parent 1) and 400 (e4, parent 1) swap emails.
	mustExec(t, ctx, src.conn, "UPDATE rc_it.acct SET email = 'tmp' WHERE id = 300")
	mustExec(t, ctx, src.conn, "UPDATE rc_it.acct SET email = 'e3' WHERE id = 400")
	mustExec(t, ctx, src.conn, "UPDATE rc_it.acct SET email = 'e4' WHERE id = 300")

	var copied int64
	if err := sink.CopyCyclicComponent(ctx, src, []engine.TableRef{acct}, func(_ engine.TableRef, n int64) { copied += n }); err != nil {
		t.Fatalf("copy over a stale target: %v", err)
	}
	requireConverged(t, ctx, src, sink)
	if want := tgtCount(t, ctx, sink, "SELECT count(*) FROM rc_it.acct"); copied != int64(want) {
		t.Errorf("reported rows copied = %d, want %d", copied, want)
	}
}
