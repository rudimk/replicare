package postgres

import (
	"reflect"
	"strings"
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

// These cover the "self-link column sits in a unique index where empty values collide"
// case: NULL-then-fill must not empty such a column, because rows that differed only by
// it would then clash (the duplicate-key failure on e.g. NULLS NOT DISTINCT (email,
// parent)). Detection comes from engine.Table.NullCollidingCols (introspected from the
// unique indexes themselves, so bare indexes count, not just constraints).

// selfRefEmp is employees.manager_id -> employees.id, nullable, non-deferrable, with
// manager_id optionally listed as null-colliding.
func selfRefEmp(colliding bool) engine.Table {
	t := tblC("public.emp",
		[]engine.Column{col("id", false), col("manager_id", true)},
		fkN("emp_mgr", "public.emp", "public.emp", []string{"manager_id"}, false),
	)
	if colliding {
		t.NullCollidingCols = []string{"manager_id"}
	}
	return t
}

func TestCyclicSelfRefNullCollidingKeptReal(t *testing.T) {
	cs := classifyCyclicFKs([]engine.Table{selfRefEmp(true)})
	c, ok := findFK(cs, "emp_mgr")
	if !ok {
		t.Fatal("expected emp_mgr to be classified cyclic")
	}
	if c.Strategy != CyclicNullThenFill || !c.NullFillUnsafe || !c.SelfReference() {
		t.Fatalf("got strategy=%q unsafe=%v selfRef=%v, want null_then_fill/true/true", c.Strategy, c.NullFillUnsafe, c.SelfReference())
	}
	if !strings.Contains(c.Reason, "real value") {
		t.Errorf("reason should explain the column is kept at its real value: %q", c.Reason)
	}
	if got := keepRealColsByTable(cs); !reflect.DeepEqual(got, map[engine.TableRef][]string{ref("public.emp"): {"manager_id"}}) {
		t.Errorf("keepRealColsByTable = %v", got)
	}
	// Streaming must not empty it: the table drops out of the null-fill map entirely.
	if got := nullFillColsByTable(cs); len(got) != 0 {
		t.Errorf("nullFillColsByTable = %v, want empty (self-link kept real)", got)
	}
	if bad := unloadableNullFill(cs); len(bad) != 0 {
		t.Errorf("a self-reference is loadable (whole, one statement); got unloadable %v", bad)
	}
	if anyBlockedCyclicFK(cs) {
		t.Error("must not be blocked")
	}
}

func TestCyclicSelfRefOrdinaryUniqueStaysNullFill(t *testing.T) {
	// An ordinary (NULLS DISTINCT) unique that includes manager_id: emptied values never
	// clash there, so it is NOT null-colliding and keeps the cheaper chunked NULL-fill.
	emp := selfRefEmp(false)
	emp.UniqueKeys = []engine.Key{{Name: "emp_email_mgr", Columns: []string{"email", "manager_id"}}}
	cs := classifyCyclicFKs([]engine.Table{emp})
	c, _ := findFK(cs, "emp_mgr")
	if c.NullFillUnsafe {
		t.Fatal("ordinary unique must not mark the column unsafe")
	}
	if got := nullFillColsByTable(cs); !reflect.DeepEqual(got, map[engine.TableRef][]string{ref("public.emp"): {"manager_id"}}) {
		t.Errorf("nullFillColsByTable = %v, want manager_id still null-filled", got)
	}
}

func TestCyclicDeferrableIgnoresNullColliding(t *testing.T) {
	// A DEFERRABLE cycle loads in one deferred transaction and never empties anything,
	// so a colliding column is irrelevant there.
	emp := tblC("public.emp",
		[]engine.Column{col("id", false), col("manager_id", true)},
		fkN("emp_mgr", "public.emp", "public.emp", []string{"manager_id"}, true),
	)
	emp.NullCollidingCols = []string{"manager_id"}
	c, _ := findFK(classifyCyclicFKs([]engine.Table{emp}), "emp_mgr")
	if c.Strategy != CyclicDeferred || c.NullFillUnsafe {
		t.Fatalf("got strategy=%q unsafe=%v, want set_constraints_deferred/false", c.Strategy, c.NullFillUnsafe)
	}
}

// crossCycle builds users.primary_order_id <-> orders.user_id (both nullable).
func crossCycle(collidingUserCol bool) []engine.Table {
	users := tblC("public.users",
		[]engine.Column{col("id", false), col("primary_order_id", true)},
		fkN("users_primary_order", "public.users", "public.orders", []string{"primary_order_id"}, false),
	)
	if collidingUserCol {
		users.NullCollidingCols = []string{"primary_order_id"}
	}
	orders := tblC("public.orders",
		[]engine.Column{col("id", false), col("user_id", true)},
		fkN("orders_user", "public.orders", "public.users", []string{"user_id"}, false),
	)
	return []engine.Table{users, orders}
}

func TestCyclicCrossTableNullCollidingIsUnloadable(t *testing.T) {
	cs := classifyCyclicFKs(crossCycle(true))
	bad := unloadableNullFill(cs)
	if len(bad) != 1 || bad[0].FK.Name != "users_primary_order" {
		t.Fatalf("unloadableNullFill = %+v, want exactly users_primary_order", bad)
	}
	if bad[0].SelfReference() {
		t.Error("a cross-table edge is not a self-reference")
	}
	if !strings.Contains(bad[0].Reason, "DEFERRABLE") {
		t.Errorf("reason should point at DEFERRABLE: %q", bad[0].Reason)
	}
	if got := keepRealColsByTable(cs); len(got) != 0 {
		t.Errorf("cross-table unsafe columns are not kept real (they are blocked); got %v", got)
	}
}

func TestNullFillKeepsCrossTableLinkOfSelfRefTable(t *testing.T) {
	// accounts has a colliding self-link (parent) AND is in a loop with teams through
	// team_id. Only parent stays real; team_id (and teams.lead_account_id) are still
	// emptied and filled, or accounts would reference teams that are not loaded yet.
	accounts := tblC("public.accounts",
		[]engine.Column{col("id", false), col("parent", true), col("team_id", true)},
		fkN("acct_parent", "public.accounts", "public.accounts", []string{"parent"}, false),
		fkN("acct_team", "public.accounts", "public.teams", []string{"team_id"}, false),
	)
	accounts.NullCollidingCols = []string{"parent"}
	teams := tblC("public.teams",
		[]engine.Column{col("id", false), col("lead_account_id", true)},
		fkN("team_lead", "public.teams", "public.accounts", []string{"lead_account_id"}, false),
	)
	cs := classifyCyclicFKs([]engine.Table{accounts, teams})

	want := map[engine.TableRef][]string{
		ref("public.accounts"): {"team_id"},
		ref("public.teams"):    {"lead_account_id"},
	}
	if got := nullFillColsByTable(cs); !reflect.DeepEqual(got, want) {
		t.Errorf("nullFillColsByTable = %v, want %v", got, want)
	}
	if got := keepRealColsByTable(cs); !reflect.DeepEqual(got, map[engine.TableRef][]string{ref("public.accounts"): {"parent"}}) {
		t.Errorf("keepRealColsByTable = %v", got)
	}
	if bad := unloadableNullFill(cs); len(bad) != 0 {
		t.Errorf("loadable component reported unloadable: %+v", bad)
	}
}

func TestWithTargetNullColliding(t *testing.T) {
	src := []engine.Table{
		{Ref: ref("public.a"), NullCollidingCols: []string{"x"}},
		{Ref: ref("public.b")},
	}
	tgt := schemaOf(
		engine.Table{Ref: ref("public.a"), NullCollidingCols: []string{"x", "y"}},
		engine.Table{Ref: ref("public.b"), NullCollidingCols: []string{"z"}},
		engine.Table{Ref: ref("public.unrelated"), NullCollidingCols: []string{"q"}},
	)
	got := withTargetNullColliding(src, tgt)
	if !reflect.DeepEqual(got[0].NullCollidingCols, []string{"x", "y"}) {
		t.Errorf("a = %v, want union [x y] without duplicates", got[0].NullCollidingCols)
	}
	if !reflect.DeepEqual(got[1].NullCollidingCols, []string{"z"}) {
		t.Errorf("b = %v, want target-only [z]", got[1].NullCollidingCols)
	}
	if !reflect.DeepEqual(src[0].NullCollidingCols, []string{"x"}) || src[1].NullCollidingCols != nil {
		t.Error("input tables must not be mutated")
	}
	if out := withTargetNullColliding(src, nil); !reflect.DeepEqual(out, src) {
		t.Error("nil target must pass tables through unchanged")
	}
}

func TestMergeInsertExistingFirstSQL(t *testing.T) {
	pk := []captureCol{{Name: "id"}}
	sql := mergeInsertExistingFirstSQL(ref("public.acct"), "replicare_stg",
		[]string{"id", "email", "parent"}, pk, colSetOf(pk), false)
	for _, want := range []string{
		`INSERT INTO "public"."acct" ("id", "email", "parent") SELECT "id", "email", "parent" FROM "replicare_stg" s`,
		`ORDER BY EXISTS (SELECT 1 FROM "public"."acct" x WHERE x."id" = s."id") DESC`,
		`ON CONFLICT ("id") DO UPDATE SET "email" = EXCLUDED."email", "parent" = EXCLUDED."parent"`,
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("SQL missing %q:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, "OVERRIDING") {
		t.Error("no identity column: must not emit OVERRIDING SYSTEM VALUE")
	}
	withID := mergeInsertExistingFirstSQL(ref("public.acct"), "replicare_stg", []string{"id"}, pk, colSetOf(pk), true)
	if !strings.Contains(withID, `("id") OVERRIDING SYSTEM VALUE SELECT`) || !strings.HasSuffix(withID, "DO NOTHING") {
		t.Errorf("identity/PK-only variant wrong:\n%s", withID)
	}
}

func TestPreflightSelfRefNullCollidingInfoNotBlock(t *testing.T) {
	// The collision lives in the TARGET's index; the source has none. Pre-flight must use
	// the target, say it is handled, and not block.
	src := schemaOf(keyed(selfRefEmp(false), "id"))
	tgtEmp := keyed(tblC("public.emp", []engine.Column{col("id", false), col("manager_id", true)}), "id")
	tgtEmp.NullCollidingCols = []string{"manager_id"}
	p := buildPreflight("s", 90600, 170000, src, schemaOf(tgtEmp))

	if p.Blocked() {
		t.Fatalf("self-reference with a colliding unique is handled, must not block: %+v", blockFindings(p))
	}
	found := false
	for _, f := range p.Findings {
		if f.Category == "cyclic-fk" && f.Severity == engine.SevInfo && strings.Contains(f.Message, "real value") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected an info cyclic-fk finding saying the self-link is kept at its real value; got %+v", p.Findings)
	}
}

func TestPreflightCrossTableNullCollidingBlocks(t *testing.T) {
	src := schemaOf(keyed(crossCycle(false)[0], "id"), keyed(crossCycle(false)[1], "id"))
	tgtUsers := keyed(tblC("public.users", []engine.Column{col("id", false), col("primary_order_id", true)}), "id")
	tgtUsers.NullCollidingCols = []string{"primary_order_id"}
	tgtOrders := keyed(tblC("public.orders", []engine.Column{col("id", false), col("user_id", true)}), "id")
	p := buildPreflight("s", 90600, 170000, src, schemaOf(tgtUsers, tgtOrders))

	if !p.Blocked() {
		t.Fatal("a cross-table cycle whose column sits in a colliding unique index must block up front")
	}
	var msg string
	for _, f := range blockFindings(p) {
		if f.Category == "cyclic-fk" {
			msg = f.Message
		}
	}
	if !strings.Contains(msg, "users_primary_order") || !strings.Contains(msg, "DEFERRABLE") {
		t.Errorf("block finding should name the FK and the fix; got %q", msg)
	}
}
