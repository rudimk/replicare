package main

import (
	"context"
	"fmt"
	"math/rand"
	"sort"

	"github.com/jackc/pgx/v5"
)

// A churn op is one SQL statement that mutates a small slice of the data. Each
// run of `loadgen run` (on an already-seeded DB) executes --ops of these, picked
// by weight, so a run is a realistic burst of concurrent-ish change for replicare
// to reconcile: inserts, updates, deletes, and the two special cases replicare
// treats specially -- PK-changing updates (SKU rename, cascaded) and the
// nullable FK-cycle edge (users.primary_order_id).
type churnOp struct {
	name   string
	weight int
	// kMin/kMax bound the rows/rows-inserted this op touches per execution.
	kMin, kMax int
	// sql builds the statement for k rows against THIS node's key slice (base). With
	// base=0 the slice is the whole table, so the emitted SQL matches the original
	// single-writer churn (active-passive unchanged); for base>0 inserts reference
	// node-owned parents (auto-identity ids already fall in the node's slice — the
	// sequences were restarted at seed) and updates/deletes only touch node-owned rows,
	// so peers never conflict on the same key (the clean disjoint-partition model).
	sql func(k int, base int64) string
}

// eventsRandIDs picks k random event ids WITHIN this node's slice by numeric range
// (rather than ORDER BY random() over a million rows). Ids in the slice run from
// base+1 up to this node's current max, so a random pick is base+1+rand*(max-base).
func eventsRandIDs(k int, base int64) string {
	return fmt.Sprintf(
		`ARRAY(SELECT (%[1]d + 1 + floor(random()*GREATEST((SELECT max(id)-%[1]d FROM loadgen.events WHERE %[2]s),1)))::bigint FROM generate_series(1,%[3]d))`,
		base, partition("id", base), k)
}

func churnOps() []churnOp {
	return []churnOp{
		{"insert_events", 20, 50, 500, func(k int, base int64) string {
			return fmt.Sprintf(`
				INSERT INTO loadgen.events (tenant_id, event_type, session, amount, ratio, flags, payload, labels, occurred_at)
				SELECT %[1]s,
				       (ARRAY['click','view','purchase','signup','error'])[1+floor(random()*5)],
				       md5(clock_timestamp()::text||random()::text)::uuid,
				       CASE WHEN random()<0.8 THEN round((random()*1000)::numeric,6) END,
				       random(), (floor(random()*256))::int,
				       jsonb_build_object('k', md5(random()::text)),
				       ARRAY['churn', md5(random()::text)],
				       now()
				FROM generate_series(1,%[2]d) g`, nodeTenantExpr(base), k)
		}},
		{"update_events", 15, 20, 200, func(k int, base int64) string {
			return fmt.Sprintf(`
				UPDATE loadgen.events
				SET flags = (floor(random()*256))::int,
				    amount = round((random()*1000)::numeric,6),
				    payload = payload || jsonb_build_object('u', md5(random()::text))
				WHERE id = ANY (%s)`, eventsRandIDs(k, base))
		}},
		{"delete_events", 8, 10, 100, func(k int, base int64) string {
			return fmt.Sprintf(`
				DELETE FROM loadgen.events WHERE id = ANY (%s)`, eventsRandIDs(k, base))
		}},

		{"insert_orders", 15, 5, 40, func(k int, base int64) string {
			// Each new order gets 1..3 line items referencing this node's products.
			return fmt.Sprintf(`
				WITH picked AS (SELECT id, tenant_id FROM loadgen.users WHERE %[2]s ORDER BY random() LIMIT %[1]d),
				ins AS (
					INSERT INTO loadgen.orders (id, tenant_id, user_id, status, total, currency, placed_at, metadata)
					SELECT md5(clock_timestamp()::text||random()::text||id::text)::uuid, tenant_id, id,
					       'pending', round((10+random()*500)::numeric,2), 'USD', now(),
					       jsonb_build_object('source','churn')
					FROM picked
					RETURNING id
				)
				INSERT INTO loadgen.order_items (order_id, line_no, tenant_id, sku, qty, unit_price)
				SELECT ins.id, gs.ln, p.tenant_id, p.sku, 1+floor(random()*3)::int, round((5+random()*100)::numeric,2)
				FROM ins
				CROSS JOIN LATERAL generate_series(1, 1+floor(random()*3)::int) gs(ln)
				CROSS JOIN LATERAL (SELECT tenant_id, sku FROM loadgen.products WHERE %[3]s ORDER BY random() LIMIT 1) p`,
				k, partition("id", base), partition("tenant_id", base))
		}},
		{"update_orders", 15, 10, 100, func(k int, base int64) string {
			return fmt.Sprintf(`
				UPDATE loadgen.orders
				SET status = (ARRAY['pending','paid','shipped','cancelled','refunded'])[1+floor(random()*5)],
				    total = round((10+random()*5000)::numeric,2),
				    metadata = metadata || jsonb_build_object('u', md5(random()::text))
				WHERE id IN (SELECT id FROM loadgen.orders WHERE %[2]s ORDER BY random() LIMIT %[1]d)`,
				k, partition("tenant_id", base))
		}},
		{"delete_orders", 6, 2, 20, func(k int, base int64) string {
			// Cascades to order_items.
			return fmt.Sprintf(`
				DELETE FROM loadgen.orders
				WHERE id IN (SELECT id FROM loadgen.orders WHERE %[2]s ORDER BY random() LIMIT %[1]d)`,
				k, partition("tenant_id", base))
		}},

		{"update_users", 12, 10, 100, func(k int, base int64) string {
			return fmt.Sprintf(`
				UPDATE loadgen.users
				SET balance = round((random()*100000)::numeric,4),
				    prefs = prefs || jsonb_build_object('seen', md5(random()::text)),
				    last_login = now(),
				    is_active = random() < 0.9
				WHERE id IN (SELECT id FROM loadgen.users WHERE %[2]s ORDER BY random() LIMIT %[1]d)`,
				k, partition("id", base))
		}},
		{"insert_users", 8, 5, 40, func(k int, base int64) string {
			return fmt.Sprintf(`
				INSERT INTO loadgen.users (tenant_id, email, full_name, balance, prefs, avatar, last_ip, is_active)
				SELECT %[1]s,
				       'user_'||md5(clock_timestamp()::text||random()::text||g::text)||'@churn.example',
				       'Churn User', round((random()*1000)::numeric,4),
				       jsonb_build_object('theme','dark'),
				       decode(md5(random()::text),'hex'),
				       (('10.'||(floor(random()*256))::int||'.'||(floor(random()*256))::int||'.'||(floor(random()*256))::int))::inet,
				       true
				FROM generate_series(1,%[2]d) g`, nodeTenantExpr(base), k)
		}},
		// The nullable FK-cycle edge: point primary_order_id at a same-tenant order
		// (or NULL). Exercises replicare's cycle handling under streaming.
		{"update_user_primary_order", 6, 5, 40, func(k int, base int64) string {
			return fmt.Sprintf(`
				UPDATE loadgen.users u
				SET primary_order_id = CASE WHEN random()<0.2 THEN NULL
					ELSE (SELECT o.id FROM loadgen.orders o WHERE o.tenant_id = u.tenant_id ORDER BY random() LIMIT 1) END
				WHERE u.id IN (SELECT id FROM loadgen.users WHERE %[2]s ORDER BY random() LIMIT %[1]d)`,
				k, partition("id", base))
		}},

		{"update_products", 10, 5, 50, func(k int, base int64) string {
			return fmt.Sprintf(`
				UPDATE loadgen.products
				SET price = round((1+random()*999)::numeric,2),
				    weight_g = (10+floor(random()*5000))::int,
				    attrs = attrs || jsonb_build_object('promo', random()<0.3)
				WHERE (tenant_id, sku) IN (SELECT tenant_id, sku FROM loadgen.products WHERE %[2]s ORDER BY random() LIMIT %[1]d)`,
				k, partition("tenant_id", base))
		}},
		// PK-changing UPDATE: rename a SKU. The composite PK (tenant_id, sku) moves,
		// and ON UPDATE CASCADE propagates it to order_items and product_categories.
		// This is replicare's "PK update = delete(old) + upsert(new)" path.
		{"rename_sku_pk_change", 5, 1, 10, func(k int, base int64) string {
			return fmt.Sprintf(`
				UPDATE loadgen.products
				SET sku = sku || '-r' || floor(random()*1000000000)::bigint
				WHERE (tenant_id, sku) IN (SELECT tenant_id, sku FROM loadgen.products WHERE %[2]s ORDER BY random() LIMIT %[1]d)`,
				k, partition("tenant_id", base))
		}},

		// audit_log is source-only (no key -> replicare skips it); still churn it so
		// the skip path sees ongoing writes.
		{"insert_audit", 5, 10, 100, func(k int, base int64) string {
			return fmt.Sprintf(`
				INSERT INTO loadgen.audit_log (tenant_id, action, actor, detail)
				SELECT %[1]s,
				       (ARRAY['create','update','delete','login'])[1+floor(random()*4)],
				       'churn', jsonb_build_object('n', g)
				FROM generate_series(1,%[2]d) g`, nodeTenantExpr(base), k)
		}},
	}
}

// churn executes `ops` weighted-random statements against THIS node's key slice
// (base). Op selection and k use the seeded rng (reproducible); the data values
// themselves come from server-side random(). Returns a per-op summary.
func churn(ctx context.Context, conn *pgx.Conn, ops int, base int64, rng *rand.Rand, log logf) (map[string]churnStat, error) {
	table := churnOps()
	total := 0
	for _, op := range table {
		total += op.weight
	}

	stats := map[string]churnStat{}
	for i := 0; i < ops; i++ {
		op := pickOp(table, total, rng)
		k := op.kMin
		if op.kMax > op.kMin {
			k += rng.Intn(op.kMax - op.kMin + 1)
		}
		tag, err := conn.Exec(ctx, op.sql(k, base))
		if err != nil {
			return stats, fmt.Errorf("churn op %s (k=%d): %w", op.name, k, err)
		}
		s := stats[op.name]
		s.calls++
		s.rows += tag.RowsAffected()
		stats[op.name] = s
	}
	return stats, nil
}

type churnStat struct {
	calls int
	rows  int64
}

func pickOp(table []churnOp, total int, rng *rand.Rand) churnOp {
	r := rng.Intn(total)
	for _, op := range table {
		if r < op.weight {
			return op
		}
		r -= op.weight
	}
	return table[len(table)-1]
}

// summaryLines renders the churn stats deterministically (sorted) for logging.
func summaryLines(stats map[string]churnStat) []string {
	names := make([]string, 0, len(stats))
	for n := range stats {
		names = append(names, n)
	}
	sort.Strings(names)
	lines := make([]string, 0, len(names))
	for _, n := range names {
		s := stats[n]
		lines = append(lines, fmt.Sprintf("  %-28s %4d calls  %8d rows", n, s.calls, s.rows))
	}
	return lines
}
