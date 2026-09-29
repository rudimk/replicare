package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"sort"
	"time"
)

// A churn op is one OR MORE SQL statements that mutate a small slice of the data. Each
// run of `loadgen-mysql run` (on an already-seeded DB) executes --ops of these, picked
// by weight, so a run is a realistic burst of change for replicare to reconcile:
// inserts, updates, deletes, and the two special cases replicare treats specially --
// PK-changing updates (SKU rename, cascaded) and the nullable FK-cycle edge
// (users.primary_order_id).
//
// MySQL differences from the Postgres harness:
//   - A statement that UPDATEs/DELETEs a table cannot SELECT from that same table in a
//     plain subquery (error 1093); wrapping the pick in a derived table (SELECT ...
//     FROM (SELECT ... LIMIT k) x) materializes it and sidesteps the rule.
//   - No RETURNING / no CTE on 5.7, so an insert that needs generated ids downstream
//     (orders -> order_items) is emitted as TWO statements sharing a client nonce, so
//     both compute the same deterministic order ids.
type churnOp struct {
	name   string
	weight int
	// kMin/kMax bound the rows/rows-inserted this op touches per execution.
	kMin, kMax int
	// sql builds the statement(s) for k rows against THIS node's key slice (base). With
	// base=0 the slice is the whole table, so the emitted SQL matches the original
	// single-writer churn (active-passive unchanged); for base>0 inserts reference
	// node-owned parents (auto-increment ids already fall in the node's slice — the
	// counters were restarted at seed) and updates/deletes only touch node-owned rows,
	// so peers never conflict on the same key (the clean disjoint-partition model). rng
	// supplies a per-call nonce where an op needs one (insert_orders).
	sql func(k int, base int64, rng *rand.Rand) []string
}

// pick materializes a random-row selection through a derived table so an UPDATE/DELETE
// may reference its own target table (MySQL error 1093 otherwise). cols is the projected
// key column list; where scopes to this node's slice.
func pick(cols, table, where string, k int) string {
	return fmt.Sprintf("SELECT %[1]s FROM (SELECT %[1]s FROM loadgen.%[2]s WHERE %[3]s ORDER BY RAND() LIMIT %[4]d) x",
		cols, table, where, k)
}

// eventsRandIDs yields k random event ids WITHIN this node's slice. The MAX(id) probe
// reads events through an AGGREGATE derived table (`m`), which MySQL always materializes,
// so an UPDATE/DELETE targeting events never sees events referenced in a mergeable
// subquery position (avoiding error 1093), and MAX is computed once. Empty slice -> mx
// NULL -> no rows matched (a safe no-op).
func eventsRandIDs(k int, base int64) string {
	return fmt.Sprintf(
		`SELECT %[1]d + 1 + FLOOR(RAND()*GREATEST(m.mx,1)) AS rid
		 FROM %[3]s CROSS JOIN (SELECT MAX(id)-%[1]d AS mx FROM loadgen.events WHERE %[2]s) m
		 WHERE gs.g <= %[4]d`,
		base, partition("id", base), genRows(k), k)
}

func churnOps() []churnOp {
	return []churnOp{
		{"insert_events", 20, 50, 500, func(k int, base int64, rng *rand.Rand) []string {
			return []string{fmt.Sprintf(`
				INSERT INTO loadgen.events (tenant_id, event_type, `+"`session`"+`, amount, ratio, flags, payload, labels, occurred_at)
				SELECT %[1]s,
				       ELT(1+FLOOR(RAND()*5),'click','view','purchase','signup','error'),
				       MD5(CONCAT(RAND(), ':', gs.g)),
				       CASE WHEN RAND()<0.8 THEN ROUND(RAND()*1000,6) END,
				       RAND(), FLOOR(RAND()*256),
				       JSON_OBJECT('k', MD5(RAND())),
				       JSON_ARRAY('churn', MD5(RAND())),
				       NOW()
				FROM %[3]s WHERE gs.g <= %[2]d`, nodeTenantExpr(base), k, genRows(k))}
		}},
		{"update_events", 15, 20, 200, func(k int, base int64, rng *rand.Rand) []string {
			return []string{fmt.Sprintf(`
				UPDATE loadgen.events
				SET flags = FLOOR(RAND()*256),
				    amount = ROUND(RAND()*1000,6),
				    payload = JSON_SET(COALESCE(payload, JSON_OBJECT()), '$.u', MD5(RAND()))
				WHERE id IN (%s)`, eventsRandIDs(k, base))}
		}},
		{"delete_events", 8, 10, 100, func(k int, base int64, rng *rand.Rand) []string {
			return []string{fmt.Sprintf(`DELETE FROM loadgen.events WHERE id IN (%s)`, eventsRandIDs(k, base))}
		}},

		{"insert_orders", 15, 5, 40, func(k int, base int64, rng *rand.Rand) []string {
			// Two statements sharing a nonce so order_items references the orders just
			// inserted (no RETURNING on MySQL): both compute the same id from (nonce, g).
			// The nonce mixes wall-clock time with the rng so ids are unique ACROSS runs
			// too (re-running with the same --seed must not collide on the orders PK — the
			// MySQL analog of the Postgres harness's clock_timestamp()-based ids).
			nonce := time.Now().UnixNano() ^ rng.Int63()
			orders := fmt.Sprintf(`
				INSERT INTO loadgen.orders (id, tenant_id, user_id, status, total, currency, placed_at, metadata)
				SELECT MD5(CONCAT('churn:', %[1]d, ':', gs.g)),
				       %[5]s,
				       (SELECT id FROM loadgen.users WHERE %[4]s ORDER BY RAND() LIMIT 1),
				       'pending', ROUND(10+RAND()*500,2), 'USD', NOW(), JSON_OBJECT('source','churn')
				FROM %[3]s WHERE gs.g <= %[2]d`,
				nonce, k, genRows(k), partition("id", base), nodeTenantExpr(base))
			// One live product per burst (no LATERAL on 5.7); FK-valid even after renames
			// because the product is read from the live table.
			items := fmt.Sprintf(`
				INSERT INTO loadgen.order_items (order_id, line_no, tenant_id, sku, qty, unit_price)
				SELECT MD5(CONCAT('churn:', %[1]d, ':', t.g)), t.ln, p.tenant_id, p.sku,
				       1+FLOOR(RAND()*3), ROUND(5+RAND()*100,2)
				FROM (
					SELECT gs.g AS g, lns.ln AS ln FROM %[3]s
					CROSS JOIN (SELECT 1 ln UNION ALL SELECT 2 UNION ALL SELECT 3) lns
					WHERE gs.g <= %[2]d
				) t
				JOIN (SELECT tenant_id, sku FROM loadgen.products WHERE %[4]s ORDER BY RAND() LIMIT 1) p ON 1=1`,
				nonce, k, genRows(k), partition("tenant_id", base))
			return []string{orders, items}
		}},
		{"update_orders", 15, 10, 100, func(k int, base int64, rng *rand.Rand) []string {
			return []string{fmt.Sprintf(`
				UPDATE loadgen.orders
				SET status = ELT(1+FLOOR(RAND()*5),'pending','paid','shipped','cancelled','refunded'),
				    total = ROUND(10+RAND()*5000,2),
				    metadata = JSON_SET(COALESCE(metadata, JSON_OBJECT()), '$.u', MD5(RAND()))
				WHERE id IN (%s)`, pick("id", "orders", partition("tenant_id", base), k))}
		}},
		{"delete_orders", 6, 2, 20, func(k int, base int64, rng *rand.Rand) []string {
			// Cascades to order_items.
			return []string{fmt.Sprintf(`DELETE FROM loadgen.orders WHERE id IN (%s)`,
				pick("id", "orders", partition("tenant_id", base), k))}
		}},

		{"update_users", 12, 10, 100, func(k int, base int64, rng *rand.Rand) []string {
			return []string{fmt.Sprintf(`
				UPDATE loadgen.users
				SET balance = ROUND(RAND()*100000,4),
				    prefs = JSON_SET(COALESCE(prefs, JSON_OBJECT()), '$.seen', MD5(RAND())),
				    last_login = NOW(),
				    is_active = RAND() < 0.9
				WHERE id IN (%s)`, pick("id", "users", partition("id", base), k))}
		}},
		{"insert_users", 8, 5, 40, func(k int, base int64, rng *rand.Rand) []string {
			return []string{fmt.Sprintf(`
				INSERT INTO loadgen.users (tenant_id, email, full_name, balance, prefs, avatar, last_ip, signup_date, is_active)
				SELECT %[1]s,
				       -- UUID() (not seeded RAND) so re-running with the same --seed can't
				       -- collide on the UNIQUE (tenant_id, email) key.
				       CONCAT('user_', REPLACE(UUID(), '-', ''), '@churn.example'),
				       'Churn User', ROUND(RAND()*1000,4),
				       JSON_OBJECT('theme','dark'),
				       UNHEX(MD5(RAND())),
				       CONCAT('10.', FLOOR(RAND()*256), '.', FLOOR(RAND()*256), '.', FLOOR(RAND()*256)),
				       CURDATE(),
				       1
				FROM %[3]s WHERE gs.g <= %[2]d`, nodeTenantExpr(base), k, genRows(k))}
		}},
		// The nullable FK-cycle edge: point primary_order_id at a same-tenant order (or
		// NULL). Exercises replicare's cycle handling under streaming (only constrained
		// under --cyclic; a harmless CHAR(32) write otherwise).
		{"update_user_primary_order", 6, 5, 40, func(k int, base int64, rng *rand.Rand) []string {
			return []string{fmt.Sprintf(`
				UPDATE loadgen.users u
				SET primary_order_id = CASE WHEN RAND()<0.2 THEN NULL
					ELSE (SELECT o.id FROM loadgen.orders o WHERE o.tenant_id = u.tenant_id ORDER BY RAND() LIMIT 1) END
				WHERE u.id IN (%s)`, pick("id", "users", partition("id", base), k))}
		}},

		{"update_products", 10, 5, 50, func(k int, base int64, rng *rand.Rand) []string {
			return []string{fmt.Sprintf(`
				UPDATE loadgen.products
				SET price = ROUND(1+RAND()*999,2),
				    weight_g = 10+FLOOR(RAND()*5000),
				    attrs = JSON_SET(COALESCE(attrs, JSON_OBJECT()), '$.promo', RAND()<0.3)
				WHERE (tenant_id, sku) IN (%s)`, pick("tenant_id, sku", "products", partition("tenant_id", base), k))}
		}},
		// PK-changing UPDATE: rename a SKU. The composite PK (tenant_id, sku) moves, and
		// ON UPDATE CASCADE propagates it to order_items and product_categories. This is
		// replicare's "PK update = delete(old) + upsert(new)" path.
		{"rename_sku_pk_change", 5, 1, 10, func(k int, base int64, rng *rand.Rand) []string {
			return []string{fmt.Sprintf(`
				UPDATE loadgen.products
				SET sku = CONCAT(sku, '-r', FLOOR(RAND()*1000000000))
				WHERE (tenant_id, sku) IN (%s)`, pick("tenant_id, sku", "products", partition("tenant_id", base), k))}
		}},

		// audit_log is source-only (no key -> replicare skips it); still churn it so the
		// skip path sees ongoing writes.
		{"insert_audit", 5, 10, 100, func(k int, base int64, rng *rand.Rand) []string {
			return []string{fmt.Sprintf(`
				INSERT INTO loadgen.audit_log (tenant_id, action, actor, detail)
				SELECT %[1]s,
				       ELT(1+FLOOR(RAND()*4),'create','update','delete','login'),
				       'churn', JSON_OBJECT('n', gs.g)
				FROM %[3]s WHERE gs.g <= %[2]d`, nodeTenantExpr(base), k, genRows(k))}
		}},
	}
}

// churn executes `ops` weighted-random ops against THIS node's key slice (base). Op
// selection and k use the seeded rng (reproducible); the data values themselves come
// from server-side RAND(). Returns a per-op summary.
func churn(ctx context.Context, db *sql.DB, ops int, base int64, rng *rand.Rand, log logf) (map[string]churnStat, error) {
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
		var rows int64
		for _, stmt := range op.sql(k, base, rng) {
			res, err := db.ExecContext(ctx, stmt)
			if err != nil {
				return stats, fmt.Errorf("churn op %s (k=%d): %w", op.name, k, err)
			}
			if n, err := res.RowsAffected(); err == nil {
				rows += n
			}
		}
		s := stats[op.name]
		s.calls++
		s.rows += rows
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
