package main

import (
	"context"
	"database/sql"
	"fmt"
)

// scale controls how many rows the initial seed writes. Everything is derived from
// these counts; children reference parents by arithmetic (product g has tenant_id
// base+1+((g-1)%tenants) and sku CONCAT('SKU-',base+g); order g has id
// MD5(CONCAT('order:',base+g))) so seeding is fully set-based -- no per-row joins, fast
// to a million rows. Mirrors test/loadgen (Postgres).
type scale struct {
	tenants  int
	cats     int
	users    int
	products int
	orders   int
	events   int
	metrics  int
	audit    int
}

// defaultScale is a realistic mix dominated by ~1M events; total lands well over a
// million rows. Multiply with (scale).mul via the --scale flag.
func defaultScale() scale {
	return scale{
		tenants:  50,
		cats:     500,
		users:    20000,
		products: 5000,
		orders:   100000,
		events:   1000000,
		metrics:  50000,
		audit:    50000,
	}
}

func (s scale) mul(f float64) scale {
	scaleOne := func(n int) int {
		v := int(float64(n) * f)
		if v < 1 {
			v = 1
		}
		return v
	}
	return scale{
		tenants:  scaleOne(s.tenants),
		cats:     scaleOne(s.cats),
		users:    scaleOne(s.users),
		products: scaleOne(s.products),
		orders:   scaleOne(s.orders),
		events:   scaleOne(s.events),
		metrics:  scaleOne(s.metrics),
		audit:    scaleOne(s.audit),
	}
}

func (s scale) total() int {
	// order_items and product_categories are ~a few per parent; this is a rough lower
	// bound for the progress line, not exact.
	return s.tenants + s.cats + s.users + s.products + s.orders + s.events + s.metrics + s.audit
}

const eventBatch = 100000 // rows per events INSERT, to bound transaction/lock growth

// digitTable is a 10-row derived table {0..9}. genRows cross-joins enough of these to
// span n, standing in for Postgres generate_series -- MySQL 5.7 (the oldest supported
// source) has no generate_series, no recursive CTE, and no window functions.
const digitTable = "(SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9)"

// genRows returns a derived table aliased `gs` yielding a column `gs.g` = 1..10^d, where
// d is the number of decimal digits needed for n. Callers filter (gs.g <= n) or
// (gs.g <= cnt) and map gs.g to the real key. It is the MySQL replacement for
// generate_series(1, n).
func genRows(n int) string {
	digits := 1
	for p := 10; p < n; p *= 10 {
		digits++
	}
	var terms, joins string
	mult := 1
	for i := 0; i < digits; i++ {
		alias := fmt.Sprintf("d%d", i)
		if i > 0 {
			terms += " + "
			joins += " CROSS JOIN "
		}
		terms += fmt.Sprintf("%s.d*%d", alias, mult)
		joins += fmt.Sprintf("%s %s", digitTable, alias)
		mult *= 10
	}
	return fmt.Sprintf("(SELECT %s + 1 AS g FROM %s) gs", terms, joins)
}

// seed populates an empty schema for ONE writer node whose key-space base is `base`
// (0 for the single-writer/active-passive case; node_id*nodeStride for a mesh member).
// Every AUTO_INCREMENT id is inserted explicitly as base+g, order/text keys mix base in,
// and every FK cross-reference is shifted by base so a node's rows reference only that
// node's own parents. With base=0 the emitted keys match the original single-writer
// seed. Order respects the FK graph; the users<->orders cycle is handled NULL-then-fill.
func seed(ctx context.Context, db *sql.DB, s scale, base int64, log logf) error {
	steps := []struct {
		name string
		sql  string
	}{
		{"tenants", fmt.Sprintf(`
			INSERT INTO loadgen.tenants (id, name, plan, config, tags, created_at, active)
			SELECT %[1]d+gs.g, CONCAT('Tenant ', %[1]d+gs.g),
			       ELT(1+FLOOR(RAND()*3), 'free','pro','enterprise'),
			       JSON_OBJECT('seats', 1+FLOOR(RAND()*100), 'region', ELT(1+FLOOR(RAND()*3),'us','eu','ap')),
			       JSON_ARRAY(CONCAT('t', gs.g%%10), ELT(1+FLOOR(RAND()*3),'blue','green','red')),
			       NOW() - INTERVAL CAST(FLOOR(RAND()*900) AS SIGNED) DAY,
			       RAND() < 0.95
			FROM %[3]s WHERE gs.g <= %[2]d`, base, s.tenants, genRows(s.tenants))},

		{"categories", fmt.Sprintf(`
			INSERT INTO loadgen.categories (id, tenant_id, name, depth)
			SELECT %[1]d+gs.g, %[1]d+1+((gs.g-1)%%%[2]d), CONCAT('Category ', %[1]d+gs.g), (gs.g%%4)
			FROM %[4]s WHERE gs.g <= %[3]d`, base, s.tenants, s.cats, genRows(s.cats))},

		// parent_id points only at a strictly-lower id WITHIN this node's slice -> a
		// forest, never a cycle (the self-ref is exercised without an unbounded cycle).
		{"categories.parent_id", fmt.Sprintf(`
			UPDATE loadgen.categories
			SET parent_id = %[1]d + 1 + FLOOR(RAND()*(id-%[1]d-1))
			WHERE id > %[1]d+1 AND id < %[2]d AND RAND() < 0.6`, base, base+nodeStride)},

		{"users", fmt.Sprintf(`
			INSERT INTO loadgen.users
			      (id, tenant_id, email, full_name, balance, prefs, avatar, last_ip, signup_date, last_login, is_active)
			SELECT %[1]d+gs.g,
			       %[1]d+1+((gs.g-1)%%%[2]d),
			       CONCAT('user', %[1]d+gs.g, '@t', %[1]d+1+((gs.g-1)%%%[2]d), '.example'),
			       CASE WHEN RAND()<0.9 THEN CONCAT('User ', %[1]d+gs.g) END,
			       ROUND(RAND()*100000, 4),
			       JSON_OBJECT('theme', ELT(1+FLOOR(RAND()*2),'light','dark'), 'notify', RAND()<0.5),
			       UNHEX(MD5(RAND())),
			       CONCAT('10.', FLOOR(RAND()*256), '.', FLOOR(RAND()*256), '.', FLOOR(RAND()*256)),
			       CURDATE() - INTERVAL CAST(FLOOR(RAND()*1000) AS SIGNED) DAY,
			       CASE WHEN RAND()<0.7 THEN NOW() - INTERVAL CAST(FLOOR(RAND()*30) AS SIGNED) DAY END,
			       RAND() < 0.9
			FROM %[4]s WHERE gs.g <= %[3]d`, base, s.tenants, s.users, genRows(s.users))},

		{"products", fmt.Sprintf(`
			INSERT INTO loadgen.products (tenant_id, sku, name, price, dims, attrs, weight_g, created_at)
			SELECT %[1]d+1+((gs.g-1)%%%[2]d), CONCAT('SKU-', %[1]d+gs.g), CONCAT('Product ', %[1]d+gs.g),
			       ROUND(1+RAND()*999, 2),
			       JSON_ARRAY(ROUND(RAND()*100,2), ROUND(RAND()*100,2), ROUND(RAND()*100,2)),
			       JSON_OBJECT('color', ELT(1+FLOOR(RAND()*3),'black','white','silver'), 'in_stock', RAND()<0.8),
			       CASE WHEN RAND()<0.8 THEN 10+FLOOR(RAND()*5000) END,
			       NOW() - INTERVAL CAST(FLOOR(RAND()*400) AS SIGNED) DAY
			FROM %[4]s WHERE gs.g <= %[3]d`, base, s.tenants, s.products, genRows(s.products))},

		{"orders", fmt.Sprintf(`
			INSERT INTO loadgen.orders (id, tenant_id, user_id, status, total, currency, placed_at, ship_by, notes, metadata)
			SELECT MD5(CONCAT('order:', %[1]d+gs.g)),
			       %[1]d+1+((gs.g-1)%%%[2]d),
			       %[1]d+1+FLOOR(RAND()*%[3]d),
			       ELT(1+FLOOR(RAND()*5),'pending','paid','shipped','cancelled','refunded'),
			       ROUND(10+RAND()*5000, 2),
			       ELT(1+FLOOR(RAND()*3),'USD','EUR','GBP'),
			       NOW() - INTERVAL CAST(FLOOR(RAND()*365) AS SIGNED) DAY,
			       CASE WHEN RAND()<0.6 THEN CURDATE() + INTERVAL CAST(FLOOR(RAND()*30) AS SIGNED) DAY END,
			       CASE WHEN RAND()<0.2 THEN CONCAT('note ', MD5(RAND())) END,
			       JSON_OBJECT('source', ELT(1+FLOOR(RAND()*3),'web','ios','android'))
			FROM %[5]s WHERE gs.g <= %[4]d`, base, s.tenants, s.users, s.orders, genRows(s.orders))},

		// 1..4 line items per order (line 1 always, 2..4 gated by RAND). The product is
		// picked by a DETERMINISTIC index k over (order, line) so tenant_id and sku both
		// reference the SAME real product row (no per-expression RAND mismatch). No
		// LATERAL: MySQL 5.7 has none.
		{"order_items", fmt.Sprintf(`
			INSERT INTO loadgen.order_items (order_id, line_no, tenant_id, sku, qty, unit_price, discount)
			SELECT MD5(CONCAT('order:', %[1]d+t.og)), t.ln,
			       %[1]d+1+((t.k-1)%%%[2]d), CONCAT('SKU-', %[1]d+t.k),
			       1+FLOOR(RAND()*5),
			       ROUND(5+RAND()*500, 2),
			       CASE WHEN RAND()<0.3 THEN ROUND(RAND()*20, 2) END
			FROM (
				SELECT gs.g AS og, lns.ln AS ln, 1+((gs.g*31 + lns.ln*7) %% %[4]d) AS k
				FROM %[5]s
				CROSS JOIN (SELECT 1 ln UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) lns
				WHERE gs.g <= %[3]d AND (lns.ln = 1 OR RAND() < 0.5)
			) t`, base, s.tenants, s.orders, s.products, genRows(s.orders))},

		// 1..2 categories per product; deterministic distinct-ish category ids per
		// product, INSERT IGNORE guards the rare arithmetic collision.
		{"product_categories", fmt.Sprintf(`
			INSERT IGNORE INTO loadgen.product_categories (tenant_id, sku, category_id)
			SELECT %[1]d+1+((gs.g-1)%%%[2]d), CONCAT('SKU-', %[1]d+gs.g),
			       %[1]d+1+((gs.g*7 + j.j*3 + 1) %% %[3]d)
			FROM %[5]s
			CROSS JOIN (SELECT 0 j UNION ALL SELECT 1) j
			WHERE gs.g <= %[4]d AND RAND() < 0.7`, base, s.tenants, s.cats, s.products, genRows(s.products))},

		{"generated_metrics", fmt.Sprintf(`
			INSERT INTO loadgen.generated_metrics (id, tenant_id, raw_value, label, computed_at)
			SELECT %[1]d+gs.g, %[1]d+1+((gs.g-1)%%%[2]d), ROUND(RAND()*10000, 4),
			       CONCAT(ELT(1+FLOOR(RAND()*4),'cpu','mem','io','net'), '.', %[1]d+gs.g),
			       NOW() - INTERVAL CAST(FLOOR(RAND()*90) AS SIGNED) DAY
			FROM %[4]s WHERE gs.g <= %[3]d`, base, s.tenants, s.metrics, genRows(s.metrics))},

		{"audit_log", fmt.Sprintf(`
			INSERT INTO loadgen.audit_log (tenant_id, action, actor, `+"`at`"+`, detail)
			SELECT %[1]d+1+((gs.g-1)%%%[2]d),
			       ELT(1+FLOOR(RAND()*4),'create','update','delete','login'),
			       CASE WHEN RAND()<0.8 THEN CONCAT('actor', 1+FLOOR(RAND()*100)) END,
			       NOW() - INTERVAL CAST(FLOOR(RAND()*120) AS SIGNED) DAY,
			       JSON_OBJECT('ok', RAND()<0.9)
			FROM %[4]s WHERE gs.g <= %[3]d`, base, s.tenants, s.audit, genRows(s.audit))},
	}

	for _, st := range steps {
		if _, err := db.ExecContext(ctx, st.sql); err != nil {
			return fmt.Errorf("seed %s: %w", st.name, err)
		}
		log("seeded %s", st.name)
	}

	// events: the big table, inserted in bounded batches with progress. Each batch
	// generates only its own rows (gs.g in 1..cnt) and maps them to ids base+offset+g.
	for lo := 1; lo <= s.events; lo += eventBatch {
		hi := lo + eventBatch - 1
		if hi > s.events {
			hi = s.events
		}
		offset := int64(lo - 1)
		cnt := hi - lo + 1
		sql := fmt.Sprintf(`
			INSERT INTO loadgen.events (id, tenant_id, event_type, `+"`session`"+`, amount, ratio, flags, payload, labels, occurred_at)
			SELECT %[1]d+%[6]d+gs.g,
			       %[1]d+1+((%[6]d+gs.g-1)%%%[2]d),
			       ELT(1+FLOOR(RAND()*5),'click','view','purchase','signup','error'),
			       MD5(CONCAT('sess:', %[1]d+((%[6]d+gs.g)%%%[3]d))),
			       CASE WHEN RAND()<0.8 THEN ROUND(RAND()*1000, 6) END,
			       RAND(),
			       FLOOR(RAND()*256),
			       JSON_OBJECT('k', MD5(RAND()), 'n', FLOOR(RAND()*1000)),
			       JSON_ARRAY(CONCAT('l', (%[6]d+gs.g)%%20), MD5(RAND())),
			       NOW() - INTERVAL CAST(FLOOR(RAND()*365) AS SIGNED) DAY
			FROM %[5]s WHERE gs.g <= %[4]d`, base, s.tenants, s.users, cnt, genRows(cnt), offset)
		if _, err := db.ExecContext(ctx, sql); err != nil {
			return fmt.Errorf("seed events [%d,%d]: %w", lo, hi, err)
		}
		log("seeded events %d/%d", hi, s.events)
	}

	// Close the cycle: back-fill primary_order_id on ~half THIS node's users now that
	// orders exist (NULL-then-fill). Order ids are MD5(CONCAT('order:', base+n)).
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`
		UPDATE loadgen.users
		SET primary_order_id = MD5(CONCAT('order:', %[1]d+1+FLOOR(RAND()*%[2]d)))
		WHERE %[3]s AND RAND() < 0.5`, base, s.orders, partition("id", base))); err != nil {
		return fmt.Errorf("seed users.primary_order_id: %w", err)
	}
	log("seeded users.primary_order_id (cycle fill)")

	// Position each AUTO_INCREMENT table just past this node's seeded ids so subsequent
	// churn inserts continue INSIDE this node's slice, above the seeded rows and below
	// the next node's base. With base=0 this is seeded_count+1 — where the original
	// auto-seed left it — so single-writer churn is unchanged.
	if err := restartAutoIncrement(ctx, db, s, base); err != nil {
		return err
	}

	return nil
}

// restartAutoIncrement moves the AUTO_INCREMENT of each auto-id table to
// base + <rows seeded for that table> + 1, so churn's auto-id inserts land in this
// node's key slice without colliding with the explicitly-seeded ids. (MySQL only lets
// AUTO_INCREMENT move forward; the seeded max is base+count, so base+count+1 is valid.)
func restartAutoIncrement(ctx context.Context, db *sql.DB, s scale, base int64) error {
	for _, t := range []struct {
		table string
		count int
	}{
		{"tenants", s.tenants},
		{"categories", s.cats},
		{"users", s.users},
		{"events", s.events},
		{"generated_metrics", s.metrics},
	} {
		sql := fmt.Sprintf("ALTER TABLE loadgen.%s AUTO_INCREMENT = %d", t.table, base+int64(t.count)+1)
		if _, err := db.ExecContext(ctx, sql); err != nil {
			return fmt.Errorf("restart %s auto_increment: %w", t.table, err)
		}
	}
	return nil
}
