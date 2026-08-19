package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// scale controls how many rows the initial seed writes. Everything is derived
// from these counts; children reference parents by arithmetic (product g has
// tenant_id 1+((g-1)%tenants) and sku 'SKU-'||g; order g has id md5('order:'||g))
// so seeding is fully set-based -- no per-row joins, fast to a million rows.
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

// defaultScale is a realistic mix dominated by ~1M events; total lands well over
// a million rows. Multiply with (scale).mul via the --scale flag.
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
	// order_items and product_categories are ~a few per parent; this is a rough
	// lower bound for the progress line, not exact.
	return s.tenants + s.cats + s.users + s.products + s.orders + s.events + s.metrics + s.audit
}

const eventBatch = 100000 // rows per events INSERT, to bound WAL/lock growth

// seed populates an empty schema. Order respects the FK graph; the users<->orders
// cycle is handled NULL-then-fill (users.primary_order_id starts NULL, back-filled
// last). All randomness is reproducible via setseed() applied before this runs.
func seed(ctx context.Context, conn *pgx.Conn, s scale, log logf) error {
	steps := []struct {
		name string
		sql  string
	}{
		{"tenants", fmt.Sprintf(`
			INSERT INTO loadgen.tenants (name, plan, config, tags, created_at, active)
			SELECT 'Tenant '||g,
			       (ARRAY['free','pro','enterprise'])[1+floor(random()*3)],
			       jsonb_build_object('seats', (1+floor(random()*100))::int, 'region', (ARRAY['us','eu','ap'])[1+floor(random()*3)]),
			       ARRAY['t'||(g%%10), (ARRAY['blue','green','red'])[1+floor(random()*3)]],
			       now() - (random()*interval '900 days'),
			       random() < 0.95
			FROM generate_series(1,%d) g`, s.tenants)},

		{"categories", fmt.Sprintf(`
			INSERT INTO loadgen.categories (tenant_id, name, depth)
			SELECT 1+((g-1)%%%d), 'Category '||g, (g%%4)::smallint
			FROM generate_series(1,%d) g`, s.tenants, s.cats)},

		// parent_id points only at a strictly-lower id -> a forest, never a cycle
		// among categories (the self-ref is exercised without an unbounded cycle).
		{"categories.parent_id", fmt.Sprintf(`
			UPDATE loadgen.categories
			SET parent_id = 1+floor(random()*(id-1))::bigint
			WHERE id > 1 AND random() < 0.6`)},

		{"users", fmt.Sprintf(`
			INSERT INTO loadgen.users
			      (tenant_id, email, full_name, balance, prefs, avatar, last_ip, signup_date, last_login, is_active)
			SELECT 1+((g-1)%%%d),
			       'user'||g||'@t'||(1+((g-1)%%%d))||'.example',
			       CASE WHEN random()<0.9 THEN 'User '||g END,
			       round((random()*100000)::numeric, 4),
			       jsonb_build_object('theme', (ARRAY['light','dark'])[1+floor(random()*2)], 'notify', random()<0.5),
			       decode(md5(random()::text), 'hex'),
			       (('10.'||(floor(random()*256))::int||'.'||(floor(random()*256))::int||'.'||(floor(random()*256))::int))::inet,
			       (current_date - (floor(random()*1000))::int),
			       CASE WHEN random()<0.7 THEN now() - (random()*interval '30 days') END,
			       random() < 0.9
			FROM generate_series(1,%d) g`, s.tenants, s.tenants, s.users)},

		{"products", fmt.Sprintf(`
			INSERT INTO loadgen.products (tenant_id, sku, name, price, dims, attrs, weight_g, created_at)
			SELECT 1+((g-1)%%%d), 'SKU-'||g, 'Product '||g,
			       round((1+random()*999)::numeric, 2),
			       ARRAY[round((random()*100)::numeric,2)::float8, round((random()*100)::numeric,2)::float8, round((random()*100)::numeric,2)::float8],
			       jsonb_build_object('color', (ARRAY['black','white','silver'])[1+floor(random()*3)], 'in_stock', random()<0.8),
			       CASE WHEN random()<0.8 THEN (10+floor(random()*5000))::int END,
			       now() - (random()*interval '400 days')
			FROM generate_series(1,%d) g`, s.tenants, s.products)},

		{"orders", fmt.Sprintf(`
			INSERT INTO loadgen.orders (id, tenant_id, user_id, status, total, currency, placed_at, ship_by, notes, metadata)
			SELECT md5('order:'||g)::uuid,
			       1+((g-1)%%%d),
			       1+floor(random()*%d)::bigint,
			       (ARRAY['pending','paid','shipped','cancelled','refunded'])[1+floor(random()*5)],
			       round((10+random()*5000)::numeric, 2),
			       (ARRAY['USD','EUR','GBP'])[1+floor(random()*3)],
			       now() - (random()*interval '365 days'),
			       CASE WHEN random()<0.6 THEN (current_date + (floor(random()*30))::int) END,
			       CASE WHEN random()<0.2 THEN 'note '||md5(random()::text) END,
			       jsonb_build_object('source', (ARRAY['web','ios','android'])[1+floor(random()*3)])
			FROM generate_series(1,%d) g`, s.tenants, s.users, s.orders)},

		// 1..4 line items per order; product picked by a random arithmetic index
		// (no join). tenant_id/sku are computed to match the real product row.
		{"order_items", fmt.Sprintf(`
			INSERT INTO loadgen.order_items (order_id, line_no, tenant_id, sku, qty, unit_price, discount)
			SELECT md5('order:'||o)::uuid, ln,
			       1+((k-1)%%%d), 'SKU-'||k,
			       1+floor(random()*5)::int,
			       round((5+random()*500)::numeric, 2),
			       CASE WHEN random()<0.3 THEN round((random()*20)::numeric, 2) END
			FROM generate_series(1,%d) o
			CROSS JOIN LATERAL generate_series(1, 1+floor(random()*4)::int) ln
			CROSS JOIN LATERAL (SELECT 1+floor(random()*%d)::int AS k) pick`,
			s.tenants, s.orders, s.products)},

		// 1..2 categories per product; distinct category ids per product, and
		// ON CONFLICT guards the rare arithmetic collision.
		{"product_categories", fmt.Sprintf(`
			INSERT INTO loadgen.product_categories (tenant_id, sku, category_id)
			SELECT 1+((g-1)%%%d), 'SKU-'||g, 1+((g*7 + j*%d/2 + j) %% %d)
			FROM generate_series(1,%d) g
			CROSS JOIN generate_series(0,1) j
			WHERE random() < 0.7
			ON CONFLICT DO NOTHING`, s.tenants, s.cats, s.cats, s.products)},

		{"generated_metrics", fmt.Sprintf(`
			INSERT INTO loadgen.generated_metrics (tenant_id, raw_value, label, computed_at)
			SELECT 1+((g-1)%%%d), round((random()*10000)::numeric, 4),
			       (ARRAY['cpu','mem','io','net'])[1+floor(random()*4)]||'.'||g,
			       now() - (random()*interval '90 days')
			FROM generate_series(1,%d) g`, s.tenants, s.metrics)},

		{"audit_log", fmt.Sprintf(`
			INSERT INTO loadgen.audit_log (tenant_id, action, actor, at, detail)
			SELECT 1+((g-1)%%%d),
			       (ARRAY['create','update','delete','login'])[1+floor(random()*4)],
			       CASE WHEN random()<0.8 THEN 'actor'||(1+floor(random()*100))::int END,
			       now() - (random()*interval '120 days'),
			       jsonb_build_object('ok', random()<0.9)
			FROM generate_series(1,%d) g`, s.tenants, s.audit)},
	}

	for _, st := range steps {
		if _, err := conn.Exec(ctx, st.sql); err != nil {
			return fmt.Errorf("seed %s: %w", st.name, err)
		}
		log("seeded %s", st.name)
	}

	// events: the big table, inserted in bounded batches with progress.
	for lo := 1; lo <= s.events; lo += eventBatch {
		hi := lo + eventBatch - 1
		if hi > s.events {
			hi = s.events
		}
		sql := fmt.Sprintf(`
			INSERT INTO loadgen.events (tenant_id, event_type, session, amount, ratio, flags, payload, labels, occurred_at)
			SELECT 1+((g-1)%%%d),
			       (ARRAY['click','view','purchase','signup','error'])[1+floor(random()*5)],
			       md5('sess:'||(g%%%d))::uuid,
			       CASE WHEN random()<0.8 THEN round((random()*1000)::numeric, 6) END,
			       random(),
			       (floor(random()*256))::int,
			       jsonb_build_object('k', md5(random()::text), 'n', (floor(random()*1000))::int),
			       ARRAY['l'||(g%%20), md5(random()::text)],
			       now() - (random()*interval '365 days')
			FROM generate_series(%d,%d) g`, s.tenants, s.users, lo, hi)
		if _, err := conn.Exec(ctx, sql); err != nil {
			return fmt.Errorf("seed events [%d,%d]: %w", lo, hi, err)
		}
		log("seeded events %d/%d", hi, s.events)
	}

	// Close the cycle: back-fill primary_order_id on ~half the users now that
	// orders exist (NULL-then-fill). Order ids are md5('order:'||n) for n in range.
	if _, err := conn.Exec(ctx, fmt.Sprintf(`
		UPDATE loadgen.users
		SET primary_order_id = md5('order:'||(1+floor(random()*%d))::int)::uuid
		WHERE random() < 0.5`, s.orders)); err != nil {
		return fmt.Errorf("seed users.primary_order_id: %w", err)
	}
	log("seeded users.primary_order_id (cycle fill)")

	return nil
}
