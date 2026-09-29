package main

import (
	"context"
	"database/sql"
	"fmt"
)

// The load-test schema lives in its own `loadgen` DATABASE (MySQL's analog of the
// Postgres `loadgen` schema) so it never collides with anything else on the server and
// can be selected wholesale by replicare (include: ["loadgen.*"]). Every statement is
// schema-qualified, so the connection's default database is irrelevant.
//
// The 10 tables mirror test/loadgen (Postgres) and exercise replicare's hard MySQL
// paths, not just hold data:
//
//   - tenants          hub table many others FK to (giant-component pressure);
//     AUTO_INCREMENT PK, JSON, DATETIME, bool (TINYINT(1)).
//   - categories       self-referential hierarchy column (self-ref FK under --cyclic).
//   - users            VARBINARY (avatar), IP text, DECIMAL, DATE, JSON, many nullable
//     columns; primary_order_id closes a users<->orders cycle under --cyclic.
//   - products         COMPOSITE VARCHAR PK (tenant_id, sku); JSON dims; the target of
//     PK-changing updates (SKU rename cascades to children).
//   - orders           CHAR(32) id PK (MD5 hex, the UUID analog); CHAR(3); nullable FK.
//   - order_items      composite PK; composite FK to products ON UPDATE CASCADE.
//   - product_categories  M:N join, composite PK, two FKs.
//   - events           the high-volume table (~1M rows); wide type coverage, DOUBLE.
//   - generated_metrics   GENERATED ... STORED column + AUTO_INCREMENT PK.
//   - audit_log        NO primary key / no unique key -> replicare skips it with a
//     loud warning. verify() excludes it for the same reason.
//
// The default schema is ACYCLIC. The optional --cyclic flag adds two nullable FK
// cycles (cyclicFKs) to exercise replicare's cyclic-copy AND cyclic-streaming paths.
//
// MySQL-5.7 tolerance (5.7 is the oldest supported source, docs/mysql-version-support.md):
//   - JSON columns get NO DEFAULT (5.7 forbids it); every INSERT supplies them.
//   - DATE columns get NO function default (5.7 forbids expression defaults); every
//     INSERT supplies signup_date.
//   - All DDL is idempotent: CREATE ... IF NOT EXISTS; the cyclic FKs are added only
//     when absent (MySQL has no ADD CONSTRAINT IF NOT EXISTS — see applyCyclic).
var ddlStatements = []string{
	`CREATE DATABASE IF NOT EXISTS loadgen CHARACTER SET utf8mb4`,

	`CREATE TABLE IF NOT EXISTS loadgen.tenants (
		id         BIGINT NOT NULL AUTO_INCREMENT,
		name       VARCHAR(255) NOT NULL,
		plan       VARCHAR(20) NOT NULL,
		config     JSON NULL,
		tags       JSON NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		active     TINYINT(1) NOT NULL DEFAULT 1,
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

	// parent_id is a self-referential hierarchy column. The self-ref FK is added only
	// under --cyclic (see cyclicFKs); the default schema keeps the column without the
	// constraint.
	`CREATE TABLE IF NOT EXISTS loadgen.categories (
		id        BIGINT NOT NULL AUTO_INCREMENT,
		tenant_id BIGINT NOT NULL,
		parent_id BIGINT NULL,
		name      VARCHAR(255) NOT NULL,
		depth     SMALLINT NOT NULL DEFAULT 0,
		PRIMARY KEY (id),
		KEY idx_categories_tenant (tenant_id),
		KEY idx_categories_parent (parent_id),
		CONSTRAINT categories_tenant_fk FOREIGN KEY (tenant_id) REFERENCES loadgen.tenants(id) ON DELETE CASCADE
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

	`CREATE TABLE IF NOT EXISTS loadgen.users (
		id               BIGINT NOT NULL AUTO_INCREMENT,
		tenant_id        BIGINT NOT NULL,
		email            VARCHAR(191) NOT NULL,
		full_name        VARCHAR(255) NULL,
		balance          DECIMAL(20,4) NOT NULL DEFAULT 0,
		prefs            JSON NULL,
		avatar           VARBINARY(255) NULL,
		last_ip          VARCHAR(45) NULL,
		signup_date      DATE NOT NULL,
		last_login       DATETIME NULL,
		primary_order_id CHAR(32) NULL,
		is_active        TINYINT(1) NOT NULL DEFAULT 1,
		PRIMARY KEY (id),
		UNIQUE KEY uq_users_tenant_email (tenant_id, email),
		CONSTRAINT users_tenant_fk FOREIGN KEY (tenant_id) REFERENCES loadgen.tenants(id) ON DELETE CASCADE
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

	`CREATE TABLE IF NOT EXISTS loadgen.products (
		tenant_id  BIGINT NOT NULL,
		sku        VARCHAR(191) NOT NULL,
		name       VARCHAR(255) NOT NULL,
		price      DECIMAL(12,2) NOT NULL,
		dims       JSON NULL,
		attrs      JSON NULL,
		weight_g   INT NULL,
		created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (tenant_id, sku),
		CONSTRAINT products_tenant_fk FOREIGN KEY (tenant_id) REFERENCES loadgen.tenants(id) ON DELETE CASCADE
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

	`CREATE TABLE IF NOT EXISTS loadgen.orders (
		id        CHAR(32) NOT NULL,
		tenant_id BIGINT NOT NULL,
		user_id   BIGINT NULL,
		status    VARCHAR(20) NOT NULL,
		total     DECIMAL(14,2) NOT NULL,
		currency  CHAR(3) NOT NULL DEFAULT 'USD',
		placed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		ship_by   DATE NULL,
		notes     VARCHAR(255) NULL,
		metadata  JSON NULL,
		PRIMARY KEY (id),
		KEY idx_orders_tenant (tenant_id),
		KEY idx_orders_user (user_id),
		CONSTRAINT orders_tenant_fk FOREIGN KEY (tenant_id) REFERENCES loadgen.tenants(id) ON DELETE CASCADE,
		CONSTRAINT orders_user_fk FOREIGN KEY (user_id) REFERENCES loadgen.users(id) ON DELETE CASCADE
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

	// Composite FK to products needs an index on the referencing columns (the PK does
	// not cover (tenant_id, sku)), hence idx_oi_product.
	`CREATE TABLE IF NOT EXISTS loadgen.order_items (
		order_id   CHAR(32) NOT NULL,
		line_no    INT NOT NULL,
		tenant_id  BIGINT NOT NULL,
		sku        VARCHAR(191) NOT NULL,
		qty        INT NOT NULL,
		unit_price DECIMAL(12,2) NOT NULL,
		discount   DECIMAL(5,2) NULL,
		PRIMARY KEY (order_id, line_no),
		KEY idx_oi_product (tenant_id, sku),
		CONSTRAINT oi_order_fk FOREIGN KEY (order_id) REFERENCES loadgen.orders(id) ON DELETE CASCADE,
		CONSTRAINT oi_product_fk FOREIGN KEY (tenant_id, sku)
			REFERENCES loadgen.products(tenant_id, sku) ON UPDATE CASCADE ON DELETE CASCADE
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

	`CREATE TABLE IF NOT EXISTS loadgen.product_categories (
		tenant_id   BIGINT NOT NULL,
		sku         VARCHAR(191) NOT NULL,
		category_id BIGINT NOT NULL,
		added_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (tenant_id, sku, category_id),
		KEY idx_pc_category (category_id),
		CONSTRAINT pc_category_fk FOREIGN KEY (category_id) REFERENCES loadgen.categories(id) ON DELETE CASCADE,
		CONSTRAINT pc_product_fk FOREIGN KEY (tenant_id, sku)
			REFERENCES loadgen.products(tenant_id, sku) ON UPDATE CASCADE ON DELETE CASCADE
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

	`CREATE TABLE IF NOT EXISTS loadgen.events (
		id          BIGINT NOT NULL AUTO_INCREMENT,
		tenant_id   BIGINT NOT NULL,
		event_type  VARCHAR(20) NOT NULL,
		` + "`session`" + ` CHAR(32) NOT NULL,
		amount      DECIMAL(18,6) NULL,
		ratio       DOUBLE NULL,
		flags       INT NOT NULL DEFAULT 0,
		payload     JSON NULL,
		labels      JSON NULL,
		occurred_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (id),
		KEY idx_events_tenant (tenant_id),
		CONSTRAINT events_tenant_fk FOREIGN KEY (tenant_id) REFERENCES loadgen.tenants(id) ON DELETE CASCADE
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

	`CREATE TABLE IF NOT EXISTS loadgen.generated_metrics (
		id          BIGINT NOT NULL AUTO_INCREMENT,
		tenant_id   BIGINT NOT NULL,
		raw_value   DECIMAL(18,4) NOT NULL,
		doubled     DECIMAL(19,4) AS (raw_value * 2) STORED,
		label       VARCHAR(255) NOT NULL,
		computed_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (id),
		KEY idx_gm_tenant (tenant_id),
		CONSTRAINT gm_tenant_fk FOREIGN KEY (tenant_id) REFERENCES loadgen.tenants(id) ON DELETE CASCADE
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,

	// No PRIMARY KEY and no UNIQUE key on purpose: replicare must skip this table with
	// a loud warning (telemetry-surfaced). It stays source-only.
	`CREATE TABLE IF NOT EXISTS loadgen.audit_log (
		tenant_id BIGINT NOT NULL,
		action    VARCHAR(20) NOT NULL,
		actor     VARCHAR(255) NULL,
		` + "`at`" + ` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
		detail    JSON NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
}

// fkDef is one nullable FK cycle added under --cyclic. MySQL has no
// ADD CONSTRAINT IF NOT EXISTS, so applyCyclic adds each only when absent.
type fkDef struct {
	name  string
	alter string
}

// cyclicFKs are the two NULLABLE FK cycles, applied only under --cyclic. Both are the
// kind replicare's pre-flight classifies as null_then_fill (the copier loads the cyclic
// FK columns NULL, then fills them). They are separated from the default schema so the
// acyclic default stays the simplest case.
var cyclicFKs = []fkDef{
	// categories self-reference (parent_id -> categories.id).
	{"categories_parent_fk",
		`ALTER TABLE loadgen.categories
			ADD CONSTRAINT categories_parent_fk
			FOREIGN KEY (parent_id) REFERENCES loadgen.categories(id) ON DELETE SET NULL`},
	// users <-> orders 2-table cycle (users.primary_order_id -> orders.id;
	// orders.user_id -> users.id already exists).
	{"users_primary_order_fk",
		`ALTER TABLE loadgen.users
			ADD CONSTRAINT users_primary_order_fk
			FOREIGN KEY (primary_order_id) REFERENCES loadgen.orders(id) ON DELETE SET NULL`},
}

// applyCyclic adds each cyclic FK only if it is not already present, giving --cyclic the
// same idempotency the base CREATE ... IF NOT EXISTS statements have.
func applyCyclic(ctx context.Context, db *sql.DB) error {
	for _, fk := range cyclicFKs {
		var n int
		if err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM information_schema.table_constraints
			 WHERE constraint_schema='loadgen' AND constraint_name=? AND constraint_type='FOREIGN KEY'`,
			fk.name).Scan(&n); err != nil {
			return fmt.Errorf("check fk %s: %w", fk.name, err)
		}
		if n > 0 {
			continue
		}
		if _, err := db.ExecContext(ctx, fk.alter); err != nil {
			return fmt.Errorf("add fk %s: %w", fk.name, err)
		}
	}
	return nil
}

// replicatedTables are the tables replicare should converge on the target (bare names
// under the loadgen database), in a stable order. audit_log is intentionally absent: it
// has no key, so replicare skips it and there is nothing to converge.
var replicatedTables = []string{
	"tenants",
	"categories",
	"users",
	"products",
	"orders",
	"order_items",
	"product_categories",
	"events",
	"generated_metrics",
}
