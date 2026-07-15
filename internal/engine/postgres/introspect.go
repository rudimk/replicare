package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
)

// Source introspection (CLAUDE.md §7, §8.1). All catalog SQL here is conservative
// and version-tolerant so it runs against very old source servers (down to the
// 9.6 floor) as well as modern ones. Features that only exist on newer servers —
// declarative partitioning (PG10), identity columns (PG10), generated columns
// (PG12) — are gated behind a server-version probe and degrade to safe literals
// on older servers rather than referencing catalog columns that do not exist.
//
// The same code introspects both the source (for capture/copy planning) and the
// target (for the pre-flight compatibility check); the Sink reuses it.

// Postgres version thresholds (server_version_num form).
const (
	pg100000 = 100000 // partitioning, identity columns
	pg120000 = 120000 // generated columns
)

// introspectConn introspects the selected tables on an open connection. version
// is the server_version_num used to gate old-server-incompatible catalog columns.
func introspectConn(ctx context.Context, conn *pgx.Conn, version int, sel engine.Selection) (*engine.Schema, error) {
	candidates, err := listCandidateTables(ctx, conn, version)
	if err != nil {
		return nil, err
	}

	// Filter the candidate list down to the selection.
	allRefs := make([]engine.TableRef, len(candidates))
	metaByRef := make(map[engine.TableRef]candidate, len(candidates))
	for i, c := range candidates {
		allRefs[i] = c.ref
		metaByRef[c.ref] = c
	}
	selectedRefs, err := selectTables(allRefs, sel)
	if err != nil {
		return nil, err
	}

	// Build the working set of selected tables and the oid filter for the
	// per-table queries.
	byOID := make(map[int64]*engine.Table, len(selectedRefs))
	oids := make([]int64, 0, len(selectedRefs))
	tables := make([]engine.Table, 0, len(selectedRefs))
	for _, r := range selectedRefs {
		meta := metaByRef[r]
		tables = append(tables, engine.Table{Ref: r, Partitioned: meta.partitioned})
		oids = append(oids, meta.oid)
	}
	for i := range tables {
		byOID[metaByRef[tables[i].Ref].oid] = &tables[i]
	}

	if len(oids) == 0 {
		return &engine.Schema{Tables: nil}, nil
	}

	if err := loadColumns(ctx, conn, version, oids, byOID); err != nil {
		return nil, err
	}
	if err := loadKeys(ctx, conn, oids, byOID); err != nil {
		return nil, err
	}
	if err := loadForeignKeys(ctx, conn, oids, byOID); err != nil {
		return nil, err
	}

	return &engine.Schema{Tables: tables}, nil
}

// candidate carries the minimal listing info plus the oid for follow-up queries.
type candidate struct {
	ref         engine.TableRef
	oid         int64
	partitioned bool
}

// listCandidateTables returns all ordinary/partitioned tables in user schemas,
// excluding system schemas and (on PG10+) partition children, which are copied
// via their parent.
func listCandidateTables(ctx context.Context, conn *pgx.Conn, version int) ([]candidate, error) {
	relkinds := "'r'"
	notChild := ""
	if version >= pg100000 {
		relkinds = "'r','p'"
		notChild = "AND c.relispartition = false"
	}
	q := fmt.Sprintf(`
SELECT c.oid::bigint, n.nspname, c.relname, (c.relkind = 'p') AS partitioned
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN (%s)
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND n.nspname NOT LIKE 'pg_toast%%'
  AND n.nspname NOT LIKE 'pg_temp%%'
  %s
ORDER BY n.nspname, c.relname`, relkinds, notChild)

	rows, err := conn.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("postgres: list tables: %w", err)
	}
	defer rows.Close()

	var out []candidate
	for rows.Next() {
		var oid int64
		var schema, name string
		var partitioned bool
		if err := rows.Scan(&oid, &schema, &name, &partitioned); err != nil {
			return nil, fmt.Errorf("postgres: scan table row: %w", err)
		}
		out = append(out, candidate{
			ref:         engine.TableRef{Schema: schema, Name: name},
			oid:         oid,
			partitioned: partitioned,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: list tables: %w", err)
	}
	return out, nil
}

// loadColumns fills each table's Columns, gating identity (PG10+) and generated
// (PG12+) on the server version.
func loadColumns(ctx context.Context, conn *pgx.Conn, version int, oids []int64, byOID map[int64]*engine.Table) error {
	generated := "false"
	identity := "false"
	if version >= pg120000 {
		generated = "(a.attgenerated = 's')"
	}
	if version >= pg100000 {
		identity = "(a.attidentity IN ('a', 'd'))"
	}
	q := fmt.Sprintf(`
SELECT a.attrelid::bigint, a.attname,
       format_type(a.atttypid, a.atttypmod) AS data_type,
       (NOT a.attnotnull) AS nullable,
       %s AS generated,
       %s AS identity
FROM pg_attribute a
WHERE a.attrelid = ANY($1::oid[])
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attrelid, a.attnum`, generated, identity)

	rows, err := conn.Query(ctx, q, oids)
	if err != nil {
		return fmt.Errorf("postgres: introspect columns: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var relid int64
		var c engine.Column
		if err := rows.Scan(&relid, &c.Name, &c.DataType, &c.Nullable, &c.Generated, &c.Identity); err != nil {
			return fmt.Errorf("postgres: scan column: %w", err)
		}
		if t := byOID[relid]; t != nil {
			t.Columns = append(t.Columns, c)
		}
	}
	return rows.Err()
}

// loadKeys fills PrimaryKey and UniqueKeys from pg_constraint (contype p/u).
// Bare unique indexes without a backing constraint are not detected in v1 (a
// conservative, documented limitation): such a table is treated as key-less and
// skipped+warned rather than replicated on an unverified identity.
func loadKeys(ctx context.Context, conn *pgx.Conn, oids []int64, byOID map[int64]*engine.Table) error {
	q := `
SELECT con.conrelid::bigint,
       con.conname,
       (con.contype = 'p') AS is_primary,
       (SELECT array_agg(att.attname ORDER BY k.ord)
          FROM unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord)
          JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = k.attnum
       ) AS cols
FROM pg_constraint con
WHERE con.conrelid = ANY($1::oid[])
  AND con.contype IN ('p', 'u')
ORDER BY con.conrelid, is_primary DESC, con.conname`

	rows, err := conn.Query(ctx, q, oids)
	if err != nil {
		return fmt.Errorf("postgres: introspect keys: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var relid int64
		var key engine.Key
		if err := rows.Scan(&relid, &key.Name, &key.IsPrimary, &key.Columns); err != nil {
			return fmt.Errorf("postgres: scan key: %w", err)
		}
		t := byOID[relid]
		if t == nil {
			continue
		}
		if key.IsPrimary {
			k := key
			t.PrimaryKey = &k
		} else {
			t.UniqueKeys = append(t.UniqueKeys, key)
		}
	}
	return rows.Err()
}

// loadForeignKeys fills each table's ForeignKeys from pg_constraint (contype f),
// resolving both child and parent schema-qualified names and column lists.
func loadForeignKeys(ctx context.Context, conn *pgx.Conn, oids []int64, byOID map[int64]*engine.Table) error {
	q := `
SELECT con.conrelid::bigint,
       con.conname,
       con.condeferrable,
       cn.nspname AS child_schema, cc.relname AS child_name,
       pn.nspname AS parent_schema, pc.relname AS parent_name,
       (SELECT array_agg(ca.attname ORDER BY ck.ord)
          FROM unnest(con.conkey) WITH ORDINALITY AS ck(attnum, ord)
          JOIN pg_attribute ca ON ca.attrelid = con.conrelid AND ca.attnum = ck.attnum
       ) AS child_cols,
       (SELECT array_agg(pa.attname ORDER BY pk.ord)
          FROM unnest(con.confkey) WITH ORDINALITY AS pk(attnum, ord)
          JOIN pg_attribute pa ON pa.attrelid = con.confrelid AND pa.attnum = pk.attnum
       ) AS parent_cols
FROM pg_constraint con
JOIN pg_class cc ON cc.oid = con.conrelid
JOIN pg_namespace cn ON cn.oid = cc.relnamespace
JOIN pg_class pc ON pc.oid = con.confrelid
JOIN pg_namespace pn ON pn.oid = pc.relnamespace
WHERE con.conrelid = ANY($1::oid[])
  AND con.contype = 'f'
ORDER BY con.conrelid, con.conname`

	rows, err := conn.Query(ctx, q, oids)
	if err != nil {
		return fmt.Errorf("postgres: introspect foreign keys: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var relid int64
		var fk engine.ForeignKey
		var childSchema, childName, parentSchema, parentName string
		if err := rows.Scan(&relid, &fk.Name, &fk.Deferrable,
			&childSchema, &childName, &parentSchema, &parentName,
			&fk.ChildCols, &fk.ParentCols); err != nil {
			return fmt.Errorf("postgres: scan foreign key: %w", err)
		}
		fk.Child = engine.TableRef{Schema: childSchema, Name: childName}
		fk.Parent = engine.TableRef{Schema: parentSchema, Name: parentName}
		if t := byOID[relid]; t != nil {
			t.ForeignKeys = append(t.ForeignKeys, fk)
		}
	}
	return rows.Err()
}
