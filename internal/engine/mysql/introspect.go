package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// systemSchemas are the MySQL-internal databases never considered for
// replication (they hold no user data / are not replicable).
var systemSchemas = map[string]bool{
	"mysql":              true,
	"information_schema": true,
	"performance_schema": true,
	"sys":                true,
}

// introspectDB builds the engine.Schema for the selected tables from
// information_schema (MM1a). It is version-tolerant across the 5.7->8.x floor:
// every catalog column used (COLUMNS.EXTRA, STATISTICS, KEY_COLUMN_USAGE,
// REFERENTIAL_CONSTRAINTS, TABLES.ENGINE/CREATE_OPTIONS) exists in 5.7 and 8.x.
// Cross-database FK edges are captured (REFERENCED_TABLE_SCHEMA may differ). All
// string results arrive as raw bytes (character_set_results=binary) and scan
// into strings faithfully.
func introspectDB(ctx context.Context, db *sql.DB, sel engine.Selection) (*engine.Schema, error) {
	candidates, engines, partitioned, err := listTables(ctx, db)
	if err != nil {
		return nil, err
	}
	selected, err := selectTables(candidates, sel)
	if err != nil {
		return nil, err
	}

	schema := &engine.Schema{Tables: make([]engine.Table, 0, len(selected))}
	for _, ref := range selected {
		cols, err := introspectColumns(ctx, db, ref)
		if err != nil {
			return nil, err
		}
		pk, uniques, err := introspectKeys(ctx, db, ref)
		if err != nil {
			return nil, err
		}
		fks, err := introspectForeignKeys(ctx, db, ref)
		if err != nil {
			return nil, err
		}
		schema.Tables = append(schema.Tables, engine.Table{
			Ref:           ref,
			Columns:       cols,
			PrimaryKey:    pk,
			UniqueKeys:    uniques,
			ForeignKeys:   fks,
			Partitioned:   partitioned[ref],
			StorageEngine: engines[ref],
		})
	}
	return schema, nil
}

// listTables returns all user base tables plus their storage engine and
// partitioned flag, keyed by ref.
func listTables(ctx context.Context, db *sql.DB) ([]engine.TableRef, map[engine.TableRef]string, map[engine.TableRef]bool, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT TABLE_SCHEMA, TABLE_NAME, IFNULL(ENGINE, ''), IFNULL(CREATE_OPTIONS, '')
		FROM information_schema.TABLES
		WHERE TABLE_TYPE = 'BASE TABLE'
		ORDER BY TABLE_SCHEMA, TABLE_NAME`)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("mysql: list tables: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var refs []engine.TableRef
	engines := map[engine.TableRef]string{}
	partitioned := map[engine.TableRef]bool{}
	for rows.Next() {
		var schema, name, eng, createOpts string
		if err := rows.Scan(&schema, &name, &eng, &createOpts); err != nil {
			return nil, nil, nil, fmt.Errorf("mysql: scan table: %w", err)
		}
		if systemSchemas[schema] {
			continue
		}
		ref := engine.TableRef{Schema: schema, Name: name}
		refs = append(refs, ref)
		engines[ref] = eng
		partitioned[ref] = strings.Contains(strings.ToLower(createOpts), "partitioned")
	}
	return refs, engines, partitioned, rows.Err()
}

// introspectColumns reads columns in ordinal order, deriving generated /
// auto_increment / on-update-timestamp from EXTRA. DataType uses COLUMN_TYPE (the
// full declared type incl. length/unsigned) for faithful pre-flight comparison.
func introspectColumns(ctx context.Context, db *sql.DB, t engine.TableRef) ([]engine.Column, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, IFNULL(EXTRA, '')
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION`, t.Schema, t.Name)
	if err != nil {
		return nil, fmt.Errorf("mysql: columns %s: %w", t, err)
	}
	defer func() { _ = rows.Close() }()

	var cols []engine.Column
	for rows.Next() {
		var name, colType, nullable, extra string
		if err := rows.Scan(&name, &colType, &nullable, &extra); err != nil {
			return nil, fmt.Errorf("mysql: scan column %s: %w", t, err)
		}
		e := strings.ToLower(extra)
		cols = append(cols, engine.Column{
			Name:       name,
			DataType:   colType,
			Nullable:   nullable == "YES",
			Generated:  strings.Contains(e, "generated"),      // VIRTUAL or STORED
			Identity:   strings.Contains(e, "auto_increment"), // auto_increment
			AutoUpdate: strings.Contains(e, "on update current_timestamp"),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(cols) == 0 {
		return nil, fmt.Errorf("mysql: table %s not found or has no columns", t)
	}
	return cols, nil
}

// introspectKeys reads the primary key and usable unique keys from STATISTICS.
// Non-unique indexes are ignored (not replication identities).
func introspectKeys(ctx context.Context, db *sql.DB, t engine.TableRef) (*engine.Key, []engine.Key, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, COLUMN_NAME
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY INDEX_NAME, SEQ_IN_INDEX`, t.Schema, t.Name)
	if err != nil {
		return nil, nil, fmt.Errorf("mysql: keys %s: %w", t, err)
	}
	defer func() { _ = rows.Close() }()

	type keyAcc struct {
		cols []string
		isPK bool
	}
	order := []string{}
	byName := map[string]*keyAcc{}
	for rows.Next() {
		var idx, col string
		var nonUnique, seq int
		if err := rows.Scan(&idx, &nonUnique, &seq, &col); err != nil {
			return nil, nil, fmt.Errorf("mysql: scan key %s: %w", t, err)
		}
		_ = seq // ordering is enforced by ORDER BY SEQ_IN_INDEX
		if nonUnique != 0 {
			continue // non-unique index: not a replication identity
		}
		acc, ok := byName[idx]
		if !ok {
			acc = &keyAcc{isPK: idx == "PRIMARY"}
			byName[idx] = acc
			order = append(order, idx)
		}
		acc.cols = append(acc.cols, col)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	var pk *engine.Key
	var uniques []engine.Key
	for _, name := range order {
		acc := byName[name]
		k := engine.Key{Name: name, Columns: acc.cols, IsPrimary: acc.isPK}
		if acc.isPK {
			kk := k
			pk = &kk
		} else {
			uniques = append(uniques, k)
		}
	}
	return pk, uniques, nil
}

// introspectForeignKeys reads FK edges (child -> parent), including
// cross-database references. MySQL FKs are never deferrable.
func introspectForeignKeys(ctx context.Context, db *sql.DB, t engine.TableRef) ([]engine.ForeignKey, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT CONSTRAINT_NAME, COLUMN_NAME,
		       REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION`, t.Schema, t.Name)
	if err != nil {
		return nil, fmt.Errorf("mysql: foreign keys %s: %w", t, err)
	}
	defer func() { _ = rows.Close() }()

	order := []string{}
	byName := map[string]*engine.ForeignKey{}
	for rows.Next() {
		var name, childCol, parentSchema, parentName, parentCol string
		if err := rows.Scan(&name, &childCol, &parentSchema, &parentName, &parentCol); err != nil {
			return nil, fmt.Errorf("mysql: scan foreign key %s: %w", t, err)
		}
		fk, ok := byName[name]
		if !ok {
			fk = &engine.ForeignKey{
				Name:   name,
				Child:  t,
				Parent: engine.TableRef{Schema: parentSchema, Name: parentName},
			}
			byName[name] = fk
			order = append(order, name)
		}
		fk.ChildCols = append(fk.ChildCols, childCol)
		fk.ParentCols = append(fk.ParentCols, parentCol)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]engine.ForeignKey, 0, len(order))
	for _, name := range order {
		out = append(out, *byName[name])
	}
	return out, nil
}
