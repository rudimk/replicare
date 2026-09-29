# Verifying data integrity — Postgres

Standalone `psql` queries to confirm a Postgres **source** and **target** are
converged, for when you can't reach the replicare pod to run
[`replicare status`](cli.md) / [`replicare verify`](cli.md). Run each block on
**both** databases and compare the output.

These mirror what `verify` does internally: columns are matched by **name**,
`GENERATED … STORED` columns are **excluded** (the target regenerates them), and the
content hash is **order-independent** (physical row order, index order, and text
collation don't affect it).

> The authoritative check is still `replicare verify <config>`. This is the no-pod
> fallback, and its results should agree with `verify`.

---

## 0. Pin session GUCs (run on both, every session)

Essential — replicare canonicalizes these on every connection. Row text must render
identically on both sides (an old source and a newer target) or you get false
mismatches on dates, floats, and `bytea`. Run this **first** in every session before
hashing.

```sql
SET DateStyle = 'ISO, YMD';
SET TimeZone = 'UTC';
SET extra_float_digits = 3;
SET IntervalStyle = 'postgres';
SET bytea_output = 'hex';
SET client_encoding = 'UTF8';
```

## 1. Row counts per table (run on both)

Exact counts for every base table in a schema, via a session-local helper
(`pg_temp.*` is dropped automatically at disconnect). A hot table reads a few rows
ahead on the source by whatever is in flight — a gap that **shrinks toward 0** is
normal streaming, not drift.

```sql
CREATE OR REPLACE FUNCTION pg_temp.rc_counts(sch text)
RETURNS TABLE(table_name text, rows bigint) AS $$
DECLARE r record;
BEGIN
  FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = sch ORDER BY tablename LOOP
    RETURN QUERY EXECUTE format('SELECT %L::text, count(*)::bigint FROM %I.%I',
                                r.tablename, sch, r.tablename);
  END LOOP;
END $$ LANGUAGE plpgsql;

SELECT * FROM pg_temp.rc_counts('public') ORDER BY table_name;
```

## 2. Content digest per table (run on both — digests must match)

The real integrity check: order-independent, column-name-matched, excludes
`GENERATED … STORED`. Identical `digest` per table = logically converged. Best for
static / lookup tables; for actively-written tables see [step 3](#3-hot-table-digest-over-a-stable-key-range-run-on-both).

```sql
CREATE OR REPLACE FUNCTION pg_temp.rc_verify(sch text)
RETURNS TABLE(table_name text, rows bigint, digest text) AS $$
DECLARE r record; cols text;
BEGIN
  FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = sch ORDER BY tablename LOOP
    -- name-sorted, generated-excluded column list: robust to column-order differences
    SELECT string_agg(quote_ident(column_name), ',' ORDER BY column_name)
      INTO cols
      FROM information_schema.columns
     WHERE table_schema = sch AND table_name = r.tablename AND is_generated = 'NEVER';
    IF cols IS NULL THEN CONTINUE; END IF;
    RETURN QUERY EXECUTE format(
      'SELECT %L::text, count(*)::bigint,
              md5(coalesce(string_agg(md5(ROW(%s)::text), '''' ORDER BY md5(ROW(%s)::text)), ''empty''))
         FROM %I.%I',
      r.tablename, cols, cols, sch, r.tablename);
  END LOOP;
END $$ LANGUAGE plpgsql;

SELECT * FROM pg_temp.rc_verify('public') ORDER BY table_name;
```

Diff the two sides cleanly inside `psql`, then compare the files in a shell:

```sql
-- in psql, on EACH side:
\pset format unaligned
\o /tmp/rc_src.txt          -- use /tmp/rc_tgt.txt on the target
SELECT * FROM pg_temp.rc_verify('public') ORDER BY table_name;
\o
```

```sh
# then in a shell:
diff /tmp/rc_src.txt /tmp/rc_tgt.txt   # no output = fully converged
```

## 3. Hot-table digest over a stable key range (run on both)

Actively-written tables won't match whole-table (the source runs ahead). Hash an
**immutable historical window** instead — a key range below the active tail — which
must match exactly. Adjust the table, key column, and range to a span you know isn't
being updated.

```sql
SELECT count(*),
       md5(string_agg(md5(t::text), '' ORDER BY id)) AS digest
FROM public.user_answer t
WHERE id BETWEEN 1000000 AND 1010000;
```

## 4. Referential integrity (run on target)

Catches any FK breakage from the copy. The query below **generates** one anti-join
per foreign key; run it, then paste and run the emitted `SELECT … UNION ALL …`. Every
`orphans` count should be 0.

```sql
SELECT string_agg(
  format('SELECT %L AS fk, count(*) AS orphans FROM %I.%I c LEFT JOIN %I.%I p ON %s WHERE %s AND %s',
    conname,
    cn.nspname, cc.relname, pn.nspname, pc.relname,
    (SELECT string_agg(format('c.%I = p.%I', a.attname, af.attname), ' AND ')
       FROM unnest(con.conkey)  WITH ORDINALITY k(attnum,ord)
       JOIN unnest(con.confkey) WITH ORDINALITY fk(attnum,ord) ON k.ord = fk.ord
       JOIN pg_attribute a  ON a.attrelid  = con.conrelid  AND a.attnum  = k.attnum
       JOIN pg_attribute af ON af.attrelid = con.confrelid AND af.attnum = fk.attnum),
    (SELECT string_agg(format('c.%I IS NOT NULL', a.attname), ' AND ')
       FROM unnest(con.conkey) k(attnum)
       JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum),
    format('p.%I IS NULL',
      (SELECT af.attname FROM pg_attribute af
        WHERE af.attrelid = con.confrelid AND af.attnum = con.confkey[1]))
  ), E'\nUNION ALL\n' ORDER BY conname)
FROM pg_constraint con
JOIN pg_class cc ON cc.oid = con.conrelid  JOIN pg_namespace cn ON cn.oid = cc.relnamespace
JOIN pg_class pc ON pc.oid = con.confrelid JOIN pg_namespace pn ON pn.oid = pc.relnamespace
WHERE con.contype = 'f' AND cn.nspname = 'public';
```

---

## Caveats

- **Live drift.** On hot tables use the step 3 stable-range check, or run during a
  quiet window. The whole-table digest (step 2) is for static / lookup tables, which
  must match exactly.
- **Schema must match.** If the target is missing or has an extra column, step 2's
  digest differs — that's correct, it's flagging a real schema mismatch, not spurious
  drift.
- **Order & collation don't matter.** Step 2 orders rows by their hash, so different
  heap / index order or text collation never cause a false diff.
- **Version gap is handled.** With the step 0 GUCs pinned, values render identically
  across a source/target major-version gap (floats, timestamps, `bytea`, intervals).
- **Nothing is left behind.** The `pg_temp.*` helpers are session-local and vanish at
  disconnect — safe to run against a source you don't own.

See also: [MySQL integrity checks](integrity-checks-mysql.md) ·
[Redis integrity checks](integrity-checks-redis.md).
