# Verifying Data Integrity on MySQL

This page collects standalone SQL that confirms a MySQL source and target have converged, for
when the `replicare` pod can't be reached to run [`replicare status`](cli.md) /
[`replicare verify`](cli.md). Run each block on both databases and compare.

Like the [Postgres guide](integrity-checks-postgres.md), these queries match columns by name,
exclude generated columns, and use an order-independent content hash. Here, that's `BIT_XOR`
over a 64-bit slice of each row's MD5, which folds the same way regardless of row order and has
no `GROUP_CONCAT` length ceiling.

> ⚠️ The authoritative check is still `replicare verify <config>`. These queries are the
> fallback for when there's no pod to run it from.

## 0. Pin the session, run on both in every session

`replicare` moves MySQL values byte-faithfully(`LOAD DATA … CHARACTER SET binary`), so the main
cross-version rendering risk in a manual hash is the time zone. Pin it to UTC on both sides
before hashing.

```sql
SET SESSION time_zone = '+00:00';
SET SESSION group_concat_max_len = 1048576;  -- so the generator below isn't truncated
```

## 1. Row counts per table, run on both

`information_schema.TABLES.TABLE_ROWS` is only an estimate for InnoDB, so we generate exact
`COUNT(*)`s instead. Run this to emit one `SELECT … UNION ALL …`, then run the emitted statement:

```sql
SELECT GROUP_CONCAT(
         CONCAT('SELECT ''', TABLE_NAME, ''' AS table_name, COUNT(*) AS `rows` ',
                'FROM `', TABLE_SCHEMA, '`.`', TABLE_NAME, '`')
         ORDER BY TABLE_NAME SEPARATOR '\nUNION ALL\n') AS sql_to_run
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE';
```

A hot table reads a few rows ahead on the source, by whatever's in flight; a gap that shrinks
towards 0 is normal streaming, not drift.

## 2. Content digest per table, run on both; the digests must match

This generates a single statement that hashes every table, with name-matched columns, generated
columns excluded, and a `BIT_XOR` fold so row order doesn't matter. Run the generator, then run
the emitted `SELECT … UNION ALL …` on each side and compare `table_name`, `rows` and `digest`.

```sql
SELECT GROUP_CONCAT(
  CONCAT(
    'SELECT ''', tab.TABLE_NAME, ''' AS table_name, COUNT(*) AS `rows`, ',
    'LPAD(CONV(BIT_XOR(CAST(CONV(SUBSTRING(MD5(CONCAT_WS(''#'', ',
        tab.collist,
    ')), 1, 16), 16, 10) AS UNSIGNED)), 16, 16), 16, ''0'') AS digest ',
    'FROM `', DATABASE(), '`.`', tab.TABLE_NAME, '`'
  )
  ORDER BY tab.TABLE_NAME SEPARATOR '\nUNION ALL\n'
) AS sql_to_run
FROM (
  SELECT c.TABLE_NAME,
         GROUP_CONCAT(CONCAT('COALESCE(CAST(`', c.COLUMN_NAME, '` AS CHAR), ''\\0N'')')
                      ORDER BY c.COLUMN_NAME SEPARATOR ', ') AS collist
  FROM information_schema.COLUMNS c
  JOIN information_schema.TABLES t
    ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME
   AND t.TABLE_TYPE = 'BASE TABLE'
  WHERE c.TABLE_SCHEMA = DATABASE()
    AND c.EXTRA NOT LIKE '%GENERATED%'   -- exclude generated columns
  GROUP BY c.TABLE_NAME
) tab;
```

Each column is `COALESCE`d to a sentinel(`\0N`), so a real `NULL` never collides with an empty
string, and `CONCAT_WS('#', …)` separates the columns, so `('a','bc')` and `('ab','c')` don't
hash alike.

## 3. Hot-table digest over a stable key range, run on both

Actively-written tables won't match as a whole, since the source runs ahead. Instead, hash an
immutable historical window - a key range below the active tail - which must match exactly.
Adjust the table, key column and range to a span that's known not to be updated.

```sql
SELECT COUNT(*),
       LPAD(CONV(BIT_XOR(CAST(CONV(SUBSTRING(
              MD5(CONCAT_WS('#', COALESCE(CAST(id AS CHAR),'\0N'),
                                 COALESCE(CAST(note AS CHAR),'\0N'))),
            1,16),16,10) AS UNSIGNED)),16,16),16,'0') AS digest
FROM `rc_it`.`orders`
WHERE id BETWEEN 1000000 AND 1010000;
```

## 4. Referential integrity, run on the target

This generates one anti-join per foreign key; run it, then run the emitted
`SELECT … UNION ALL …`. Every `orphans` count should be 0.

```sql
SELECT GROUP_CONCAT(q SEPARATOR '\nUNION ALL\n') AS sql_to_run FROM (
  SELECT CONCAT(
    'SELECT ''', k.CONSTRAINT_NAME, ''' AS fk, COUNT(*) AS orphans ',
    'FROM `', k.TABLE_SCHEMA, '`.`', k.TABLE_NAME, '` c ',
    'LEFT JOIN `', k.REFERENCED_TABLE_SCHEMA, '`.`', k.REFERENCED_TABLE_NAME, '` p ON ',
    GROUP_CONCAT(CONCAT('c.`', k.COLUMN_NAME, '` = p.`', k.REFERENCED_COLUMN_NAME, '`')
                 ORDER BY k.ORDINAL_POSITION SEPARATOR ' AND '),
    ' WHERE ',
    GROUP_CONCAT(CONCAT('c.`', k.COLUMN_NAME, '` IS NOT NULL')
                 ORDER BY k.ORDINAL_POSITION SEPARATOR ' AND '),
    ' AND p.`', MIN(k.REFERENCED_COLUMN_NAME), '` IS NULL'
  ) AS q
  FROM information_schema.KEY_COLUMN_USAGE k
  WHERE k.CONSTRAINT_SCHEMA = DATABASE()
    AND k.REFERENCED_TABLE_NAME IS NOT NULL
  GROUP BY k.CONSTRAINT_NAME, k.TABLE_SCHEMA, k.TABLE_NAME,
           k.REFERENCED_TABLE_SCHEMA, k.REFERENCED_TABLE_NAME
) fks;
```

## Caveats

> ⚠️ Live writes cause drift. On hot tables, use the stable-range check from step 3, or run the
> checks during a quiet window. The whole-table digest from step 2 is meant for static / lookup
> tables, which must match exactly.

There's one quirk of the `BIT_XOR` fold to be aware of: two identical rows XOR to zero and cancel
each other out. Every replicated MySQL table has a primary / unique key(`replicare` requires
one), so its rows are distinct and this is safe - but it's why the digest is a drift
*indicator*, with `verify` as the authority. It's the same fold `replicare`'s own Redis verify
uses.

The schema has to match, too. A missing or extra column on the target changes step 2's digest,
which correctly flags a real schema mismatch.

> ⚠️ MySQL has no `extra_float_digits` equivalent, so `FLOAT`/`DOUBLE` text can differ across
> major versions. If a float-heavy table shows a digest diff with matching counts, spot-check a
> few rows before treating it as real drift.

Finally - charsets. `CAST(... AS CHAR)` renders under the connection charset, so use the same
client charset on both sides(`replicare` reads under `character_set_results = binary`).

The equivalent checks for the other engines are in [Postgres integrity checks](integrity-checks-postgres.md)
and [Redis integrity checks](integrity-checks-redis.md).
