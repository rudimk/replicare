package postgres

import (
	"context"
	"fmt"
	"math"

	"github.com/rudimk/replicare/internal/engine"
)

// Storage-order copy (CLAUDE.md §4.1 "storage-order copy"). A key-range chunk reads
// its rows through the key index; when the table's rows are not stored in key order
// (pg_stats.correlation of the key's first column near 0), every row is a random read
// of a different block, and a large table copies at the speed of the source's disk
// seeks. Reading block ranges instead is sequential. Postgres 14+ reads a ctid range
// directly (TID range scan); an older server would scan the whole table for every
// chunk, so storage order is never used there.

const (
	// storageOrderMinVersion is the first Postgres with TID range scans.
	storageOrderMinVersion = 140000
	// storageOrderMinBytes is the smallest table auto picks storage order for: below
	// it, a key-order copy is quick whatever the order on disk.
	storageOrderMinBytes = 1 << 30
	// storageOrderMaxCorrelation: auto picks storage order when the key's first column
	// is stored further from key order than this (|correlation| below it).
	storageOrderMaxCorrelation = 0.9
	// storageChunkMinBlocks is the smallest storage-order chunk (8 MiB at the default
	// block size), so each chunk is one long sequential read.
	storageChunkMinBlocks = 1024
)

// StorageOrder implements engine.StorageOrderReader.
func (s *Source) StorageOrder(ctx context.Context, t engine.TableRef, pref engine.CopyOrder) (engine.StorageOrderChoice, error) {
	if err := s.requireConn(); err != nil {
		return engine.StorageOrderChoice{}, err
	}
	version, err := serverVersion(ctx, s.conn)
	if err != nil {
		return engine.StorageOrderChoice{}, err
	}
	if version < storageOrderMinVersion {
		return engine.StorageOrderChoice{Reason: "the source runs a Postgres older than 14, which cannot read a range of storage positions directly"}, nil
	}
	table, err := s.tableMeta(ctx, t)
	if err != nil {
		return engine.StorageOrderChoice{}, err
	}
	keyCol := ""
	if cols := captureColsFor(table); len(cols) > 0 {
		keyCol = cols[0].Name
	}
	var (
		relkind     string
		file        int64
		size        int64
		correlation *float64
	)
	err = s.conn.QueryRow(ctx, `
		SELECT c.relkind::text,
		       COALESCE(pg_relation_filenode(c.oid)::bigint, 0),
		       pg_relation_size(c.oid),
		       (SELECT st.correlation::float8 FROM pg_stats st
		         WHERE st.schemaname = n.nspname AND st.tablename = c.relname
		           AND st.attname = $2 AND NOT st.inherited
		         LIMIT 1)
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE c.oid = $1::regclass`, qualifyTable(t), keyCol).Scan(&relkind, &file, &size, &correlation)
	if err != nil {
		return engine.StorageOrderChoice{}, fmt.Errorf("postgres: read storage stats for %s: %w", t, err)
	}
	return decideStorageOrder(relkind, file, size, correlation, pref), nil
}

// decideStorageOrder is StorageOrder's decision once the server is known to be 14+
// (pure, for tests).
func decideStorageOrder(relkind string, file, size int64, correlation *float64, pref engine.CopyOrder) engine.StorageOrderChoice {
	if relkind != "r" || file == 0 {
		return engine.StorageOrderChoice{Reason: "not an ordinary table (a partitioned table is read in key order)"}
	}
	if pref == engine.CopyOrderStorage {
		return engine.StorageOrderChoice{Use: true, File: file, Reason: "storage order was asked for"}
	}
	if size < storageOrderMinBytes {
		return engine.StorageOrderChoice{File: file, Reason: fmt.Sprintf("smaller than 1 GiB (%s)", humanBytes(size))}
	}
	if correlation == nil {
		return engine.StorageOrderChoice{File: file, Reason: "no statistics on how the key is stored (the table has not been analysed)"}
	}
	if math.Abs(*correlation) >= storageOrderMaxCorrelation {
		return engine.StorageOrderChoice{File: file, Reason: fmt.Sprintf("stored close to key order (correlation %.2f)", *correlation)}
	}
	return engine.StorageOrderChoice{Use: true, File: file,
		Reason: fmt.Sprintf("%s, stored out of key order (correlation %.2f)", humanBytes(size), *correlation)}
}

// PlanStorageChunks implements engine.StorageOrderReader: block ranges from block
// `from` to the end of the table, sized from the table's rows per block so each holds
// about targetRows rows, never fewer than storageChunkMinBlocks blocks.
func (s *Source) PlanStorageChunks(ctx context.Context, t engine.TableRef, from int64, targetRows int) ([]engine.Chunk, error) {
	if err := s.requireConn(); err != nil {
		return nil, err
	}
	if targetRows <= 0 {
		targetRows = defaultChunkRows
	}
	var (
		blocks    int64
		relpages  int64
		reltuples float64
	)
	err := s.conn.QueryRow(ctx, `
		SELECT pg_relation_size(c.oid) / current_setting('block_size')::bigint, c.relpages::bigint, c.reltuples::float8
		FROM pg_class c WHERE c.oid = $1::regclass`, qualifyTable(t)).Scan(&blocks, &relpages, &reltuples)
	if err != nil {
		return nil, fmt.Errorf("postgres: read block count for %s: %w", t, err)
	}
	return storageChunks(t, from, blocks, relpages, reltuples, targetRows), nil
}

// storageChunks builds the block-range chunks (pure, for tests). The first starts at
// `from` (unbounded below when 0), the last is unbounded above.
func storageChunks(t engine.TableRef, from, blocks, relpages int64, reltuples float64, targetRows int) []engine.Chunk {
	per := int64(storageChunkMinBlocks)
	if relpages > 0 && reltuples > 0 {
		if n := int64(float64(targetRows) / (reltuples / float64(relpages))); n > per {
			per = n
		}
	}
	var lo engine.KeyValues
	if from > 0 {
		lo = engine.KeyValues{int(from)}
	}
	var out []engine.Chunk
	for b := from + per; b < blocks; b += per {
		hi := engine.KeyValues{int(b)}
		out = append(out, engine.Chunk{Table: t, Method: engine.ChunkCTID, Lo: lo, Hi: hi})
		lo = hi
	}
	return append(out, engine.Chunk{Table: t, Method: engine.ChunkCTID, Lo: lo})
}

// StorageFile implements engine.StorageOrderReader: the table's relfilenode, which
// every rewrite of the table changes.
func (s *Source) StorageFile(ctx context.Context, t engine.TableRef) (int64, error) {
	if err := s.requireConn(); err != nil {
		return 0, err
	}
	var file int64
	if err := s.conn.QueryRow(ctx, `SELECT COALESCE(pg_relation_filenode($1::regclass)::bigint, 0)`,
		qualifyTable(t)).Scan(&file); err != nil {
		return 0, fmt.Errorf("postgres: read storage identity of %s: %w", t, err)
	}
	return file, nil
}

// humanBytes renders a size for log messages.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

var _ engine.StorageOrderReader = (*Source)(nil)
