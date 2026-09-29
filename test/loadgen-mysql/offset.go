package main

import "fmt"

// Active-active support: each writer node claims a DISJOINT slice of every table's
// key space so N nodes can be seeded/churned concurrently and the mesh converges to
// the clean UNION (no cross-node same-key conflicts — that is the separate, future
// "conflict-storm" mode). A node's slice is [base+1, base+nodeStride), where
//
//	base = node_id * nodeStride
//
// Every generated key (AUTO_INCREMENT ids, order ids via MD5(CONCAT('order:',n)),
// product SKUs via CONCAT('SKU-',n)) and every FK cross-reference is shifted by base,
// so a node's rows only ever reference that node's own parents. node_id 0 → base 0 →
// byte-for-byte the original single-writer behaviour (active-passive), so the existing
// flow is unchanged: the offset is purely additive and only engages for node_id > 0.
//
// nodeStride bounds a node's key space. It is far larger than any realistic --scale
// (default seed ≈ 1M rows), so seeded + churn-inserted keys never reach the next
// node's base; and node_id up to ~9e9 stays within int64. A --scale whose row count
// approaches nodeStride is rejected in run (would risk overlapping the next node).
//
// This is the MySQL sibling of test/loadgen/offset.go (Postgres): the model is
// identical, only the emitted SQL dialect differs.
const nodeStride int64 = 1_000_000_000

// baseFor returns the key-space base for a node id.
func baseFor(nodeID int) int64 { return int64(nodeID) * nodeStride }

// partition is the SQL predicate selecting rows in THIS node's slice by a numeric key
// column (id or tenant_id). For node 0 (base 0) it is `col > 0 AND col < 1e9`, which
// matches every row (ids start at 1, counts ≪ nodeStride) — i.e. a no-op, preserving
// the original whole-table churn scope for the single-writer case.
func partition(col string, base int64) string {
	return fmt.Sprintf("%s > %d AND %s < %d", col, base, col, base+nodeStride)
}

// nodeTenantExpr is a random tenant_id owned by THIS node: base + 1 + rand*(count of
// this node's tenants). Used by churn inserts so new child rows reference a node-owned
// tenant, never one replicated in from a peer.
func nodeTenantExpr(base int64) string {
	return fmt.Sprintf("%d + 1 + FLOOR(RAND()*(SELECT COUNT(*) FROM loadgen.tenants WHERE %s))",
		base, partition("id", base))
}
