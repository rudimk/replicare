package main

import (
	"strings"
	"testing"
)

// intPow is a tiny integer power for the test's coverage assertions.
func intPow(base, exp int) int {
	r := 1
	for i := 0; i < exp; i++ {
		r *= base
	}
	return r
}

// TestGenRowsSpansN is the load-bearing check on the digit-table row generator: it stands
// in for Postgres generate_series on MySQL 5.7, and if it under-generates, seeds silently
// come up short. For each n the generator must cross-join just enough {0..9} digit tables
// that 10^d >= n (so `WHERE gs.g <= n` yields all n rows), and no more than needed.
func TestGenRowsSpansN(t *testing.T) {
	for _, n := range []int{1, 2, 9, 10, 11, 50, 99, 100, 101, 999, 1000, 5000, 20000, 100000, 1000000} {
		sql := genRows(n)
		d := strings.Count(sql, "SELECT 0 d UNION ALL")
		if d < 1 {
			t.Fatalf("genRows(%d): expected at least one digit table, got %d", n, d)
		}
		if got := intPow(10, d); got < n {
			t.Fatalf("genRows(%d): %d digit tables span only %d rows (< n)", n, d, got)
		}
		// Minimality: one fewer digit table must NOT already cover n (except the n<=10
		// floor, where a single digit table is the minimum).
		if n > 10 && intPow(10, d-1) >= n {
			t.Fatalf("genRows(%d): %d digit tables is not minimal (%d already covers n)", n, d, intPow(10, d-1))
		}
		if !strings.HasPrefix(sql, "(SELECT ") || !strings.HasSuffix(sql, ") gs") || !strings.Contains(sql, "+ 1 AS g") {
			t.Fatalf("genRows(%d): unexpected shape: %s", n, sql)
		}
	}
}

// TestOffsetHelpers pins the active-active partitioning arithmetic: node 0 is a whole-table
// no-op (active-passive unchanged), and node k claims the disjoint slice [k*1e9+1, (k+1)*1e9).
func TestOffsetHelpers(t *testing.T) {
	if baseFor(0) != 0 {
		t.Fatalf("baseFor(0) = %d, want 0", baseFor(0))
	}
	if baseFor(3) != 3*nodeStride {
		t.Fatalf("baseFor(3) = %d, want %d", baseFor(3), 3*nodeStride)
	}
	if got, want := partition("id", 0), "id > 0 AND id < 1000000000"; got != want {
		t.Fatalf("partition(id,0) = %q, want %q", got, want)
	}
	if got, want := partition("tenant_id", nodeStride), "tenant_id > 1000000000 AND tenant_id < 2000000000"; got != want {
		t.Fatalf("partition(tenant_id, 1e9) = %q, want %q", got, want)
	}
	// nodeTenantExpr must scope its tenant count to the node's own slice, so a node never
	// picks a tenant replicated in from a peer.
	if !strings.Contains(nodeTenantExpr(nodeStride), partition("id", nodeStride)) {
		t.Fatalf("nodeTenantExpr must scope tenant count to the node slice: %s", nodeTenantExpr(nodeStride))
	}
}
