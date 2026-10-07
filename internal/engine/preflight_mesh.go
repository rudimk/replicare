package engine

import (
	"fmt"
	"strings"
)

// MeshIDAllocCategory is the pre-flight Finding category for active-active
// id-allocation policing (CLAUDE.md §6, docs/multi-master.md §5.6, §6.3).
const MeshIDAllocCategory = "mesh-id-alloc"

// MeshIDAllocationFindings polices id allocation for an active-active (mesh) member.
// It is PURE over the introspected schema (no I/O), so it is engine-neutral and
// CI-testable: each engine's introspection sets Column.Identity / Column.DefaultSequence
// / Column.DataType, and this classifier reasons about them the same way.
//
// The hazard (docs/multi-master.md §5.6): in a converged mesh every node holds the
// union of all rows and allocates from the same local counter, so two nodes mint the
// same integer id for DIFFERENT rows; they replicate and HLC last-write-wins silently
// discards one — the §1.7 cardinal sin. replicare cannot de-collide independently minted
// ids after the fact (allocation is upstream of capture), so the only safe posture is to
// refuse at startup and point the operator at globally-unique keys (UUID v7 / ULID).
//
// It flags, as SevBlock, every replicated table that has a LOCALLY-ALLOCATED INTEGER
// column (serial/identity/AUTO_INCREMENT — Identity || DefaultSequence, integer-typed)
// participating in its PRIMARY KEY or any SECONDARY UNIQUE key. A narrow (32-bit or
// smaller) key gets an extra-loud note, since even the deferred integer-window fallback
// has little range to work with there. Keys that are NOT locally allocated — UUID/text
// PKs, natural keys, or app-assigned globally-unique integers (e.g. snowflake, where the
// DB does not generate the value, so Identity and DefaultSequence are both false) — pass:
// those ARE the collision-free schemes, so there is nothing to refuse.
//
// It returns findings only; the caller decides whether to block (the daemon refuses to
// bring up a cluster edge with any block finding). One finding per offending column.
func MeshIDAllocationFindings(s *Schema) []Finding {
	if s == nil {
		return nil
	}
	var out []Finding
	for _, t := range s.Tables {
		out = append(out, meshFindingsForTable(t)...)
	}
	return out
}

func meshFindingsForTable(t Table) []Finding {
	// Index columns by name for type/allocation lookup.
	cols := make(map[string]Column, len(t.Columns))
	for _, c := range t.Columns {
		cols[c.Name] = c
	}

	// For each column, record the key role it plays (prefer the primary key for the
	// message when a column is in both), preserving a stable report order.
	type role struct {
		inPrimary bool
		uniqueKey string // name of a secondary unique key it participates in (first seen)
	}
	roles := map[string]*role{}
	var order []string
	note := func(col string) *role {
		r := roles[col]
		if r == nil {
			r = &role{}
			roles[col] = r
			order = append(order, col)
		}
		return r
	}
	if t.PrimaryKey != nil {
		for _, c := range t.PrimaryKey.Columns {
			note(c).inPrimary = true
		}
	}
	for _, k := range t.UniqueKeys {
		for _, c := range k.Columns {
			r := note(c)
			if r.uniqueKey == "" {
				r.uniqueKey = k.Name
			}
		}
	}

	var out []Finding
	for _, name := range order {
		col, ok := cols[name]
		if !ok {
			continue // key references an unknown column; the FK/key introspection owns that
		}
		if !col.Identity && !col.DefaultSequence {
			continue // not locally DB-allocated → app/natural key → collision-free, allowed
		}
		isInt, narrow := classifyIntType(col.DataType)
		if !isInt {
			continue // a locally-allocated non-integer key (rare) is not the counter hazard
		}
		r := roles[name]
		keyRole := "a unique key"
		if r.inPrimary {
			keyRole = "the primary key"
		} else if r.uniqueKey != "" {
			keyRole = fmt.Sprintf("unique key %q", r.uniqueKey)
		}
		msg := fmt.Sprintf(
			"%s.%s is a locally-allocated integer (%s) in %s — in an active-active mesh "+
				"every node allocates from the same counter, so two nodes mint the same id "+
				"for different rows and HLC last-write-wins silently drops one. Use "+
				"globally-unique keys (UUID v7 / ULID); see docs/multi-master.md §5.6.",
			t.Ref, col.Name, strings.TrimSpace(col.DataType), keyRole)
		if narrow {
			msg += " (This key is 32-bit or narrower, so even the deferred integer-window fallback would have little range.)"
		}
		out = append(out, Finding{
			Severity: SevBlock,
			Table:    t.Ref,
			Category: MeshIDAllocCategory,
			Message:  msg,
		})
	}
	return out
}

// classifyIntType reports whether a catalog data-type string names an integer type,
// and whether that integer is narrow (32-bit or smaller — Postgres int4/integer and
// below, MySQL int/mediumint/smallint/tinyint, and the serial family). It normalizes
// Postgres format_type output ("integer", "bigint") and MySQL COLUMN_TYPE ("int(11)
// unsigned", "bigint unsigned") alike.
func classifyIntType(dataType string) (isInt, narrow bool) {
	base := strings.ToLower(strings.TrimSpace(dataType))
	// Drop a display width / modifier — "int(11)" / "numeric(10,0)" → "int" / "numeric".
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = base[:i]
	}
	// Drop trailing modifiers like "unsigned", "zerofill".
	base = strings.TrimSpace(strings.Fields(base + " ")[0])

	switch base {
	case "bigint", "int8", "bigserial", "serial8":
		return true, false
	case "integer", "int", "int4", "serial", "serial4",
		"smallint", "int2", "smallserial", "serial2",
		"mediumint", "tinyint":
		return true, true
	default:
		return false, false
	}
}
