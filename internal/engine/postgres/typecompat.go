package postgres

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// Pre-flight type-compatibility classification (CLAUDE.md §4.2). replicare is a
// faithful text transporter — the target's input function is the real validator —
// so this check is a best-effort upfront classification that turns likely
// mid-stream failures into an actionable report, not a substitute for the
// target's own validation.
//
// Each source->target column pair is classified:
//
//   - Identical / Widen  -> OK (start).
//   - Risky              -> WARN (possible loss/truncation; operator decides).
//   - Incompatible       -> BLOCK (refuse to start).
//
// Types are compared by canonical name (as produced by format_type) grouped into
// families. Unrecognized-but-equal types are Identical; unrecognized-and-
// different types are Risky (we can't prove incompatibility, so we surface it
// rather than hard-blocking a valid custom-type setup).

// CompatLevel is the classification of one source->target type pair.
type CompatLevel int

const (
	CompatIdentical CompatLevel = iota
	CompatWiden
	CompatRisky
	CompatIncompatible
)

func (l CompatLevel) String() string {
	switch l {
	case CompatIdentical:
		return "identical"
	case CompatWiden:
		return "widening"
	case CompatRisky:
		return "risky"
	case CompatIncompatible:
		return "incompatible"
	default:
		return "unknown"
	}
}

// OK reports whether the level permits starting (identical or safe widening).
func (l CompatLevel) OK() bool { return l == CompatIdentical || l == CompatWiden }

// Blocks reports whether the level must refuse start.
func (l CompatLevel) Blocks() bool { return l == CompatIncompatible }

// ColumnCompat is the classification of one matched column pair.
type ColumnCompat struct {
	Column     string
	SourceType string
	TargetType string
	Level      CompatLevel
	Detail     string
}

// TableCompat is the pre-flight column comparison for one table. A missing
// target table, or a (transported) source column absent from the target, blocks.
type TableCompat struct {
	Table           engine.TableRef
	TargetExists    bool
	Columns         []ColumnCompat // matched columns, classified
	MissingInTarget []string       // transported source columns absent from target (block)
	ExtraNotNull    []string       // target NOT NULL columns with no source data (warn, best-effort)
}

// Blocks reports whether this table's comparison prevents starting the sync.
func (tc TableCompat) Blocks() bool {
	if !tc.TargetExists {
		return true
	}
	if len(tc.MissingInTarget) > 0 {
		return true
	}
	for _, c := range tc.Columns {
		if c.Level.Blocks() {
			return true
		}
	}
	return false
}

// compareTable matches source and target columns by name and classifies each
// transported pair. Source GENERATED columns are excluded from transport (the
// target regenerates them), so they are never required in the target.
func compareTable(src engine.Table, tgt *engine.Table) TableCompat {
	out := TableCompat{Table: src.Ref, TargetExists: tgt != nil}
	if tgt == nil {
		return out
	}

	tgtByName := make(map[string]engine.Column, len(tgt.Columns))
	for _, c := range tgt.Columns {
		tgtByName[c.Name] = c
	}
	srcNames := make(map[string]bool, len(src.Columns))

	for _, sc := range src.Columns {
		if sc.Generated {
			continue // not transported; target regenerates it
		}
		srcNames[sc.Name] = true
		tc, ok := tgtByName[sc.Name]
		if !ok {
			out.MissingInTarget = append(out.MissingInTarget, sc.Name)
			continue
		}
		level, detail := classifyTypes(sc.DataType, tc.DataType)
		out.Columns = append(out.Columns, ColumnCompat{
			Column:     sc.Name,
			SourceType: sc.DataType,
			TargetType: tc.DataType,
			Level:      level,
			Detail:     detail,
		})
	}

	// Target NOT NULL columns we won't supply (and that aren't in the source):
	// best-effort warn, since without a default the insert would fail. We can't
	// see defaults yet, so this is advisory.
	for _, tc := range tgt.Columns {
		if !srcNames[tc.Name] && !tc.Nullable && !tc.Generated {
			out.ExtraNotNull = append(out.ExtraNotNull, tc.Name)
		}
	}

	sort.Strings(out.MissingInTarget)
	sort.Strings(out.ExtraNotNull)
	sort.Slice(out.Columns, func(i, j int) bool { return out.Columns[i].Column < out.Columns[j].Column })
	return out
}

// classifyTypes classifies a source->target canonical type pair.
func classifyTypes(srcType, tgtType string) (CompatLevel, string) {
	if srcType == tgtType {
		return CompatIdentical, ""
	}
	s := parseType(srcType)
	t := parseType(tgtType)
	sf, tf := s.family(), t.family()

	// Target text can faithfully hold any value's text form, unless a length
	// limit risks truncation.
	if tf == famText {
		return classifyToText(s, t)
	}

	if sf == tf {
		return classifySameFamily(sf, s, t)
	}
	return classifyCrossFamily(sf, tf, s, t)
}

func classifyToText(s, t pgType) (CompatLevel, string) {
	limit := t.textLimit()
	if limit < 0 {
		return CompatWiden, "target holds any value as text"
	}
	// Bounded target: safe only if the source is text-family with a known equal
	// or smaller bound.
	if s.family() == famText {
		sl := s.textLimit()
		if sl >= 0 && sl <= limit {
			return CompatWiden, "target text length accommodates source"
		}
	}
	return CompatRisky, fmt.Sprintf("target length %d may truncate the source value", limit)
}

func classifySameFamily(fam string, s, t pgType) (CompatLevel, string) {
	switch fam {
	case famInt:
		if t.intWidth() >= s.intWidth() {
			return CompatWiden, "wider or equal integer"
		}
		return CompatRisky, "narrower integer may overflow"
	case famDecimal:
		return classifyNumeric(s, t)
	case famFloat:
		if t.floatWidth() >= s.floatWidth() {
			return CompatWiden, "wider or equal float"
		}
		return CompatRisky, "narrower float loses precision"
	case famTimestamp, famTimestampTZ, famTime, famTimeTZ:
		return classifyPrecision(s, t)
	case famOther:
		// Two distinct unrecognized types are not provably compatible.
		return CompatRisky, "unrecognized type pair; verify the target accepts the source text form"
	default:
		// A recognized single-form family (bool/uuid/date/json/...): different
		// spelling of the same type, no modifiers to compare.
		return CompatWiden, "same type family"
	}
}

// classifyNumeric compares numeric(p,s) precision/scale. Unlimited target is a
// widening; a smaller precision or scale is risky.
func classifyNumeric(s, t pgType) (CompatLevel, string) {
	if !t.hasMods() {
		return CompatWiden, "unconstrained numeric target"
	}
	if !s.hasMods() {
		return CompatRisky, "constraining an unconstrained numeric may overflow"
	}
	sp, ss := s.numericPrecisionScale()
	tp, ts := t.numericPrecisionScale()
	if tp >= sp && ts >= ss {
		return CompatWiden, "wider or equal numeric precision/scale"
	}
	return CompatRisky, "narrower numeric precision/scale may lose data"
}

// classifyPrecision compares temporal precision (fractional seconds). More or
// equal precision is widening; less is risky.
func classifyPrecision(s, t pgType) (CompatLevel, string) {
	sp := s.firstMod(6)
	tp := t.firstMod(6)
	if tp >= sp {
		return CompatWiden, "equal or higher temporal precision"
	}
	return CompatRisky, "lower temporal precision truncates fractional seconds"
}

// classifyCrossFamily handles known cross-family pairs; anything else recognized
// is incompatible, while unrecognized types are surfaced as risky.
func classifyCrossFamily(sf, tf string, s, t pgType) (CompatLevel, string) {
	type pair struct{ from, to string }
	rules := map[pair]struct {
		level  CompatLevel
		detail string
	}{
		{famInt, famDecimal}:           {widenOrRisky(t.numericHoldsIntegers()), "integer into numeric"},
		{famInt, famFloat}:             {CompatRisky, "large integers lose precision as float"},
		{famDecimal, famInt}:           {CompatRisky, "numeric into integer loses the fractional part"},
		{famDecimal, famFloat}:         {CompatRisky, "numeric into float may lose precision"},
		{famFloat, famDecimal}:         {CompatRisky, "float into numeric may lose precision"},
		{famFloat, famInt}:             {CompatRisky, "float into integer loses the fractional part"},
		{famDate, famTimestamp}:        {CompatWiden, "date widens to timestamp"},
		{famDate, famTimestampTZ}:      {CompatRisky, "date into timestamptz assumes a time zone"},
		{famTimestamp, famTimestampTZ}: {CompatRisky, "timestamp into timestamptz reinterprets the wall clock"},
		{famTimestampTZ, famTimestamp}: {CompatRisky, "timestamptz into timestamp drops the zone"},
		{famTimestamp, famDate}:        {CompatRisky, "timestamp into date drops the time"},
		{famTimestampTZ, famDate}:      {CompatRisky, "timestamptz into date drops the time"},
		{famTime, famTimeTZ}:           {CompatRisky, "time into timetz assumes a zone"},
		{famTimeTZ, famTime}:           {CompatRisky, "timetz into time drops the zone"},
		{famJSON, famJSONB}:            {CompatWiden, "json into jsonb"},
		{famJSONB, famJSON}:            {CompatWiden, "jsonb into json"},
	}
	if r, ok := rules[pair{sf, tf}]; ok {
		return r.level, r.detail
	}
	// Unrecognized on either side: can't prove incompatibility -> surface risk.
	if sf == famOther || tf == famOther {
		return CompatRisky, "unrecognized type pair; verify the target accepts the source text form"
	}
	return CompatIncompatible, fmt.Sprintf("%s and %s are not compatible", sf, tf)
}

func widenOrRisky(ok bool) CompatLevel {
	if ok {
		return CompatWiden
	}
	return CompatRisky
}

// --- type parsing & family model ---

const (
	famInt         = "integer"
	famDecimal     = "numeric"
	famFloat       = "float"
	famText        = "text"
	famBool        = "boolean"
	famUUID        = "uuid"
	famDate        = "date"
	famTimestamp   = "timestamp"
	famTimestampTZ = "timestamptz"
	famTime        = "time"
	famTimeTZ      = "timetz"
	famJSON        = "json"
	famJSONB       = "jsonb"
	famBytea       = "bytea"
	famInterval    = "interval"
	famOther       = "other"
)

// pgType is a parsed canonical Postgres type: a base name (modifiers removed)
// plus the numeric modifiers that were in parentheses.
type pgType struct {
	base string
	mods []int
}

var typeModRE = regexp.MustCompile(`\(\s*([0-9]+(?:\s*,\s*[0-9]+)*)\s*\)`)

// parseType strips the "(...)" modifiers from a canonical type string (they can
// appear mid-string, e.g. "timestamp(3) without time zone") and records them.
func parseType(s string) pgType {
	s = strings.ToLower(strings.TrimSpace(s))
	var mods []int
	if m := typeModRE.FindStringSubmatch(s); m != nil {
		for _, part := range strings.Split(m[1], ",") {
			if n, err := strconv.Atoi(strings.TrimSpace(part)); err == nil {
				mods = append(mods, n)
			}
		}
	}
	base := strings.TrimSpace(typeModRE.ReplaceAllString(s, ""))
	base = strings.Join(strings.Fields(base), " ") // collapse whitespace
	return pgType{base: base, mods: mods}
}

func (p pgType) hasMods() bool { return len(p.mods) > 0 }

func (p pgType) firstMod(def int) int {
	if len(p.mods) > 0 {
		return p.mods[0]
	}
	return def
}

func (p pgType) family() string {
	switch p.base {
	case "smallint", "int2", "integer", "int", "int4", "bigint", "int8":
		return famInt
	case "numeric", "decimal":
		return famDecimal
	case "real", "float4", "double precision", "float8":
		return famFloat
	case "text", "character varying", "varchar", "character", "char", "bpchar", `"char"`:
		return famText
	case "boolean", "bool":
		return famBool
	case "uuid":
		return famUUID
	case "date":
		return famDate
	case "timestamp without time zone", "timestamp":
		return famTimestamp
	case "timestamp with time zone", "timestamptz":
		return famTimestampTZ
	case "time without time zone", "time":
		return famTime
	case "time with time zone", "timetz":
		return famTimeTZ
	case "json":
		return famJSON
	case "jsonb":
		return famJSONB
	case "bytea":
		return famBytea
	case "interval":
		return famInterval
	default:
		return famOther
	}
}

func (p pgType) intWidth() int {
	switch p.base {
	case "smallint", "int2":
		return 16
	case "integer", "int", "int4":
		return 32
	case "bigint", "int8":
		return 64
	default:
		return 0
	}
}

func (p pgType) floatWidth() int {
	switch p.base {
	case "real", "float4":
		return 32
	case "double precision", "float8":
		return 64
	default:
		return 0
	}
}

// textLimit returns the declared length for a text type, or -1 for unlimited
// (text, or character varying with no length). character/char defaults to 1.
func (p pgType) textLimit() int {
	switch p.base {
	case "text":
		return -1
	case "character varying", "varchar":
		if p.hasMods() {
			return p.mods[0]
		}
		return -1
	case "character", "char", "bpchar":
		if p.hasMods() {
			return p.mods[0]
		}
		return 1
	default:
		return -1
	}
}

func (p pgType) numericPrecisionScale() (precision, scale int) {
	if len(p.mods) >= 1 {
		precision = p.mods[0]
	}
	if len(p.mods) >= 2 {
		scale = p.mods[1]
	}
	return precision, scale
}

// numericHoldsIntegers reports whether the target numeric can hold arbitrary
// integers (unconstrained precision, or scale 0 with generous precision).
func (p pgType) numericHoldsIntegers() bool {
	if !p.hasMods() {
		return true
	}
	prec, scale := p.numericPrecisionScale()
	return scale == 0 && prec >= 19 // covers int8 (max 19 digits)
}
