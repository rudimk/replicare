package mysql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// Type-compatibility classification for MySQL column pairs (mysql-plan §MM1b,
// CLAUDE.md §4.2): identical/widening are OK, narrowing/semantic-shift is a WARN
// (risky), and a structurally incompatible pair is a BLOCK. Faithful transport
// means the target's own input function is the final judge — so unknown or
// cross-family pairs default to WARN (the target halts loud at apply if truly
// bad) rather than a false BLOCK. Only clearly-incompatible pairs block up front.

type compatLevel int

const (
	compatIdentical compatLevel = iota
	compatWiden
	compatRisky
	compatIncompatible
)

func (l compatLevel) blocks() bool { return l == compatIncompatible }

// myType is a parsed MySQL COLUMN_TYPE, e.g. "int unsigned", "varchar(50)",
// "decimal(10,2)".
type myType struct {
	raw      string
	base     string // e.g. "int", "varchar", "decimal"
	unsigned bool
	args     []int // numeric parenthesized args (length / precision, scale)
}

func parseMyType(s string) myType {
	t := myType{raw: strings.TrimSpace(strings.ToLower(s))}
	body := t.raw
	t.unsigned = strings.Contains(body, "unsigned")
	// Strip trailing modifiers (" unsigned", " zerofill").
	if i := strings.IndexByte(body, ' '); i >= 0 {
		// keep only up to the first space unless the space is inside parens
		if p := strings.IndexByte(body, '('); p < 0 || i < p {
			body = body[:i]
		}
	}
	if p := strings.IndexByte(body, '('); p >= 0 {
		t.base = body[:p]
		inner := body[p+1:]
		if q := strings.IndexByte(inner, ')'); q >= 0 {
			inner = inner[:q]
		}
		for _, part := range strings.Split(inner, ",") {
			if n, err := strconv.Atoi(strings.TrimSpace(part)); err == nil {
				t.args = append(t.args, n)
			}
		}
	} else {
		t.base = body
	}
	return t
}

func (t myType) arg(i, def int) int {
	if i < len(t.args) {
		return t.args[i]
	}
	return def
}

// family groups a base type. Same family -> detailed rules; same group different
// family -> risky; disjoint groups -> mostly block.
func (t myType) family() string {
	switch t.base {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
		return "int"
	case "decimal", "numeric", "dec", "fixed":
		return "decimal"
	case "float", "double", "real", "double precision":
		return "float"
	case "bit":
		return "bit"
	case "char", "varchar":
		return "char"
	case "tinytext", "text", "mediumtext", "longtext":
		return "text"
	case "binary", "varbinary":
		return "binary"
	case "tinyblob", "blob", "mediumblob", "longblob":
		return "blob"
	case "date":
		return "date"
	case "time":
		return "time"
	case "datetime":
		return "datetime"
	case "timestamp":
		return "timestamp"
	case "year":
		return "year"
	case "enum":
		return "enum"
	case "set":
		return "set"
	case "json":
		return "json"
	default:
		return "geometry-or-other" // spatial and unknowns
	}
}

var intRank = map[string]int{"tinyint": 1, "smallint": 2, "mediumint": 3, "int": 4, "integer": 4, "bigint": 5}
var textRank = map[string]int{"tinytext": 1, "text": 2, "mediumtext": 3, "longtext": 4}
var blobRank = map[string]int{"tinyblob": 1, "blob": 2, "mediumblob": 3, "longblob": 4}

// classifyColumn classifies a source->target column pair including charset.
func classifyColumn(src, tgt engine.Column) (compatLevel, string) {
	lvl, reason := classifyTypes(src.DataType, tgt.DataType)
	// Charset narrowing applies to character families only.
	if cl, cr := classifyCharset(src, tgt); cl > lvl {
		lvl, reason = cl, cr
	}
	return lvl, reason
}

func classifyTypes(srcType, tgtType string) (compatLevel, string) {
	s, t := parseMyType(srcType), parseMyType(tgtType)
	if s.raw == t.raw {
		return compatIdentical, ""
	}
	sf, tf := s.family(), t.family()
	if sf == tf {
		return classifySameFamily(sf, s, t)
	}
	return classifyCrossFamily(sf, tf)
}

func classifySameFamily(fam string, s, t myType) (compatLevel, string) {
	switch fam {
	case "int":
		sr, tr := intRank[s.base], intRank[t.base]
		if s.unsigned != t.unsigned {
			return compatRisky, fmt.Sprintf("integer signedness change (%s -> %s) may lose values", s.raw, t.raw)
		}
		if tr >= sr {
			return compatWiden, ""
		}
		return compatRisky, fmt.Sprintf("integer narrowing %s -> %s may overflow", s.raw, t.raw)
	case "decimal":
		sp, ss := s.arg(0, 10), s.arg(1, 0)
		tp, ts := t.arg(0, 10), t.arg(1, 0)
		if tp >= sp && ts >= ss {
			return compatWiden, ""
		}
		return compatRisky, fmt.Sprintf("decimal precision/scale narrowing %s -> %s", s.raw, t.raw)
	case "float":
		if s.base == "float" && (t.base == "double" || t.base == "real" || t.base == "double precision") {
			return compatWiden, ""
		}
		if s.base == t.base {
			return compatIdentical, ""
		}
		return compatRisky, fmt.Sprintf("floating-point change %s -> %s", s.raw, t.raw)
	case "char":
		if t.arg(0, 0) >= s.arg(0, 0) {
			return compatWiden, ""
		}
		return compatRisky, fmt.Sprintf("string length narrowing %s -> %s may truncate", s.raw, t.raw)
	case "text":
		if textRank[t.base] >= textRank[s.base] {
			return compatWiden, ""
		}
		return compatRisky, fmt.Sprintf("text narrowing %s -> %s may truncate", s.raw, t.raw)
	case "binary":
		if t.arg(0, 0) >= s.arg(0, 0) {
			return compatWiden, ""
		}
		return compatRisky, fmt.Sprintf("binary length narrowing %s -> %s may truncate", s.raw, t.raw)
	case "blob":
		if blobRank[t.base] >= blobRank[s.base] {
			return compatWiden, ""
		}
		return compatRisky, fmt.Sprintf("blob narrowing %s -> %s may truncate", s.raw, t.raw)
	case "datetime", "timestamp", "time":
		// fractional-seconds precision: widen ok, narrow risky.
		if t.arg(0, 0) >= s.arg(0, 0) {
			return compatWiden, ""
		}
		return compatRisky, fmt.Sprintf("fractional-seconds precision narrowing %s -> %s", s.raw, t.raw)
	case "enum", "set":
		if s.raw == t.raw {
			return compatIdentical, ""
		}
		return compatRisky, fmt.Sprintf("%s member-set change %s -> %s", fam, s.raw, t.raw)
	default:
		return compatIdentical, "" // date/year/json/bit same-family, same base handled by raw== above
	}
}

// numericFamilies and stringishFamilies group families for cross-family rules.
func isNumeric(f string) bool   { return f == "int" || f == "decimal" || f == "float" || f == "bit" }
func isStringish(f string) bool { return f == "char" || f == "text" }
func isTemporal(f string) bool {
	return f == "date" || f == "time" || f == "datetime" || f == "timestamp" || f == "year"
}

// isStructured is limited to opaque types with no textual interchange form
// (JSON, spatial). enum/set are textual (their values are strings) and are
// handled separately — they can interchange with string columns.
func isStructured(f string) bool {
	return f == "json" || f == "geometry-or-other"
}

func isEnumSet(f string) bool { return f == "enum" || f == "set" }

func classifyCrossFamily(sf, tf string) (compatLevel, string) {
	msg := fmt.Sprintf("cross-type change %s -> %s", sf, tf)
	switch {
	// Within the numeric group, cross-family (int<->decimal<->float) is risky.
	case isNumeric(sf) && isNumeric(tf):
		return compatRisky, msg
	// char <-> text is same conceptual string data.
	case isStringish(sf) && isStringish(tf):
		return compatWiden, ""
	// binary <-> blob is same conceptual byte data.
	case (sf == "binary" || sf == "blob") && (tf == "binary" || tf == "blob"):
		return compatWiden, ""
	// Temporal <-> temporal (e.g. datetime<->timestamp) shifts tz/semantics.
	case isTemporal(sf) && isTemporal(tf):
		return compatRisky, msg + " (temporal semantics differ)"
	// enum/set values are textual: they can interchange with string columns
	// (target validates members / length) — risky, not a hard block.
	case (isEnumSet(sf) && isStringish(tf)) || (isStringish(sf) && isEnumSet(tf)):
		return compatRisky, msg
	// A structured opaque type (json/spatial) to/from an unrelated family is
	// structurally incompatible: the target input function cannot accept it.
	case isStructured(sf) || isStructured(tf) || isEnumSet(sf) || isEnumSet(tf):
		return compatIncompatible, msg + " (structurally incompatible)"
	// Anything else -> a text/char target can carry the source's text form.
	case isStringish(tf):
		return compatRisky, msg
	// Numeric <-> temporal etc.: block.
	default:
		return compatIncompatible, msg
	}
}

// normCharset folds MySQL's utf8 alias to utf8mb3 for comparison.
func normCharset(c string) string {
	c = strings.ToLower(c)
	if c == "utf8" {
		return "utf8mb3"
	}
	return c
}

// classifyCharset flags charset narrowing for character columns. utf8mb3->utf8mb4
// widens (ok); utf8mb4->utf8mb3 is lossy for 4-byte characters (risky — the
// target's strict mode halts loud on an actual 4-byte value, so a WARN is the
// faithful classification, not a blanket block). Same charset or non-character
// columns are unaffected.
func classifyCharset(src, tgt engine.Column) (compatLevel, string) {
	sc, tc := normCharset(src.Charset), normCharset(tgt.Charset)
	if sc == "" || tc == "" || sc == tc {
		return compatIdentical, ""
	}
	if sc == "utf8mb4" && tc == "utf8mb3" {
		return compatRisky, "charset narrowing utf8mb4 -> utf8mb3 truncates 4-byte characters"
	}
	if sc == "utf8mb3" && tc == "utf8mb4" {
		return compatWiden, ""
	}
	return compatRisky, fmt.Sprintf("charset change %s -> %s", src.Charset, tgt.Charset)
}
