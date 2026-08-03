package redis

import "github.com/rudimk/replicare/internal/engine"

// selection is a compiled key-pattern filter. Redis keys have no schema.table
// structure, so include/exclude are KEY globs (Redis glob semantics: `*`, `?`,
// `[set]`, `[^set]`, `\escape`) matched against the whole key. exclude wins; an
// empty include matches every key (redis-plan §0.5).
type selection struct {
	include []string
	exclude []string
}

func compileSelection(sel engine.Selection) *selection {
	return &selection{include: sel.Include, exclude: sel.Exclude}
}

// match reports whether a key is selected: it matches some include (or include is
// empty) and matches no exclude.
func (s *selection) match(key string) bool {
	for _, ex := range s.exclude {
		if globMatch(ex, key) {
			return false
		}
	}
	if len(s.include) == 0 {
		return true
	}
	for _, in := range s.include {
		if globMatch(in, key) {
			return true
		}
	}
	return false
}

// globMatch implements Redis-compatible glob matching (a byte-wise port of
// Redis's stringmatchlen): `*` matches any run, `?` any single byte, `[...]` a
// character class (with `^` negation and `a-z` ranges), and `\` escapes the next
// pattern byte. This matches how Redis itself evaluates `SCAN … MATCH`, so a
// replicare selection glob and a server-side MATCH agree.
func globMatch(pattern, s string) bool {
	p, str := []byte(pattern), []byte(s)
	pi, si := 0, 0
	for pi < len(p) && si <= len(str) {
		switch p[pi] {
		case '*':
			// Collapse consecutive stars.
			for pi+1 < len(p) && p[pi+1] == '*' {
				pi++
			}
			if pi+1 == len(p) {
				return true // trailing star matches the rest
			}
			for i := si; i <= len(str); i++ {
				if globMatch(string(p[pi+1:]), string(str[i:])) {
					return true
				}
			}
			return false
		case '?':
			if si >= len(str) {
				return false
			}
			si++
			pi++
		case '[':
			if si >= len(str) {
				return false
			}
			pi++
			neg := false
			if pi < len(p) && (p[pi] == '^') {
				neg = true
				pi++
			}
			matched := false
			for pi < len(p) && p[pi] != ']' {
				if p[pi] == '\\' && pi+1 < len(p) {
					pi++
					if p[pi] == str[si] {
						matched = true
					}
				} else if pi+2 < len(p) && p[pi+1] == '-' && p[pi+2] != ']' {
					lo, hi := p[pi], p[pi+2]
					if lo > hi {
						lo, hi = hi, lo
					}
					if str[si] >= lo && str[si] <= hi {
						matched = true
					}
					pi += 2
				} else if p[pi] == str[si] {
					matched = true
				}
				pi++
			}
			if pi < len(p) { // skip the ']'
				pi++
			}
			if matched == neg {
				return false
			}
			si++
		case '\\':
			if pi+1 < len(p) {
				pi++
			}
			if si >= len(str) || p[pi] != str[si] {
				return false
			}
			si++
			pi++
		default:
			if si >= len(str) || p[pi] != str[si] {
				return false
			}
			si++
			pi++
		}
	}
	// Trailing stars in the pattern still match the empty remainder.
	for pi < len(p) && p[pi] == '*' {
		pi++
	}
	return pi == len(p) && si == len(str)
}
