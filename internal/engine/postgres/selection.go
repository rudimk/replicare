package postgres

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// selectTables filters candidate tables by an include/exclude glob selection
// (CLAUDE.md §11). Semantics:
//
//   - A table is selected iff it matches at least one include pattern AND matches
//     no exclude pattern.
//   - An empty include list means "match all" (then excludes still apply), which
//     is the common "replicate everything except ..." shape.
//
// Patterns are matched against the schema-qualified name "schema.table". `*`
// matches any run of characters (including dots) and `?` matches a single
// character, so `public.*` selects a schema and `*_audit` excludes audit tables
// in any schema. Matching is case-sensitive against the catalog name.
//
// The returned slice preserves the input order. A malformed pattern is a loud
// error (surfaced at validate time), never a silent skip.
func selectTables(all []engine.TableRef, sel engine.Selection) ([]engine.TableRef, error) {
	inc, err := compileGlobs(sel.Include, "include")
	if err != nil {
		return nil, err
	}
	exc, err := compileGlobs(sel.Exclude, "exclude")
	if err != nil {
		return nil, err
	}

	out := make([]engine.TableRef, 0, len(all))
	for _, t := range all {
		if isSelected(t.String(), inc, exc) {
			out = append(out, t)
		}
	}
	return out, nil
}

// isSelected applies the include-then-exclude rule to one schema-qualified name.
// An empty include set matches all names.
func isSelected(name string, include, exclude []*regexp.Regexp) bool {
	included := len(include) == 0
	for _, re := range include {
		if re.MatchString(name) {
			included = true
			break
		}
	}
	if !included {
		return false
	}
	for _, re := range exclude {
		if re.MatchString(name) {
			return false
		}
	}
	return true
}

// compileGlobs compiles a list of glob patterns to anchored regexps. kind is
// "include"/"exclude" for error context. Empty patterns are rejected as likely
// mistakes.
func compileGlobs(patterns []string, kind string) ([]*regexp.Regexp, error) {
	out := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		if strings.TrimSpace(p) == "" {
			return nil, fmt.Errorf("empty %s selection pattern", kind)
		}
		re, err := globToRegexp(p)
		if err != nil {
			return nil, fmt.Errorf("invalid %s pattern %q: %w", kind, p, err)
		}
		out = append(out, re)
	}
	return out, nil
}

// globToRegexp converts a `*`/`?` glob into an anchored regexp. All other
// characters are matched literally (regexp metacharacters are quoted).
func globToRegexp(pat string) (*regexp.Regexp, error) {
	var b strings.Builder
	b.WriteString("^")
	for _, r := range pat {
		switch r {
		case '*':
			b.WriteString(".*")
		case '?':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	b.WriteString("$")
	return regexp.Compile(b.String())
}
