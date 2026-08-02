package mysql

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

// selectTables filters candidate tables by an include/exclude glob selection
// (CLAUDE.md §11). A table is selected iff it matches at least one include
// pattern (empty include = match all) AND no exclude pattern. Patterns match the
// qualified name "db.table" (a MySQL schema IS a database), so `app.*` selects a
// database and `*_audit` excludes audit tables across databases. Input order is
// preserved; a malformed pattern is a loud error.
//
// (This mirrors the Postgres engine's selection logic; the glob semantics are
// engine-neutral, but each engine owns its own selection per the §11 split.)
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
