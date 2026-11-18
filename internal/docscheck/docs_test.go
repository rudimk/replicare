// Package docscheck holds the docs-site checks (the mdBook site built from docs/, served at
// https://replicare.rudimk.com). They run with the ordinary unit tests, so a broken docs link
// fails CI like a broken build.
package docscheck

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"unicode"
)

const (
	repoRoot = "../.."
	docsDir  = repoRoot + "/docs"
	siteURL  = "https://replicare.rudimk.com/"
	repoURL  = "https://github.com/rudimk/replicare/"
)

var (
	summaryLinkRe = regexp.MustCompile(`\]\(([^)]*)\)`)
	includeRe     = regexp.MustCompile(`\{\{#include ([^}]+)\}\}`)
	mdLinkRe      = regexp.MustCompile(`\]\(([^)\s]+)\)`)
	headingRe     = regexp.MustCompile(`^#{1,6}\s+(.*?)\s*#*\s*$`)
	inlineLinkRe  = regexp.MustCompile(`\[([^\]]*)\]\([^)]*\)`)
	fenceRe       = regexp.MustCompile("^\\s*(```|~~~)")
	inlineCodeRe  = regexp.MustCompile("`[^`]*`")
)

// page is one chapter of the site, with {{#include}} directives expanded the way mdBook does.
type page struct {
	file    string // path under docs/
	content string
}

func loadSite(t *testing.T) (map[string]page, []string) {
	t.Helper()
	summary, err := os.ReadFile(filepath.Join(docsDir, "SUMMARY.md"))
	if err != nil {
		t.Fatal(err)
	}
	pages := map[string]page{}
	var order []string
	for _, m := range summaryLinkRe.FindAllStringSubmatch(string(summary), -1) {
		target := m[1]
		if target == "" { // a draft chapter (a heading-only entry), not a page
			continue
		}
		raw, err := os.ReadFile(filepath.Join(docsDir, target))
		if err != nil {
			t.Errorf("SUMMARY.md lists %s, which does not exist", target)
			continue
		}
		content := includeRe.ReplaceAllStringFunc(string(raw), func(dir string) string {
			rel := strings.TrimSpace(includeRe.FindStringSubmatch(dir)[1])
			inc, err := os.ReadFile(filepath.Join(docsDir, rel))
			if err != nil {
				t.Errorf("%s includes %s, which does not exist", target, rel)
				return ""
			}
			return string(inc)
		})
		pages[target] = page{file: target, content: content}
		order = append(order, target)
	}
	return pages, order
}

// TestEveryDocIsOnTheSite: a page in docs/ that SUMMARY.md does not list is never built.
func TestEveryDocIsOnTheSite(t *testing.T) {
	pages, _ := loadSite(t)
	files, err := filepath.Glob(filepath.Join(docsDir, "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		name := filepath.Base(f)
		if name == "SUMMARY.md" {
			continue
		}
		if _, ok := pages[name]; !ok {
			t.Errorf("docs/%s is not listed in docs/SUMMARY.md, so the site never shows it", name)
		}
	}
}

// TestSiteNeverReferencesInternalDocs: CLAUDE.md (the internal design log) and the .sisyphus
// plans are not published, so no site page may mention or link to them.
func TestSiteNeverReferencesInternalDocs(t *testing.T) {
	pages, order := loadSite(t)
	for _, name := range order {
		for i, line := range strings.Split(pages[name].content, "\n") {
			for _, banned := range []string{"CLAUDE.md", ".sisyphus", "-plan.md"} {
				if strings.Contains(line, banned) {
					t.Errorf("%s:%d mentions %s, which is not part of the docs site: %s", name, i+1, banned, strings.TrimSpace(line))
				}
			}
		}
	}
}

// TestSiteLinksResolve: every link on the site goes somewhere real — another page (and the
// heading it names), a file in the repository, or an external URL.
func TestSiteLinksResolve(t *testing.T) {
	pages, order := loadSite(t)
	anchors := map[string]map[string]bool{}
	for name, p := range pages {
		anchors[name] = headingAnchors(p.content)
	}
	checkPageLink := func(from, file, anchor, raw string) {
		if file == "" {
			file = from
		}
		if _, ok := pages[file]; !ok {
			if _, err := os.Stat(filepath.Join(docsDir, file)); err != nil {
				t.Errorf("%s links to %q: %s does not exist in docs/", from, raw, file)
			} else if strings.HasSuffix(file, ".md") {
				t.Errorf("%s links to %q: %s is not on the site (missing from SUMMARY.md)", from, raw, file)
			}
			return
		}
		if anchor != "" && !anchors[file][anchor] {
			t.Errorf("%s links to %q: %s has no heading with anchor #%s", from, raw, file, anchor)
		}
	}
	for _, name := range order {
		for _, link := range pageLinks(pages[name].content) {
			target, anchor, _ := strings.Cut(link, "#")
			switch {
			case strings.HasPrefix(link, siteURL):
				path := strings.TrimPrefix(target, siteURL)
				if path == "" || path == "index.html" {
					path = "README.html"
				}
				checkPageLink(name, strings.TrimSuffix(path, ".html")+".md", anchor, link)
			case strings.HasPrefix(link, repoURL):
				rest := strings.TrimPrefix(target, repoURL)
				for _, prefix := range []string{"blob/main/", "tree/main/"} {
					if strings.HasPrefix(rest, prefix) {
						if _, err := os.Stat(filepath.Join(repoRoot, strings.TrimPrefix(rest, prefix))); err != nil {
							t.Errorf("%s links to %q, which does not exist in the repository", name, link)
						}
					}
				}
			case strings.Contains(link, "://") || strings.HasPrefix(link, "mailto:"):
				// External: not checked offline.
			case strings.HasPrefix(target, "../") || strings.HasPrefix(target, "/"):
				t.Errorf("%s links to %q, outside docs/ — that file is not on the site; link to it on GitHub (%sblob/main/...)", name, link, repoURL)
			default:
				checkPageLink(name, target, anchor, link)
			}
		}
	}
}

// pageLinks returns the markdown link targets of a page, ignoring code blocks and inline code.
func pageLinks(content string) []string {
	var out []string
	inFence := false
	for _, line := range strings.Split(content, "\n") {
		if fenceRe.MatchString(line) {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		line = inlineCodeRe.ReplaceAllString(line, "")
		for _, m := range mdLinkRe.FindAllStringSubmatch(line, -1) {
			out = append(out, m[1])
		}
	}
	return out
}

// headingAnchors returns the ids mdBook gives a page's headings: the heading's text,
// lowercased, keeping letters, digits, '_' and '-', spaces turned into '-', everything else
// dropped; a repeated id gets "-1", "-2", ...
func headingAnchors(content string) map[string]bool {
	ids := map[string]bool{}
	seen := map[string]int{}
	inFence := false
	for _, line := range strings.Split(content, "\n") {
		if fenceRe.MatchString(line) {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		m := headingRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		id := slug(m[1])
		if n := seen[id]; n > 0 {
			seen[id]++
			id = id + "-" + itoa(n)
		} else {
			seen[id] = 1
		}
		ids[id] = true
	}
	return ids
}

func slug(heading string) string {
	text := inlineLinkRe.ReplaceAllString(heading, "$1")
	text = strings.NewReplacer("`", "", "*", "", "<", "", ">", "").Replace(text)
	var b strings.Builder
	for _, r := range strings.TrimSpace(text) {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-':
			b.WriteRune(unicode.ToLower(r))
		case unicode.IsSpace(r):
			b.WriteRune('-')
		}
	}
	return b.String()
}

func itoa(n int) string {
	digits := []byte{}
	for n > 0 || len(digits) == 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}

// TestSlugMatchesMdBook pins the anchor rule against ids mdBook actually generated.
func TestSlugMatchesMdBook(t *testing.T) {
	cases := map[string]string{
		"Load & convergence testing":                           "load--convergence-testing",
		"Sequences & identity counters (DR / promotion)":       "sequences--identity-counters-dr--promotion",
		"1. A MySQL sync still needs a Postgres for state":     "1-a-mysql-sync-still-needs-a-postgres-for-state",
		"Pausing a sync (`enabled`)":                           "pausing-a-sync-enabled",
		"Checking progress from the CLI (no Grafana required)": "checking-progress-from-the-cli-no-grafana-required",
	}
	keys := make([]string, 0, len(cases))
	for k := range cases {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, h := range keys {
		if got := slug(h); got != cases[h] {
			t.Errorf("slug(%q) = %q, want %q", h, got, cases[h])
		}
	}
}
