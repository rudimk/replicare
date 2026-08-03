package redis

import (
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

func TestGlobMatch(t *testing.T) {
	cases := []struct {
		pat, s string
		want   bool
	}{
		{"*", "anything", true},
		{"*", "", true},
		{"user:*", "user:42", true},
		{"user:*", "order:42", false},
		{"user:?", "user:7", true},
		{"user:?", "user:77", false},
		{"h[ae]llo", "hello", true},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"h[^x]llo", "hello", true},
		{"h[^e]llo", "hello", false},
		{"key:[0-9]", "key:5", true},
		{"key:[0-9]", "key:a", false},
		{`ab\*cd`, "ab*cd", true}, // escaped star is literal
		{`ab\*cd`, "abXcd", false},
		{"a:*:b", "a:x:y:b", true}, // star crosses ':'
		{"", "", true},
		{"", "x", false},
		{"prefix*", "prefix", true}, // trailing star matches empty
	}
	for _, c := range cases {
		if got := globMatch(c.pat, c.s); got != c.want {
			t.Errorf("globMatch(%q, %q) = %v, want %v", c.pat, c.s, got, c.want)
		}
	}
}

func TestSelectionMatch(t *testing.T) {
	s := compileSelection(engine.Selection{
		Include: []string{"user:*", "session:*"},
		Exclude: []string{"user:*:tmp"},
	})
	cases := map[string]bool{
		"user:1":      true,
		"session:abc": true,
		"user:1:tmp":  false, // exclude wins
		"order:9":     false, // not included
	}
	for k, want := range cases {
		if got := s.match(k); got != want {
			t.Errorf("match(%q) = %v, want %v", k, got, want)
		}
	}
	// Empty include matches everything (minus excludes).
	all := compileSelection(engine.Selection{Exclude: []string{"*:tmp"}})
	if !all.match("anything") || all.match("x:tmp") {
		t.Errorf("empty-include selection wrong: anything=%v x:tmp=%v", all.match("anything"), all.match("x:tmp"))
	}
}
