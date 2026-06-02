package config

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// envRefRe matches ${VAR} and ${VAR:-default} references.
var envRefRe = regexp.MustCompile(`\$\{([^}]*)\}`)

// expandEnv walks a parsed YAML node tree and expands ${VAR} / ${VAR:-default}
// references in every scalar value from the environment (CLAUDE.md §11). This is
// the env-override / secret-resolution mechanism and applies to any string-valued
// field. Precedence: the env var value if set, else the inline default, else a
// hard error (fail loud — never silently empty). A literal "${" is written "$${".
func expandEnv(node *yaml.Node) error {
	switch node.Kind {
	case yaml.DocumentNode, yaml.SequenceNode, yaml.MappingNode:
		for _, child := range node.Content {
			if err := expandEnv(child); err != nil {
				return err
			}
		}
	case yaml.ScalarNode:
		if !strings.Contains(node.Value, "${") {
			return nil
		}
		v, err := expandString(node.Value)
		if err != nil {
			return err
		}
		node.Value = v
		// Drop the tag so the expanded value's type is re-resolved on decode
		// (e.g. an expanded "5432" can decode into an int field). Quoted scalars
		// keep their string style via re-marshal, so secrets stay strings.
		if node.Style == 0 {
			node.Tag = ""
		}
	}
	return nil
}

// expandString resolves all ${VAR} / ${VAR:-default} references in s.
func expandString(s string) (string, error) {
	const escSentinel = "\x00REPLICARE_ESC\x00"
	s = strings.ReplaceAll(s, "$${", escSentinel)

	var firstErr error
	out := envRefRe.ReplaceAllStringFunc(s, func(match string) string {
		inner := match[2 : len(match)-1] // strip "${" and "}"
		name := inner
		def := ""
		hasDef := false
		if i := strings.Index(inner, ":-"); i >= 0 {
			name = inner[:i]
			def = inner[i+2:]
			hasDef = true
		}
		name = strings.TrimSpace(name)
		if name == "" {
			if firstErr == nil {
				firstErr = fmt.Errorf("empty env var reference %q", match)
			}
			return ""
		}
		if val, ok := os.LookupEnv(name); ok {
			return val
		}
		if hasDef {
			return def
		}
		if firstErr == nil {
			firstErr = fmt.Errorf("env var %q is not set and no default was provided (use ${%s:-default})", name, name)
		}
		return ""
	})

	out = strings.ReplaceAll(out, escSentinel, "${")
	return out, firstErr
}
