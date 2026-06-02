package config

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"gopkg.in/yaml.v3"

	"github.com/rudimk/replicare/internal/engine"
)

// StrictDecode decodes a captured engine-block node into out, rejecting unknown
// fields. Engine parsers should use this so typos in an engine block are caught.
// (yaml.Node.Decode has no strict mode, so we round-trip through the strict
// Decoder.)
func StrictDecode(node *yaml.Node, out any) error {
	b, err := yaml.Marshal(node)
	if err != nil {
		return err
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	return dec.Decode(out)
}

// EngineConn is an engine-specific connection block (the typed `postgres:` /
// `mysql:` / `redis:` block under an endpoint). Each engine owns and validates
// its own block, keeping the neutral config layer decoupled (CLAUDE.md §11) —
// the same pattern as the engine.Source/Sink registry.
type EngineConn interface {
	// EngineName is the engine this block belongs to (e.g. "postgres").
	EngineName() string
	// Validate checks the block. role is "source", "target", or "state_store";
	// name is the endpoint key, both used for actionable error messages.
	Validate(role, name string) error
	// ConnConfig produces the resolved low-level connection descriptor consumed
	// by the engine's Source/Sink (M1+).
	ConnConfig() engine.ConnConfig
}

// ConnParser parses (and strictly decodes) an engine's connection block node
// into a typed EngineConn. Implemented by each engine package.
type ConnParser func(block *yaml.Node) (EngineConn, error)

var (
	engMu         sync.RWMutex
	engineParsers = map[string]ConnParser{}
)

// RegisterEngine registers an engine's connection-block parser. Engine packages
// call this from init(); panics on empty name or duplicate registration.
func RegisterEngine(name string, p ConnParser) {
	engMu.Lock()
	defer engMu.Unlock()
	if name == "" {
		panic("config: RegisterEngine called with empty name")
	}
	if p == nil {
		panic("config: RegisterEngine called with nil parser for " + name)
	}
	if _, dup := engineParsers[name]; dup {
		panic(fmt.Sprintf("config: duplicate engine registration for %q", name))
	}
	engineParsers[name] = p
}

// lookupEngineParser returns the parser for an engine, if registered.
func lookupEngineParser(name string) (ConnParser, bool) {
	engMu.RLock()
	defer engMu.RUnlock()
	p, ok := engineParsers[name]
	return p, ok
}

// RegisteredEngines returns the sorted names of engines with a registered config
// parser (used for error messages).
func RegisteredEngines() []string {
	engMu.RLock()
	defer engMu.RUnlock()
	out := make([]string, 0, len(engineParsers))
	for n := range engineParsers {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
