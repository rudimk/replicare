package engine

import (
	"fmt"
	"sort"
	"sync"
)

// registry holds the set of available engines, keyed by Name(). Engines register
// themselves from their package init (e.g. the postgres package), keeping the
// core decoupled from concrete implementations (CLAUDE.md §5).
var (
	registryMu sync.RWMutex
	registry   = map[string]Engine{}
)

// Register adds an engine to the registry. It panics on a duplicate name, since
// that indicates a programming error (two engines claiming the same id).
func Register(e Engine) {
	registryMu.Lock()
	defer registryMu.Unlock()
	name := e.Name()
	if name == "" {
		panic("engine: Register called with empty Name()")
	}
	if _, dup := registry[name]; dup {
		panic(fmt.Sprintf("engine: duplicate registration for %q", name))
	}
	registry[name] = e
}

// Get returns the engine registered under name, or an error if none is.
func Get(name string) (Engine, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	e, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("engine: no engine registered for %q (have: %v)", name, names())
	}
	return e, nil
}

// List returns the sorted names of all registered engines.
func List() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	return names()
}

// names returns sorted registry keys; caller must hold registryMu.
func names() []string {
	out := make([]string, 0, len(registry))
	for n := range registry {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
