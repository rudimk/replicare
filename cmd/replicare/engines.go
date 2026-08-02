package main

// Engine registration. Importing an engine package runs its init(), which
// registers the engine's config parser (and, from M1, its Source/Sink factory).
// Add MySQL/Redis here when implemented.
import (
	_ "github.com/rudimk/replicare/internal/engine/mysql"
	_ "github.com/rudimk/replicare/internal/engine/postgres"
)
