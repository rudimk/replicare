package main

// Engine registration. Importing an engine package runs its init(), which
// registers the engine's config parser and its Source/Sink factory. Both engines
// are pure Go (pgx, go-sql-driver/mysql), so the binary stays CGO-free and static.
// Add Redis here when implemented.
import (
	_ "github.com/rudimk/replicare/internal/engine/mysql"
	_ "github.com/rudimk/replicare/internal/engine/postgres"
)
