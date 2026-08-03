package main

// Engine registration. Importing an engine package runs its init(), which
// registers the engine's config parser and its Source/Sink factory. All engines
// are pure Go (pgx, go-sql-driver/mysql, go-redis), so the binary stays CGO-free
// and static.
import (
	_ "github.com/rudimk/replicare/internal/engine/mysql"
	_ "github.com/rudimk/replicare/internal/engine/postgres"
	_ "github.com/rudimk/replicare/internal/engine/redis"
)
