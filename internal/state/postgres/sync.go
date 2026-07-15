package statepg

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/rudimk/replicare/internal/engine"
	"github.com/rudimk/replicare/internal/state"
)

// PutSync upserts a sync definition by name. It is idempotent: re-registering a
// sync overwrites its stored source/targets/selection and bumps updated_at.
func (s *Store) PutSync(ctx context.Context, def state.SyncDef) error {
	if err := s.requirePool(); err != nil {
		return err
	}
	if def.Name == "" {
		return errors.New("statepg: PutSync requires a sync name")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO replicare_state.syncs (name, source, targets, include, exclude, updated_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (name) DO UPDATE SET
			source     = EXCLUDED.source,
			targets    = EXCLUDED.targets,
			include    = EXCLUDED.include,
			exclude    = EXCLUDED.exclude,
			updated_at = now()`,
		def.Name, def.Source, targetsToStrings(def.Targets), orEmpty(def.Selection.Include), orEmpty(def.Selection.Exclude))
	if err != nil {
		return fmt.Errorf("statepg: put sync %q: %w", def.Name, err)
	}
	return nil
}

// GetSync returns the stored sync definition, or an error if it is not found.
func (s *Store) GetSync(ctx context.Context, name string) (state.SyncDef, error) {
	if err := s.requirePool(); err != nil {
		return state.SyncDef{}, err
	}
	var (
		def     state.SyncDef
		targets []string
		include []string
		exclude []string
	)
	err := s.pool.QueryRow(ctx, `
		SELECT name, source, targets, include, exclude
		FROM replicare_state.syncs WHERE name = $1`, name).
		Scan(&def.Name, &def.Source, &targets, &include, &exclude)
	if errors.Is(err, pgx.ErrNoRows) {
		return state.SyncDef{}, fmt.Errorf("statepg: sync %q not found", name)
	}
	if err != nil {
		return state.SyncDef{}, fmt.Errorf("statepg: get sync %q: %w", name, err)
	}
	def.Targets = stringsToTargets(targets)
	def.Selection = engine.Selection{Include: include, Exclude: exclude}
	return def, nil
}

// ListSyncs returns all stored sync definitions, ordered by name.
func (s *Store) ListSyncs(ctx context.Context) ([]state.SyncDef, error) {
	if err := s.requirePool(); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT name, source, targets, include, exclude
		FROM replicare_state.syncs ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("statepg: list syncs: %w", err)
	}
	defer rows.Close()

	var out []state.SyncDef
	for rows.Next() {
		var (
			def     state.SyncDef
			targets []string
			include []string
			exclude []string
		)
		if err := rows.Scan(&def.Name, &def.Source, &targets, &include, &exclude); err != nil {
			return nil, fmt.Errorf("statepg: scan sync: %w", err)
		}
		def.Targets = stringsToTargets(targets)
		def.Selection = engine.Selection{Include: include, Exclude: exclude}
		out = append(out, def)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("statepg: list syncs: %w", err)
	}
	return out, nil
}

// targetsToStrings converts []engine.TargetID to a non-nil []string for text[]
// storage (nil would encode as SQL NULL, violating the NOT NULL columns).
func targetsToStrings(ts []engine.TargetID) []string {
	out := make([]string, len(ts))
	for i, t := range ts {
		out[i] = string(t)
	}
	return out
}

// orEmpty coalesces a nil slice to a non-nil empty slice so it encodes as an
// empty array rather than SQL NULL.
func orEmpty(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// stringsToTargets converts a stored text[] back to []engine.TargetID.
func stringsToTargets(ss []string) []engine.TargetID {
	if ss == nil {
		return nil
	}
	out := make([]engine.TargetID, len(ss))
	for i, s := range ss {
		out[i] = engine.TargetID(s)
	}
	return out
}
