package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/rudimk/replicare/internal/engine"
)

var _ engine.KeyRequeuer = (*Source)(nil)

// requeueBatch bounds how many keys one insert into a delta table carries.
const requeueBatch = 500

// RequeueKeys implements engine.KeyRequeuer: it inserts each key into the table's delta
// queue as a change, as the capture trigger would. Keys are in the text form the verify
// path reads them in, cast to the key columns' own types.
func (s *Source) RequeueKeys(ctx context.Context, t engine.TableRef, keys []engine.KeyValues) error {
	if err := s.requireConn(); err != nil {
		return err
	}
	if len(keys) == 0 {
		return nil
	}
	relID, _, ok, err := lookupCapture(ctx, s.conn, t)
	if err != nil {
		return fmt.Errorf("postgres: requeue %s: %w", t, err)
	}
	if !ok {
		return fmt.Errorf("postgres: requeue %s: the table is not captured", t)
	}
	meta, err := s.tableMeta(ctx, t)
	if err != nil {
		return err
	}
	key := captureColsFor(meta)
	if err := requireKey(t, key); err != nil {
		return err
	}
	cols := strings.Join(deltaColumns(len(key)), ", ")
	for i := 0; i < len(keys); i += requeueBatch {
		end := min(i+requeueBatch, len(keys))
		rows := make([]string, 0, end-i)
		for _, k := range keys[i:end] {
			vt, err := valueTuple(key, k)
			if err != nil {
				return fmt.Errorf("postgres: requeue %s: %w", t, err)
			}
			rows = append(rows, "('U', "+strings.TrimPrefix(vt, "("))
		}
		q := fmt.Sprintf("INSERT INTO %s (rc_op, %s) VALUES %s",
			qualifiedCapture(deltaTableName(relID)), cols, strings.Join(rows, ", "))
		if _, err := s.conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("postgres: requeue %s: %w", t, withPgDetail(err))
		}
	}
	return nil
}
