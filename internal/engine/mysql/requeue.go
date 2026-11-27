package mysql

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
// queue as a change, as the capture trigger would.
func (s *Source) RequeueKeys(ctx context.Context, t engine.TableRef, keys []engine.KeyValues) error {
	if s.db == nil {
		return errNotConnected
	}
	if len(keys) == 0 {
		return nil
	}
	relID, pkCols, ok, err := s.lookupRegistry(ctx, t)
	if err != nil {
		return fmt.Errorf("mysql: requeue %s: %w", t, err)
	}
	if !ok {
		return fmt.Errorf("mysql: requeue %s: the table is not captured", t)
	}
	n := len(pkCols)
	cols := make([]string, n)
	for i, c := range deltaColumns(n) {
		cols[i] = bq(c)
	}
	row := "('U', " + placeholders(n) + ")"
	for i := 0; i < len(keys); i += requeueBatch {
		end := min(i+requeueBatch, len(keys))
		rows := make([]string, 0, end-i)
		args := make([]any, 0, (end-i)*n)
		for _, k := range keys[i:end] {
			if len(k) != n {
				return fmt.Errorf("mysql: requeue %s: key %v has %d values, want %d", t, k, len(k), n)
			}
			rows = append(rows, row)
			args = append(args, keyArgs(k)...)
		}
		q := fmt.Sprintf("INSERT INTO %s (rc_op, %s) VALUES %s",
			captureRef(deltaTableName(relID)), strings.Join(cols, ", "), strings.Join(rows, ", "))
		if _, err := s.db.ExecContext(ctx, q, args...); err != nil {
			return fmt.Errorf("mysql: requeue %s: %w", t, err)
		}
	}
	return nil
}
