package pipeline

import (
	"context"
	"testing"

	"github.com/rudimk/replicare/internal/engine"
)

// requeueSource records which connection queued keys and whether it was closed.
type requeueSource struct {
	engine.Source
	requeued int
	closed   bool
}

func (r *requeueSource) RequeueKeys(context.Context, engine.TableRef, []engine.KeyValues) error {
	r.requeued++
	return nil
}

func (r *requeueSource) Close(context.Context) error { r.closed = true; return nil }

// The check's repair runs beside streaming, so it must never queue keys on the streaming
// connection (a pgx connection is not safe for concurrent use: "conn busy").
func TestRequeueUsesItsOwnConnection(t *testing.T) {
	streaming := &requeueSource{}
	own := &requeueSource{}
	s := &Syncer{Source: streaming, OpenSource: func(context.Context) (engine.Source, error) { return own, nil }}
	if err := s.requeueKeys(context.Background(), engine.TableRef{Schema: "s", Name: "t"}, []engine.KeyValues{{"1"}}); err != nil {
		t.Fatal(err)
	}
	if streaming.requeued != 0 || own.requeued != 1 || !own.closed {
		t.Fatalf("streaming requeued %d, own requeued %d (closed %v); want 0, 1, closed", streaming.requeued, own.requeued, own.closed)
	}
}
