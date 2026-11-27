package copy

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rudimk/replicare/internal/engine"
)

// hookedStorageSource wraps a Postgres source, keeps its storage-order capability
// (embedding engine.Source alone would hide it), and runs a hook before each chunk.
type hookedStorageSource struct {
	engine.Source
	sr     engine.StorageOrderReader
	mu     sync.Mutex
	n      int
	before func(n int) error
}

func newHooked(t *testing.T, src engine.Source, before func(n int) error) *hookedStorageSource {
	t.Helper()
	sr, ok := src.(engine.StorageOrderReader)
	if !ok {
		t.Fatal("the Postgres source should read in storage order")
	}
	return &hookedStorageSource{Source: src, sr: sr, before: before}
}

func (h *hookedStorageSource) CopyChunk(ctx context.Context, c engine.Chunk, w io.Writer) error {
	h.mu.Lock()
	h.n++
	n := h.n
	h.mu.Unlock()
	if h.before != nil {
		if err := h.before(n); err != nil {
			return err
		}
	}
	return h.Source.CopyChunk(ctx, c, w)
}

func (h *hookedStorageSource) calls() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

func (h *hookedStorageSource) StorageOrder(ctx context.Context, t engine.TableRef, pref engine.CopyOrder) (engine.StorageOrderChoice, error) {
	return h.sr.StorageOrder(ctx, t, pref)
}

func (h *hookedStorageSource) PlanStorageChunks(ctx context.Context, t engine.TableRef, from int64, targetRows int) ([]engine.Chunk, error) {
	return h.sr.PlanStorageChunks(ctx, t, from, targetRows)
}

func (h *hookedStorageSource) StorageFile(ctx context.Context, t engine.TableRef) (int64, error) {
	return h.sr.StorageFile(ctx, t)
}

// eventLog collects the copy's events.
type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) option() Option {
	return WithEvents(func(_ context.Context, level, event string, _ engine.TableRef, msg string) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.events = append(l.events, level+" "+event+" "+msg)
	})
}

func (l *eventLog) has(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range l.events {
		if strings.Contains(e, substr) {
			return true
		}
	}
	return false
}

// storageFixture builds rc_it.orders spread over a few thousand blocks (low fill
// factor, rows written in random key order), so a storage-order copy has several
// chunks of the minimum size.
func storageFixture(t *testing.T, ctx context.Context, rows int) (*fixture, engine.TableRef) {
	t.Helper()
	f := newFixture(t, ctx)
	exec(t, ctx, f.rawSrc, "CREATE TABLE rc_it.orders (id int PRIMARY KEY, note text) WITH (fillfactor = 10)")
	exec(t, ctx, f.rawTgt, ordersDDL)
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders SELECT g, repeat(md5(g::text), 8) FROM generate_series(1, "+strconv.Itoa(rows)+") g ORDER BY random()")
	exec(t, ctx, f.rawSrc, "ANALYZE rc_it.orders")
	var blocks int
	if err := f.rawSrc.QueryRow(ctx, "SELECT pg_relation_size('rc_it.orders') / 8192").Scan(&blocks); err != nil {
		t.Fatal(err)
	}
	if blocks < 3*1024 {
		t.Fatalf("fixture has %d blocks, want several storage-order chunks", blocks)
	}
	var version int
	if err := f.rawSrc.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version < 140000 {
		t.Skip("storage-order copy needs Postgres 14+ on the source")
	}
	return f, engine.TableRef{Schema: "rc_it", Name: "orders"}
}

var storageOpts = engine.ChunkOptions{TargetRows: 100, Order: engine.CopyOrderStorage}

func TestStorageOrderCopyFaithful(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	f, ref := storageFixture(t, ctx, 10_000)

	var log eventLog
	if err := Component(ctx, f.newWorkers(t, ctx, 3), f.store, f.syncName, "dst", []engine.TableRef{ref}, storageOpts, log.option()); err != nil {
		t.Fatalf("copy: %v", err)
	}
	f.faithful(t, ctx)
	if !log.has("INFO " + EventStorageOrder) {
		t.Errorf("expected a %s event, got %v", EventStorageOrder, log.events)
	}
	prog, err := f.store.LoadCopyProgress(ctx, f.syncName, "dst", ref)
	if err != nil {
		t.Fatal(err)
	}
	if !prog.Done || prog.StorageOrder {
		t.Errorf("finished progress = %+v, want done (storage progress cleared)", prog)
	}
}

func TestStorageOrderCopyResumes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	f, ref := storageFixture(t, ctx, 10_000)

	// The first run stops after two chunks.
	crashing := newHooked(t, f.src, func(n int) error {
		if n > 2 {
			return errors.New("injected crash")
		}
		return nil
	})
	if err := Component(ctx, []Worker{{Src: crashing, Sink: f.sink}}, f.store, f.syncName, "dst", []engine.TableRef{ref}, storageOpts); err == nil {
		t.Fatal("expected the injected crash to stop the copy")
	}
	prog, err := f.store.LoadCopyProgress(ctx, f.syncName, "dst", ref)
	if err != nil {
		t.Fatal(err)
	}
	if !prog.StorageOrder || prog.BlockWatermark <= 0 || prog.RelFile == 0 || len(prog.Watermark) > 0 || prog.Done {
		t.Fatalf("progress after the crash = %+v, want storage-order progress past block 0 and no key watermark", prog)
	}
	if !prog.Started() {
		t.Error("a storage-order copy in progress should count as started")
	}

	// The second run carries on from the block watermark rather than block 0.
	counting := newHooked(t, f.src, nil)
	if err := Component(ctx, []Worker{{Src: counting, Sink: f.sink}}, f.store, f.syncName, "dst", []engine.TableRef{ref}, storageOpts); err != nil {
		t.Fatalf("resume: %v", err)
	}
	f.faithful(t, ctx)
	full := newHooked(t, f.src, nil)
	chunks, err := full.PlanStorageChunks(ctx, ref, 0, storageOpts.TargetRows)
	if err != nil {
		t.Fatal(err)
	}
	if counting.calls() >= len(chunks) {
		t.Errorf("the resumed copy read %d chunks, want fewer than the %d of a full copy", counting.calls(), len(chunks))
	}
}

func TestStorageOrderCopyLoadsDuplicateAgainMerging(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	f, ref := storageFixture(t, ctx, 10_000)
	// A row already on the target stands for one seen twice (before and after it
	// moved): the direct load of its chunk fails on the key, and the chunk is merged.
	exec(t, ctx, f.rawTgt, "INSERT INTO rc_it.orders VALUES (5, 'stale')")

	// The failed load can also break the source connection (pgx closes it when a COPY
	// out is cut short), and then the reconnect-and-retry path merges the chunk instead;
	// either way the row ends up with the source's value.
	if err := Component(ctx, f.newWorkers(t, ctx, 2), f.store, f.syncName, "dst", []engine.TableRef{ref}, storageOpts,
		WithLoadMode(engine.LoadDirect)); err != nil {
		t.Fatalf("copy: %v", err)
	}
	f.faithful(t, ctx)
}

func TestStorageOrderCopyStartsAgainAfterRewrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	f, ref := storageFixture(t, ctx, 10_000)
	// Deleting half the rows first makes VACUUM FULL move the rest to other blocks.
	exec(t, ctx, f.rawSrc, "DELETE FROM rc_it.orders WHERE id % 2 = 0")

	var once sync.Once
	rewriting := newHooked(t, f.src, func(n int) error {
		if n == 2 {
			once.Do(func() { exec(t, ctx, f.rawSrc, "VACUUM FULL rc_it.orders") })
		}
		return nil
	})
	var log eventLog
	if err := Component(ctx, []Worker{{Src: rewriting, Sink: f.sink}}, f.store, f.syncName, "dst", []engine.TableRef{ref}, storageOpts, log.option()); err != nil {
		t.Fatalf("copy: %v", err)
	}
	f.faithful(t, ctx)
	if !log.has("WARN " + EventStorageRewritten) {
		t.Errorf("expected a %s event, got %v", EventStorageRewritten, log.events)
	}
}

func TestStorageOrderAutoKeepsKeyOrderForSmallTables(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	f := newFixture(t, ctx)
	exec(t, ctx, f.rawSrc, ordersDDL)
	exec(t, ctx, f.rawTgt, ordersDDL)
	exec(t, ctx, f.rawSrc, "INSERT INTO rc_it.orders SELECT g, 'note '||g FROM generate_series(1,500) g ORDER BY random()")
	exec(t, ctx, f.rawSrc, "ANALYZE rc_it.orders")
	ref := engine.TableRef{Schema: "rc_it", Name: "orders"}

	// Stop after one key-range chunk, so the key watermark shows which order ran.
	crashing := newHooked(t, f.src, func(n int) error {
		if n > 1 {
			return errors.New("injected crash")
		}
		return nil
	})
	var log eventLog
	opts := engine.ChunkOptions{TargetRows: 100, Order: engine.CopyOrderAuto}
	if err := Component(ctx, []Worker{{Src: crashing, Sink: f.sink}}, f.store, f.syncName, "dst", []engine.TableRef{ref}, opts, log.option()); err == nil {
		t.Fatal("expected the injected crash")
	}
	prog, err := f.store.LoadCopyProgress(ctx, f.syncName, "dst", ref)
	if err != nil {
		t.Fatal(err)
	}
	if prog.StorageOrder || len(prog.Watermark) == 0 {
		t.Fatalf("progress = %+v, want key-order progress", prog)
	}
	if !log.has("DEBUG "+EventStorageOrder) || !log.has("key order") {
		t.Errorf("expected a DEBUG key-order event, got %v", log.events)
	}
	if err := Component(ctx, []Worker{{Src: f.src, Sink: f.sink}}, f.store, f.syncName, "dst", []engine.TableRef{ref}, opts); err != nil {
		t.Fatalf("resume: %v", err)
	}
	f.faithful(t, ctx)
}
