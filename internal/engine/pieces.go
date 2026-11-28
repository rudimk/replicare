package engine

// MaxKeysPerStatement is the most keys one SQL statement lists (CLAUDE.md §3.3 "key
// lists in pieces"). A statement that inlines every key of a batch grows with
// drain_batch: on Postgres a row-value IN list of ~10k composite keys fails to plan
// ("stack depth limit exceeded"), MySQL caps a prepared statement at 65,535
// placeholders, and both plan a long list slowly. So every query over a list of keys
// (re-reads, target deletes, verify look-ups, catalog look-ups by table) runs in
// pieces of at most this many, whatever the batch size. 500 keeps planning under a
// few milliseconds for a 3-column key.
const MaxKeysPerStatement = 500

// Pieces splits items into consecutive pieces of at most size items each (size < 1
// means MaxKeysPerStatement). An empty input gives no pieces.
func Pieces[T any](items []T, size int) [][]T {
	if size < 1 {
		size = MaxKeysPerStatement
	}
	out := make([][]T, 0, (len(items)+size-1)/size)
	for lo := 0; lo < len(items); lo += size {
		hi := min(lo+size, len(items))
		out = append(out, items[lo:hi])
	}
	return out
}
