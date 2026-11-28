package engine

import "testing"

func TestPieces(t *testing.T) {
	if got := Pieces([]int(nil), 3); len(got) != 0 {
		t.Errorf("empty input: %v", got)
	}
	got := Pieces([]int{1, 2, 3, 4, 5, 6, 7}, 3)
	if len(got) != 3 || len(got[0]) != 3 || len(got[2]) != 1 || got[2][0] != 7 {
		t.Errorf("pieces of 3: %v", got)
	}
	big := make([]int, 1200)
	if got := Pieces(big, 0); len(got) != 3 || len(got[0]) != MaxKeysPerStatement || len(got[2]) != 200 {
		t.Errorf("default size: %d pieces", len(got))
	}
}
