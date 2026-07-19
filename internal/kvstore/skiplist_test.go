package kvstore

import (
	"fmt"
	"reflect"
	"testing"
)

func TestSkipList(t *testing.T) {
	t.Run("InsertAndSearch_Success", func(t *testing.T) {
		sl := newSkipList()
		sl.insert(10.5, "Alice")
		score, ok := sl.search("Alice")
		if !ok || score != 10.5 {
			t.Errorf("expected 10.5, got %v (ok: %v)", score, ok)
		}
	})

	t.Run("Insert_UpdateScore", func(t *testing.T) {
		sl := newSkipList()
		sl.insert(10.5, "Bob")
		sl.insert(20.0, "Bob")
		score, ok := sl.search("Bob")
		if !ok || score != 20.0 {
			t.Errorf("expected 20.0, got %v (ok: %v)", score, ok)
		}
		if sl.length != 1 {
			t.Errorf("expected list length 1, got %d", sl.length)
		}
	})

	t.Run("Delete_Existing", func(t *testing.T) {
		sl := newSkipList()
		sl.insert(15.0, "Charlie")
		delOk := sl.delete("Charlie")
		if !delOk {
			t.Error("expected delete to return true")
		}
		_, ok := sl.search("Charlie")
		if ok {
			t.Error("expected Charlie to be absent after deletion")
		}
		if sl.length != 0 {
			t.Errorf("expected list length 0, got %d", sl.length)
		}
	})

	t.Run("Delete_NonExisting", func(t *testing.T) {
		sl := newSkipList()
		delOk := sl.delete("Diana")
		if delOk {
			t.Error("expected delete to return false for absent member")
		}
	})

	t.Run("Rank_Success", func(t *testing.T) {
		sl := newSkipList()
		// Insert out of order
		sl.insert(30.0, "c")
		sl.insert(10.0, "a")
		sl.insert(20.0, "b")

		// Ranks should be 0:a, 1:b, 2:c
		r, ok := sl.rank("a")
		if !ok || r != 0 {
			t.Errorf("expected a to have rank 0, got %d", r)
		}
		r, ok = sl.rank("b")
		if !ok || r != 1 {
			t.Errorf("expected b to have rank 1, got %d", r)
		}
		r, ok = sl.rank("c")
		if !ok || r != 2 {
			t.Errorf("expected c to have rank 2, got %d", r)
		}
	})

	t.Run("RangeByRank_Success", func(t *testing.T) {
		sl := newSkipList()
		sl.insert(10.0, "x")
		sl.insert(20.0, "y")
		sl.insert(30.0, "z")

		nodes := sl.rangeByRank(0, 1)
		if len(nodes) != 2 {
			t.Fatalf("expected 2 nodes, got %d", len(nodes))
		}
		if nodes[0].member != "x" || nodes[1].member != "y" {
			t.Errorf("expected x and y, got %v", nodes)
		}
	})

	t.Run("RangeByRank_OutOfBoundsSafe", func(t *testing.T) {
		sl := newSkipList()
		sl.insert(10.0, "1")

		// start > stop
		nodes := sl.rangeByRank(2, 1)
		if len(nodes) != 0 {
			t.Errorf("expected empty array for start > stop, got %v", nodes)
		}

		// stop > length
		nodes = sl.rangeByRank(0, 10)
		if len(nodes) != 1 || nodes[0].member != "1" {
			t.Errorf("expected safe bounds capping, got %v", nodes)
		}
	})

	t.Run("ScoreTieBreaking_Lexicographical", func(t *testing.T) {
		sl := newSkipList()
		sl.insert(10.0, "c")
		sl.insert(10.0, "a")
		sl.insert(10.0, "b")

		nodes := sl.rangeByRank(0, 2)
		expected := []string{"a", "b", "c"}
		var actual []string
		for _, n := range nodes {
			actual = append(actual, n.member)
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected score ties to sort lexicographically %v, got %v", expected, actual)
		}
	})
	t.Run("Insert_HighVolume_LengthConsistency", func(t *testing.T) {
		t.Parallel()
		sl := newSkipList()
		for i := 1; i <= 500; i++ {
			sl.insert(float64(i), fmt.Sprintf("mem%d", i))
		}
		// Delete 250 of them (odd indices)
		for i := 1; i <= 500; i += 2 {
			sl.delete(fmt.Sprintf("mem%d", i))
		}
		if sl.length != 250 {
			t.Errorf("expected list length 250, got %d", sl.length)
		}
		if len(sl.dict) != 250 {
			t.Errorf("expected dict length 250, got %d", len(sl.dict))
		}
	})

	t.Run("RangeByRank_SingleElement", func(t *testing.T) {
		t.Parallel()
		sl := newSkipList()
		sl.insert(100.0, "lonely")
		nodes := sl.rangeByRank(0, 0)
		if len(nodes) != 1 || nodes[0].member != "lonely" {
			t.Errorf("expected only 'lonely', got %v", nodes)
		}
	})

	t.Run("ScoreTieBreaking_RankConsistency", func(t *testing.T) {
		t.Parallel()
		sl := newSkipList()
		sl.insert(10.0, "c")
		sl.insert(10.0, "a")
		sl.insert(10.0, "b")

		rA, _ := sl.rank("a")
		rB, _ := sl.rank("b")
		rC, _ := sl.rank("c")

		if rA != 0 { t.Errorf("expected a to have rank 0, got %d", rA) }
		if rB != 1 { t.Errorf("expected b to have rank 1, got %d", rB) }
		if rC != 2 { t.Errorf("expected c to have rank 2, got %d", rC) }
	})

	t.Run("RevRangeByRank_Success", func(t *testing.T) {
		sl := newSkipList()
		sl.insert(10.0, "x")
		sl.insert(20.0, "y")
		sl.insert(30.0, "z")

		// RevRange: rank 0 is z (highest score), rank 1 is y
		nodes := sl.revRangeByRank(0, 1)
		if len(nodes) != 2 {
			t.Fatalf("expected 2 nodes, got %d", len(nodes))
		}
		if nodes[0].member != "z" || nodes[1].member != "y" {
			t.Errorf("expected z and y, got %v", nodes)
		}
	})
}
