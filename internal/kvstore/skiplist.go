package kvstore

import (
	"fmt"
	"math/rand/v2"
)

const (
	maxLevel = 16
	p        = 0.5
)

type skipListNode struct {
	score    float64
	member   string
	forward  []*skipListNode
	backward *skipListNode // Added for reverse traversal
}

type skipList struct {
	header *skipListNode
	tail   *skipListNode // Added for O(1) access to tail
	level  int
	length int
	dict   map[string]float64
}

func newSkipList() *skipList {
	return &skipList{
		header: &skipListNode{
			forward: make([]*skipListNode, maxLevel),
		},
		level:  1,
		length: 0,
		dict:   make(map[string]float64),
	}
}

func (sl *skipList) randomLevel() int {
	level := 1
	for rand.Float64() < p && level < maxLevel {
		level++
	}
	return level
}

// less compares two nodes. Returns true if (score1, member1) < (score2, member2).
func less(score1 float64, member1 string, score2 float64, member2 string) bool {
	if score1 == score2 {
		return member1 < member2
	}
	return score1 < score2
}

func (sl *skipList) insert(score float64, member string) {
	if oldScore, exists := sl.dict[member]; exists {
		if oldScore == score {
			return // Zaten aynı score ile mevcut
		}
		// Varsa ve skoru değiştiyse, önce eski kaydı siliyoruz
		sl.delete(member)
	}

	update := make([]*skipListNode, maxLevel)
	curr := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && less(curr.forward[i].score, curr.forward[i].member, score, member) {
			curr = curr.forward[i]
		}
		update[i] = curr
	}

	lvl := sl.randomLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			update[i] = sl.header
		}
		sl.level = lvl
	}

	newNode := &skipListNode{
		score:   score,
		member:  member,
		forward: make([]*skipListNode, lvl),
	}

	for i := 0; i < lvl; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	// Set backward pointer
	if update[0] == sl.header {
		newNode.backward = nil
	} else {
		newNode.backward = update[0]
	}
	if newNode.forward[0] != nil {
		newNode.forward[0].backward = newNode
	} else {
		sl.tail = newNode // It's the last node
	}

	sl.length++
	sl.dict[member] = score
}

func (sl *skipList) delete(member string) bool {
	score, exists := sl.dict[member]
	if !exists {
		return false
	}

	update := make([]*skipListNode, maxLevel)
	curr := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && less(curr.forward[i].score, curr.forward[i].member, score, member) {
			curr = curr.forward[i]
		}
		update[i] = curr
	}

	curr = curr.forward[0]
	if curr != nil && curr.score == score && curr.member == member {
		for i := 0; i < sl.level; i++ {
			if update[i].forward[i] != curr {
				break
			}
			update[i].forward[i] = curr.forward[i]
		}

		// Update backward pointer and tail
		if curr.forward[0] != nil {
			curr.forward[0].backward = curr.backward
		} else {
			sl.tail = curr.backward
		}

		for sl.level > 1 && sl.header.forward[sl.level-1] == nil {
			sl.level--
		}

		sl.length--
		delete(sl.dict, member)
		return true
	}

	return false
}

func (sl *skipList) search(member string) (float64, bool) {
	score, exists := sl.dict[member]
	return score, exists
}

// rank returns the 0-based rank of the member. Returns 0, false if not found.
func (sl *skipList) rank(member string) (int, bool) {
	score, exists := sl.dict[member]
	if !exists {
		return 0, false
	}

	// Without span calculations, we traverse level 0 to find rank = O(N).
	rank := 0
	curr := sl.header.forward[0]
	for curr != nil {
		if curr.score == score && curr.member == member {
			return rank, true
		}
		rank++
		curr = curr.forward[0]
	}

	panic(fmt.Sprintf("skiplist inconsistency: member %q found in dict but not in list", member))
}

// rangeByRank returns nodes within 0-based bounds [start, stop].
// Includes both ends if within list bounds.
func (sl *skipList) rangeByRank(start, stop int) []skipListNode {
	if start < 0 {
		start = 0
	}
	if stop >= sl.length {
		stop = sl.length - 1
	}
	if start > stop || start >= sl.length {
		return []skipListNode{}
	}

	res := make([]skipListNode, 0, stop-start+1)
	curr := sl.header.forward[0]
	idx := 0

	for curr != nil && idx <= stop {
		if idx >= start {
			res = append(res, skipListNode{score: curr.score, member: curr.member})
		}
		curr = curr.forward[0]
		idx++
	}

	return res
}

// revRangeByRank returns nodes within 0-based bounds [start, stop] in reverse order.
// Rank 0 is the highest score.
func (sl *skipList) revRangeByRank(start, stop int) []skipListNode {
	if start < 0 {
		start = 0
	}
	if stop >= sl.length {
		stop = sl.length - 1
	}
	if start > stop || start >= sl.length {
		return []skipListNode{}
	}

	res := make([]skipListNode, 0, stop-start+1)
	
	// Start from the tail (highest score)
	curr := sl.tail
	idx := 0

	// Skip nodes until we reach 'start' rank
	for curr != nil && idx < start {
		curr = curr.backward
		idx++
	}

	// Collect nodes until we reach 'stop' rank
	for curr != nil && idx <= stop {
		res = append(res, skipListNode{score: curr.score, member: curr.member})
		curr = curr.backward
		idx++
	}

	return res
}
