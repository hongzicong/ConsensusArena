// Package replicaset provides replica identity sets without protocol policy.
// Owners serialize mutations and validate IDs against their actual membership.
package replicaset

import "math/bits"

const MaxSize = 64

// Set stores IDs 0..63 without allocations. Its zero value is empty; assignment
// copies a set without aliasing. Wire encoders can use its uint64 representation.
type Set uint64

func New(ids ...int) Set {
	var s Set
	for _, id := range ids {
		if id < 0 || id >= MaxSize {
			panic("replicaset: ID outside 0..63")
		}
		s.Add(id)
	}
	return s
}

// All returns IDs 0..size-1 and rejects memberships that cannot fit in Set.
func All(size int) Set {
	if size < 0 || size > MaxSize {
		panic("replicaset: membership exceeds 64 IDs")
	}
	return ^Set(0) >> uint(MaxSize-size)
}

// With returns a copy containing id. Invalid IDs leave the set unchanged.
func (s Set) With(id int) Set {
	if id < 0 || id >= MaxSize {
		return s
	}
	return s | Set(1)<<uint(id)
}

// Add reports whether id was new. Duplicate and invalid IDs return false.
func (s *Set) Add(id int) bool {
	before := *s
	*s = s.With(id)
	return *s != before
}

func (s *Set) Remove(id int) {
	if id >= 0 && id < MaxSize {
		*s &^= Set(1) << uint(id)
	}
}

func (s Set) Contains(id int) bool {
	return id >= 0 && id < MaxSize && s&(Set(1)<<uint(id)) != 0
}

func (s Set) Size() int                  { return bits.OnesCount64(uint64(s)) }
func (s Set) Union(other Set) Set        { return s | other }
func (s Set) Intersection(other Set) Set { return s & other }
func (s Set) Covers(other Set) bool      { return s&other == other }
func (s Set) Equal(other Set) bool       { return s == other }

// Range visits members in ascending ID order. Returning false stops iteration.
// Iteration uses a snapshot, so callbacks may mutate the original set.
func (s Set) Range(visit func(int) bool) {
	for s != 0 {
		id := bits.TrailingZeros64(uint64(s))
		if !visit(id) {
			return
		}
		s &= s - 1
	}
}
