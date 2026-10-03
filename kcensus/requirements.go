package kcensus

// Requirement representation and compatibility validation.
// Based on KCensus Algorithms 1 and 4 and LPD-EPFL/kcensus (b232c332),
// src/consensus/kcensus/propagation.rs. See LICENSE.upstream for its MIT license.
import (
	"fmt"
	"math/bits"
)

type Requirement []uint64

func (r Requirement) Quorum() uint64 {
	var q uint64
	for w, a := range r {
		if a != 0 {
			q |= 1 << w
		}
	}
	return q
}

func (r Requirement) valid(n, f int) bool {
	if len(r) != n {
		return false
	}
	q := r.Quorum()
	if bits.OnesCount64(q) <= f || q>>(2*f+1) != 0 {
		return false
	}
	for w, a := range r {
		if a != 0 && (a&(1<<w) == 0 || a & ^q != 0) {
			return false
		}
	}
	return true
}

func Compatible(a, b Requirement, f int) bool {
	i := a.Quorum() & b.Quorum()
	var witnesses uint64
	for w := range a {
		if a[w]&i != 0 || b[w]&i != 0 {
			witnesses |= 1 << w
		}
	}
	return bits.OnesCount64(witnesses) > f
}

func ValidateRequirements(rs []Requirement) error { return validateRequirements(rs, len(rs)) }

func validateRequirements(rs []Requirement, voters int) error {
	m := len(rs)
	if voters < 3 || voters > m || m > 63 || voters%2 != 1 {
		return fmt.Errorf("KCensus requires odd 3..63 voters and at most 63 processes")
	}
	for p, r := range rs {
		if !r.valid(m, voters/2) {
			return fmt.Errorf("invalid requirement for proposer %d", p)
		}
		for q := 0; q < p; q++ {
			if !Compatible(r, rs[q], voters/2) {
				return fmt.Errorf("incompatible requirements %d/%d", p, q)
			}
		}
	}
	return nil
}
