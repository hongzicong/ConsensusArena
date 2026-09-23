package bodega

// readSlots orders hold targets, so a protocol message need not scan every
// blocked reader while the executed prefix has not reached its slot.
type readSlots []uint64

func (q readSlots) Len() int           { return len(q) }
func (q readSlots) Less(i, j int) bool { return q[i] < q[j] }
func (q readSlots) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *readSlots) Push(x any)        { *q = append(*q, x.(uint64)) }
func (q *readSlots) Pop() any          { old := *q; x := old[len(old)-1]; *q = old[:len(old)-1]; return x }
