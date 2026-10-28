package agreement

import "github.com/hongzicong/ConsensusArena/state"

type completionSpan struct {
	Start, End int64
	Owner      int
}

func (e *engine) plannedCompletionOwner(key state.Key) (int, bool) {
	rank := e.opt.KeyPolicy.rank(key)
	if rank < 0 {
		return e.current.Leader, false
	}
	for _, span := range e.opt.Completion {
		if rank < span.Start {
			break
		}
		if rank < span.End {
			return span.Owner, true
		}
	}
	return e.current.Leader, false
}

func (e *engine) delegatedKey(key state.Key) bool {
	_, delegated := e.plannedCompletionOwner(key)
	return delegated
}

func (e *engine) completionOwner(key state.Key) int {
	owner, delegated := e.plannedCompletionOwner(key)
	if delegated && e.current.respondersFor(key)&bit(owner) != 0 {
		return owner
	}
	return e.current.Leader
}

// Only these writes permit the owner to keep serving its committed value.
func (e *engine) ownerMayBypass(v entry, r request) bool {
	return !(e.opt.ClientLearning && (e.opt.AllKeyLearning || e.opt.KeyPolicy.rank(r.Proposal.Command.K) >= int64(e.opt.KeyPolicy.hotCutoff))) &&
		r.Origin == e.completionOwner(r.Proposal.Command.K) && v.ReadFresh && v.Ballot == e.current.Ballot
}

// An entry has at most one delayed voter. Multiple owners withholding their
// votes for the same entry would deadlock. Normal proposals split such batches;
// any unsplit entry conservatively disables the old-value shortcut instead.
func (e *engine) completionGate(v entry) int {
	owner := -1
	for _, r := range v.requests() {
		if r.Proposal.Command.Op != state.PUT || !e.ownerMayBypass(v, r) {
			continue
		}
		next := e.completionOwner(r.Proposal.Command.K)
		if owner >= 0 && owner != next {
			return -1
		}
		owner = next
	}
	return owner
}
