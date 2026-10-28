package swift

import (
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

// All local work belongs to the replica event-loop owner. A generation keeps a
// queued event from touching a descriptor that has been returned to its pool.
type localWorkKind uint8

const (
	descriptorWork localWorkKind = iota
	replyWork
	retryWork
)

type localWork struct {
	kind       localWorkKind
	desc       *commandDesc
	generation uint64
	cmdID      defs.RequestID
	message    any
}

const localWorkBudget = 256

func (c *core) hasDelivered(id defs.RequestID) bool {
	_, exists := c.delivered[id]
	return exists
}

func (c *core) hasLocalWork() bool { return c.localHead < len(c.localWork) }

func (c *core) drainLocalWork(budget int) {
	for budget > 0 && c.status == NORMAL && c.hasLocalWork() {
		work := c.localWork[c.localHead]
		c.localWork[c.localHead] = localWork{}
		c.localHead++
		budget--
		if work.kind == retryWork {
			c.beginProposalBatch(1)
			c.submit(work.message.(*proposal))
			continue
		}
		if !work.desc.active || work.desc.generation != work.generation || c.descriptors[work.cmdID] != work.desc {
			continue
		}
		switch work.kind {
		case descriptorWork:
			c.handleMsg(work.message, work.desc, work.cmdID)
		case replyWork:
			c.completeReply(work.desc, work.message.(*replyArgs))
		}
	}
	if !c.hasLocalWork() {
		c.localWork = c.localWork[:0]
		c.localHead = 0
	} else if c.localHead > len(c.localWork)/2 {
		pending := copy(c.localWork, c.localWork[c.localHead:])
		clear(c.localWork[pending:])
		c.localWork = c.localWork[:pending]
		c.localHead = 0
	}
}

// Recovery freezes the current descriptor states and drops their queued work,
// as stopping the old workers did. Requeued proposals survive another recovery.
func (c *core) pauseLocalWork() {
	kept := c.localWork[:0]
	for _, work := range c.localWork[c.localHead:] {
		if work.kind == retryWork {
			kept = append(kept, work)
		}
	}
	clear(c.localWork[len(kept):])
	c.localWork = kept
	c.localHead = 0
}

func (c *core) scheduleDelivery(id defs.RequestID) { c.getCmdDesc(id, "deliver", nil) }

func (c *core) requeueProposal(p *proposal) {
	c.localWork = append(c.localWork, localWork{kind: retryWork, message: p})
}

func (c *core) queueReply(desc *commandDesc, id defs.RequestID, value state.Value) {
	hs := make([]SHash, len(desc.hs))
	copy(hs, desc.hs)
	args := &replyArgs{
		hs: hs, val: value, cmdID: id, propose: desc.propose,
	}
	c.localWork = append(c.localWork, localWork{kind: replyWork, desc: desc, generation: desc.generation, cmdID: id, message: args})
}
