package client

import (
	"time"

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

type bodegaPending struct {
	cmd            defs.Propose
	initial, hedge int
	timer          *time.Timer
}

func (c *Client) trackBodegaRequest(cmd defs.Propose, initial int) {
	b := c.bodega
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.pending[cmd.CommandId]; exists {
		return
	}
	p := &bodegaPending{cmd: cmd, initial: initial, hedge: -1}
	b.pending[cmd.CommandId] = p
	if cmd.Command.Op == state.GET {
		delay := c.BodegaUnhold
		if delay == 0 {
			delay = 250 * time.Millisecond
		}
		p.timer = time.AfterFunc(delay, func() { c.hedgeBodegaRead(cmd.CommandId) })
	}
}

func (c *Client) hedgeBodegaRead(id int32) {
	b := c.bodega
	b.mu.Lock()
	p := b.pending[id]
	if p == nil || p.cmd.Command.Op != state.GET || p.hedge >= 0 {
		b.mu.Unlock()
		return
	}
	hint := b.roster.Load()
	if hint == nil || !c.bodegaPeerAlive(hint.Leader) {
		// A routing refresh may supply a new leader; never replay a write.
		p.timer = time.AfterFunc(100*time.Millisecond, func() { c.hedgeBodegaRead(id) })
		b.mu.Unlock()
		return
	}
	if hint.Leader == p.initial {
		b.mu.Unlock()
		return
	}
	p.hedge = hint.Leader
	cmd, target := p.cmd, p.hedge
	b.mu.Unlock()
	c.writeBodegaProposal(cmd, target, true)
}

func (c *Client) completeBodegaRequest(id int32, source int) bool {
	b := c.bodega
	b.mu.Lock()
	p := b.pending[id]
	if p == nil {
		b.duplicates.Add(1)
		b.mu.Unlock()
		return false
	}
	delete(b.pending, id)
	if p.timer != nil {
		p.timer.Stop()
	}
	b.mu.Unlock()
	if p.hedge >= 0 {
		if source == p.hedge {
			b.hedgeWins.Add(1)
		}
		loser := p.initial
		if source == p.initial {
			loser = p.hedge
		}
		// Best-effort transport cleanup: does not wait before delivering the winner.
		go func() {
			select {
			case <-b.stop:
				return
			default:
			}
			cancel := defs.Propose{ClientId: p.cmd.ClientId, CommandId: id}
			cancel.Command.Op = defs.BodegaCancelRead
			b.cancels.Add(1)
			c.writeBodegaProposal(cancel, loser, false)
		}()
	}
	return true
}

func (c *Client) stopBodegaReads() {
	b := c.bodega
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, p := range b.pending {
		if p.timer != nil {
			p.timer.Stop()
		}
	}
	b.pending = make(map[int32]*bodegaPending)
}
