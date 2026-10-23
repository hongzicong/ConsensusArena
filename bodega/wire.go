package bodega

import (
	"fmt"
	rpc "github.com/hongzicong/ConsensusArena/rpc"
	"github.com/hongzicong/ConsensusArena/state"
)

const maxFrame = 64 << 20

func (m *message) FrameTag() uint8 { return uint8(m.Kind) }

func (*message) New() rpc.Serializable { return &message{} }

// These are shallow projections of the generated native messages. Payloads
// and batches are shared, and only fields allowed by the message kind survive.
func (m *Message) wireFields() Message {
	p := Message{Kind: m.Kind, From: m.From, Roster: Roster{Ballot: m.Roster.Ballot}, ReadPrefix: m.ReadPrefix}
	switch m.Kind {
	case commit:
		p.CommitPrefix, p.CommitSlot = m.CommitPrefix, m.CommitSlot
	case heartbeat:
		p.Roster = m.Roster
		p.Prefix, p.CommitPrefix = m.Prefix, m.CommitPrefix
	case lightHeartbeat:
		p.Prefix, p.CommitPrefix = m.Prefix, m.CommitPrefix
	case rosterRequest:
	case leaseRequest, leaseRevoke, leaseRevoked:
		p.Sequence = m.Sequence
	case leaseReply:
		p.Sequence, p.Threshold = m.Sequence, m.Threshold
	case prepare:
		p.PrepareStart = m.PrepareStart
	case promise:
		p.PrepareStart, p.Part, p.Parts = m.PrepareStart, m.Part, m.Parts
		p.Entries = m.Entries
	case accept, committedEntry:
		p.Entry = m.Entry
	case accepted, acceptNote:
		p.Entry = Entry{Slot: m.Entry.Slot}
		p.Prefix = m.Prefix
	case forward:
		p.Request = m.Request
	case result:
		p.Request = Request{Origin: m.Request.Origin}
		p.Request.Proposal.ClientId = m.Request.Proposal.ClientId
		p.Request.Proposal.CommandId = m.Request.Proposal.CommandId
		p.Value = m.Value
	case repairRequest:
		p.RepairStart = m.RepairStart
	default:
		panic("bodega: invalid outgoing message kind")
	}
	return p
}

func (m *Entry) wireFields() Entry {
	p := *m
	if len(p.Batch) == 1 {
		p.Request = p.Batch[0]
		p.Batch = nil
	} else if len(p.Batch) > 1 {
		p.Request = Request{}
	}
	return p
}

func validateProtoMessage(m *message) error {
	if m.From < 0 || m.From >= 63 || m.Kind >= kindCount {
		return fmt.Errorf("bodega: invalid sender or kind")
	}
	checkRequest := func(r request) error {
		if r.Origin < -1 || r.Origin >= 63 || r.Proposal.Command.Op > state.SCAN {
			return fmt.Errorf("bodega: invalid request")
		}
		return nil
	}
	checkEntry := func(e *entry) error {
		rs := e.requests()
		if len(rs) == 0 || len(rs) > maxBatchCommands {
			return fmt.Errorf("bodega: invalid batch")
		}
		for _, r := range rs {
			if err := checkRequest(r); err != nil {
				return err
			}
		}
		if len(e.Batch) == 1 {
			e.Request = e.Batch[0]
			e.Batch = nil
		}
		return nil
	}
	switch m.Kind {
	case promise:
		if m.Parts < 1 || m.Part < 0 || m.Part >= m.Parts {
			return fmt.Errorf("bodega: invalid snapshot part")
		}
		for i := range m.Entries {
			if err := checkEntry(&m.Entries[i]); err != nil {
				return err
			}
		}
	case accept, committedEntry:
		return checkEntry(&m.Entry)
	case accepted, acceptNote:
		m.Entry.Ballot = m.Roster.Ballot
	case forward:
		return checkRequest(m.Request)
	}
	return nil
}
