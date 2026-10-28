package agreement

import (
	"fmt"
	"github.com/hongzicong/ConsensusArena/state"
)

const maxFrame = 64 << 20

func (m *Message) FrameTag() uint8 { return uint8(m.Kind) }

// Preserve the imported codec's per-kind fields. In particular, only a
// heartbeat transmits the complete roster: the receiver relies on that rule
// when deciding whether a packet may introduce a new configuration.
func (m *Message) wireFields() Message {
	p := Message{Kind: m.Kind, From: m.From, Roster: roster{Ballot: m.Roster.Ballot}, ReadPrefix: m.ReadPrefix}
	switch m.Kind {
	case heartbeat:
		p.Roster, p.Prefix, p.CommitPrefix = m.Roster, m.Prefix, m.CommitPrefix
	case leaseRequest, leaseRevoke, leaseRevoked:
		p.Sequence = m.Sequence
	case leaseReply:
		p.Sequence, p.Threshold = m.Sequence, m.Threshold
	case prepare:
		p.PrepareStart = m.PrepareStart
	case promise:
		p.PrepareStart, p.Part, p.Parts, p.Entries = m.PrepareStart, m.Part, m.Parts, m.Entries
	case accept:
		p.DeliveryAttempt, p.Entry = m.DeliveryAttempt, m.Entry
	case committedEntry:
		p.Entry = m.Entry
	case accepted:
		p.DeliveryAttempt, p.Entry.Slot, p.Prefix = m.DeliveryAttempt, m.Entry.Slot, m.Prefix
		p.Entry.Ballot = m.Roster.Ballot // The imported decoder reconstructed this field.
	case acceptNote:
		p.Entry.Slot, p.Prefix = m.Entry.Slot, m.Prefix
		p.Entry.Ballot = m.Roster.Ballot
	case commit:
		p.CommitPrefix, p.CommitSlot = m.CommitPrefix, m.CommitSlot
	case forward:
		p.Request = m.Request
	case result:
		p.Request.Origin = m.Request.Origin
		p.Request.Proposal.ClientId, p.Request.Proposal.CommandId = m.Request.Proposal.ClientId, m.Request.Proposal.CommandId
		p.Request.Proposal.Command.Op, p.Request.Proposal.Command.K = m.Request.Proposal.Command.Op, m.Request.Proposal.Command.K
		p.Value = m.Value
	case coldControl:
		p.Value = m.Value
	case repairRequest:
		p.RepairStart = m.RepairStart
	default:
		panic("agreement: invalid outgoing message kind")
	}
	return p
}

func validateMessage(m *Message) error {
	if m.From < 0 || m.From >= 63 || m.Kind >= kindCount {
		return fmt.Errorf("agreement: invalid sender or kind")
	}
	checkRequest := func(r request) error {
		if r.Origin < -1 || r.Origin >= 63 || (r.Proposal.Command.Op > state.SCAN && r.Proposal.Command.Op != bridgeOp) || (r.Proposal.Command.Op == bridgeOp && (r.Origin != -1 || r.Proposal.ClientId >= 0)) {
			return fmt.Errorf("agreement: invalid request")
		}
		return nil
	}
	checkEntry := func(v entry) error {
		rs := v.requests()
		if len(rs) < 1 || len(rs) > maxBatchCommands {
			return fmt.Errorf("agreement: invalid batch")
		}
		for _, r := range rs {
			if err := checkRequest(r); err != nil {
				return err
			}
		}
		return nil
	}
	switch m.Kind {
	case promise:
		if m.Parts < 1 || m.Part < 0 || m.Part >= m.Parts {
			return fmt.Errorf("agreement: invalid snapshot part")
		}
		for _, v := range m.Entries {
			if err := checkEntry(v); err != nil {
				return err
			}
		}
	case accept, committedEntry:
		return checkEntry(m.Entry)
	case forward:
		return checkRequest(m.Request)
	}
	return nil
}
