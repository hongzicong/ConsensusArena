package swift

import (
	"bytes"
	"crypto/sha256"
	"fmt"

	"github.com/hongzicong/ConsensusArena/state"
)

type SHash struct {
	H [32]byte
}

type HashNode struct {
	id           CommandId
	next         *HashNode
	previous     *HashNode
	proposalHash SHash
}

type UpdateEntry struct {
	hash   []SHash
	seqnum int
}

// TODO: commutative reads

type HashLog struct {
	ballot int32 // sequence numbers are local to this installed ballot
	// seqnum of the highest synced command
	synced int
	// hash of the highest synced command
	syncedHash SHash
	// head and tail of the linked list of pending commands
	pendingHead *HashNode
	pendingTail *HashNode
	// hash of the last command in the log
	hash  SHash
	dirty bool // rebuild only when a proposal actually needs the path hash
	// mapping from cmdIds to nodes
	nodes map[CommandId]*HashNode
	// mapping from yet-to-be-synced commands to seqnums and hashes
	// this is for the cases when a command is updated before being appended
	pendingUpd map[CommandId]*UpdateEntry
}

func (n *HashNode) String() string {
	if n == nil {
		return "[]"
	}
	return n.id.String() + " -> " + n.next.String()
}

func NewHashLog() *HashLog {
	return &HashLog{
		ballot:     -1,
		synced:     -1,
		nodes:      make(map[CommandId]*HashNode),
		pendingUpd: make(map[CommandId]*UpdateEntry),
	}
}

// BeginBallot preserves path evidence while replacing the sequence comparison
// domain. It never resets the prefix hash to manufacture matching evidence.
func (l *HashLog) BeginBallot(ballot int32) int {
	if ballot <= l.ballot {
		return 0
	}
	// Seal the actual local history, not an invented common checkpoint. The
	// next ballot replays its proposals into a new chain in their new order.
	count := len(l.nodes)
	l.syncedHash = l.pathHash()
	l.hash = l.syncedHash
	l.ballot, l.synced = ballot, -1
	l.pendingHead, l.pendingTail = nil, nil
	l.nodes = make(map[CommandId]*HashNode)
	l.pendingUpd = make(map[CommandId]*UpdateEntry)
	l.dirty = false
	return count
}

func (l *HashLog) Append(_ state.Command, cmdId CommandId) SHash {
	if upd, exists := l.pendingUpd[cmdId]; exists {
		delete(l.pendingUpd, cmdId)
		l.update(cmdId, upd.seqnum, upd.hash[0], false)
		return upd.hash[0]
	}
	if node, exists := l.nodes[cmdId]; exists {
		return node.proposalHash
	}
	l.pathHash()

	node := &HashNode{
		id: cmdId,
	}
	l.nodes[cmdId] = node

	if l.pendingHead == nil {
		l.pendingHead = node
		l.hash = SHash{hash(l.syncedHash.H, cmdId)}
		node.proposalHash = l.hash
		return l.hash
	}
	if l.pendingTail == nil {
		l.pendingTail = node
		node.previous = l.pendingHead
		l.pendingHead.next = node
	} else {
		l.pendingTail.next = node
		node.previous = l.pendingTail
		l.pendingTail = node
	}
	l.hash = SHash{hash(l.hash.H, cmdId)}
	node.proposalHash = l.hash
	return l.hash
}

func (l *HashLog) Update(cmdId CommandId, s int, h SHash) {
	l.update(cmdId, s, h, true)
}

// AppendDeferred retains the exact local order without making a fast proposal.
// Replica ballot deduplication prevents later ordinary reinitialization of id.
func (l *HashLog) AppendDeferred(cmdId CommandId) {
	if upd, exists := l.pendingUpd[cmdId]; exists {
		delete(l.pendingUpd, cmdId)
		l.update(cmdId, upd.seqnum, upd.hash[0], false)
		return
	}
	if _, exists := l.nodes[cmdId]; exists {
		return
	}
	node := &HashNode{id: cmdId}
	l.nodes[cmdId] = node
	if l.pendingHead == nil {
		l.pendingHead = node
	} else if l.pendingTail == nil {
		node.previous = l.pendingHead
		l.pendingHead.next = node
		l.pendingTail = node
	} else {
		node.previous = l.pendingTail
		l.pendingTail.next = node
		l.pendingTail = node
	}
	l.dirty = true
}

// AppendAndUpdate has the same future path as Append followed immediately by
// Update, without computing the unused proposal digest. Callers must already
// have genuine leader evidence and must not emit a local fast vote for this id.
func (l *HashLog) AppendAndUpdate(cmdId CommandId, s int, h SHash) {
	if upd, exists := l.pendingUpd[cmdId]; exists {
		delete(l.pendingUpd, cmdId)
		l.update(cmdId, upd.seqnum, upd.hash[0], false)
		// Append consumed its earlier update without adding a node; the
		// subsequent Update therefore retains the new deferred entry.
		l.update(cmdId, s, h, true)
		return
	}
	l.update(cmdId, s, h, false)
}

func (l *HashLog) String() string {
	s := fmt.Sprintf("%v, syncedHash: %v\n", l.synced, l.syncedHash)
	s += l.pendingHead.String() + "\n"
	return s + fmt.Sprintf("hash: %v", l.hash)
}

func (l *HashLog) update(cmdId CommandId, s int, h SHash, save bool) {
	n, exists := l.nodes[cmdId]
	if !exists && save {
		l.pendingUpd[cmdId] = &UpdateEntry{
			hash:   []SHash{h},
			seqnum: s,
		}
		return
	}
	if exists {
		previous := n.previous
		next := n.next
		if previous != nil {
			previous.next = next
		}
		if next != nil {
			next.previous = previous
		}
		if l.pendingHead == n {
			l.pendingHead = next
		}
		if l.pendingTail == n {
			l.pendingTail = previous
		}
		n.previous = nil
		n.next = nil
		delete(l.nodes, cmdId)
	}
	// TODO: do not update if !exist and s > l.synced but l.syncedHash == h
	if s <= l.synced && !exists {
		return
	}
	if s > l.synced {
		l.synced = s
		l.syncedHash = h
	}
	l.dirty = true
}

// Updates can arrive in batches. Intermediate digests have no consumer; only
// Append exposes a hash to the protocol. Compute the identical final fold once.
func (l *HashLog) pathHash() SHash {
	if l.dirty {
		initial := l.syncedHash.H
		for n := l.pendingHead; n != nil; n = n.next {
			initial = hash(initial, n.id)
		}
		l.hash = SHash{initial}
		l.dirty = false
	}
	return l.hash
}

func hash(initial [32]byte, cmdId CommandId) [32]byte {
	bs := make([]byte, 40)
	for i, b := range initial {
		bs[i] = b
	}

	tmp32 := cmdId.ClientId
	bs[32] = byte(tmp32)
	bs[33] = byte(tmp32 >> 8)
	bs[34] = byte(tmp32 >> 16)
	bs[35] = byte(tmp32 >> 24)
	tmp32 = cmdId.SeqNum
	bs[36] = byte(tmp32)
	bs[37] = byte(tmp32 >> 8)
	bs[38] = byte(tmp32 >> 16)
	bs[39] = byte(tmp32 >> 24)

	return sha256.Sum256(bs)
}

func SHashesEq(hs1 []SHash, hs2 []SHash) bool {
	if len(hs1) != len(hs2) {
		return false
	}

	for _, h := range hs1 {
		eq := false
		for _, g := range hs2 {
			if bytes.Equal(h.H[:], g.H[:]) {
				eq = true
				break
			}
		}
		if !eq {
			return false
		}
	}

	return true
}
