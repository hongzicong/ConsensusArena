package agreement

import (
	"crypto/sha256"
	"encoding/json"
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
	"time"
)

const coldPUT = state.Operation(250)
const coldProof = state.Operation(249)
const coldVoteOK = uint8(3)
const coldDoneOK = uint8(4)
const coldPartBytes = 512 << 10

type coldGeneration struct{ B, E uint64 }

func (a coldGeneration) less(b coldGeneration) bool { return a.B < b.B || a.B == b.B && a.E < b.E }

type coldRound struct {
	G coldGeneration
	K uint64
}

func (a coldRound) less(b coldRound) bool { return a.G.less(b.G) || a.G == b.G && a.K < b.K }

type ColdVoteIdentity struct {
	Round      coldRound
	Key        state.Key
	Version, Q uint64
	ID         defs.RequestID
	Origin     int
	Timestamp  int64
	Hash       [32]byte
}
type coldRecord struct {
	ColdVoteIdentity
	Fast   bool
	Voters uint64
}
type coldID struct {
	ID   defs.RequestID
	Hash [32]byte
	Key  state.Key
}
type coldKey struct {
	Prefix   uint64
	Last     coldRecord
	Promise  coldRound
	Fast     bool
	Ready    bool
	Q        uint64
	Accepted map[uint64]coldRecord
	Choices  map[uint64]coldRecord
	Pending  map[defs.RequestID]request
	Done     map[defs.RequestID][32]byte
	Since    time.Time
}
type coldSnapshot struct {
	Inputs []request
	Keys   []coldKeySnapshot
	Done   []coldID
}
type coldKeySnapshot struct {
	Key      state.Key
	Prefix   uint64
	Last     coldRecord
	Promise  coldRound
	Accepted []coldRecord
	Done     []coldID
}
type coldMessage struct {
	Hashes      [][32]byte `json:",omitempty"`
	Payloads    []request  `json:",omitempty"`
	Type        string
	G           coldGeneration
	Round       coldRound
	Key         state.Key
	Q           uint64
	Record      coldRecord
	Request     request
	Keys        []coldKeySnapshot
	Done        []coldID
	Part, Parts int
	Hash        [32]byte
	Data        []byte
	Ref         coldScanRef
	Frozen      bool
}
type coldParts struct {
	N     int
	Parts map[int][]byte
}
type coldPrepareState struct {
	CatalogDraft                       *coldBundle
	CatalogMissing                     map[[32]byte]bool
	CatalogLast                        time.Time
	PayloadHolders                     map[[32]byte]uint64
	Bundle                             []byte
	BundleHash                         [32]byte
	BulkVotes, BulkApplied, ChoiceMask uint64
	G                                  coldGeneration
	Frozen                             bool
	Sources                            map[int]*coldParts
	Snapshots                          map[int]coldSnapshot
	Q                                  uint64
	Merged                             bool
	Install                            []byte
	Installed                          uint64
	Since                              time.Time
	Last                               time.Time
	Tails                              map[state.Key]coldRecord
	Recovered                          map[state.Key]coldKeySnapshot
	Ready                              map[int]bool
	Enabled                            bool
	FetchPending                       map[[32]byte]bool
}
type coldKeyPrepare struct {
	Since    time.Time
	Round    coldRound
	Promises map[int]coldKeySnapshot
	Sources  map[int]*coldParts
	Selected []coldRecord
	Votes    map[uint64]uint64
	Ready    uint64
	Resume   bool
	Prepared bool
	Last     time.Time
}
type coldStatistics struct {
	RecoveryCodecInputBytes, RecoveryCodecOutputBytes, RecoveryCodecEncodeNanos, RecoveryCodecDecodeNanos                                                                                                                                                                                                                                                                                           uint64
	RecoveryRankChecks, RecoveryRankAcceptedBelowNumericCutoff, RecoveryRankRejected                                                                                                                                                                                                                                                                                                                uint64
	QueuedBytes, MaxQueuedBytes, MaxQueuedDepth                                                                                                                                                                                                                                                                                                                                                     uint64
	BridgeProposals, BridgeBytes, BridgeWaits, BridgeMissingBodies, BridgeInstallations, BridgeWaitNanos, BridgeReplay, BridgeIgnored                                                                                                                                                                                                                                                               uint64
	CatalogSourceBytes, CatalogBundleBytes, SourceReferencedBodies, ManifestReferences, ManifestCacheHits, ManifestCacheMisses, ManifestWaitSamples, ManifestWaitNanos, ManifestDecodeSamples, ManifestPendingDuplicates, PayloadNeedFrames, PayloadRequestedBodies, PayloadReplyFrames, PayloadReplyBodies, PayloadBodyBytes, PayloadReceivedBodies, PayloadRejectedBodies, MissingHolderCampaigns uint64
	CompletedReadRetirements, ForwardedReadAttempts                                                                                                                                                                                                                                                                                                                                                 uint64
	PeerHealthRefreshes, TransferPauses, TransferCompleted, TransferRetries                                                                                                                                                                                                                                                                                                                         uint64
	BulkSourceBytes, BulkBodyBytes, BulkTailKeys, BulkAccepts, BulkVotes, BulkCommits, BulkApplications, BulkDuplicates, BulkRejected, BulkEncodeNanos, BulkDecodeNanos                                                                                                                                                                                                                             uint64
	FastWindowSamples, FastWindowNanos, ClassicWindowSamples, ClassicWindowNanos                                                                                                                                                                                                                                                                                                                    uint64
	KeyRecoverySamples, KeyRecoveryNanos, GlobalRecoverySamples, GlobalRecoveryNanos                                                                                                                                                                                                                                                                                                                uint64
	HeldReadSamples, HeldReadNanos                                                                                                                                                                                                                                                                                                                                                                  uint64
	Arrivals, Fresh, Repeated, FastVotes, FastChoices, ClassicChoices, ColdDone, ColdReads, HeldReads                                                                                                                                                                                                                                                                                               uint64
	Collisions, KeyPrepares, ResumePrepares, GlobalPrepares, SnapshotParts, DataParts, DataNeeds                                                                                                                                                                                                                                                                                                    uint64
	ProofGaps, CheckpointInstalls, ReadyGenerations, QChanges, AuxiliaryFailures                                                                                                                                                                                                                                                                                                                    uint64
	WireFrames, WireBytes, QueueErrors, ClientFrames, ClientBytes, ClientErrors                                                                                                                                                                                                                                                                                                                     uint64
	ScanBegins, ScanResults, ScanDataHeld                                                                                                                                                                                                                                                                                                                                                           uint64
}
type coldTransferKey struct {
	To   int
	Type string
	G    coldGeneration
	Hash [32]byte
}
type coldTransfer struct {
	Cursor    int
	Completed time.Time
}

type coldReplica struct {
	migrated            bool
	bridgeProposed      bool
	bridgeBallot        uint64
	bridgeRoots         map[[32]byte]*bridgePending
	bridgeSince         time.Time
	manifestPending     map[[32]byte]*coldPendingManifest
	manifestGeneration  map[coldGeneration][32]byte
	payloadRetry        map[coldPayloadTarget]time.Time
	transfers           map[coldTransferKey]*coldTransfer
	bulkBodies          map[[32]byte]coldBundle
	bulkAccepted        map[coldGeneration][32]byte
	bulkApplied         map[[32]byte]bool
	dataRetry           map[[32]byte]time.Time
	r                   *Replica
	enabled             bool
	cutoff              state.Key
	plannedQ            uint64
	promised, installed coldGeneration
	ready               bool
	frozen              bool
	q                   uint64
	keys                map[state.Key]*coldKey
	done                map[defs.RequestID][32]byte
	inputs              map[[32]byte]request
	proofs              map[ColdVoteIdentity]uint64
	held                map[defs.RequestID]request
	reads               map[defs.RequestID]request
	readSince           map[defs.RequestID]time.Time
	prep                *coldPrepareState
	keyPrep             map[state.Key]*coldKeyPrepare
	snapshots           map[coldGeneration][]byte
	chunks              map[[32]byte]*coldParts
	bodies              map[[32]byte][]byte
	outgoing            map[int][][]byte
	outgoingBytes       map[int]int
	scan                *coldScanState
	scanRefs            map[defs.RequestID]request
	stats               coldStatistics
	lastRetry           time.Time
	checkpointHash      [32]byte
	replayQueue         []defs.RequestID
	replayHead          int
	replayPending       map[defs.RequestID]request
}

func newColdReplica(r *Replica, p config.DeploymentPlan) *coldReplica {
	return &coldReplica{bridgeRoots: map[[32]byte]*bridgePending{}, manifestPending: map[[32]byte]*coldPendingManifest{}, manifestGeneration: map[coldGeneration][32]byte{}, payloadRetry: map[coldPayloadTarget]time.Time{}, transfers: map[coldTransferKey]*coldTransfer{}, bulkBodies: map[[32]byte]coldBundle{}, bulkAccepted: map[coldGeneration][32]byte{}, bulkApplied: map[[32]byte]bool{}, dataRetry: map[[32]byte]time.Time{}, r: r, enabled: p.ColdEnabled, cutoff: state.Key(p.ColdCutoff), plannedQ: p.ColdQuorum,
		replayPending: map[defs.RequestID]request{}, keys: map[state.Key]*coldKey{}, done: map[defs.RequestID][32]byte{}, inputs: map[[32]byte]request{}, proofs: map[ColdVoteIdentity]uint64{}, held: map[defs.RequestID]request{}, reads: map[defs.RequestID]request{}, readSince: map[defs.RequestID]time.Time{}, keyPrep: map[state.Key]*coldKeyPrepare{}, snapshots: map[coldGeneration][]byte{}, chunks: map[[32]byte]*coldParts{}, bodies: map[[32]byte][]byte{}, outgoing: map[int][][]byte{}, outgoingBytes: map[int]int{}, scanRefs: map[defs.RequestID]request{}}
}
func (c *coldReplica) key(k state.Key) *coldKey {
	s := c.keys[k]
	if s == nil {
		s = &coldKey{Promise: coldRound{G: c.promised}, Fast: c.ready && !c.frozen, Ready: c.ready, Q: c.q, Accepted: map[uint64]coldRecord{}, Choices: map[uint64]coldRecord{}, Pending: map[defs.RequestID]request{}, Done: map[defs.RequestID][32]byte{}}
		c.keys[k] = s
	}
	return s
}
func (c *coldReplica) isCold(k state.Key) bool {
	return c.enabled && c.r.engine.opt.KeyPolicy.cold(k, c.cutoff)
}
func (c *coldReplica) requiredRoles() uint64 {
	if c.migrated || c.hasBridge() {
		return 0
	}
	if c.enabled && c.prep != nil && c.prep.Merged && c.prep.G.B == c.r.engine.current.Ballot {
		return c.prep.Q
	}
	if c.enabled && c.q != 0 && c.installed.B == c.r.engine.current.Ballot {
		return c.q
	}
	return 0
}
func (c *coldReplica) send(to int, m coldMessage) bool {
	if to < 0 || to >= c.r.N {
		return false
	}
	data, _ := json.Marshal(m)
	if to == int(c.r.ID) {
		c.receive(to, data, time.Now())
		return true
	}
	if len(c.outgoing[to]) == 0 && c.r.engine.emit(to, message{Kind: coldControl, Value: data}) {
		c.stats.WireFrames++
		c.stats.WireBytes += uint64(len(data) + 32)
		return true
	}
	if !c.r.engine.peerHealthy(to, time.Now()) {
		c.stats.QueueErrors++
		return false
	}
	queuedBytes := c.outgoingBytes[to]
	if queuedBytes+len(data) > 16<<20 {
		c.stats.QueueErrors++
		return false
	}
	c.outgoing[to] = append(c.outgoing[to], data)
	c.outgoingBytes[to] += len(data)
	c.stats.QueuedBytes += uint64(len(data))
	if c.stats.QueuedBytes > c.stats.MaxQueuedBytes {
		c.stats.MaxQueuedBytes = c.stats.QueuedBytes
	}
	if depth := uint64(len(c.outgoing[to])); depth > c.stats.MaxQueuedDepth {
		c.stats.MaxQueuedDepth = depth
	}
	return true
}
func (c *coldReplica) broadcast(m coldMessage) {
	for i := 0; i < c.r.N; i++ {
		c.send(i, m)
	}
}
func (c *coldReplica) sendParts(to int, typ string, g coldGeneration, hash [32]byte, data []byte, frozen bool, q uint64) {
	n := (len(data) + coldPartBytes - 1) / coldPartBytes
	if n == 0 {
		n = 1
	}
	paced := typ == "promise" || typ == "bulk_accept"
	var transfer *coldTransfer
	if paced {
		key := coldTransferKey{to, typ, g, hash}
		transfer = c.transfers[key]
		if transfer == nil {
			transfer = &coldTransfer{}
			c.transfers[key] = transfer
		}
		if transfer.Cursor == n {
			if time.Since(transfer.Completed) < 3*time.Second {
				return
			}
			transfer.Cursor = 0
			c.stats.TransferRetries++
		}
	}
	first := 0
	if transfer != nil {
		first = transfer.Cursor
	}
	for i := first; i < n; i++ {
		a, b := i*coldPartBytes, min((i+1)*coldPartBytes, len(data))
		if !c.send(to, coldMessage{Type: typ, G: g, Hash: hash, Data: data[a:b], Part: i, Parts: n, Frozen: frozen, Q: q}) {
			if transfer != nil {
				transfer.Cursor = i
				c.stats.TransferPauses++
			}
			return
		}
		if transfer != nil {
			transfer.Cursor = i + 1
		}
	}
	if transfer != nil {
		transfer.Completed = time.Now()
		c.stats.TransferCompleted++
	}
}

func addColdPart(p *coldParts, m coldMessage) ([]byte, bool) {
	if m.Parts < 1 || m.Part < 0 || m.Part >= m.Parts || len(m.Data) > coldPartBytes {
		return nil, false
	}
	if p.N == 0 {
		p.N = m.Parts
		p.Parts = map[int][]byte{}
	}
	if p.N != m.Parts {
		return nil, false
	}
	if _, ok := p.Parts[m.Part]; !ok {
		p.Parts[m.Part] = append([]byte(nil), m.Data...)
	}
	if len(p.Parts) != p.N {
		return nil, false
	}
	var all []byte
	for i := 0; i < p.N; i++ {
		all = append(all, p.Parts[i]...)
	}
	if m.Type != "data" && m.Hash != ([32]byte{}) && sha256.Sum256(all) != m.Hash {
		return nil, false
	}
	return all, true
}
func (c *coldReplica) printStats() {
	if !c.enabled {
		return
	}
	s, _ := json.Marshal(c.stats)
	c.r.Printf("COLD_STATS replica=%d generation=%v quorum=%x ready=%t frozen=%t keys=%d read_cache=%d counters=%s", c.r.ID, c.installed, c.q, c.ready, c.frozen, len(c.keys), len(c.reads), s)
}

// Both ordinary cold processing and post-Bridge body transport retain this FIFO.
func (c *coldReplica) flushOutgoing() {
	for to, q := range c.outgoing {
		budget := 64
		for len(q) > 0 && budget > 0 {
			data := q[0]
			if !c.r.engine.emit(to, message{Kind: coldControl, Value: data}) {
				c.stats.QueueErrors++
				break
			}
			c.stats.WireFrames++
			c.stats.WireBytes += uint64(len(data) + 32)
			c.outgoingBytes[to] -= len(data)
			c.stats.QueuedBytes -= uint64(len(data))
			q = q[1:]
			budget--
		}
		if len(q) == 0 {
			delete(c.outgoing, to)
			delete(c.outgoingBytes, to)
		} else {
			c.outgoing[to] = q
		}
	}
}
