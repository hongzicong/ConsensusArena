package bodega

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hongzicong/ConsensusArena/client"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

type Client struct {
	client.StandardClient
	Unhold time.Duration
	bodega *bodegaRouting
}

func NewClient(b *client.BufferClient, _ int) *Client {
	c := &Client{StandardClient: client.StandardClient{BufferClient: b}}
	b.SkipPing = true
	b.SetProtocol(c)
	return c
}

var _ client.Adapter = (*Client)(nil)

func (c *Client) RetryPolicy() string {
	delay := c.Unhold
	if delay == 0 {
		delay = 250 * time.Millisecond
	}
	return fmt.Sprintf("GET: original-ID hedge to installed leader after %s; writes: no client retransmission", delay)
}

type bodegaRouting struct {
	roster                                 atomic.Pointer[defs.BodegaRosterReply]
	dead                                   []atomic.Bool
	rtt                                    []atomic.Int64 // control RPC round-trip through DialAddress; zero means unknown
	readDestinations                       []atomic.Uint64
	stop, wake                             chan struct{}
	closeOnce                              sync.Once
	mu                                     sync.Mutex
	pending                                map[int32]*bodegaPending
	hedges, hedgeWins, duplicates, cancels atomic.Uint64
	reads, writes, forwarded               atomic.Uint64
}

func newBodegaRouting(n int) *bodegaRouting {
	return &bodegaRouting{pending: make(map[int32]*bodegaPending), dead: make([]atomic.Bool, n), rtt: make([]atomic.Int64, n), readDestinations: make([]atomic.Uint64, n), stop: make(chan struct{}), wake: make(chan struct{}, 1)}
}

func (c *Client) bodegaDistance(id int) int64 {
	if id >= 0 && id < len(c.bodega.rtt) {
		if ns := c.bodega.rtt[id].Load(); ns > 0 {
			return ns
		}
	}
	return 1<<63 - 1 // unknown distances sort last, then by replica ID
}

func (c *Client) bodegaPeerAlive(id int) bool {
	return id >= 0 && id < c.PeerCount() && c.Writer(id) != nil && !c.bodega.dead[id].Load()
}

func (c *Client) nearestBodegaPeer() int {
	if c.bodegaPeerAlive(c.ClosestId) {
		return c.ClosestId
	}
	best := -1
	for id := 0; id < c.PeerCount(); id++ {
		if c.bodegaPeerAlive(id) && (best < 0 || c.bodegaDistance(id) < c.bodegaDistance(best)) {
			best = id
		}
	}
	return best
}

// Hints are monotonic and never authorize execution. The destination replica
// still checks its installed ballot, preparation and responder-covered quorum.
func (c *Client) learnBodegaRoster(hint defs.BodegaRosterReply) bool {
	if !hint.Ready || hint.Ballot == 0 || !c.bodegaPeerAlive(hint.Leader) || hint.Responders>>uint(c.PeerCount()) != 0 || !defs.ValidBodegaRanges(hint.Ranges, c.PeerCount()) {
		return false
	}
	hint.Ranges = slices.Clone(hint.Ranges)
	for {
		old := c.bodega.roster.Load()
		if old != nil && hint.Ballot <= old.Ballot {
			return hint.Ballot == old.Ballot && hint.Leader == old.Leader && hint.Responders == old.Responders && slices.Equal(hint.Ranges, old.Ranges)
		}
		if c.bodega.roster.CompareAndSwap(old, &hint) {
			c.Printf("BODEGA_CLIENT_ROSTER ballot=%d leader=%d\n", hint.Ballot, hint.Leader)
			return true
		}
	}
}

func (c *Client) queryBodegaRoster(id int) (defs.BodegaRosterReply, error) {
	var reply defs.BodegaRosterReply
	host, portText, err := net.SplitHostPort(c.ReplicaAddress(id))
	if err != nil {
		return reply, err
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return reply, err
	}
	endpoint := defs.DialAddress(net.JoinHostPort(host, strconv.Itoa(port+1000)))
	conn, err := net.DialTimeout("tcp", endpoint, time.Second)
	if err != nil {
		return reply, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	if _, err = io.WriteString(conn, "CONNECT "+rpc.DefaultRPCPath+" HTTP/1.0\n\n"); err != nil {
		return reply, err
	}
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err != nil {
		return reply, err
	}
	if response.StatusCode != http.StatusOK {
		return reply, fmt.Errorf("Bodega control HTTP status %s", response.Status)
	}
	control := rpc.NewClient(conn)
	defer control.Close()
	started := time.Now()
	err = control.Call("Replica.GetBodegaRoster", &defs.GetLeaderArgs{}, &reply)
	if err == nil && c.bodega != nil && id < len(c.bodega.rtt) {
		ns := time.Since(started).Nanoseconds()
		if ns < 1 {
			ns = 1
		}
		c.bodega.rtt[id].Store(ns)
	}
	return reply, err
}

// Sampling is outside the request path and uses the same modeled delays as
// data RPCs. Per-peer probes are parallel and bounded by the query deadline.
func (c *Client) probeBodegaPeers(samples int) {
	var wg sync.WaitGroup
	for id := 0; id < c.PeerCount(); id++ {
		if !c.bodegaPeerAlive(id) {
			continue
		}
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			best := int64(1<<63 - 1)
			for i := 0; i < samples; i++ {
				select {
				case <-c.bodega.stop:
					return
				default:
				}
				if hint, err := c.queryBodegaRoster(id); err == nil {
					if ns := c.bodegaDistance(id); ns < best {
						best = ns
					}
					c.learnBodegaRoster(hint)
				}
			}
			if best != 1<<63-1 {
				c.bodega.rtt[id].Store(best)
			}
		}(id)
	}
	wg.Wait()
	rtts := make([]int64, len(c.bodega.rtt))
	for i := range rtts {
		rtts[i] = c.bodega.rtt[i].Load()
	}
	c.Printf("BODEGA_CLIENT_RTT source=proxied_control_rpc rtt_ns=%v\n", rtts)
}

func (c *Client) refreshBodegaRoster() bool {
	// A successful local query costs one control RPC per second, outside the
	// request path. Try other connected replicas when that hint is unavailable.
	near := c.nearestBodegaPeer()
	for attempt := 0; attempt <= c.PeerCount(); attempt++ {
		select {
		case <-c.bodega.stop:
			return false
		default:
		}
		id := near
		if attempt > 0 {
			id = attempt - 1
			if id == near {
				continue
			}
		}
		if !c.bodegaPeerAlive(id) {
			continue
		}
		if hint, err := c.queryBodegaRoster(id); err == nil && c.learnBodegaRoster(hint) {
			return true
		}
	}
	return false
}

func (c *Client) Start() error {
	if c.Unhold < 0 {
		return fmt.Errorf("Bodega unhold must be positive")
	}
	c.bodega = newBodegaRouting(c.PeerCount())
	deadline := time.Now().Add(30 * time.Second)
	for !c.refreshBodegaRoster() {
		if time.Now().After(deadline) {
			return fmt.Errorf("no installed Bodega roster available")
		}
		time.Sleep(100 * time.Millisecond)
	}
	c.probeBodegaPeers(3)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		lastProbe := time.Now()
		for {
			select {
			case <-c.bodega.stop:
				return
			case <-ticker.C:
			case <-c.bodega.wake:
			}
			c.refreshBodegaRoster()
			if time.Since(lastProbe) >= 10*time.Second {
				c.probeBodegaPeers(1)
				lastProbe = time.Now()
			}
		}
	}()
	return nil
}

func (c *Client) markBodegaPeer(id int) {
	if !c.bodega.dead[id].Swap(true) {
		c.MarkFaultPeer(id)
		select {
		case c.bodega.wake <- struct{}{}:
		default:
		}
	}
}

func (c *Client) SendProposal(cmd defs.Propose) {
	id := c.nearestBodegaPeer()
	if cmd.Command.Op == state.GET {
		if hint := c.bodega.roster.Load(); hint != nil {
			mask := defs.BodegaRespondersFor(hint.Responders, hint.Ranges, hint.Leader, int64(cmd.Command.K))
			id = c.nearestBodegaResponder(mask)
		}
		c.bodega.reads.Add(1)
	} else if roster := c.bodega.roster.Load(); roster != nil && c.bodegaPeerAlive(roster.Leader) {
		id = roster.Leader
		c.bodega.writes.Add(1)
	} else {
		// During discovery after a disconnect, the survivor preserves the
		// request identity and uses the existing engine's forwarding queue.
		c.bodega.forwarded.Add(1)
	}
	if id < 0 {
		return
	}
	if cmd.Command.Op == state.GET && id < len(c.bodega.readDestinations) {
		c.bodega.readDestinations[id].Add(1)
	}
	c.trackBodegaRequest(cmd, id)
	c.writeBodegaProposal(cmd, id, false)
}

// The write lock orders initial/hedged sends before a cancellation on that stream.
func (c *Client) writeBodegaProposal(cmd defs.Propose, id int, hedge bool) {
	c.WriteMutex(id).Lock()
	defer c.WriteMutex(id).Unlock()
	if !c.bodegaPeerAlive(id) {
		return
	}
	if cmd.Command.Op != defs.BodegaCancelRead {
		c.bodega.mu.Lock()
		_, pending := c.bodega.pending[cmd.CommandId]
		c.bodega.mu.Unlock()
		if !pending {
			return
		}
		if hedge {
			c.bodega.hedges.Add(1)
		}
	}
	_ = c.Connection(id).SetWriteDeadline(time.Now().Add(2 * time.Second))
	_ = c.Writer(id).WriteByte(defs.PROPOSE)
	cmd.Marshal(c.Writer(id))
	if err := c.Writer(id).Flush(); err != nil {
		c.ObserveWriteError()
		c.markBodegaPeer(id)
		_ = c.Connection(id).Close()
	}
}

func (c *Client) nearestBodegaResponder(mask uint64) int {
	if c.bodegaPeerAlive(c.ClosestId) && mask&(uint64(1)<<uint(c.ClosestId)) != 0 {
		return c.ClosestId
	}
	best := -1
	for id := 0; id < c.PeerCount(); id++ {
		if mask&(uint64(1)<<uint(id)) != 0 && c.bodegaPeerAlive(id) && (best < 0 || c.bodegaDistance(id) < c.bodegaDistance(best)) {
			best = id
		}
	}
	if best < 0 {
		return c.nearestBodegaPeer()
	} // stale roster: server forwards safely
	return best
}

func (c *Client) WaitReplies(_ int) {
	for id := 0; id < c.PeerCount(); id++ {
		reader := c.Reader(id)
		if reader == nil {
			continue
		}
		go func(id int) {
			for {
				r, err := c.GetReplyFrom(id)
				if err != nil {
					c.markBodegaPeer(id)
					return
				}
				if r.OK != defs.TRUE || !c.completeBodegaRequest(r.CommandId, id) {
					continue
				}
				select {
				case c.Reply <- &client.ReqReply{Val: r.Value, Seqnum: int(r.CommandId), Time: time.Now()}:
				case <-c.bodega.stop:
					return
				}
			}
		}(id)
	}
}

func (c *Client) Close() {
	if c.bodega != nil {
		c.bodega.closeOnce.Do(func() {
			close(c.bodega.stop)
			c.stopBodegaReads()
			c.Printf("BODEGA_CLIENT_ROUTES local_reads=%d leader_commands=%d discovery_forwards=%d\n", c.bodega.reads.Load(), c.bodega.writes.Load(), c.bodega.forwarded.Load())
			counts := make([]uint64, len(c.bodega.readDestinations))
			for i := range counts {
				counts[i] = c.bodega.readDestinations[i].Load()
			}
			c.Printf("BODEGA_CLIENT_DESTINATIONS read_attempts=%v\n", counts)
			c.Printf("BODEGA_CLIENT_HEDGES sent=%d wins=%d duplicate_replies=%d cancels=%d\n", c.bodega.hedges.Load(), c.bodega.hedgeWins.Load(), c.bodega.duplicates.Load(), c.bodega.cancels.Load())
		})
	}
}

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
		delay := c.Unhold
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
