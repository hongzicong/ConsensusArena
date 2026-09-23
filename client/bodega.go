package client

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

	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

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
	return id >= 0 && id < len(c.servers) && c.writers[id] != nil && !c.bodega.dead[id].Load()
}

func (c *Client) nearestBodegaPeer() int {
	if c.bodegaPeerAlive(c.ClosestId) {
		return c.ClosestId
	}
	best := -1
	for id := range c.servers {
		if c.bodegaPeerAlive(id) && (best < 0 || c.bodegaDistance(id) < c.bodegaDistance(best)) {
			best = id
		}
	}
	return best
}

// Hints are monotonic and never authorize execution. The destination replica
// still checks its installed ballot, preparation and responder-covered quorum.
func (c *Client) learnBodegaRoster(hint defs.BodegaRosterReply) bool {
	if !hint.Ready || hint.Ballot == 0 || !c.bodegaPeerAlive(hint.Leader) || hint.Responders>>uint(len(c.servers)) != 0 || !defs.ValidBodegaRanges(hint.Ranges, len(c.servers)) {
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
	host, portText, err := net.SplitHostPort(c.replicas[id])
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
	for id := range c.servers {
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
	for attempt := 0; attempt <= len(c.servers); attempt++ {
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

func (c *Client) startBodegaRouting() error {
	if c.BodegaUnhold < 0 {
		return fmt.Errorf("Bodega unhold must be positive")
	}
	c.bodega = newBodegaRouting(len(c.servers))
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
		c.markFaultPeer(id)
		select {
		case c.bodega.wake <- struct{}{}:
		default:
		}
	}
}

func (c *Client) sendBodegaProposal(cmd defs.Propose) {
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
	c.writeMu[id].Lock()
	defer c.writeMu[id].Unlock()
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
	_ = c.servers[id].SetWriteDeadline(time.Now().Add(2 * time.Second))
	_ = c.writers[id].WriteByte(defs.PROPOSE)
	cmd.Marshal(c.writers[id])
	if err := c.writers[id].Flush(); err != nil {
		if c.fault != nil {
			c.fault.errors.Add(1)
		}
		c.markBodegaPeer(id)
		_ = c.servers[id].Close()
	}
}

func (c *Client) nearestBodegaResponder(mask uint64) int {
	if c.bodegaPeerAlive(c.ClosestId) && mask&(uint64(1)<<uint(c.ClosestId)) != 0 {
		return c.ClosestId
	}
	best := -1
	for id := range c.servers {
		if mask&(uint64(1)<<uint(id)) != 0 && c.bodegaPeerAlive(id) && (best < 0 || c.bodegaDistance(id) < c.bodegaDistance(best)) {
			best = id
		}
	}
	if best < 0 {
		return c.nearestBodegaPeer()
	} // stale roster: server forwards safely
	return best
}

func (c *BufferClient) waitBodegaReplies() {
	for id, reader := range c.readers {
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
				case c.Reply <- &ReqReply{Val: r.Value, Seqnum: int(r.CommandId), Time: time.Now()}:
				case <-c.bodega.stop:
					return
				}
			}
		}(id)
	}
}
