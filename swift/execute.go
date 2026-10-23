package swift

import "github.com/hongzicong/ConsensusArena/replica/defs"

// Execution ordering, result deduplication, and protocol completion.
func (c *core) apply(desc *commandDesc, cmdID defs.RequestID) {
	// TODO: what if desc.propose is nil ?
	//       is that possible ?
	//
	//       Don't think so
	//       Now I do
	// TODO: How is that possible ?

	if desc.propose == nil || c.delivered.Has(cmdID.String()) || !c.executeEnabled() {
		return
	}

	if desc.phase != COMMIT && (!c.optExec || c.id != c.leader()) {
		return
	}

	for _, cmdIDPrime := range desc.dep {
		if !c.delivered.Has(cmdIDPrime.String()) {
			return
		}
	}

	c.delivered.Set(cmdID.String(), struct{}{})
	v := c.execute(desc.cmd)

	desc.successorsL.Lock()
	if desc.successors != nil {
		for _, sucCmdID := range desc.successors {
			c.scheduleDelivery(sucCmdID)
		}
	}
	desc.successorsL.Unlock()

	if !c.replyAfterExecution() {
		return
	}

	c.queueReply(desc, cmdID, v)
	if desc.seq {
		c.awaitReply(desc, cmdID)
	}
}

// completeReply runs on the existing reply worker, preserving send/bookkeeping order.
func (c *core) completeReply(args *replyArgs) {
	if args.propose.Proxy && !c.optExec {
		acc := &MAccept{
			Replica: c.id,
			Ballot:  c.ballot,
			CmdId:   args.cmdID,
			Rep:     args.val,
		}
		c.sendClient(args.propose.ClientId, c.codes.acceptRPC, acc)
	} else if c.optExec && c.id == c.leader() {
		reply := &MReply{
			Replica:  c.id,
			Ballot:   c.ballot,
			CmdId:    args.cmdID,
			Checksum: args.hs,
			Rep:      args.val,
		}
		c.sendClient(args.propose.ClientId, c.codes.replyRPC, reply)
	}
	// TODO: what if it is optimistically executed by the leader?
	c.historySize = (c.historySize % HISTORY_SIZE) + 1
	args.finish <- (c.historySize - 1)
}
