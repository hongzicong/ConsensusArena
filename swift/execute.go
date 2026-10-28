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

	if desc.propose == nil || c.hasDelivered(cmdID) || !c.executeEnabled() {
		return
	}

	if desc.phase != COMMIT && (!c.optExec || c.id != c.leader()) {
		return
	}

	for _, cmdIDPrime := range desc.dep {
		if !c.hasDelivered(cmdIDPrime) {
			return
		}
	}

	c.delivered[cmdID] = struct{}{}
	v := c.execute(desc.cmd)

	if desc.successors != nil {
		for _, sucCmdID := range desc.successors {
			c.scheduleDelivery(sucCmdID)
		}
	}

	if !c.replyAfterExecution() {
		return
	}

	c.queueReply(desc, cmdID, v)
}

// Reply sending and history handoff run as a later event-loop task, after the
// descriptor transition that authorized them has returned.
func (c *core) completeReply(desc *commandDesc, args *replyArgs) {
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
	slot := c.historySize - 1
	c.history[slot] = commandStaticDesc{
		cmdID: args.cmdID, phase: desc.phase, cmd: desc.cmd, dep: desc.dep,
		slowPath: desc.slowPath, defered: desc.defered,
	}
	desc.active = false
	desc.slowPathH.Free()
	desc.fastPathH.Free()
	delete(c.descriptors, args.cmdID)
	c.freeDesc(desc)
}
