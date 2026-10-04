package swift

import "github.com/hongzicong/ConsensusArena/replica/defs"

// Execution ordering, result deduplication, and protocol completion.
func (r *Replica) deliver(desc *commandDesc, cmdId defs.RequestID) {
	// TODO: what if desc.propose is nil ?
	//       is that possible ?
	//
	//       Don't think so
	//       Now I do
	// TODO: How is that possible ?

	if desc.propose == nil || r.delivered.Has(cmdId.String()) || !r.Exec {
		return
	}

	if desc.phase != COMMIT && (!r.optExec || r.Id != r.leader()) {
		return
	}

	for _, cmdIdPrime := range desc.dep {
		if !r.delivered.Has(cmdIdPrime.String()) {
			return
		}
	}

	r.delivered.Set(cmdId.String(), struct{}{})
	v := desc.cmd.Execute(r.State)

	desc.successorsL.Lock()
	if desc.successors != nil {
		for _, sucCmdId := range desc.successors {
			go func(sucCmdId defs.RequestID) {
				r.deliverChan <- sucCmdId
			}(sucCmdId)
		}
	}
	desc.successorsL.Unlock()

	if !r.Dreply {
		return
	}

	r.repchan.reply(desc, cmdId, v)
	if desc.seq {
		// wait for the slot number and ignore any other message
		for {
			switch slot := (<-desc.msgs).(type) {
			case int:
				r.handleMsg(slot, desc, cmdId)
				return
			}
		}
	}
}

// completeReply runs on the existing reply worker, preserving send/bookkeeping order.
func (r *Replica) completeReply(args *replyArgs) {
	if args.propose.Proxy && !r.optExec {
		acc := &MAccept{
			Replica: r.Id,
			Ballot:  r.ballot,
			CmdId:   args.cmdId,
			Rep:     args.val,
		}
		r.SendClientMsg(args.propose.ClientId, r.cs.acceptRPC, acc)
	} else if r.optExec && r.Id == r.leader() {
		reply := &MReply{
			Replica:  r.Id,
			Ballot:   r.ballot,
			CmdId:    args.cmdId,
			Checksum: args.hs,
			Rep:      args.val,
		}
		r.SendClientMsg(args.propose.ClientId, r.cs.replyRPC, reply)
	}
	// TODO: what if it is optimistically executed by the leader?
	r.historySize = (r.historySize % HISTORY_SIZE) + 1
	args.finish <- (r.historySize - 1)
}
