package swift

// Protocol-specific message or proposal batching.
import (
	"github.com/hongzicong/ConsensusArena/replica/defs"
	fastrpc "github.com/hongzicong/ConsensusArena/rpc"
)

type messageBatcher struct {
	fastAcks      chan batchOperation
	lightSlowAcks chan batchOperation
}

type batchOperation struct {
	cid          int32
	msg          fastrpc.Serializable
	sendToClient bool
}

func newMessageBatcher(r *Replica, size int) *messageBatcher {
	b := &messageBatcher{
		fastAcks:      make(chan batchOperation, size),
		lightSlowAcks: make(chan batchOperation, size),
	}

	go func() {
		for !r.Shutdown {
			select {
			case op := <-b.fastAcks:
				fastAck := op.msg.(*MFastAck)

				fLen := len(b.fastAcks) + 1
				sLen := len(b.lightSlowAcks)

				acks := &MAcks{
					FastAcks:      make([]MFastAck, fLen),
					LightSlowAcks: make([]MLightSlowAck, sLen),
				}

				ballot := fastAck.Ballot
				optAcks := &MOptAcks{
					Replica: r.ID,
					Ballot:  fastAck.Ballot,
					Acks: []Ack{{
						CmdId:    fastAck.CmdId,
						Dep:      fastAck.Dep,
						Checksum: fastAck.Checksum,
						Seqnum:   fastAck.Seqnum,
					}},
				}
				is := map[defs.RequestID]int{fastAck.CmdId: 0}
				fastAckClientMsgs := make(map[defs.RequestID]MFastAckClient)
				slowAckClientMsgs := make(map[defs.RequestID]MLightSlowAck)

				if op.sendToClient {
					fastAckClientMsgs[fastAck.CmdId] = MFastAckClient{
						Replica:  fastAck.Replica,
						Ballot:   fastAck.Ballot,
						CmdId:    fastAck.CmdId,
						Checksum: fastAck.Checksum,
					}
				}

				acks.FastAcks[0] = *fastAck
				for i := 1; i < fLen; i++ {
					opP := <-b.fastAcks
					f := opP.msg.(*MFastAck)
					acks.FastAcks[i] = *f

					if ballot == f.Ballot {
						is[f.CmdId] = len(optAcks.Acks)
						optAcks.Acks = append(optAcks.Acks, Ack{
							CmdId:    f.CmdId,
							Dep:      f.Dep,
							Checksum: f.Checksum,
							Seqnum:   f.Seqnum,
						})
					} else {
						ballot = -1
					}

					if opP.sendToClient {
						fastAckClientMsgs[f.CmdId] = MFastAckClient{
							Replica:  f.Replica,
							Ballot:   f.Ballot,
							CmdId:    f.CmdId,
							Checksum: f.Checksum,
						}
					}
				}
				for i := 0; i < sLen; i++ {
					opP := <-b.lightSlowAcks
					s := opP.msg.(*MLightSlowAck)
					acks.LightSlowAcks[i] = *s

					if ballot == s.Ballot {
						iCmdID, exists := is[s.CmdId]
						if exists {
							optAcks.Acks[iCmdID].Dep = NilDepOfCmdID(s.CmdId)
							optAcks.Acks[iCmdID].Checksum = []SHash{}
						} else {
							is[s.CmdId] = len(optAcks.Acks)
							optAcks.Acks = append(optAcks.Acks, Ack{
								CmdId:    s.CmdId,
								Dep:      NilDepOfCmdID(s.CmdId),
								Checksum: []SHash{},
							})
						}
					} else {
						ballot = -1
					}

					if opP.sendToClient {
						_, exists := fastAckClientMsgs[s.CmdId]
						if exists {
							delete(fastAckClientMsgs, s.CmdId)
						}
						slowAckClientMsgs[s.CmdId] = *s
					}
				}

				for _, f := range fastAckClientMsgs {
					cf := f
					r.SendClientMsg(cf.CmdId.Client, r.channels.fastAckClientRPC, &cf)
				}
				for _, s := range slowAckClientMsgs {
					channels := s
					r.SendClientMsg(channels.CmdId.Client, r.channels.lightSlowAckRPC, &channels)
				}

				var (
					m   fastrpc.Serializable
					rpc uint8
				)
				if ballot != -1 {
					m = optAcks
					rpc = r.channels.optAcksRPC
				} else {
					m = acks
					rpc = r.channels.acksRPC
				}
				r.SendToAll(m, rpc)

			case op := <-b.lightSlowAcks:
				slowAck := op.msg.(*MLightSlowAck)

				fLen := len(b.fastAcks)
				sLen := len(b.lightSlowAcks) + 1

				acks := &MAcks{
					FastAcks:      make([]MFastAck, fLen),
					LightSlowAcks: make([]MLightSlowAck, sLen),
				}

				ballot := slowAck.Ballot
				optAcks := &MOptAcks{
					Replica: r.ID,
					Ballot:  slowAck.Ballot,
					Acks: []Ack{{
						CmdId:    slowAck.CmdId,
						Dep:      NilDepOfCmdID(slowAck.CmdId),
						Checksum: []SHash{},
					}},
				}
				is := map[defs.RequestID]int{slowAck.CmdId: 0}
				fastAckClientMsgs := make(map[defs.RequestID]MFastAckClient)
				slowAckClientMsgs := make(map[defs.RequestID]MLightSlowAck)

				if op.sendToClient {
					slowAckClientMsgs[slowAck.CmdId] = *slowAck
				}

				acks.LightSlowAcks[0] = *slowAck
				for i := 1; i < sLen; i++ {
					opP := <-b.lightSlowAcks
					s := opP.msg.(*MLightSlowAck)
					acks.LightSlowAcks[i] = *s

					if ballot == s.Ballot {
						is[s.CmdId] = len(optAcks.Acks)
						optAcks.Acks = append(optAcks.Acks, Ack{
							CmdId:    s.CmdId,
							Dep:      NilDepOfCmdID(s.CmdId),
							Checksum: []SHash{},
						})
					} else {
						ballot = -1
					}

					if opP.sendToClient {
						slowAckClientMsgs[s.CmdId] = *s
					}
				}
				for i := 0; i < fLen; i++ {
					opP := <-b.fastAcks
					f := opP.msg.(*MFastAck)
					acks.FastAcks[i] = *f

					if ballot == f.Ballot {
						_, exists := is[f.CmdId]
						if !exists {
							is[f.CmdId] = len(optAcks.Acks)
							optAcks.Acks = append(optAcks.Acks, Ack{
								CmdId:    f.CmdId,
								Dep:      f.Dep,
								Checksum: f.Checksum,
								Seqnum:   f.Seqnum,
							})
						}
					} else {
						ballot = -1
					}

					if opP.sendToClient {
						_, exists := slowAckClientMsgs[f.CmdId]
						if !exists {
							fastAckClientMsgs[f.CmdId] = MFastAckClient{
								Replica:  f.Replica,
								Ballot:   f.Ballot,
								CmdId:    f.CmdId,
								Checksum: f.Checksum,
							}
						}
					}
				}

				for _, f := range fastAckClientMsgs {
					cf := f
					r.SendClientMsg(cf.CmdId.Client, r.channels.fastAckClientRPC, &cf)
				}
				for _, s := range slowAckClientMsgs {
					channels := s
					r.SendClientMsg(channels.CmdId.Client, r.channels.lightSlowAckRPC, &channels)
				}

				var (
					m   fastrpc.Serializable
					rpc uint8
				)
				if ballot != -1 {
					m = optAcks
					rpc = r.channels.optAcksRPC
				} else {
					m = acks
					rpc = r.channels.acksRPC
				}
				r.SendToAll(m, rpc)
			}
		}
	}()

	return b
}

func (b *messageBatcher) SendFastAck(f *MFastAck) {
	b.fastAcks <- batchOperation{
		msg:          f,
		sendToClient: false,
	}
}

func (b *messageBatcher) SendLightSlowAck(s *MLightSlowAck) {
	b.lightSlowAcks <- batchOperation{
		msg:          s,
		sendToClient: false,
	}
}

func (b *messageBatcher) SendFastAckClient(f *MFastAck, cid int32) {
	b.fastAcks <- batchOperation{
		msg:          f,
		cid:          cid,
		sendToClient: true,
	}
}

func (b *messageBatcher) SendLightSlowAckClient(s *MLightSlowAck, cid int32) {
	b.lightSlowAcks <- batchOperation{
		msg:          s,
		cid:          cid,
		sendToClient: true,
	}
}
