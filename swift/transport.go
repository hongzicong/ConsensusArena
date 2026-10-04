package swift

// Message sending, connection writers, and output queues.
import (
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

type replyArgs struct {
	dep     Dep
	hs      []SHash
	val     state.Value
	cmdId   defs.RequestID
	finish  chan interface{}
	propose *defs.GPropose
}

type replyChan struct {
	rep  *defs.ProposeReplyTS
	ok   chan struct{}
	exit chan struct{}
	args chan *replyArgs
}

func NewReplyChan(r *Replica) *replyChan {
	rc := &replyChan{
		rep: &defs.ProposeReplyTS{
			OK: defs.TRUE,
		},
		ok:   make(chan struct{}, 1),
		exit: make(chan struct{}, 2),
		args: make(chan *replyArgs, defs.CHAN_BUFFER_SIZE),
	}

	go func() {
		for !r.Shutdown {
			select {
			case <-rc.exit:
				rc.ok <- struct{}{}
				return
			case args := <-rc.args:
				r.completeReply(args)
			}
		}
	}()

	return rc
}

func (r *replyChan) stop() {
	r.exit <- struct{}{}
	<-r.ok
}

func (r *replyChan) reply(desc *commandDesc, cmdId defs.RequestID, val state.Value) {
	dep := make([]defs.RequestID, len(desc.dep))
	copy(dep, desc.dep)

	hs := make([]SHash, len(desc.hs))
	copy(hs, desc.hs)

	r.args <- &replyArgs{
		dep:     dep,
		hs:      hs,
		val:     val,
		cmdId:   cmdId,
		finish:  desc.msgs,
		propose: desc.propose,
	}
}
