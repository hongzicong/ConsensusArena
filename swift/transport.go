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
	cmdID   defs.RequestID
	finish  chan interface{}
	propose *proposal
}

type replyWorker struct {
	rep  *defs.ProposeReplyTS
	ok   chan struct{}
	exit chan struct{}
	args chan *replyArgs
}

func newReplyWorker(r *Replica) *replyWorker {
	rc := &replyWorker{
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
				r.core.completeReply(args)
			}
		}
	}()

	return rc
}

func (r *replyWorker) stop() {
	r.exit <- struct{}{}
	<-r.ok
}

func (r *replyWorker) reply(desc *commandDesc, cmdID defs.RequestID, val state.Value) {
	dep := make([]defs.RequestID, len(desc.dep))
	copy(dep, desc.dep)

	hs := make([]SHash, len(desc.hs))
	copy(hs, desc.hs)

	r.args <- &replyArgs{
		dep:     dep,
		hs:      hs,
		val:     val,
		cmdID:   cmdID,
		finish:  desc.msgs,
		propose: desc.propose,
	}
}
