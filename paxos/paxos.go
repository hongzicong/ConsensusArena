package paxos

import (
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/recoverylog"
	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"log"
)

// Replica runs classic Multi-Paxos: acceptors reply to the leader, which learns
// a majority and disseminates Commit. All transitions belong to one event loop.
type Replica struct {
	*replica.Replica
	recovery *recoverylog.Runtime
}

func New(alias string, id int, addrs []string, isLeader bool, f int, conf *config.Config, logger *dlog.Logger) *Replica {
	r := &Replica{Replica: replica.New(alias, id, f, addrs, false, true, false, conf, logger)}
	leader := int32(0)
	qs, leaders, err := replica.NewQuorumsFromFile(conf.Quorum, r.Replica)
	if err == nil && len(qs) > 0 {
		leader = leaders[0]
	} else if err != replica.NO_QUORUM_FILE {
		log.Fatal(err)
	}
	r.recovery = recoverylog.NewRuntime(r.Replica, leader, false)
	r.recovery.Core.Classic = true
	go r.recovery.Run()
	return r
}

// The master discovers the ballot owner; changing a flag cannot install a leader.
func (r *Replica) BeTheLeader(args *defs.BeTheLeaderArgs, reply *defs.BeTheLeaderReply) error {
	return r.recovery.LeaderHint(reply)
}
