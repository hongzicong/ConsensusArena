package paxos

import fastrpc "github.com/hongzicong/ConsensusArena/rpc"

func (*Packet) New() fastrpc.Serializable { return &Packet{} }
