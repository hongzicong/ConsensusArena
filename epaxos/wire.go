package epaxos

import fastrpc "github.com/hongzicong/ConsensusArena/rpc"

func (*repairRequest) New() fastrpc.Serializable { return &repairRequest{} }

func (m *PrepareReply) New() fastrpc.Serializable {
	return new(PrepareReply)
}

func (m *PreAccept) New() fastrpc.Serializable {
	return new(PreAccept)
}

func (m *Accept) New() fastrpc.Serializable {
	return new(Accept)
}

func (m *AcceptReply) New() fastrpc.Serializable {
	return new(AcceptReply)
}

func (m *TryPreAccept) New() fastrpc.Serializable {
	return new(TryPreAccept)
}

func (m *Prepare) New() fastrpc.Serializable {
	return new(Prepare)
}

func (m *PreAcceptReply) New() fastrpc.Serializable {
	return new(PreAcceptReply)
}

func (m *PreAcceptOK) New() fastrpc.Serializable {
	return new(PreAcceptOK)
}

func (m *Commit) New() fastrpc.Serializable {
	return new(Commit)
}

func (m *TryPreAcceptReply) New() fastrpc.Serializable {
	return new(TryPreAcceptReply)
}
