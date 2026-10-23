package curp

import fastrpc "github.com/hongzicong/ConsensusArena/rpc"

func (m *MReply) New() fastrpc.Serializable {
	return new(MReply)
}

func (m *MAccept) New() fastrpc.Serializable {
	return new(MAccept)
}

func (m *MAcceptAck) New() fastrpc.Serializable {
	return new(MAcceptAck)
}

func (m *MAAcks) New() fastrpc.Serializable {
	return new(MAAcks)
}

func (m *MRecordAck) New() fastrpc.Serializable {
	return new(MRecordAck)
}

func (m *MCommit) New() fastrpc.Serializable {
	return new(MCommit)
}

func (m *MSync) New() fastrpc.Serializable {
	return new(MSync)
}

func (m *MSyncReply) New() fastrpc.Serializable {
	return new(MSyncReply)
}

func (*Packet) New() fastrpc.Serializable { return &Packet{} }
