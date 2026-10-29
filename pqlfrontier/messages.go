package pqlfrontier

import "github.com/hongzicong/ConsensusArena/pql"

// This variant changes only local read eligibility. Reuse the unchanged PQL
// wire types, framing and validation; all protocol state is owned by this package.
// Aliases also avoid registering a second copy of the same protobuf schema.
type Packet = pql.Packet
type Record = pql.Record
type Request = pql.Request
type HolderUpdate = pql.HolderUpdate
type LeaseChange = pql.LeaseChange
