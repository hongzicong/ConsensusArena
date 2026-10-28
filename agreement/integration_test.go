package agreement

import (
	"bytes"
	"testing"

	"github.com/hongzicong/ConsensusArena/replica"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

func TestProtocolFramesPreserveImportedFields(t *testing.T) {
	r := request{Proposal: defs.Propose{ClientId: -123, CommandId: 19, Timestamp: 42,
		Command: state.Command{Op: state.SCAN, K: 7, V: state.Value{1, 2, 3}}}, Origin: 2, Stage: 2,
		Scan: coldScanRef{Begin: 4, Length: 17, Voters: 0b111}}
	r.Scan.Hash[0], r.Scan.Hash[31] = 5, 7
	v := entry{Slot: 6, Ballot: 8, Request: r, ReadFresh: true}
	for k := kind(0); k < kindCount; k++ {
		t.Run(string(rune('A'+k)), func(t *testing.T) {
			m := message{Kind: k, From: 2, Roster: roster{Ballot: 8, Leader: 1, Responders: 7,
				Ranges: []ResponderRange{{Start: 1, End: 9, Responders: 3}}},
				Sequence: 13, Threshold: 14, Prefix: 15, ReadPrefix: 16, CommitPrefix: 17,
				CommitSlot: 18, RepairStart: 19, PrepareStart: 20, Part: 0, Parts: 1,
				Entry: v, Entries: []entry{v}, Request: r, Value: state.Value("cold body"), DeliveryAttempt: 21}
			// Use the real shared sender boundary, which rejects legacy codecs.
			frame := replica.Encode(9, &m, true)
			if frame.Data[0] != 9 || frame.Tag != uint8(k) || string(frame.Data[1:4]) != "CAP" {
				t.Fatal("frame did not use the shared protobuf transport")
			}
			reader := bytes.NewReader(append(frame.Data[1:], frame.Data[1:]...))
			want := m.wireFields()
			for i := 0; i < 2; i++ {
				var got message
				if err := got.Unmarshal(reader); err != nil {
					t.Fatal(err)
				}
				// Compare canonical payloads so nil and empty byte slices have
				// protobuf's common representation without changing command data.
				wantData, _ := want.MarshalProto()
				gotData, _ := got.MarshalProto()
				if !bytes.Equal(wantData, gotData) {
					t.Fatalf("wire changed per-kind fields: got=%+v want=%+v", got, want)
				}
			}
			if reader.Len() != 0 {
				t.Fatal("decoder consumed an incorrect frame boundary")
			}
		})
	}
}

func TestOnlyHeartbeatCarriesCompleteRoster(t *testing.T) {
	for k := kind(0); k < kindCount; k++ {
		m := message{Kind: k, From: 1, Roster: roster{Ballot: 4, Leader: 2, Responders: 7}, Parts: 1,
			Entry: entry{Slot: 1, Request: request{Origin: 1}}}
		data, err := m.MarshalProtoFrame(0, false)
		if err != nil {
			t.Fatal(err)
		}
		var decoded message
		if err := decoded.Unmarshal(bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		if (decoded.Roster.Responders != 0) != (k == heartbeat) {
			t.Fatalf("kind %d changed imported roster-discovery semantics", k)
		}
	}
}

func TestProtocolFrameRejectsInvalidSender(t *testing.T) {
	m := message{Kind: heartbeat, From: 63}
	data, err := m.MarshalProtoFrame(0, false)
	if err != nil {
		t.Fatal(err)
	}
	var got message
	if got.Unmarshal(bytes.NewReader(data)) == nil {
		t.Fatal("invalid sender accepted")
	}
}
