# Native Protobuf protocol messages

All seven protocols use generated Protobuf structs directly in their state
machines. There is no separate wire object model and no protocol-to-Protobuf
conversion graph. `state.Command` and the shared `replica/defs` messages are
also generated types.

Go 1.23.2+ is required. Ordinary builds use checked-in generated files and do not
need Python or protoc. The generator is `cosmos/gogoproto v1.7.2`, selected for
its value fields and cast types. This differs from current etcd/raft's
`protoc-gen-go`; the shared design choice is to use generated message types
directly. Removing conversion does not establish a performance improvement.

## Source ownership

| File | Responsibility |
| --- | --- |
| Each package's `messages.proto` | Authoritative wire fields, numbers, names and native Go type options. |
| `messages.pb.go` | All generated declarations: native structs, payload encoding/decoding, stream entry points, wire checks, normalization and local aliases. |
| `wire.go` | Hand-written factories, connection context, scheduling tags, field selection and protocol-specific validation/recovery indexes. |

`manifest.json` records stream entry points, local aliases and scalar type
metadata. It no longer owns a second set of message field declarations. Do not
edit generated Go files manually. The generator applies deterministic payload
method naming and field-selection hooks after protoc runs.
All seven protocol packages use this three-file message layout. The shared
`state` and `replica/defs` generated support is consolidated in their respective
`messages.pb.go` files as well. Regeneration does not create separate wire helper
files.

## Values, identity and ownership

Generated structs have no hidden message mutex, size cache or unknown-field
slice. `RequestID` remains a comparable pair of signed integers. Nested messages
and repeated messages preserve their existing value or pointer ownership as
specified in the schema. Hashes use the comparable `wiretypes.Hash` scalar.

Selection of fields remains part of wire semantics:

- KCensus references carry UID/proposer only. Batch values carry member UIDs and
  omit local execution caches. Nil payload-list entries have an explicit marker.
  Reference selection copies a shallow header of the same generated type.
- Bodega retains per-kind logical fields; result replies do not echo PUT data.
  Projections copy generated headers and share their existing payloads.
- FastPaxos identity-only votes and no-ops omit command bodies using generated
  identity-only record encoders.
- EPaxos known empty/no-op command arrays stay non-nil. Its local
  `PrepareReply.VBallot` remains intentionally absent from the wire.
- Swift recovery transitions construct generated entry lists directly. The state
  machine builds lookup indexes when installing recovery state. Duplicate keys
  are rejected and recovery-map presence is retained.
- Existing context-specific nil/empty command and result rules are applied in
  place after decoding, without constructing another message graph.

The immutable encoded frame continues to be shared within broadcasts. Buffer
pooling, queue admission, scheduling, quorums and retry decisions are unchanged.

## Shared framing

The optional one-byte RPC registration code precedes:

```text
"CAP" | version byte 1 | uint32 little-endian payload length | protobuf payload
```

`wireproto.Encode` sizes the generated message and writes directly into one final
allocation containing the RPC code, header and body. Incoming frames are bounded
before allocation. A schema-derived byte scan checks recursion, narrow integer
bounds and hash widths before generated decoding. In-place normalization and
protocol validation follow. A failed decode does not replace the caller's message.
The outer bound is 64 MiB; existing lower per-protocol bounds remain in effect.

The proto2 schemas preserve field numbers and wire scalar types from the initial
Protobuf migration. Explicit default fields and unpacked repeated scalars can
change the bytes and size; old/new decoder compatibility was checked for the
recorded corpus. Optional bytes retain nil/empty presence. Never reuse released
field numbers or assume byte-for-byte encoding identity.

Master/admin `net/rpc`, connection identity handshakes, stable-store records,
experiment configuration and JSON diagnostics keep their separate formats.
Generated public fields retain their original JSON names. `state.Command`'s
existing stream methods still encode stable-store records; its generated payload
methods encode nested network commands.

## Regeneration and validation

With Python 3 and protoc available, run from the module root:

```text
go install github.com/cosmos/gogoproto/protoc-gen-gogo@v1.7.2
python wireproto/generate.py
go build ./...
go vet ./...
```

The generator uses the pinned dependency's includes and needs `protoc-gen-gogo`
on PATH. It creates no test files. No legacy codec branch or conversion adapters
remain.

Evidence is in the parent repository's `report/protobuf_direct_types_20261023/`:
1,040 decoded-message comparisons, bidirectional compatibility with the prior
implementation, and five-replica network smoke runs for all seven methods.
These are functional checks, not performance benchmarks or a safety proof.
