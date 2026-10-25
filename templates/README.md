# New protocol skeleton

From `ConsensusArena/`, run:

```powershell
go install github.com/cosmos/gogoproto/protoc-gen-gogo@v1.7.2
python templates/new_protocol.py xpaxos
go build ./xpaxos
go vet ./xpaxos
```

Python 3.10+, Go, `protoc`, `gofmt` and `protoc-gen-gogo` must be available. Use the
pinned v1.7.2 plugin. If it is not on PATH, pass `--plugin PATH_TO_PROTOC_GEN_GOGO`
to either Python generator.

This creates a new independent package with nine handwritten Go files,
`messages.proto`, `wireproto.json`, generated `messages.pb.go`, and a README.
It refuses an existing destination and never edits a baseline,
the registry, or experiment configuration. Rendering, Protobuf generation and
formatting finish in a private staging directory before the new package is
published. Missing tools or generation failures leave the destination absent.
`--output` selects another directory inside this repository; its path components
must be identifiers. The generated Protobuf namespace, Go import path and
regeneration command follow that destination.

The generated package compiles but deliberately rejects startup and transitions
until a protocol is implemented. It is not a baseline and cannot produce latency
measurements. Read its README before replacing the guards. The generator does
not choose a leader, quorum size, commit rule, recovery rule, or routing policy.

## Native Protobuf and Sender integration

The generated `Message` is the protocol message itself; `message` is a local Go
alias, not a wrapper. `messages.proto` owns its fields. `wireproto.json` selects
stream message types, optional semantic validators, local aliases and frame
limits. The starter contains only `From` and a checked `uint8` `Kind`; add the
design's ballots, commands and evidence in the schema. Shared fields can import
`state/messages.proto` and `replica/defs/messages.proto` as the baselines do.

After editing the schema or metadata, run from the module root:

```powershell
python wireproto/generate.py --package xpaxos
```

Alternatively, `go generate ./xpaxos` uses the generated directive when the plugin
is on PATH. Selected-package generation writes only that package. It does not
regenerate its shared imports or edit the central baseline manifest. Multiple
`--package` arguments select several packages explicitly. The no-argument command
continues to regenerate only the existing baseline/shared package set.

Generated methods provide `New`, `Marshal`, `Unmarshal`, `MarshalProtoFrame`,
payload bounds checks and normalization. The starter limits payloads to 16 MiB,
below the shared 64 MiB ceiling. `wire.go` contains compile-time integration
checks and the `validateProtoMessage` hook; it contains no alternate codec.
The hook initially accepts decoded headers, so it is not a protocol validator.
Define kind-specific rules before enabling an implementation.

`transport.go` freezes messages with the common `replica.Encode` and enqueues
frames through the existing per-connection `replica.Sender`. Stream methods use
the shared CAP/v1 framing. Failed framing, schema checks or a semantic validator
leave the receiving message unchanged. Frames retain FIFO queue admission and
immutable byte ownership. The starter still rejects self-delivery and returns
send-queue errors: choose local-delivery and retry semantics in the protocol
design before removing those guards.

## Responsibility boundaries

- `Replica -> core` is the ownership chain used by the seven baselines and this
  template. `replica.go` owns fields, construction, callback wiring and the
  event-loop, timer and worker methods on `*Replica`; it introduces no
  separate runtime type or forwarding facade. There is one framework host/state.
- The replica implements the shared `protocol.Machine` interface in `replica.go`:
  `Propose(*defs.GPropose, time.Time) error`, `Step(any, time.Time) error`, and
  `Tick(protocol.Tick) error`. Live loops and external event drivers use the same
  entry points. See [the event contract](../protocol/README.md) for time, ownership,
  errors and output ordering.
- `core.go` owns protocol state and normal transitions. Its private `submit`,
  `receive`, and `tick` entries have explicit inputs and return protocol-authorized
  outputs; the Replica adapters call them directly.
- `recovery.go` owns recovery evidence and safe selection through `beginRecovery`;
  ordinary gap repair belongs here too. Recovery does not belong to the network writer.
- `execute.go` owns order, deduplication, state-machine application and completion
  through `apply`.
  Committed, executable, executed, and client-completed are distinct conditions.
- `replica.go` constructs, validates, owns framework resources and request contexts,
  binds `execute`, drives transitions and owns timer
  lifecycle; `transport.go` dispatches outputs through the common `replica.Sender`
  and reply infrastructure; generated code handles framing and `wire.go` supplies
  protocol-specific wire validation. Queue retry decisions stay
  in core. Use `c` for core receivers and `r` for Replica receivers.
- `client.go` chooses routing and validates completion, using the common client
  framework for workload, connections, timing and fault instrumentation.
- `plan.go` owns startup leader, quorum, responder, and ingress policy, even when
  the policy is fixed. Common `placement` helpers only read inputs and do arithmetic.

Keep local state types with their owning component; `defs.go` is for shared
protocol vocabulary and messages, not every structure in the package. Add
`batch.go`, `conflicts.go`, `lease.go`, or other cohesive mechanism files only when
the design needs them. Do not copy a baseline's quorum/recovery implementation
into a shared runtime to fill out the skeleton.

## Registering an implemented protocol

All seven existing protocols are integrated through `protocols.go`.
Every replica factory returns `protocolapi.Machine` (the root file's import alias
for `protocol.Machine`), so missing event methods fail at compilation.

Add your package import and one `protocolSpec` entry. For the generated API:

```go
"xpaxos": {
    planDeployment: xpaxos.PlanDeployment,
    configureClient: func(c *config.Config) {
        // Set Fast/Leaderless/WaitClosest according to the finalized routing design.
    },
    installClient: func(b *client.BufferClient, c *config.Config, clone int) {
        if _, err := xpaxos.NewClient(b, len(c.ReplicaAddrs)); err != nil {
            panic(err)
        }
    },
    newReplica: func(s replicaStart) protocolapi.Machine {
        r, err := xpaxos.New(s.config.Alias, s.id, s.addrs, s.config, s.logger)
        if err != nil { panic(err) }
        return r
    },
},
```

The required `planDeployment` hook receives the parsed configuration and topology
before any process starts. Its returned plan is applied in memory by the launcher.
`-run plan` prints the same result for inspection; that JSON is never startup input.
Client flag configuration runs before common client construction. Adapter
installation receives the clone index and runs before connection setup. Use `_ int`
when the protocol does not need the clone index. Replica construction occurs after
master registration and before control RPC registration. Each protocol keeps its
own configuration validation and message registration order. No implicit `init()`
registration or reflection is used to discover protocols.

This centralizes process integration, not every deployment change: protocol
options still need configuration parsing, and crash target selection may need
explicit support in the experiment harness. Placement belongs in `plan.go`, never
in the harness. Describe
these choices in the design and implementation notes before benchmarking.
