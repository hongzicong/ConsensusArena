# New protocol skeleton

From `ConsensusArena/`, run:

```powershell
python templates/new_protocol.py xpaxos
go build ./xpaxos
```

This creates a new independent package with the ten responsibility files.
It refuses an existing destination and never edits a baseline,
the registry, or experiment configuration. `--output` is available for checking a
template under another directory inside this repository.

The generated package compiles but deliberately rejects startup and transitions
until a protocol is implemented. It is not a baseline and cannot produce latency
measurements. Read its README before replacing the guards. The generator does
not choose a leader, quorum size, commit rule, recovery rule, or routing policy.

## Responsibility boundaries

- The replica implements the shared `protocol.Machine` interface in `runtime.go`:
  `Propose(*defs.GPropose, time.Time) error`, `Handle(any, time.Time) error`, and
  `Tick(protocol.Tick) error`. Live loops and external event drivers use the same
  entry points. See [the event contract](../protocol/README.md) for time, ownership,
  errors and output ordering.
- `core.go` owns protocol state and normal transitions. `Propose`, `Handle`, and
  `Tick` have explicit inputs and return protocol-authorized outputs.
- `recovery.go` owns recovery evidence and safe selection; ordinary gap repair
  belongs here too. Recovery does not belong to the network writer.
- `execute.go` owns order, deduplication, state-machine application and completion.
  Committed, executable, executed, and client-completed are distinct conditions.
- `replica.go` constructs and validates; `runtime.go` drives transitions;
  `transport.go` adapts outputs to the common `replica.Sender`; `wire.go` encodes messages.
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
