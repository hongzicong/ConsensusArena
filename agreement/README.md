# Agreement

Agreement is the protocol previously named XPaxos. Select it with
`protocol: Agreement` in a deployment configuration or
`CONSENSUSARENA_PROTOCOL=agreement` in the Slurm launcher exports.

The implementation accepts odd memberships of 3–63 replicas. The planner uses
an exhaustive responder search through 13 replicas and a bounded search for
larger memberships, including 17. The existing fixed benchmark matrix and fault
harness remain 5/9/13; use generated latency inputs for 17 replicas.

## Startup and benchmark integration

The protocol is registered in `protocols.go` with its own planner, replica and
client adapter. Startup loads a frozen empirical heat profile. For a new generated
deployment, prepare it before asking for a plan or starting participants:

```powershell
python slurm/prepare-heat-profile.py <binary> <deployment.conf>
<binary> -run plan -config <deployment.conf>
```

The latency and fault launchers perform this preparation automatically after
applying workload overrides. The planner caches the validated sample and derives
rank/probability tables; unknown keys have rank -1 and use the ordinary path.
The sample's configured hash remains mandatory.

Agreement retains the imported `bodegaLease`, `bodegaMargin`, `bodegaHeartbeat`,
`bodegaFailure`, `bodegaFailureMax`, `bodegaUnhold`, `bodegaResponders` and
`bodegaResponderRanges` configuration names. This does not change baseline
Bodega. Its historical `BODEGA_*` and `XPAXOS_*` diagnostics and the
`GetBodegaRoster` control RPC name are also retained. The fault harness uses a
majority of installed roster records to identify Agreement's leader rather than
treating the advisory master as its election authority. Toxiproxy shapes those
control connections with the same topology as data connections.

## Porting from the supplied checkout

The required startup-plan fields and routing-hint definitions were recovered from
`ConsensusArena-xpaxos_evaluation`. The unused historical `legacy/v6`, `legacy/v8`
and `legacy/v14` planners are not dependencies of this implementation.

The current framework uses `ID`/`ClientID`/`ClosestID`, `protocol.Machine.Step`,
and an execution-enabled replica constructor. Agreement uses those APIs. Client
completions go through `BufferClient.CompleteReply` so shared deduplication and
throughput accounting see authorized replies, including client certificates.

The imported hand-written binary transport was replaced with native generated
protobuf types and the shared `protobuf-v1` framing. All imported message fields,
including delivery attempts, scan-stage metadata and cold-path bodies, remain
represented. Cold-path JSON/recovery catalogs inside message bodies retain their
existing encoding. Voting, ordering, lease and recovery transitions were not
redesigned in this integration.

Regenerate only Agreement's messages with:

```powershell
python wireproto/generate.py --package agreement --plugin <protoc-gen-gogo>
```

This requires `protoc` on PATH and the same gogoproto version as the module.
Baseline protocol packages are not regenerated or modified.

## Validation

```powershell
go test ./agreement/... . -count=1
go vet ./agreement/... .
```

Tests cover registration and sample-based startup planning at 5/9/13/17 replicas,
and shared-frame round trips for every message kind. Integration artifacts under
`results/agreement-integration/` record the native executable build, Linux/amd64
cross-build, launcher checks, and successful five-replica mixed/read-only process
smoke tests. The Windows smoke test uses a test-only Linux-argument adapter that
runs actual Windows ICMP ping for the unchanged master's readiness check; it is
not a performance benchmark or a Slurm deployment.
