# PQLFrontier

Select `protocol: pqlfrontier` to use the modified PQL read policy. The original
`protocol: pql` remains unchanged. The design and safety argument are in
[DESIGN.md](DESIGN.md).

## 1. Implementation scope

PQLFrontier retains PQL's write log, lease/configuration log, timers, placement,
nearest-replica GET routing, non-holder forwarding, retries and executed-result
completion. A waiting eligible read captures a finite accepted-log frontier;
later writes do not extend that request's frontier while its authority remains
valid. No speculative value, client ACK, or leader execution alone serves as a
read certificate. The serving replica must apply the required chosen prefix.

Single-key GET/PUT and existing no-ops are supported in the parent's in-memory
crash-stop model. There is no new multi-key, durable restart or promotion claim.

## 2. Mapping to protocol design

- `read_frontier.go`: frozen prefix, ballot/epoch/deadline validity, and release.
- `core.go`: invokes the read predicate; preserves deduplication/forwarding and
  write majority plus all-holder notification checks.
- `execute.go`, `recovery.go`, `lease.go`, `configuration.go`: parent's ordered
  application, value selection, timing and two-log management.
- `messages.go`: aliases only the unchanged exported PQL wire objects and their
  validated framing. Protocol state and handlers are independent in this folder.
- `parent-source-sha256.json`: records the copied parent's source hashes and
  commit; it is provenance, not a mixed-version cluster compatibility claim.

## 3. Runtime architecture

The normal shared master/client/replica pipeline is used, with one serialized
replica event loop. Only local read eligibility differs from PQL. A small map
stores one frontier for each currently waiting read, and removes it at release,
invalidation, or non-holder forwarding. There is no per-request extra network
exchange or log entry. The original lease/holder placement remains sampled and
does not inspect workload-generator ground truth.

`PQL_FRONTIER_READS` periodically reports `captured`, `released`,
`released_with_pending_writes`, `invalidated` and `waiting`. The concurrent-release
counter tests whether the change actually avoids waiting for complete key
quiescence. `LOCAL_READ_STATS protocol=pqlfrontier` retains the same lifetime
reply-enqueue semantics as the parent; it is not an immediate-hit rate.

## 4. Determinism and tests

Run from ConsensusArena:

```powershell
go test ./pqlfrontier -count=1 -v
```

Tests cover 3/5/7/9/13 replicas, continuous writes, an immutable retry frontier,
unchosen values, execution gaps, leader-versus-follower application, original
result replay, activation catch-up, expired/captured lease intervals, epoch and
ballot changes, preparation, exclusion/forwarding, and write-holder ACK coverage.
The deterministic multi-replica harness uses real packet framing, asynchronous
delivery, and separate client reply delays. Bounded single-register histories
are checked by exact real-time-constrained linearizability search. These tests
are not a mechanized proof or an exhaustive arbitrary-network history check.

## 5. Known gaps and inherited limitations

The captured whole-log prefix can wait for unrelated earlier slots. Its ballot,
epoch and original deadline must remain valid; otherwise the request reacquires
its frontier, so no unconditional latency bound is claimed. Existing PQL clock,
reconfiguration and failure-detection assumptions still apply.

A deterministic leader-crash schedule additionally reproduces an inherited
availability limitation in **unchanged PQL**: after a long configuration pause,
stale per-peer response times can make healthy peers suspect each other and
prevent lease reacquisition. The frontier variant likewise keeps the read
blocked while authority is absent. `TestClusterCrashKeepsReadBlockedWithoutLeaseAuthority`
checks safety in that schedule; it does not assert recovery progress. Ordinary
responsive leader replacement is separately tested and completes the pending
read. Repair of the parent's lease failure detector is outside this scoped
read-frontier change.

No existing report or result is relabelled as PQLFrontier. A new frozen executable
and new run are required for performance claims. The existing KCensus launcher
accepts `-Protocols pqlfrontier`; its default protocol list remains unchanged.

The 2026-10-29 KCensus evaluation completed all 12 scenes and its collector
(array 40735269, collector 40735270), with zero timeouts and complete measured
cohorts. The combined report reuses the five unchanged historical suites:
`report/kcensus-evaluation/20261029-n7-20krps-pqlfrontier/index.html` in the parent
repository. Its raw launch snapshot includes the exact frozen executable and
evaluated source archive. Source comment/documentation cleanup after freezing
does not change the measured protocol behavior.
