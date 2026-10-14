# Bodega implementation notes

This page describes the Bodega port in ConsensusArena. See the
[main README](../README.md) for build and participant commands.

## Configuration

Select `protocol: Bodega` in the deployment configuration or pass
`-protocol bodega` to every participant. The implementation supports odd fixed
memberships of 3–63 replicas and requires `noop: false`.

Place protocol options before the `-- Proxy --` section:

```text
bodegaResponders: all // or leader, or comma-separated replica aliases
bodegaLease: 2500ms
bodegaMargin: 100ms
bodegaHeartbeat: 120ms
bodegaFailure: 1200ms
bodegaFailureMax: 2400ms
bodegaUnhold: 250ms
```

The timing defaults follow Summerset's `bodega-artifact` commit `16c6f352`,
including `scripts/bodega/bench_ycsb_zk_etcd.py`. Failure detection samples a
timeout uniformly from 1200–2400 ms on each peer-timer refresh and reuses that
deadline for subsequent checks. Failure deadlines do not renew leases.
Setting only `bodegaFailure` retains the legacy fixed timeout; also set
`bodegaFailureMax` to configure a range. `BODEGA_TIMING` logs effective settings.

A responder range override can select a different mask for inclusive,
nonoverlapping key ranges or a single key:

```text
bodegaResponderRanges: 0..999=r1,r3;1000..1999=r2;2000=leader
```

Unlisted keys use `bodegaResponders`; the current leader is always included.

## Client routing and reads

Clients send GET to the nearest connected responder for the key and send
PUT/SCAN directly to the known roster leader. A read-only replica control RPC
refreshes roster hints once per second. Stale or temporarily unavailable hints
retain follower forwarding as a fallback. Fallback read routing uses proxied
control-RPC duration rather than ICMP.

`bodegaUnhold` is a client GET timer: retry the same request ID once at the leader
and deliver the first reply, without forcing a logged read.

The port uses the commit-notification read path with the optional early
AcceptNote optimization disabled (paper Section 3.2 and Figure 7). A stable
leader reads the latest committed value for the key without waiting for a later
uncommitted write. Other responders hold reads of an accepted write until commit
evidence arrives. Leader and followers use the same lookup for out-of-order
committed values, so an execution gap cannot make the leader return an older
value. Recovered entries wait for execution.

## Writes, transport, and recovery

Writes use 1 ms batches. Commitment requires a majority plus the union of
responders for every written key in the batch.

Non-commit messages use versioned typed binary frames; only heartbeats carry
full rosters. Compact CommitNotice frames carry commit positions rather than
values. Lost Accept data is repaired in bounded windows.

Recovery follows Summerset's `bodega-artifact` commit `16c6f352`: acknowledged
lease revocation with expiry fallback and suffix-only Prepare recovery. Promise
chunks advance when the network writer frees queue space, independently of
heartbeat retry intervals. Peer connections are established concurrently;
roster installation restarts peer failure timers without renewing leases.

Log garbage collection waits for execution by every fixed member. The live
state machine and retained deduplication results form the in-memory snapshot;
a crashed member can stop all-member log garbage collection.

## Diagnostics

| Log or counter | Purpose |
| --- | --- |
| `BODEGA_TIMING` | Effective startup timing settings. |
| `BODEGA_STATS`, `BODEGA_ROSTER` | Counters and installed rosters. |
| `BODEGA_CLIENT_ROUTES` | Local-read, leader-command, and discovery-forward routing counts. |
| `BODEGA_CLIENT_DESTINATIONS` | Initial read destinations. |
| `BODEGA_CLIENT_RTT` | Control-RPC routing latency observations. |
| `BODEGA_CLIENT_HEDGES` | Hedge sends, wins, duplicate replies, and cancellations. |
| `Batches`, `BatchCommands`, `MaxBatchCommands` | Write batch sizes. |
| `BODEGA_WIRE` | Encoded send attempts by message kind. |
| `CommitNotices`, `CommitRepairRequests`, `CommitRepairEntries` | Compact commit-notification and repair behavior. |
| `StableLeaderReads` | Leader-local completions. |
| `UncommittedReadHolds` | Reads held behind uncommitted writes. |
| `OutOfOrderReads` | Reads of committed slots before prefix execution. |
| `PrepareEntries`, `PrepareNanos` | Prepare recovery volume and time. |
| `Revokes`, `RevokeAcks` | Lease-revocation traffic. |
| `CompactedSlots` | Log garbage collection. |
| `BODEGA_TIMER_RESET`, `BODEGA_ROSTER_FILTER` | Timer and roster-filter changes. |

## Compatibility and limitations

Upgrade replicas and clients together for the current wire/control format.
The reserved AcceptNote kind remains in the codec, but its read evidence is
ignored. All replicas must use the same read policy; mixing commit-notification
readers with pre-commit readers is unsafe.

With delayed delivery to one leased responder, a majority of AcceptNotes can
expose a new value before that responder learns the write. A subsequent read
there can still return the old value. Earlier deterministic regression checks
covered this schedule; that evidence argues against directly enabling the
optimization in this port and does not constitute a complete audit of the
authors' system.

The port retains fixed-prefix and fresh-entry checks for deduplicated,
out-of-order reads. These are conservative differences from the artifact.
Durable WAL/checkpoint restart and automatic responder tuning remain
unimplemented. Bounded clock-rate drift is required.

Archived benchmark results from before the timing change do not describe the
current defaults and require remeasurement for that comparison.

## Package checks and fault experiments

From `ConsensusArena/`, check the package with:

```powershell
go test -timeout 7200s ./bodega
```

For networked fault experiments in a Laputa checkout, use the
[benchmarking skill](../../.agents/skills/benchmark-consensus-prototype/SKILL.md)
with `CONSENSUSARENA_PROTOCOL=bodega`. It defines the deployment, fault schedule,
measurement validation, and recovery gate.
