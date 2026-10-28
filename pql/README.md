# PQL

This package implements Paxos Quorum Leases (PQL) on ConsensusArena's shared,
single-threaded replica event loop. It follows the authors' `qlease`
implementation, using the paper's lease timing protocol:

- a stable Multi-Paxos log orders writes;
- every sampled key has a majority-sized holder set containing the deployment
  leader, selected from the independent pre-run regional heat sample;
- writes send `Accept` to the holder set plus enough replicas for an ordinary
  Paxos majority; commitment requires both majority acceptance and every
  required holder's acknowledgement (the same majority in the steady state);
- guards precede promises, promises are renewed periodically, and a holder may
  read only while a majority lease is active and it has executed through every
  accepted frontier advertised by its activation quorum;
- an accepted but unexecuted write makes local reads of its key wait; and
- a request sent to a non-holder is forwarded to the current Paxos leader.

A configured holder waits for an active majority lease, its activation frontier,
and execution of accepted interfering writes. Non-holders forward reads to the
leader, which uses the same reader path. This follows the authors' `handlePropose`,
`handleForward`, and `reader`; lease expiry does not automatically log a read
through Paxos. The paper's broader suggestion of Paxos-served reads is not used
as an alternative read policy in this baseline.
If a replacement leader is not yet a holder of a key, it waits for the normal
read-frequency configuration update to add it before serving that key locally.
Production replicas sample the current local monotonic clock at each
transition; a delayed ticker timestamp cannot reactivate an expired lease.
Deterministic event drivers clamp supplied times to the latest observation.

The independent regional heat sample supplies an agreed initial configuration.
Unobserved keys initially have a default lease at the leader. During execution,
the leader counts forwarded reads and proposes holder updates every 10 seconds,
following §3.4. Each update selects the leader and the `floor(N/2)` most
read-active additional replicas, preserving existing holders on tied counts.

Lease configurations are incremental values in a separate majority Paxos log,
with independent ballots, accepted values, recovery and catch-up. Configuration
consensus and command consensus both run on the same event loop. A grantor stops
renewing when it learns a new configuration, waits for its outgoing promises to
expire, and then establishes promises for the new configuration (§3.5).

An unresponsive holder is suspected after the paper's five-second grace period.
Grantors stop renewing; the leader proposes exclusions from its own observations,
as in the authors' failure detector. Remote suspicion masks are not imported as
local failure evidence, and a replica cannot suspect itself. Writes remain
blocked where necessary until old promises expire, then continue with an
ordinary accepting majority and the remaining required holder acknowledgements.
Leader changes recover both logs. Missing command-log heartbeats for the
five-second grace period trigger election even if the TCP connection remains
open; missing lease replies alone do not preempt a responsive Paxos leader.
Local reads resume after replacement promises and prefix catch-up; there is no
permanent switch to plain Multi-Paxos.

Waiting readers request catch-up through their accepted/activation frontier.
After phase one has fenced the remaining suffix, the leader resolves slots
above its recovered log with ordinary Paxos no-ops, in bounded batches. This
also handles a minority-accepted slot absent from the recovery quorum. The
reader's barrier is preserved until the resulting prefix is executed. Catch-up
never sends new-ballot Accept messages while phase one is still in progress.

Lease duration and guard duration are both two seconds, with renewal every
500 milliseconds, as in the paper's evaluation (§§3.8, 5). The supplied authors'
code uses a one-second guard constant. Renewal messages carry the acknowledgement
sequence and elapsed time from Figure 5, bounding even delayed, unacknowledged
renewals. Holder timers use local durations; synchronized clocks are not required,
but the paper's similar-clock-rate assumption still applies.
Acknowledgements shorten the existing grant deadline using Figure 5's minimum
rule. This is an explicit paper-based deviation from the authors' ACK handler,
which can conservatively extend that deadline when a reply is delayed. The
source's longer wait is not by itself a demonstrated safety violation.
The latest-exchange ACK filter retains a source availability limitation: a guard
ACK arriving after the next 500ms renewal tick can be superseded. RTT below the
guard duration alone therefore does not guarantee activation. This limitation
is documented and tested without adding ACK pipelining.

Like the other ConsensusArena baselines, completion means ordered state-machine
execution, not merely commitment. Replica protocol work, lease maintenance,
recovery, and execution all run on the shared serialized event loop.
Client no-ops complete with an empty cached result after ordered execution.
All signed client IDs are valid; recovery no-ops use the internal origin sentinel
and never enter client deduplication or pending-request maps. An excluded live
ingress forwards reads even if it retains an older incoming lease epoch.

This package targets the same in-memory crash-stop deployments as the other
Arena baselines. It does not claim durable process-restart recovery; the paper's
rejoin wait alone would not recover an acceptor's lost Paxos state. `PUT`, `GET`
and no-op operations are supported; SCAN and composite operations remain outside
this baseline's current workload scope.

Source mapping: paper §§3.4–3.8 / Figure 5; authors' `src/paxos/paxos.go`
(`proposeReplicasDead`, `updateKeyQuorumInfo`, `getLeaseQuorumForKey`),
`src/paxos/read_stats.go`, `src/lpaxos/lpaxos.go`, and the lease exchange in
`src/genericsmr/genericsmr.go`. The port retains their two-log structure without
their extra replica, executor and reader goroutines.

Wire types are generated from `messages.proto`:

```sh
python wireproto/generate.py --package pql
```

Run `go test ./pql` from the ConsensusArena root to exercise the protocol and
permanent regression tests. `go test ./pql -run TestRegression -count=1` runs
the original review regressions with the corrected source-first read policy.
`go test ./pql -run TestSourceCoherence -count=1` runs the second-pass regressions.
`go test ./pql -run TestOriginalSafety -count=1` checks the port against concrete
correctness defects reproduced in the authors' implementation. The source audit
and explicit record of safety defects, availability limitations, conditional
risks and existing departures are in
[`report/analysis/pql-review/source-findings.md`](../../report/analysis/pql-review/source-findings.md).
