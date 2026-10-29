# PQLFrontier design

## 1. Goal and comparison target

Implement the user's scoped PQL modification as protocol `pqlfrontier`, in an
independent package. Preserve `pql` unchanged. A waiting GET must not repeatedly
acquire dependencies on writes accepted after its original eligible observation.
Do not remove holder notification, majority choice, ordered execution, or lease
authority. Do not change client routing, placement, workload, or timing settings.

### Wiki grounding and candidate comparison

Reviewed on 2026-10-29, before implementing this variant:

- `ConsensusWiki/wiki/open-questions/new-protocol-ideas.md`: D1/D6/D10 and
  C002/C003/C030 distinguish finite predecessor closure, serving authority, and
  per-request progress. These are speculative mechanisms, not inherited proofs.
- `ConsensusWiki/wiki/protocols/PQL.md`: notification obligations, activation
  frontiers, ordered execution, and lease containment.
- `ConsensusWiki/wiki/papers/Paxos-Quorum-Leases-2014.md`: sections Normal path,
  Slow path, Commit rule, Conflict handling, Safety argument, and Recovery path.
- `ConsensusWiki/wiki/concepts/quorum-lease.md`: holder/grantor distinction and
  majority activation; a configured holder is not automatically a fresh reader.
- `ConsensusWiki/wiki/proof-notes/read-freshness.md`: visibility of earlier
  completed writes is distinct from stability/application of returned state.
- `ConsensusWiki/wiki/protocols/PQR.md` and
  `ConsensusWiki/wiki/comparisons/by-dimension/read-strategies.md`: a contrasting
  lease-free family discovers an accepted frontier with an intersecting read
  quorum and then waits for actual execution. A frontier alone is not a value.
- Actual parent `pql/{core,execute,lease,configuration,recovery}.go`, and authors'
  `qlease/src/paxos/paxos.go` reader/updating-counter handlers.

Candidates, ranked for this explicitly requested change:

1. **PQL extension: frozen local execution frontier (selected).** Retain PQL's
   notification/activation visibility, wait for a finite existing log prefix,
   and execute the GET on the holder's real state. Adds one small record per
   waiting GET, no new network round, wire fields, or quorum. Principal risk:
   reusing a frontier after its authority or ballot is stale.
2. **Lease-free PQR discovery and rinse.** Adds per-read quorum traffic and new
   failure/data-binding obligations. Potentially avoids lease waits but changes
   the baseline much more and is outside the user's requested tiny modification.
3. **Ordered Paxos GET fallback.** A robust alternative that gives the read a log
   position, at the cost of read consensus traffic and a changed request policy.
   Not selected because a valid lease already supplies visibility here.

This frozen-frontier synthesis is a modification of the implementation, not a
claim of a new protocol or a rule stated verbatim in the PQL paper.

### Performance feasibility

Archived Ring A had READ mean/P99 2,852/12,219 ms versus UPDATE 384/1,048 ms;
26.6% of reads exceeded five seconds. The parent characterization executed 101
overlapping writes while an older GET remained pending. A finite already
accepted prefix plausibly removes seconds of waiting for global key quiescence,
while retaining the cost of executing that prefix. This is a hypothesis, not a
measured speedup. The user subsequently requested the existing 12-scene KCensus
suite for this variant, reusing the other five historical protocol suites in a
new report. No numerical promotion gate is specified. No protocol promotion or
change to historical results is claimed. A falsification
signal is continued old-read blocking while its recorded target has executed
and its original lease/ballot remain valid. Structured release counters expose
whether a read completed while newer same-key writes were still pending.

## 2. Model and assumptions

Inherit PQL's odd membership of 3..63, crash-stop in-memory model, stable-log
Paxos safety, similar-rate monotonic clocks, notification obligations, two-second
leases/guards, 500 ms renewal, and serialized event loop. Only single-key GET,
PUT and existing no-ops are supported; no new multi-key guarantee is introduced.
The application's state equals the contiguous chosen-and-executed log prefix.
No restart/durable-state claim is made. Inherited recovery and lease limitations
are not claimed repaired by this change.

Completion of a write at a client is not the read authorization. Write completion
continues to follow ordered execution after majority acceptance and all required
holder ACKs. A holder can serve a GET without contacting the leader again only
after its own required prefix has executed and its own lease checks succeed.

## 3. Protocol objects

Retain the parent's wire objects exactly; reuse exported PQL wire types to avoid
duplicating protobuf registrations or changing serialization. Protocol state,
runtime, client routing, policy and recovery are independently defined here.

Add a local `readFrontier` per waiting request ID: `(slot, ballot, leaseEpoch,
leaseUntil)`. `slot` is the holder's highest accepted log slot at the first
eligible observation. Using the whole accepted prefix conservatively includes
all preceding same-key notifications and may wait for unrelated preceding slots.
No speculative value is cached with the frontier. Bookkeeping is removed on
completion/forwarding; retries with the same ID retain a still-valid frontier.

## 4. Normal-case pseudocode

On GET, retain result deduplication and existing non-holder forwarding.
At a holder:

1. Require active command-log service, no preparation in progress, a live incoming
   lease, and execution through the existing lease activation frontier. Otherwise
   invalidate any saved frontier and retain the request in the normal wait queue.
2. Invalidate a saved frontier if its ballot or incoming lease epoch changed, or
   its captured lease deadline passed. A renewal cannot extend the deadline
   captured for that particular frontier.
3. If no saved frontier and the key has no pending writes, serve the ordinary
   local read. Otherwise capture `(high, ballot, promisedTo, readUntil)` once.
4. If `executed < frontier.slot`, keep waiting. Later Accepts do not extend it.
5. With the same still-valid authority, after `executed >= frontier.slot`, execute
   GET atomically against current applied state, even when newer same-key writes
   remain pending. Cache the executed result and use the unchanged reply path.

Read-frontier capture/release/invalidation counters are updated in the serialized
loop and emitted with the existing periodic status output; no per-request logs.

## 5. Commit rule

No write commit or reply rule changes. A PUT is chosen only with an ordinary
distinct-voter majority and every required holder notification ACK, then applied
in prefix order before an authorized execution reply. Accept receipt, a slot
number, a leader response, or client delivery alone cannot authorize a local GET.
GET completion in this variant requires steps 1-5 above or existing cached-result
replay/forwarding. No provisional accepted PUT value is returned.

## 6. Fallback and recovery pseudocode

Non-holders retain the parent's forward-to-leader path. Waiting holders retain
the parent's fetch/catch-up and retry queue. No automatic consensus GET fallback
is introduced. On changed ballot/lease epoch, lost authority or captured deadline
expiration, discard the old read frontier; reacquire one only after eligibility
has been re-established. During preparation or inactive service, no new local
read may escape. Higher-ballot recovery preserves chosen prefixes and resolves
unselected suffixes exactly as in PQL; a saved provisional value is never reused.

## 7. Safety obligations

**Visibility.** At eligible observation time `t`, every same-key write completed
before `t` is either in the executed activation prefix or has notified the holder
before completion. Its accepted slot is therefore at most the captured `high`.
If the key is already clean, all such notifications have been executed.

**Stable result.** Waiting through the captured slot means executing a contiguous
chosen prefix, not merely observing acceptance/commit. The actual GET reads that
state, possibly a later executed prefix. Thus it includes every write completed
before `t`, and therefore every write completed before the GET invocation.

**Real time and read/read ordering.** Later writes are concurrent with this GET
and can be ordered after its chosen read position. A write already observed by
an earlier completed GET is chosen, and its notification/activation frontier is
also visible to a subsequently invoked eligible GET. The second GET cannot
return a state preceding that write. Reads return final applied values, including
after higher-ballot replacement of merely accepted entries.

**Authority.** The observation and local execution occur in one captured live
lease interval and unchanged ballot/epoch; every transition rechecks current
eligibility. Different intervals require a fresh observation. No interpretation
of "some concurrent write completed" replaces these checks.

This is a scoped safety argument conditional on the inherited PQL obligations,
not a mechanized proof of the entire parent or the variant. Deterministic tests
must exercise continuous writes, unchosen values, prefix gaps, delayed follower
execution, duplicate IDs, expiry, epoch/ballot changes, and recovery.

## 8. Resolved risks

- Never wait for an advancing global cleanliness predicate after capturing a
  frontier; use a finite immutable slot until its authority is invalidated.
- Never use a count of writes or a client ACK as a freshness certificate.
- Preserve actual state execution, duplicate result identity, forwarding and
  write quorum/coverage checks.
- Revalidate service activity and preparation; a serving lease alone cannot
  excuse an unresolved recovery suffix in this variant.
- Pin the captured deadline as well as ballot/epoch. Recovery or expiration may
  legitimately make a read wait again; no unconditional latency bound is claimed.
- Original PQL remains selectable and byte-for-byte unchanged. `pqlfrontier`
  receives its own registration/log labels and retains independent code state.
- No benchmark or formal verification result is claimed before running it.
- A crash schedule can stall lease reacquisition in the unchanged parent as well
  as this variant (see README). The scoped change preserves safe blocking and
  does not claim to repair that independent availability limitation.
