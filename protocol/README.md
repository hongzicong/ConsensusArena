# Replica event interface

All seven replica implementations and the generated skeleton implement `Machine`:

```go
Propose(*defs.GPropose, time.Time) error
Step(any, time.Time) error
Tick(protocol.Tick) error
```

`protocols.go` requires this interface from every replica factory. Live event
loops use these entry points; they are not unused adapters for a test harness.
Paxos and CURP also embed it in their existing runtime adapter interfaces.
Core data structures, ballots, evidence and commit rules remain protocol-local.

## Replica and core ownership

All seven baselines and the generated skeleton use `Replica -> core`. Replica
directly owns protocol runtime resources and one core. `replica.go` contains its
fields, public constructor, initialization helpers, event loop, timers and worker
methods; no separate runtime object
or forwarding facade is allocated. Existing exported fields and methods remain
accessible, including EPaxos's durable/beacon resources and Send override.

Event and control methods in `replica.go` use `*Replica` receivers.
`transport.go` retains protocol transport helpers and RPC
bindings on that same owner. `core` retains algorithm state and decisions.
Paxos/CURP keep their shared EventLoop and protocolAdapter, now referencing
Replica directly. Their core send/broadcast callbacks explicitly use EventLoop
transport so synchronous self delivery and send-admission accounting are retained.
The other five keep their native loops and workers. There is no new event queue,
goroutine, timer, cancellation branch or output barrier.
Construction checks, RPC registration order, queue policies, synchronous local
delivery, post-turn drains and execution/reply timing remain unchanged.

RPC factory registration is separate from subscribing a receive queue. CURP
retains all eight legacy message codes and factories in their original order;
only clients allocate the three reply queues they consume. Its replica receives
protocol traffic through the existing Packet inbox. A registration with no
destination still decodes and validates its frame, then the generic dispatcher
ignores it instead of waiting on a nil channel. Queues with consumers retain
their capacities, FIFO delivery and blocking backpressure. EPaxos no longer
allocates the unused preAcceptOKChan; its active phase queues are unchanged.

SwiftPaxos also separates registration from queue allocation. Replicas allocate
only FastAck, LightSlowAck, Acks, OptAcks, NewLeader, NewLeaderAckN and Sync queues;
the NewLeaderAckN queue remains available to the recovery loop. Clients allocate
only Reply, Accept, FastAck, FastAckClient, LightSlowAck, Acks and OptAcks queues.
Both roles retain all fourteen original factories and wire codes in the same
order. Unsubscribed messages are decoded and validated, then ignored instead of
accumulating in unread queues. Subscribed queue capacities and delivery policies
are unchanged.

The owner calls the methods on its existing event-loop goroutine. Calling them
concurrently from another goroutine is not supported. `Step` accepts the
protocol's decoded message types plus its explicitly recognized local control
events. Unknown types and nil wire messages return an error. Nil proposals and
unsupported timer kinds also return errors. These errors describe driver wiring
errors; ordinary protocol rejection retains its existing reply/drop behavior.
Live loops use `protocol.Must` for this trusted dispatch.

| Protocol | `Step` inputs | `Tick` behavior |
| --- | --- | --- |
| Paxos / CURP | `*Packet`; self delivery retains synchronous dispatch | Maintenance, with a complete `Alive` snapshot |
| FastPaxos | `*wireMessage` or a local `message` | Maintenance using `Elapsed` from runtime startup |
| EPaxos | Its registered prepare/accept/commit/reply/repair messages, beacon and local recovery request | Maintenance for repair/execution; Slow for existing beacons |
| SwiftPaxos | Its acknowledgment and leader/sync messages, local recovery and delivery events | Maintenance is explicitly a no-op: the existing implementation has no periodic protocol timer |
| Bodega | `*message`, drained promise queue and leader control events | Maintenance for heartbeat/leases/recovery; Batch for proposal flush |
| KCensus | `*wireMessage`, received `message`, or local self-delivery event | Maintenance using `Elapsed`, with existing connectivity observation |

Time is explicit where the existing event loop supplied it. Callers supply
`Tick.Elapsed` for FastPaxos/KCensus and `Tick.Now` for absolute-time transitions.
Paxos/CURP receive a copied connectivity observation, not a new failure oracle.
Other proposal/message handlers ignore the time argument and live callers pass
zero instead of adding a clock read to their hot paths. The original timer
periods, batching gates, queue priorities, statistics and post-turn draining stay
in each runtime. Read-only control/status RPCs keep their existing synchronization.

## Output ordering

Outputs use the existing callbacks and queues. They are not collected into a
universal effects list and released at the end of the method. This preserves
Paxos/CURP's reentrant local delivery, Bodega's immediate send-admission feedback,
and SwiftPaxos's descriptor/batcher/reply workers. FastPaxos/KCensus retain their
post-turn drains, including the original order of local outputs. No extra
goroutine, message queue, timer, codec, or network round trip is introduced here.

The skeleton's core may return its own `effects`; its runtime adapter dispatches
them within these same three methods. Other cores may use callbacks. The shared
interface deliberately does not choose one protocol's output policy for another.

New skeletons use native generated Protobuf messages and the same frame encoder
and Sender as the baselines. Their package-local `wireproto.json` scopes codec
generation, frame limits and validation hooks; it does not select scheduling,
commit, retry or client-completion policy. Template creation and selected-package
regeneration leave existing baseline codecs untouched. See `templates/README.md`.

This is a uniform **runtime event boundary**, not a claim that all cores are pure
state machines. Existing internal clock reads, workers, transport dependencies,
construction and output capture still require protocol-specific setup for a
deterministic simulator. Client adapters retain the existing `client.Adapter`
contract; protocol client routing and completion evidence are unchanged.
