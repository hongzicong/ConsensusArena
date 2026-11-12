# Workload-B scalability and resources

The default suite contains **aws-random only**, odd replica counts **3, 5, …, 31**,
one colocated client per replica, one run per size, and **workload B only**
(95% GET / 5% PUT). Compare Agreement, Paxos, SwiftPaxos, Bodega, KCensus and
**pqlfrontier**, displayed as PQL. The launchers reject original `pql`.

The 15 checked-in cases reuse `benchmark-suite.py --suite generated`, including
its synchronized barrier, readiness checks, independent heat sampling, preload
validation, scheduled-arrival latency, throughput validation, and collector.
Paris is deferred: its frozen sources are bundled, but none of its scenes are
generated or submitted by default.

SwiftPaxos's original placement search enumerates every majority for each leader,
which is impractical at 31 replicas. The integration uses `swiftplacement/`:
the original exact planner through 17 replicas, and a deterministic bounded
candidate search above 17. Candidates are leader-containing majorities nearest
each replica/client anchor and aggregate client distance. It uses the original
fast/slow objective and runtime rules, but does not guarantee the globally best
fast quorum. The large-size plan records `swift-c2-bounded-majorities-above-17`;
retain this methodological distinction when interpreting scaling curves.

## Inputs and provenance

`sources/aws-{random,from-paris}/N.toml` are byte-for-byte copies of the sibling
KCensus checkout's `configs/` inputs. Each generated `case.json` records the
source SHA256. Preserve directed delays: multiply the source one-way milliseconds
by two for the existing RTT-input emulator, which applies `ceil(RTT/2)` per
direction. Local client/replica pairs have zero added delay. Physical transport
and emulator overhead remain additional to the source matrices. This is an
emulated AWS placement on Jed, not a deployment on AWS machines.

As requested for the new paper, keep the existing evaluation's workload:
20,000 **total** offered requests/s divided by replica/client count, 100,000
shared keys, Zipf 0.99, shuffled ranks, workload/key mapping/preload seed 1,
16-byte values, 5 s warmup, 10 s measurement, 2 s sustain, and a 19 s deadline.
Bodega's timers and the existing protocol defaults come from the existing
KCensus suite template. This deliberately differs from KCensus Figure 9's
100% writes, uniform keys and 1,000 total requests/s; its deployment matrices
are reused, while the new paper's contended B workload is retained. Values
remain the existing fixed-size approximation to KCensus's variable strings.

Reproduce and validate locally from `ConsensusArena/` (Python 3.11+):

```powershell
python -B slurm/scalability-suite/validate.py
python -B slurm/scalability-suite/generate.py .local-artifacts/scalability-reproduction
# Optional native-binary check of sampling and placement for all protocols at 3/31:
python -B slurm/scalability-suite/smoke.py PATH_TO_NATIVE_BINARY
```

The generator refuses nonempty destinations. `--include-paris` is an explicit
future opt-in, requiring a separate output directory. The default validator and
plotter intentionally require the current random-only experiment.

## Launch

The PowerShell launcher builds and freezes the executable, verifies the pinned
proxy, validates configs, uploads an immutable launch workspace, and submits
six arrays with collectors. Agreement runs first. Arrays are chained with
`afterany` dependencies; default `MaxParallel=1` limits this launch to one scene
at a time across all protocols. Failures are retained for diagnosis and do not
silently trigger retries. This does not limit unrelated jobs already on Jed.

```powershell
# Build/freeze and validate locally, without connecting or submitting:
./slurm/scalability-suite/launch.ps1 -Server zihong@jed.hpc.epfl.ch -PrepareOnly
# Submit all six protocols (90 scenes), aws-random B only:
./slurm/scalability-suite/launch.ps1 -Server zihong@jed.hpc.epfl.ch
```

For an initial smaller expenditure, `-Protocols agreement` submits 15 scenes.
At the maximum size each scene reserves 2 exclusive nodes, 63 tasks, 2 CPUs/task.
Memory remains 8 GiB per task, requested as 4 GiB per allocated CPU.
The existing runner reserves for the largest selected scene in an array,
including smaller replica counts. `MaxParallel` controls simultaneous allocations,
not the work performed or a monetary budget. Standard walltime is 2 hours per
scene; actual runtime determines allocation time. No runs are submitted by
generation, validation, or plotting.

For an already uploaded workspace, `bash slurm/scalability-suite/submit.sh
agreement` submits just that protocol using the same config files. Use the
PowerShell launcher to chain multiple protocols. Retain every printed job ID,
collector ID, run directory, binary hash, and raw archive as in the benchmark
skill. Wait for collectors and require `suite-status.json: complete=true`.

## Resource semantics

The launchers select `GOMAXPROCS=1` and `taskset` before Go starts. Every replica
OS thread shares one logical CPU, including networking, garbage collection and
resource observation. Clients, master, network emulation and job control remain
separate. Each task reserves two CPUs; the replica uses one and its proxy and
supervisor use the other. Clients/master each have two CPUs. Startup and
per-thread affinity evidence are required by collection. See the
[paper evaluation overview](../../results/scalability/README.md).

`CONSENSUSARENA_RESOURCES=1` enables an observer in replica processes only.
Its goroutine waits for the same barrier epoch as clients and records counters
only at seconds 5 and 15, publishing `results/ALIAS-resources.json` atomically
after measurement and before the normal 19 s shutdown. Memory is a kernel peak
counter, with no periodic polling. GNU time lifetime records remain supplemental.
The report uses:

* **Traffic:** sent plus received serialized data-plane bytes, including the
  protobuf framing and RPC tags when present. Includes client requests/replies, peer traffic,
  beacons and lease/maintenance messages. A peer message contributes at both
  endpoint replicas. Excludes TCP/IP overhead, retransmission, handshakes,
  master/admin RPC, client-process CPU, and emulator processes.
* **Message count:** successfully flushed outgoing frames plus completely read
  incoming protobuf frames, independent of socket batching. Unsent queue entries
  are excluded. Partial/failed flushes are excluded on the outgoing side.
* **Traffic/request/replica and messages/request/replica:** each replica's window
  delta divided by **system-wide authorized completions during [5,15)**, then
  averaged across replicas for figures. The denominator is throughput × 10,
  not measured-cohort latency count, offered requests or per-replica ingress.
  Zero completions produce missing normalized metrics, never zeros.
* **CPU:** Linux `getrusage` user+system time delta divided by actual boundary
  elapsed time, ×100. One saturated logical CPU is 100%.
* **Memory:** Linux `getrusage(RUSAGE_SELF).ru_maxrss` at measurement end,
  converted from KiB to MiB. This kernel peak covers process startup through
  measurement end, including preload and warmup. Startup/preload CPU and traffic
  are excluded from the window deltas. Memory is not a time average.

Each frame adds constant-time atomic counter updates; the observer reads at the
two boundaries and writes once after measurement. This introduces overhead and
is enabled equally for all six comparisons; its magnitude has not been measured.
Counters do not change protocol transitions, timing parameters or client routing.
All samples must fall within 100 ms of their intended boundary. Missing replicas,
invalid throughput, regressing counters or late samples fail collection. Raw
timestamps preserve the residual boundary uncertainty.

Each scene and suite exports `resources.csv` with one row per replica, separate
send/receive counters, CPU, kernel peak memory, memory scope, denominator, window timestamps,
input hash, protocol, timeout and cohort completeness. The collector revalidates
raw observations. Completed-request latency remains available for timeout runs;
no missing observations are imputed.

## Figures

Retrieve the six complete suite roots with raw evidence, then run:

```powershell
python -B slurm/scalability-suite/plot.py RUN_AGREEMENT RUN_PAXOS RUN_SWIFT RUN_BODEGA RUN_KCENSUS RUN_PQLFRONTIER --output results/scalability/figures
```

Requires matplotlib. Produces `scalability.{png,pdf}`, `resources.{png,pdf}`
and `scalability-summary.csv`. Figures show overall average latency and four
resource metrics against replica count. PQL's legend maps only to `pqlfrontier`.
No per-datacenter latency detail is plotted. Timeout or incomplete cohorts are
omitted; dotted connectors span gaps without imputing values. Memory uses a
linear scale. Legacy sampled-memory records keep their original meaning and
cannot be mixed with kernel peaks. One observation per point has no repeated-run
confidence interval.
