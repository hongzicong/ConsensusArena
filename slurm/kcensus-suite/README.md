# Regular latency evaluation

This folder supplies frozen inputs to the existing generated-suite pipeline.
Keep the four original seven-replica deployments and add North American
deployments with three and five replicas. Each has one modeled colocated client
per replica. All six deployments run A/B, giving 12 scenes per protocol and
72 scenes for the six comparison protocols. Read-only workload C is excluded.
Measurement timing and total offered load stay unchanged. Generation and
validation do not launch benchmarks.

The launcher now restricts each replica to `GOMAXPROCS=1` plus `taskset` on one
logical CPU, before Go starts. Networking and garbage collection share that CPU.
Clients, master, network emulation and job control remain separate. Slurm still
reserves two CPUs per task: the replica uses one, its proxy and supervisor the
other. Clients and master each receive two CPUs. The final run keeps exclusive
nodes and the previous 8 GiB memory budget per task (4 GiB per allocated CPU).
Collection requires startup and per-thread affinity evidence. Periodic replica
resource observation is disabled for this latency suite. See the
[paper evaluation overview](../../results/regular/README.md).

| Runtime topology ID | Input case | KCensus deployment |
| --- | --- | --- |
| 1 | `n7-c7-topology-00` | `ring-7` |
| 2 | `n7-c7-topology-01` | `europe-7` |
| 3 | `n7-c7-topology-02` | `east-asia-7` |
| 4 | `n7-c7-topology-03` | `north-america-7` |
| 5 | `n3-c3-topology-04` | `north-america-3` |
| 6 | `n5-c5-topology-05` | `north-america-5` |

The case directory index is zero-based; the runtime topology ID is one-based.
Case metadata retains deployment names, full region labels, membership mapping,
source file hashes and conversion details. Protocol planners still choose their
own leaders, quorums and ingress routes. Summaries retain their standard format;
the `OVERALL` rows provide average and P99 latency. Per-client rows may remain in
raw outputs and need not be shown in the paper. Raw latency samples remain
available for subsequent CDF plots.

NA-3 uses Northern Virginia, Northern California and Oregon. NA-5 adds Canada
Central and Ohio. Both are submatrices of the existing NA-7 snapshot, preserving
source order, exact directed delays and their original source checksum. The
case metadata records the selected source indices. These are explicitly chosen
North American subsets, rather than a claim of a standard three/five-site layout.

Report every topology and workload separately; never average across topologies.
The initial figure plan includes A and B for all six deployments with mean and
p99 latency. If space is limited, A on ring 7 may be shown in a separate figure;
that presentation choice does not remove the other A measurements.

## Workload and network choices

| Parameter | Value | Basis |
| --- | --- | --- |
| Aggregate offered load | **20,000 requests/s** | New paper's latency experiment target |
| Per-client arrival rate | 20,000 / replica count requests/s | One independent Poisson generator per replica; no clones |
| Keys | 100,000 | KCensus `KEYS` |
| Zipf skew | **0.99** in every case | User requirement; KCensus's skewed contention point |
| Read/write mix | A: 50/50; B: 95/5 | Two requested ConsensusArena profiles |
| Key sharing | Shared keyspace across clients | Contention enabled; no requester keyspace partitioning |
| Value size | 16 bytes | Small-value approximation, explained below |
| Warmup / measured arrivals | 5 s / 10 s | Existing pipeline; also KCensus's default 10 s latency run with 5 s warmup |
| Sustain / timeout | Existing 2 s / 19 s from common epoch | Preserve current ConsensusArena measurement semantics |
| Runs | One per deployment/profile | Existing pipeline |
| Workload / key mapping / preload seeds | 1 / 1 / 1 | Reproducible streams shared across protocols and deployments |
| Preload / shuffled key mapping | Enabled / enabled | Existing ConsensusArena behavior |
| Additional jitter | 0 ms | KCensus simulated-delay default (`KC_JITTER` unset) |
| Bandwidth | 125,000 KB/s per connection per direction | Existing ConsensusArena default; not an AWS measurement |

KCensus's original latency/CDF scripts set a total offered load of 1,000
requests/s. This suite instead uses 20,000 total requests/s following the new
paper's experiment directions. That is an arrival-rate target; achieved
completion throughput is measured by the existing pipeline and may be lower.
The four original placements remain Europe, East Asia, North America and the
worldwide Northern-Hemisphere ring, all with seven replicas. The two additions
use subsets of North America with three and five replicas.

KCensus's generator uses short variable strings of the form
`v<process>.<request>!` (`src/cassandra.rs`), without a fixed value-size setting.
Sixteen-byte values preserve a small-value workload in ConsensusArena; they do
not reproduce KCensus's exact serialization or variable payload sizes. The
frozen `workload-template.conf` retains ConsensusArena's protocol timing defaults,
including its lease settings, independently of the network snapshots. KCensus's
internal shard configuration and implementation-specific batching are not
equivalent configuration controls in this pipeline.

The four source snapshots are bundled unchanged in `sources/` from
`LPD-EPFL/kcensus`, checkout revision
`b232c3320d7853ea332e9cb17a0acbf8386e125c`. Workload constants were inspected in
that checkout's `lib.sh`; the input files' SHA256 hashes identify the actual
snapshot bytes. Generation uses the bundled snapshots, not live AWS queries.

KCensus `raw_latencies[i][j]` contains an estimated one-way delay in milliseconds
(ping RTT divided by two). We write `2 * raw_latencies[i][j]` to the existing
RTT-equivalent input format, preserving decimals, direction, asymmetry and source
membership order. The runtime applies `ceil(input / 2)` milliseconds per TCP
stream direction. A colocated client/replica pair has zero additional delay;
physical transport and process overhead still exist. The snapshots contain no
empirical jitter distributions, bandwidth limits or packet-loss model.

## Validate and submit

For upload and submission from Windows, `launch.ps1` accepts the SSH destination
at runtime; no account-specific edit is needed:

```powershell
.\slurm\kcensus-suite\launch.ps1 -Server user@host
```

The defaults submit SwiftPaxos, Bodega, Paxos, KCensus, Agreement and PQL, each
with all 12 deployment/profile scenes, for 72 scenes total. PQL uses the
`pqlfrontier` implementation; original `pql` is rejected. The launcher validates
the suite, builds the current branch, checks the pinned proxy, uploads the binary,
proxy and complete Slurm directory into a unique remote workspace, verifies remote
hashes and submits six Slurm arrays plus their collectors. `submit-<protocol>.log`
in that workspace retains each array ID, collector ID and result directory.

Keep PowerShell open through upload and all six submissions. Once the script
reports that all submissions were accepted, SSH and PowerShell can close; Slurm
owns the experiments and result collection. No `nohup` or persistent local
connection is required. If submission fails partway through, inspect already
printed job IDs and submission logs before retrying to avoid duplicate runs.

The default concurrency is one scene per protocol, up to six scenes / six exclusive
nodes across the six protocols. `-MaxParallel` changes the per-protocol limit.
`-Protocols` selects a subset. `-SlurmAccount` controls the Slurm billing account
(default `dcl`), independently of the SSH login. `-PrepareOnly` validates and builds
locally without SSH, upload or submission. These commands assume Go, Python 3.11+,
OpenSSH, access to Jed and the EPFL VPN when required.

From `ConsensusArena/`, validate locally without launching Slurm:

```powershell
python -B slurm/kcensus-suite/validate.py
python -B slurm/benchmark-suite.py plan latency --suite generated --generated-root slurm/kcensus-suite
```

Build and deploy the current branch using the existing prototype benchmarking
skill before submitting, so the new local-read logging is in the executable.
The following PowerShell commands, run from `ConsensusArena/`, build a frozen
Linux binary, obtain the existing pinned proxy, and upload the executable and
current Slurm scripts (including this folder). They do not submit a benchmark:

```powershell
$kcensusArtifact = powershell -NoProfile -ExecutionPolicy Bypass -File slurm/build-linux.ps1 | ConvertFrom-Json
$kcensusToxi = .\slurm\fetch-toxiproxy.ps1 | ConvertFrom-Json
$kcensusRemoteRoot = '/home/zihong/ConsensusArena'
$kcensusRemoteBinary = "$kcensusRemoteRoot/binaries/$($kcensusArtifact.SHA256)/consensusarena-linux-amd64"
ssh zihong@jed.hpc.epfl.ch "mkdir -p '$kcensusRemoteRoot/binaries/$($kcensusArtifact.SHA256)'"
scp $kcensusArtifact.FrozenPath "zihong@jed.hpc.epfl.ch:$kcensusRemoteBinary"
scp $kcensusToxi.Path "zihong@jed.hpc.epfl.ch:$kcensusRemoteRoot/toxiproxy-server-linux-amd64"
scp -r slurm "zihong@jed.hpc.epfl.ch:$kcensusRemoteRoot/"
ssh zihong@jed.hpc.epfl.ch "chmod +x '$kcensusRemoteBinary' '$kcensusRemoteRoot/toxiproxy-server-linux-amd64'; file '$kcensusRemoteBinary'; command -v ping; sha256sum '$kcensusRemoteBinary' '$kcensusRemoteRoot/toxiproxy-server-linux-amd64'"
```

Verify the binary hash against `$kcensusArtifact.SHA256` and the proxy hash
against `$kcensusToxi.SHA256`. Submit from PowerShell using those same variables:

```powershell
$kcensusRunExports = "ALL,CONSENSUSARENA_BINARY=$kcensusRemoteBinary,CONSENSUSARENA_BINARY_SHA256=$($kcensusArtifact.SHA256),CONSENSUSARENA_TOXIPROXY_SERVER=$kcensusRemoteRoot/toxiproxy-server-linux-amd64,CONSENSUSARENA_TOXIPROXY_SHA256=$($kcensusToxi.SHA256)"
ssh zihong@jed.hpc.epfl.ch "cd '$kcensusRemoteRoot' && bash slurm/kcensus-suite/submit.sh agreement --export='$kcensusRunExports,CONSENSUSARENA_PROTOCOL=agreement'"
```

On Jed, from the prepared `ConsensusArena/` directory:

```bash
bash slurm/kcensus-suite/submit.sh agreement
```

Replace `agreement` with `pqlfrontier`, `bodega`, `kcensus`, `swiftpaxos` or `paxos`.
The wrapper delegates directly to `benchmark-suite.py submit latency --suite
generated --generated-root ...`, selects both A/B profiles and all six cases,
network controls and workload seed, and clears inherited case/arrival-rate
filters. Existing binary paths and binary/proxy hash environment settings remain
in effect. It uses the ordinary array collector and reserves one node / 15 tasks
per scene, with at most six simultaneous scenes by default. Additional runner
arguments can be appended, for example `--mode serial` or `--max-parallel 3`.
Use a fresh result directory per submission if overriding `CONSENSUSARENA_RUN_DIR`.

To regenerate elsewhere with Python 3.11+, using only files in this folder:

```powershell
python -B slurm/kcensus-suite/generate.py .local-artifacts/kcensus-regenerated
```

The generator refuses to replace existing cases or a manifest. Checked-in inputs
are immediately runnable; no generation step is needed on Jed.

## Successful local-read logging

The `agreement`, `pql` and `bodega` replica logs now emit `LOCAL_READ_STATS`:

Coverage note: the first archived executable omitted Agreement's separate
cold-key local lease-read path from this tag. That affects ring-7/A and
north-america-7/A in the October 28 run. Their historical untagged fractions
cannot be interpreted as nonlocal reads. The source now tags immediate and
previously held cold-key local reads as well; the fix requires a fresh build/run.
Existing latency measurements and raw logs are unchanged.

```text
LOCAL_READ_STATS protocol=agreement replica=0 snapshot_unix_ns=... scope=replica_reply_enqueue_lifetime completed_reads=100 local_read_successes=75 local_read_fraction=0.750000
```

`completed_reads` counts distinct GET request IDs whose replies this replica
successfully enqueued. `local_read_successes` counts the subset executed at that
same ingress through the protocol's authorized local lease-read path. Forwarded
results do not count as local successes. Invalid authority, a pending read, a
failed reply enqueue, duplicate response attempts and retransmissions cannot
inflate the local success count. These metrics do not alter consensus or reply
behavior. They require the freshly built executable.

These are cumulative replica-lifetime counters (including warmup and sustain),
not measured-arrival-cohort statistics. A reply being enqueued is not proof of
client receipt; use existing client completion/latency logs for delivered results.
Hedges to different replicas can each enqueue a response, so cross-replica sums
are not globally deduplicated client counts. The fraction is zero when the read
denominator is zero; interpret the accompanying count. Snapshots occur once per
second for agreement/Bodega and every five seconds for PQL, through the existing
status cadence. Instrumentation adds constant-time map lookups per read and one
small record per observed request; it does not log per read. Other protocols do
not expose a corresponding lease-local read path and do not emit this metric.
