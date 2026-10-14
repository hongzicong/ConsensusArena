# SwiftPaxos source-style Slurm benchmark

Branch: `codex/swiftpaxos-slurm`.
Protocol source: `imdea-software/swiftpaxos` at
`35c69365f1c7737a08e237bfbaf828ee68897080`.
Matched benchmark reference: modular ConsensusArena `master` at
`34cb7b872ab6c8b8b4ede80d86e00398f4d7e345`.
`upstream-provenance.json` pins both sources and hashes the reference inputs.

## 1. Implementation scope

The original Go module, `main.go`/`run.go` protocol switches, six protocol
packages, native wire messages, and event loops are retained. No protocol
registry, `protocol.Machine`, skeleton generator, or modular protocol split is
imported. Shared platform adapters provide Linux builds, Slurm deployment,
Toxiproxy routing, preload, and benchmark sampling/collection.

**Matched mode is now the Slurm default.** The common workload, initial data,
measurement clocks, and startup roles match the pinned master. Native mode is
an explicit upstream reference; the two modes must not be mixed in aggregates.

| Setting | Matched default |
| --- | --- |
| Scenes | 5/9/13 replicas × A/B/C × three topologies; serial, fresh processes |
| Clients | 10, no clones |
| Arrivals | Open-loop Poisson, 2,000 requests/s/client |
| Keys | 1,000,000; Zipf skew 0.9; shared shuffled mapping seeded by topology ID |
| Request seed | Base 1, derived from alias and clone exactly as master |
| Writes | A/B/C: 50%/5%/0%; same `< prob` sampler as master |
| UPDATE values | 1,000 bytes; fresh value/version per update; separate RNG |
| Preload | All keys, 1,000 bytes/value, seed 1; every replica digest verified |
| Timing | Unmeasured bootstrap, 5s warmup, 10s measured generation, drain replies |
| Latency | Send timestamp to observed completion; warmup excluded; READ/UPDATE/ALL |
| Network | Same frozen CloudPing 1Y/P50, catalog, directed Toxiproxy shaping |
| Roles | Same per-protocol leader/quorum/nearest-ingress policy and tie breaks |

The standard allocation is three exclusive CPU nodes, 24 tasks, eight tasks/node,
eight CPUs/task, 1 GiB/CPU, `standard` partition, `parallel` QOS, account `dcl`,
and two hours. Five-replica scenes use two nodes; nine/thirteen use three.
The one-node `smoke` allocation is diagnostic only: 16 tasks, one CPU/task,
10,000 keys and 50 requests/s/client by default, with the same 5s/10s clocks.
Its results always carry `diagnostic_only=true`.

## 2. Mapping to source and master

| Adapter | Files and boundary |
| --- | --- |
| Optional outgoing dial map and listener barrier | `main.go`, `client/client.go`, `replica/replica.go`, `replica/defs/network.go` |
| Original switches enable the matched client | `main.go`, `config/config.go`, `client/buffer.go` |
| Master sampling and clock functions, copied without behavior changes | `client/benchmark.go` |
| Deterministic genesis dataset before protocol startup | `state/preload.go`, `replica/replica.go` |
| External master-equivalent startup policies; original quorum config format | `slurm/master_placement.py`, `slurm/upstream_harness.py` |
| Replica host/port registration and endpoint-based quorum lookup | `run.go`, `replica/quorum.go` |
| Process lifecycle, raw evidence and resource observation | `slurm/node-runner.sh`, `slurm/upstream_harness.py` |

SwiftPaxos receives master's selected leader and leader-including fixed majority.
Paxos and CURP receive master's selected initial leader; their quorums remain
dynamic. Fast Paxos keeps a cardinality quorum, and EPaxos receives nearest
client ingress without a global leader or fixed voter set. Both `slow-first`
(default) and `fast-first` objectives match master. Custom client weights are
rejected explicitly. N²Paxos has no master counterpart and requires native mode.

The implementation-time comparison used the pinned master's actual Go policies
for all nine size/topology cases, five shared protocols and both objectives
(90 cases). The sampling/clock functions, preload functions and workload template
were also checked against that reference. The temporary test and source-check
scripts have been removed; source pins and hashes remain in
`upstream-provenance.json`, and observations remain in the validation record.

Wiki grounding: `ConsensusWiki/wiki/protocols/SwiftPaxos.md`, Roles and Fast path;
`ConsensusWiki/wiki/papers/SwiftPaxos-2024.md`, quorum system and artifact mapping.
Leader-including fixed-majority configuration is existing SwiftPaxos behavior;
this adaptation introduces no new protocol or safety claim.

## 3. Runtime and workflow

Run preparation from this branch's ConsensusArena directory in PowerShell.
Use an isolated remote root for each committed adapter revision.

```powershell
$artifact = powershell -NoProfile -ExecutionPolicy Bypass -File .\slurm\build-linux.ps1 | ConvertFrom-Json
$toxi = .\slurm\fetch-toxiproxy.ps1 | ConvertFrom-Json
$revision = git rev-parse HEAD
$remoteRoot = "/home/zihong/ConsensusArena-upstream-slurm/$revision"
$remoteBinary = "$remoteRoot/binaries/$($artifact.SHA256)/consensusarena-linux-amd64"
$remoteToxi = "$remoteRoot/toxiproxy-server-linux-amd64"
ssh zihong@jed.hpc.epfl.ch "mkdir -p '$remoteRoot/binaries/$($artifact.SHA256)'"
scp $artifact.FrozenPath "zihong@jed.hpc.epfl.ch:$remoteBinary"
scp $toxi.Path "zihong@jed.hpc.epfl.ch:$remoteToxi"
scp -r slurm "zihong@jed.hpc.epfl.ch:$remoteRoot/"
scp upstream-provenance.json "zihong@jed.hpc.epfl.ch:$remoteRoot/"
ssh zihong@jed.hpc.epfl.ch "chmod +x '$remoteBinary' '$remoteToxi'"
$baseExports = "ALL,CONSENSUSARENA_BINARY=$remoteBinary,CONSENSUSARENA_BINARY_SHA256=$($artifact.SHA256),CONSENSUSARENA_TOXIPROXY_SERVER=$remoteToxi,CONSENSUSARENA_TOXIPROXY_SHA256=$($toxi.SHA256),UPSTREAM_ADAPTER_REVISION=$revision"
ssh zihong@jed.hpc.epfl.ch "cd '$remoteRoot' && python3 slurm/benchmark-suite.py plan latency"
ssh zihong@jed.hpc.epfl.ch "cd '$remoteRoot' && python3 slurm/benchmark-suite.py submit latency --account dcl --export='$baseExports'"
```

For a five-replica topology-1 diagnostic, submit `smoke` instead of `latency`.
On an eligible alternative partition, set `SBATCH_PARTITION=bigmem` in its
exports. Smoke load/key overrides are explicit diagnostic changes, not standard
master-comparison evidence. For a standard diagnostic subset use
`CONSENSUSARENA_REPLICAS=5,CONSENSUSARENA_CASES=5-1,CONSENSUSARENA_YCSB_PROFILES=A:C`.

Matched overrides: `CONSENSUSARENA_PROTOCOL`, `CONSENSUSARENA_WORKLOAD_SEED`,
`CONSENSUSARENA_ARRIVAL_RATE`, `CONSENSUSARENA_KEY_COUNT`, `CONSENSUSARENA_ZIPF_SKEW`,
`CONSENSUSARENA_COMMAND_SIZE`, `CONSENSUSARENA_PRELOAD_SEED`,
`CONSENSUSARENA_WARMUP`, `CONSENSUSARENA_DURATION`, and
`CONSENSUSARENA_QUORUM_OBJECTIVE`. Compared master runs must use identical
configuration values; these names do not imply master supports every export.
Freeze aliases, key mapping, seeds, resource allocation and proxy settings.

Native reference: add `CONSENSUSARENA_BENCHMARK_MODE=native`. Its defaults are
1,000 measured requests/client, 1,000-byte reused payload, conflict knob 2,
128 pending requests, random seeding, no preload, one excluded bootstrap reply,
and first-replica/first-majority roles. `UPSTREAM_REQUESTS`,
`UPSTREAM_COMMAND_SIZE`, `UPSTREAM_CONFLICTS`, and `UPSTREAM_PENDING` are accepted
only in native mode. Native A/B write probabilities retain upstream's 51%/6%
sampler behavior; C is pure read. Native logs support only `operation=ALL`.

Record the printed job ID and new run directory; never reuse result directories.
Use `squeue -j JOB_ID`, `sacct -j JOB_ID`, bounded log reads, and
`python3 slurm/benchmark-suite.py collect latency RUN_DIRECTORY` when needed.
Archive each complete raw directory to `~/consensusarena-results/` before scratch
expiry and retrieve its raw logs/configuration/binaries plus portable summaries.

The scene coordinator records immutable binary and proxy hashes, reference
commit, workload settings, startup placement, physical addresses, topology and
snapshot hashes, and per-replica preload digests. Collection requires exactly
all measured replies for every client, successful client exits, matching logged
settings, and every participant's GNU-time CPU/RSS/context-switch record.
`summary.csv` contains region/operation rows; `baseline-latency.csv` contains
pooled READ/UPDATE/ALL statistics with workload provenance. Mean and nearest-rank
p99 use the same sample semantics as master. Missing evidence invalidates a run.

## 4. Determinism and fault model

Replica IDs retain upstream master-registration order. Quorum aliases resolve
through full advertised endpoints, so physical co-location does not merge votes;
selected logical leader/quorum identities match master. Request streams depend
on configured aliases/seeds, not UUID client IDs or registration order.

Toxiproxy v2.12.0 shapes each stream direction with `ceil(directed RTT/2)` ms,
zero jitter and 125,000 KB/s by default. Advertised addresses retain real endpoints.
The original `-latency` injector is disabled when proxy routing is enabled.
Preload runs before listeners and outside consensus and measurement clocks.
Linux listener inspection releases peer dials only when every listener exists.

No request retry, replica restart, crash injection or durable recovery adaptation
is included. The `fault` CLI is rejected. Latency completion is upstream's
protocol-specific reply completion, not an independent proof of safety.

## 5. Known gaps and validation

Workload/preload/clocks/startup roles are aligned to the pinned master; the six
protocol packages still match upstream. Master includes later protocol fixes
and runtime changes, including SwiftPaxos reply/ack handling, so these two
branches are not yet a pure same-semantics architecture ablation. Do not attribute
all observed latency differences to modularity. Per-protocol logging and
completion behavior also remain source-specific. `optread` is disabled.

Native Windows builds retain upstream's Linux-only SIGUSR1 hook; build the
complete executable for Linux. The complete executable targets Linux.
All six protocols compile; the real-network validation currently targets SwiftPaxos.



See [validation.md](validation.md) for real-job evidence and its limits.
Historical native evidence remains tagged as native; it is not relabeled matched.
