# SwiftPaxos upstream source-style Slurm baseline

Branch: `codex/swiftpaxos-slurm` in the ConsensusArena Git repository.
Source: `imdea-software/swiftpaxos` at
`35c69365f1c7737a08e237bfbaf828ee68897080`.
The upstream Go module name, `main.go`/`run.go` protocol switches, protocol
packages, wire messages, state-machine semantics, and event loops are retained.
`upstream-provenance.json` records the upstream source and shared experiment assets.

## 1. Implementation scope

This branch adds Linux cross-build, immutable artifact hashes, serial Slurm
latency scenes, the frozen ConsensusArena topology/CloudPing inputs, Toxiproxy
TCP shaping, original-client result validation, and process resource records.
All six original protocols compile; the initial network validation targets
SwiftPaxos. Bodega and KCensus are absent, matching upstream.

The complete scene selection is 5/9/13 replicas, A/B/C, three topologies, and
one run per topology. Subsets are supported for smoke tests. Allocations use
three exclusive nodes, 24 tasks, eight CPUs/task, 1 GiB/CPU, `standard` partition,
`parallel` QOS, `dcl` account, and a two-hour limit. Like the current latency
harness, individual scenes use the first two allocated nodes for five replicas
and three for nine/thirteen, with fresh processes for each scene.

This is an **upstream request-count workload baseline**. The original client
uses one unmeasured bootstrap request, then `UPSTREAM_REQUESTS` measured requests
per client. It keeps upstream random seeding, conflict-key selection, reply
handling, payload reuse, and pipeline scheduling. A/B/C set upstream's `writes`
knob to 50/5/0; the realized sampler remains upstream-defined. There is no
Poisson arrival rate, Zipf workload, deterministic preload, five-second warmup,
or fixed-duration recovery experiment. `operation=ALL` is the available latency
classification because upstream logs do not label READ/UPDATE per sample.
Results must not be merged into matched-workload ConsensusArena promotion data.

## 2. Mapping to upstream behavior

| Adaptation | Files and boundary |
| --- | --- |
| Load an optional outgoing endpoint map; reject double delay injection | `main.go`, new `replica/defs/network.go` |
| Dial Toxiproxy while keeping advertised addresses and voter identities | `client/client.go`, `replica/replica.go` |
| Register a replica's host and port separately when configuration uses endpoints | `run.go` |
| Resolve configured quorum aliases by endpoint, distinguishing co-located replicas; preserve bare-host compatibility | `replica/quorum.go` |
| Generate original deployment/quorum/proxy files | `slurm/upstream_harness.py` |
| Start participants and collect original logs | `slurm/node-runner.sh`, `slurm/benchmark-suite.py` |

The six protocol packages are byte-identical to the pin after LF normalization.
No `protocol.Machine`, protocol registry, per-protocol module split, or skeleton
generator from modular ConsensusArena is imported. Registration-order replica
IDs are retained and resolved through actual advertised endpoints; rank is not
treated as a voter ID.

The external config generator chooses the first catalog replica as initial
leader and the first majority as the configured SwiftPaxos fast quorum. This
uses upstream's fixed-majority configuration, including the leader. Client
ingress is selected by the frozen RTT matrix. All choices are recorded in
metadata; current ConsensusArena placement policy can differ, so placement
must also be aligned before a pure architecture-overhead comparison.

Wiki grounding inspected for the adaptation boundary:
`ConsensusWiki/wiki/protocols/SwiftPaxos.md` (roles, fast path), and
`ConsensusWiki/wiki/papers/SwiftPaxos-2024.md` (quorum system, artifact mapping).
These confirm that fixed-majority fast quorums are an existing configuration;
this branch proposes no new protocol or safety claim.

## 3. Runtime and operating workflow

Use Windows PowerShell locally and non-interactive SSH for Jed. Connect to the
EPFL VPN when required. Linux is required by upstream's signal handling; a
native Windows build of the full upstream program is unsupported.

From this checkout:

```powershell
python slurm/verify-upstream.py
python -m unittest discover -s slurm -p 'test_*.py' -v
go test -timeout 7200s ./client ./replica/...
$artifact = powershell -NoProfile -ExecutionPolicy Bypass -File .\slurm\build-linux.ps1 | ConvertFrom-Json
$toxi = powershell -NoProfile -ExecutionPolicy Bypass -File .\slurm\fetch-toxiproxy.ps1 | ConvertFrom-Json
```

Use an isolated remote root so this branch does not overwrite modular
ConsensusArena's runtime scripts or data:

```powershell
$remoteRoot = '/home/zihong/ConsensusArena-upstream-slurm'
$remoteBinary = "$remoteRoot/binaries/$($artifact.SHA256)/consensusarena-linux-amd64"
$remoteToxi = "$remoteRoot/toxiproxy-server-linux-amd64"
ssh zihong@jed.hpc.epfl.ch "mkdir -p '$remoteRoot/binaries/$($artifact.SHA256)'"
scp $artifact.FrozenPath "zihong@jed.hpc.epfl.ch:$remoteBinary"
scp $toxi.Path "zihong@jed.hpc.epfl.ch:$remoteToxi"
scp -r slurm "zihong@jed.hpc.epfl.ch:$remoteRoot/"
scp upstream-provenance.json "zihong@jed.hpc.epfl.ch:$remoteRoot/"
ssh zihong@jed.hpc.epfl.ch "chmod +x '$remoteBinary' '$remoteToxi'"
$exports = "ALL,CONSENSUSARENA_BINARY=$remoteBinary,CONSENSUSARENA_BINARY_SHA256=$($artifact.SHA256),CONSENSUSARENA_TOXIPROXY_SERVER=$remoteToxi,CONSENSUSARENA_TOXIPROXY_SHA256=$($toxi.SHA256),CONSENSUSARENA_PROTOCOL=swiftpaxos"
```

Inspect the default 27-scene matrix locally with
`python slurm/benchmark-suite.py plan latency`. For one five-replica smoke scene:

```powershell
$smokeExports = "$exports,CONSENSUSARENA_REPLICAS=5,CONSENSUSARENA_CASES=5-1,CONSENSUSARENA_YCSB_PROFILES=A,UPSTREAM_REQUESTS=32"
ssh zihong@jed.hpc.epfl.ch "cd '$remoteRoot' && python3 slurm/benchmark-suite.py submit latency --account dcl --export='$smokeExports'"
```

Omit subset exports for the complete matrix. Native workload exports are
`UPSTREAM_REQUESTS=1000`, `UPSTREAM_COMMAND_SIZE=1000`, `UPSTREAM_CONFLICTS=2`,
and `UPSTREAM_PENDING=128` by default. All runs use ten clients, `clones: 0`,
`pipeline: true`, and `optread: false`. Optional `CONSENSUSARENA_RUN_DIR` must
name a new directory; `CLIENT_TIMEOUT_SECONDS` defaults to 300 and replica
readiness timeout to 900. `UPSTREAM_ADAPTER_REVISION` records the tested adapter
revision. Duplicate submission export keys are rejected.

Record the printed job ID and run directory. Monitor with bounded
`ssh ... "squeue -j JOB_ID"` and `ssh ... "tail -n 100 .../slurm-JOB_ID.out"`.
Each scene has raw client logs, process logs, stdout, GNU-time CPU/RSS/context
switch records, generated configuration/quorum/network inputs, binary hashes,
proxy settings, source pin, and explicit workload/placement metadata.

`summary.csv` contains client-region statistics; `baseline-latency.csv` contains
pooled overall mean and nearest-rank p99, explicitly tagged
`workload_kind=upstream-request-count`. This portable file is not a claim of
compatibility with the current Poisson baseline's measurement semantics.
The causal engineering hypothesis is that this source-style baseline requires
more integration edits for a new protocol. Latency/resource records permit
later overhead comparisons once semantics and workload are matched. Native
per-request verbose logging is preserved and is material measurement overhead.

Recollect with `python3 slurm/benchmark-suite.py collect latency RUN_DIRECTORY`.
Inspect `suite-status.json`, per-scene `validation.json`, and `outcome.json`.
A valid run requires every client to exit successfully, emit exactly the
configured measured sample count, and report one positive elapsed duration.
Missing, malformed, failed, or timed-out runs remain incomplete/invalid;
no missing samples are filled with zero. Preserve `/scratch` results with
`cp -a RUN_DIRECTORY ~/consensusarena-results/` over SSH and retain frozen
binaries separately. This branch does not update XPaxos promotion plots.

## 4. Determinism and fault model

Topology, endpoints, configured quorum, and ingress are reproducible and logged.
Request streams and client identities retain upstream randomness. TCP proxies
shape both stream directions with `ceil(RTT/2)` latency, zero jitter by default,
and 125000 KB/s bandwidth per connection/direction. Listeners and registered
identities keep physical endpoints. Upstream's `-latency` injector is disabled
when a dial map is configured to prevent double injection.

The runner performs no request retries, replica restarts, or crash injections.
Upstream protocol recovery logic is unchanged. There is no adapted fault
suite, durable crash-restart claim, or promotion/recovery guarantee; asking
the CLI for `fault` is rejected rather than silently substituting a latency run.

## 5. Known gaps and validation

Only SwiftPaxos is initially exercised end-to-end. Other upstream protocols
retain their original behavior and limitations. Optread/local-client modes are
not exposed by the harness because upstream identifies proxied clients from
source IP, which Toxiproxy changes. The branch is a source-style reference;
same-semantics architecture ablation and matched Poisson workloads are separate
follow-up experiments.

Local verification covers endpoint routing, duplicate-map rejection, quorum
membership with co-located/shuffled replica identities, bare-host compatibility,
scene selection, result accounting, incomplete-run rejection, Bash syntax,
source preservation, and Linux cross-compilation. Network smoke evidence is
recorded separately in `validation.md` after the Jed run.
