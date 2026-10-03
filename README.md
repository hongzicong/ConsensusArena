# ConsensusArena

ConsensusArena is an experimental framework for running and comparing
Paxos-family state-machine replication protocols. It provides a common master,
replica, client, workload, latency-injection, quorum, and result-analysis path
for local or multi-node experiments.

The repository includes the prototype implementation of the SwiftPaxos
protocol presented at [NSDI '24](https://www.usenix.org/conference/nsdi24/presentation/ryabinin),
alongside several related protocols. ConsensusArena originated from the SwiftPaxos
and [Egalitarian Paxos](https://github.com/otrack/epaxos) codebases.

## Implemented protocols

| Protocol | Notes |
| --- | --- |
| SwiftPaxos | Geo-replicated protocol described in the NSDI '24 paper. |
| Paxos | Classic leader-based Paxos. |
| CURP | CURP with an independent N²Paxos-style all-to-all consensus extension. |
| Fast Paxos | Fast Paxos with uncoordinated collision recovery. |
| EPaxos | Corrected EPaxos implementation. |
| Bodega | Roster leases for local reads; crash-stop core prototype. |

## Requirements

- Go 1.20 or newer for building.
- Linux for running the generated cluster binary.
- The system `ping` command on experiment nodes.
- Slurm commands when using the SCITAS deployment.

The project builds with `CGO_ENABLED=0`, so the Linux artifact is a standalone
executable and does not require a container runtime.

## Build

Clone and build for the current platform:

```bash
git clone https://github.com/hongzicong/ConsensusArena.git
cd ConsensusArena
go build -trimpath -o consensusarena .
```

To cross-compile the Linux x86-64 binary from Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\slurm\build-linux.ps1
```

This creates `consensusarena-linux-amd64` in the repository root and restores the
previous Go environment variables after the build.

## Architecture

An experiment contains three participant types:

- **Master:** registers replicas and publishes membership information.
- **Replica:** runs the selected consensus protocol and state machine.
- **Client:** generates requests and records completion latency.

Launch participants with a deployment configuration:

```bash
./consensusarena -run master -config deployment.conf -alias m0

./consensusarena -run replica -config deployment.conf \
  -alias replica-name

./consensusarena -run client -config deployment.conf \
  -alias client-name
```

Inspect the protocol's deployment plan without starting participants:

```bash
./consensusarena -run plan -config deployment.conf
```

Important command-line options:

| Option | Meaning |
| --- | --- |
| `-alias` | Participant alias from the deployment configuration. |
| `-config` | Deployment and workload configuration file. |
| `-log` | Application log path. |
| `-protocol` | Override the protocol selected in the configuration. |
| `-run` | Participant type: `master`, `replica`, or `client`; `plan` prints the deployment plan. |

## Select a protocol

ConsensusArena accepts these case-insensitive protocol values:

| Configuration value | Protocol |
| --- | --- |
| `SwiftPaxos` | SwiftPaxos |
| `CURP` | CURP |
| `FastPaxos` | Fast Paxos |
| `Paxos` | Classic Paxos |
| `EPaxos` | EPaxos |
| `Bodega` | Bodega |

For repeated runs, change the default in `slurm/workload.conf`:

```text
protocol: EPaxos
```

For a single Slurm run, leave the file unchanged and export an override:

```bash
sbatch --account=dcl \
  --export=ALL,CONSENSUSARENA_PROTOCOL=epaxos \
  slurm/run-latency.sbatch
```

The Slurm launcher writes the override into the generated `cluster.conf` used
by the master, all configured replicas, and all ten clients. Direct participant runs
can use `-protocol epaxos` on every process instead.

### Bodega

Select `protocol: Bodega` or `-protocol bodega`. Clients use the nearest connected
responder for each GET key and send PUT/SCAN directly to the known roster leader. Clients refresh
roster hints through a read-only replica control RPC once per second; stale or
temporarily unavailable leader hints retain follower forwarding as a fallback.
`BODEGA_CLIENT_ROUTES` records routing counts. Add optional settings
before `-- Proxy --`:

```text
bodegaResponders: all // or leader, or comma-separated replica aliases
bodegaLease: 2500ms
bodegaMargin: 100ms
bodegaHeartbeat: 120ms
bodegaFailure: 1200ms
bodegaFailureMax: 2400ms
bodegaUnhold: 250ms
```

These timing defaults match the authors' YCSB experiment script at Summerset
`16c6f352`. Failure detection samples a timeout uniformly from 1200–2400 ms
on each peer-timer refresh; checks reuse that deadline. Setting only
`bodegaFailure` retains the legacy fixed timeout; set `bodegaFailureMax` as well
to configure a range. Failure deadlines do not renew leases. `BODEGA_TIMING`
logs effective timing values at startup. Existing archived benchmark results
predate this timing change and require remeasurement to describe these defaults.
Optional `bodegaResponderRanges: 0..999=r1,r3;1000..1999=r2;2000=leader`
overrides the default mask on inclusive, nonoverlapping key ranges (or a single key).
Unlisted keys use `bodegaResponders`; the current leader is always included.
Writes use 1 ms batches and require a majority plus the union of responders for
all written keys in the batch. `bodegaUnhold` is now a client GET timer: retry the
same ID once at the leader and deliver the first reply, without forcing a logged
read. `BODEGA_CLIENT_HEDGES` reports sends/wins/duplicates/cancellations;
`BODEGA_CLIENT_DESTINATIONS` counts initial reads. `Batches`, `BatchCommands`,
`MaxBatchCommands` and `BODEGA_WIRE` expose batch sizes and encoded send attempts
by message kind. Non-commit messages use versioned typed binary frames; only
heartbeats carry full rosters. Supports odd fixed
memberships of 3–63 and `noop: false`.
Compact CommitNotice frames carry commit positions, not values; lost Accept data
is repaired in bounded windows. `CommitNotices`, `CommitRepairRequests` and
`CommitRepairEntries` expose this path. Upgrade all replicas and clients together for the new wire/control format.
The port uses acknowledged lease revocation with expiry fallback and suffix-only
Prepare recovery, following Summerset's `bodega-artifact` commit `16c6f352`.
Promise chunks advance when the network writer frees queue space, independently
of heartbeat retry intervals. Log GC waits for execution by every fixed member;
the live state machine and retained deduplication results are the in-memory snapshot.
This port uses Bodega's commit-notification read path with the optional early
AcceptNote optimization disabled (paper Section 3.2 and Figure 7). A stable leader
reads the latest committed value for the key, without waiting for a later,
uncommitted write. Other responders hold reads of an accepted write until commit
evidence arrives. Out-of-order committed values use the same lookup at leader
and followers, so an execution gap cannot make the leader return an older value.
Write commitment still requires a majority plus every responder for written keys.

The disabled optimization matters: with delayed delivery to one leased responder,
a majority of AcceptNotes can expose a new value before that responder learns the
write; a subsequent read there can still return the old value. The deterministic
regression tests cover that schedule. This is evidence against directly enabling
the optimization in this port, not a complete audit of the authors' system.
The port retains its fixed-prefix and fresh-entry checks for deduplicated
out-of-order reads; recovered entries wait for execution. These are explicit
conservative differences from the artifact. Durable WAL/checkpoint restart and
automatic responder tuning remain unimplemented. Bounded clock-rate drift is
required, and a crashed member can stop all-member log GC.

`BODEGA_STATS` and `BODEGA_ROSTER` record counters and installed rosters.
`StableLeaderReads` counts leader-local completions; `UncommittedReadHolds` counts
reads held behind uncommitted writes; `OutOfOrderReads` counts reads of committed
slots before prefix execution. `PrepareEntries`/`PrepareNanos`, `Revokes`/
`RevokeAcks`, and `CompactedSlots` expose recovery and GC costs. Wire version 3
retains the reserved AcceptNote kind but ignores its read evidence. All replicas
must use this read policy together; mixing it with pre-commit readers is unsafe.
Bodega establishes peer connections concurrently; roster installation restarts
peer failure timers without renewing leases.
Bodega fallback read routing uses proxied control-RPC duration, not ICMP;
`BODEGA_CLIENT_RTT` / `BODEGA_CLIENT_DESTINATIONS` and
`BODEGA_TIMER_RESET` / `BODEGA_ROSTER_FILTER` expose routing and timer changes.
Test with `go test -timeout 7200s ./bodega`; run networked fault tests with
`CONSENSUSARENA_PROTOCOL=bodega` and `slurm/run-fault.sbatch`.

## Workload and network model

The SCITAS experiment uses these configuration files:

- `slurm/workload.conf`: five replicas, ten regional clients, protocol and
  workload parameters.
- `slurm/cloudping-1y-p50.csv`: all 35 CloudPing regions and 1,225 directed RTT values, using the website's 1 Year / P50 selection. The raw JSON and fetch metadata are stored beside it.
- `slurm/topologies.json`: three fixed replica layouts, with the same ten client regions. Replica counts 5/9/13 use prefixes of each layout.
- `latency.conf`: the generated 5-replica topology-1 RTT matrix; the harness generates a separate matrix for every size and topology.

Each protocol's `plan.go` selects its initial leader, quorums, responders, or
client ingress from the topology and workload at startup. All participants use
the same deployment configuration and topology inputs; no generated `quorum.conf`
or `leader.conf` is required. The topology matrix defaults to `latency.conf`
beside the deployment configuration, or can be set with `topology: path` relative
to that configuration. Placement planning does not inject network delay;
Toxiproxy applies the latency matrix separately in experiments. Use `python slurm/topology.py inspect --replicas 5 --topology 1` to inspect membership; `-run plan` inspects each protocol's placement choices.

The workload is open-loop. Each logical client generates requests according to
a Poisson process. Keys are selected with a Zipfian distribution.

Default workload values:

```text
writes: 50
commandSize: 1000
clones: 0
arrivalRate: 2000
warmup: 5s
duration: 10s
repetitions: 1
keyCount: 1000000
zipfSkew: 0.9
workloadSeed: 1
preload: true
preloadSeed: 1
```

`writes` is the percentage of generated commands that are writes. Direct runs
use the base value above. By default, `slurm/run-latency.sbatch` runs these
YCSB read/write ratio profiles:

| Profile | Reads | Writes | ConsensusArena mapping |
| --- | ---: | ---: | --- |
| A | 50% | 50% | READ to GET, UPDATE to PUT |
| B | 95% | 5% | READ to GET, UPDATE to PUT |
| C | 100% | 0% | READ to GET |

These are ratio-only profiles rather than complete YCSB semantics. In
particular, they use ConsensusArena's blob values instead of field-oriented
records.

Every profile runs once on each of three fixed topologies. Each run generates warm-up traffic
for 5 seconds without recording latency, records requests generated during the
following 10 seconds, and then waits for all in-flight replies. The Slurm job
restarts the master, replicas, and clients before every run. All protocols use the same topology catalog and frozen CloudPing snapshot; client regions, workload seed and offered load remain fixed. Results carry `topology_id`, `repetition=1` and topology/snapshot SHA256 values. Cross-topology ranges are not repeated-run confidence intervals. Historical measurements retain their original repetition labels.

`workloadSeed` makes request generation reproducible across protocol runs. Each
logical client derives a stable stream seed from the base seed, its configured
client alias, and its clone index. Request selection and value generation use
independent random streams, so generating a new value for every UPDATE does not
change the key, operation, or arrival sequence. Every UPDATE value contains its
per-client version, stream seed, and key before the generated payload bytes.

When `preload` is enabled, every replica constructs the same deterministic
genesis state before accepting clients. The state contains `keyCount` records
with keys from `0` through `keyCount - 1`; every value is `commandSize` bytes and
is derived from the key and `preloadSeed`. Preloading is outside the consensus
log and outside the warm-up and measurement windows. The Slurm launcher extracts
the reported SHA-256 state digest from all five replica logs and starts clients
only after all digests match. With the defaults, the raw value data is about
1 GB per replica, excluding tree and runtime overhead.

To run a subset or a different order, pass a colon-separated profile override:

```bash
sbatch --account=dcl \
  --export=ALL,CONSENSUSARENA_YCSB_PROFILES=A:C \
  slurm/run-latency.sbatch
```

The Slurm launcher discovers the physical node addresses and rewrites the
logical endpoints before starting the processes. Replica ports `7070` through
`7074` are network listeners. Client ports `17000` through `17009` are logical
latency identities only.

## SCITAS Jed experiment

The latency job sequentially tests 5, 9 and 13 replicas on three Jed CPU nodes;
use `CONSENSUSARENA_REPLICAS=5:9:13` to select sizes:

- Up to 24 Slurm tasks: thirteen replicas, ten clients, and one master.
- Eight tasks per node.
- Eight CPU cores and 1 GiB per allocated core for each task.
- 192 allocated CPU cores in total.
- `standard` partition and `parallel` QOS.

### Upload the runtime files

Connect to the EPFL VPN when outside the EPFL network. From the repository
directory in Windows PowerShell:

```powershell
ssh zihong@jed.hpc.epfl.ch "mkdir -p ~/ConsensusArena"
scp consensusarena-linux-amd64 latency.conf zihong@jed.hpc.epfl.ch:~/ConsensusArena/
scp -r slurm zihong@jed.hpc.epfl.ch:~/ConsensusArena/
ssh zihong@jed.hpc.epfl.ch
```

Prepare and verify the binary on Jed:

```bash
cd ~/ConsensusArena
chmod +x consensusarena-linux-amd64
file consensusarena-linux-amd64
command -v ping
```

The `file` output must identify an x86-64 Linux executable, and the `ping`
check must return a path.

### Submit and monitor

```bash
cd ~/ConsensusArena
sbatch --account=dcl slurm/run-latency.sbatch
```

Submit from the repository root as shown above. The launcher uses Slurm's
`SLURM_SUBMIT_DIR` to locate the binary, configuration, and helper scripts. If
submitting from another directory, set `CONSENSUSARENA_ROOT=$HOME/ConsensusArena`.

Monitor or cancel the job:

```bash
squeue --me
tail -f slurm-JOB_ID.out
scancel JOB_ID
```

Slurm removes all experiment processes when the allocation ends. The cost
estimator uses the requested one-hour wall-time; billing uses the resources
for the actual allocation duration.

### Results

The job prints a result directory such as:

```text
/scratch/zihong/consensusarena-JOB_ID
```

Important output paths:

| Path | Contents |
| --- | --- |
| `summary.csv` | Per-topology summaries keyed by replica count, topology ID, protocol, profile, region and operation. |
| `baseline-latency.csv` | Portable overall latency rows with explicit topology IDs and SHA256 provenance; merge protocol outputs for the report. |
| `repetition-summaries.csv` | Combined per-repetition rows with separate `READ`, `UPDATE`, and `ALL` operation values. |
| `replicas-N/topology-T/ycsb-X/summary.csv` | One-run per-region and per-operation summary for topology `T`. |
| `replicas-N/topology-T/ycsb-X/repetition-summaries.csv` | All per-repetition, per-operation rows for profile `X`. |
| `replicas-N/topology-T/ycsb-X/repetition-01/results/summary.csv` | Per-region READ, UPDATE, and ALL latency statistics for the topology's single run. |
| `replicas-N/topology-T/ycsb-X/repetition-01/results/` | Raw measured client latency logs; warm-up latency is excluded. |
| `replicas-N/topology-T/ycsb-X/repetition-01/stdout/` | Master, replica, and client process output. |
| `replicas-N/topology-T/ycsb-X/repetition-01/logs/` | Application logs. |
| `replicas-N/topology-T/ycsb-X/repetition-01/metadata.txt` | Topology/snapshot provenance, profile, write ratio, preload status, and verified preload digest. |
| `config/` | Generated physical-address configuration and latency matrix. |
| `metadata.txt` | Job ID, timestamps, binary path, and repository path. |

Preserve important results because `/scratch` is temporary:

```bash
mkdir -p ~/consensusarena-results
cp -a /scratch/zihong/consensusarena-JOB_ID ~/consensusarena-results/
```

To override the binary, result directory, YCSB profiles, workload seed, or client timeout:

```bash
sbatch --account=dcl \
  --export=ALL,CONSENSUSARENA_BINARY=$HOME/bin/consensusarena,CONSENSUSARENA_RUN_DIR=/scratch/zihong/custom-run,CONSENSUSARENA_YCSB_PROFILES=A:B:C,CONSENSUSARENA_WORKLOAD_SEED=1,CLIENT_TIMEOUT_SECONDS=300 \
  slurm/run-latency.sbatch
```

## Crash recovery experiment

`slurm/run-fault.sbatch` runs for 40 seconds in total, including 5 seconds of
warmup. All times are from workload start: warmup at 0–5 seconds, kill the
protocol-specific target at 5 seconds, then kill additional replicas at
20 seconds to reach a cumulative `f = (n - 1) / 2` crashes. End at 40 seconds.
The post-warmup measurement window is 5–40 seconds (35 seconds); the generated
client configuration therefore uses `warmup: 5s` and `duration: 35s`.

Rebuild the executable before running this schedule so client request cohorts
use the matching `warmup`, `crashed`, and `maximum_crashes` phases and timestamps
relative to workload start. The summarizer validates this timeline and also
accepts historical runs with timestamps relative to measurement start.

Existing recovery figures describe historical experiments and remain unchanged.
After replacing their data with new 40-second measurements, including the
`timeline_origin`, `warmup_s`, `observation_s`, `crash_s`, `max_crash_s`, and
`measurement_s` columns exported by the summarizer,
render with
`python report/plot_recovery.py --baselines-only` from the parent repository. The plotter does
not relabel or truncate historical data. Existing XPaxos figures are not
redrawn by this command. New plots show 0–40 seconds from workload start,
with the 0–5-second warmup shaded. This pre-crash interval supplies the recovery
reference; it must pass the existing stability check before recovery can be claimed.

### Ordered-log recovery and lagging replicas

CURP's consensus extension and Paxos each own their ordered-log core,
message codec, event loop, and output queues in their package. See
`BASELINE_LAYOUT.md` for the common file organization. An acceptor
may accept a current-ballot slot even when earlier slots are missing. Its
vote records that slot's value; execution and client completion still wait
for a contiguous committed prefix. This lets the surviving majority finish
leader recovery while a lagging member repairs older, already chosen slots.
Phase-one value selection, ballot checks, distinct-voter majorities, and
the leader's recovery-completion boundary are unchanged.

Missing-prefix notifications and leader heartbeats share a 100 ms Fetch
retry interval, so incoming accepts do not each trigger a duplicate suffix
transfer. The existing five-second `BASELINE_RECOVERY` status includes
`gap_accepts`, `fetch_requests`, `fetch_suppressed`, and `recovery_end`.
Compare these with `accepted`, `executed`, `active`, and `send_drops` to
distinguish background catch-up from blocked leader recovery. These counters
add constant-time updates; they do not log every message.

The runtime split was checked against the previous implementation using 5/9/13
replicas, leader replacement with a pending request, cumulative 2/4/6 failures,
continued service, duplicate evidence, and stale ballots. Codec cross-decoding
also passed. Temporary checks were removed after validation. These remain
in-memory crash-stop prototypes; the checks do not establish durable
crash-restart recovery or measure WAN performance.

## License

See `LICENSE`.
