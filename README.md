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
| N²Paxos | All-to-all Paxos variant. |
| CURP | CURP implemented over N²Paxos. |
| Fast Paxos | Fast Paxos with uncoordinated collision recovery. |
| EPaxos | Corrected EPaxos implementation. |

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
  -latency latency.conf -quorum quorum.conf -alias replica-name

./consensusarena -run client -config deployment.conf \
  -latency latency.conf -alias client-name
```

Important command-line options:

| Option | Meaning |
| --- | --- |
| `-alias` | Participant alias from the deployment configuration. |
| `-config` | Deployment and workload configuration file. |
| `-latency` | Artificial network-latency matrix. |
| `-log` | Application log path. |
| `-protocol` | Override the protocol selected in the configuration. |
| `-quorum` | Custom quorum configuration. |
| `-run` | Participant type: `master`, `replica`, or `client`. |

## Select a protocol

ConsensusArena accepts these case-insensitive protocol values:

| Configuration value | Protocol |
| --- | --- |
| `SwiftPaxos` | SwiftPaxos |
| `CURP` | CURP |
| `FastPaxos` | Fast Paxos |
| `N2Paxos` | N²Paxos |
| `Paxos` | Classic Paxos |
| `EPaxos` | EPaxos |

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
by the master, all five replicas, and all ten clients. Direct participant runs
can use `-protocol epaxos` on every process instead.

## Workload and network model

The SCITAS experiment uses these configuration files:

- `slurm/workload.conf`: five replicas, ten regional clients, protocol and
  workload parameters.
- `latency.conf`: a 15-endpoint, 225-entry round-trip latency matrix.
- `quorum.conf`: the custom C2 quorum definition.

The workload is open-loop. Each logical client generates requests according to
a Poisson process. Keys are selected with a Zipfian distribution.

Default workload values:

```text
writes: 50
commandSize: 1000
clones: 0
arrivalRate: 4000
warmup: 10s
duration: 20s
repetitions: 3
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

Every profile runs three repetitions. Each repetition generates warm-up traffic
for 10 seconds without recording latency, records requests generated during the
following 20 seconds, and then waits for all in-flight replies. The Slurm job
restarts the master, replicas, and clients before every repetition.

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

The provided Slurm job runs a native Linux binary on two Jed CPU nodes:

- 16 Slurm tasks: five replicas, ten clients, and one master.
- Eight tasks per node.
- Eight CPU cores and 1 GiB per allocated core for each task.
- 128 allocated CPU cores in total.
- `standard` partition and `parallel` QOS.

### Upload the runtime files

Connect to the EPFL VPN when outside the EPFL network. From the repository
directory in Windows PowerShell:

```powershell
ssh zihong@jed.hpc.epfl.ch "mkdir -p ~/ConsensusArena"
scp consensusarena-linux-amd64 latency.conf quorum.conf zihong@jed.hpc.epfl.ch:~/ConsensusArena/
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
estimator uses the requested 30-minute wall-time; billing uses the resources
for the actual allocation duration. Five repetitions normally finish well
before that limit.

### Results

The job prints a result directory such as:

```text
/scratch/zihong/consensusarena-JOB_ID
```

Important output paths:

| Path | Contents |
| --- | --- |
| `summary.csv` | Combined averages and sample standard deviations, keyed by YCSB profile, read/write ratio, region, and operation. |
| `repetition-summaries.csv` | Combined per-repetition rows with separate `READ`, `UPDATE`, and `ALL` operation values. |
| `ycsb-X/summary.csv` | Three-run per-region, per-operation averages and sample standard deviations for profile `X`. |
| `ycsb-X/repetition-summaries.csv` | All per-repetition, per-operation rows for profile `X`. |
| `ycsb-X/repetition-XX/results/summary.csv` | Per-region READ, UPDATE, and ALL latency statistics for one repetition. |
| `ycsb-X/repetition-XX/results/` | Raw measured client latency logs; warm-up latency is excluded. |
| `ycsb-X/repetition-XX/stdout/` | Master, replica, and client process output. |
| `ycsb-X/repetition-XX/logs/` | Application logs. |
| `ycsb-X/repetition-XX/metadata.txt` | Profile, write ratio, preload status, and verified preload digest. |
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

## License

See `LICENSE`.
