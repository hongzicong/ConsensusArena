# ConsensusArena

ConsensusArena is an experimental framework for implementing and comparing
state-machine replication protocols. It provides shared master, replica,
client, workload, network, and result-analysis components for local and
multi-node experiments.

The repository includes the SwiftPaxos prototype presented at
[NSDI '24](https://www.usenix.org/conference/nsdi24/presentation/ryabinin)
and related protocols. It originated from the SwiftPaxos and
[Egalitarian Paxos](https://github.com/otrack/epaxos) codebases.

## Supported protocols

Protocol names are case-insensitive. Set the `protocol` configuration field
or pass `-protocol` to select one of the registered implementations.

| Configuration value | Implementation |
| --- | --- |
| `SwiftPaxos` | SwiftPaxos prototype from the NSDI '24 paper. |
| `Paxos` | Classic leader-based Paxos. |
| `CURP` | CURP with an independent N²Paxos-style all-to-all consensus extension. |
| `FastPaxos` | Any fast quorum of `floor(3n/4)+1` voters; majority classic recovery. |
| `EPaxos` | Corrected EPaxos implementation. |
| `Bodega` | Roster leases for local reads; see the [Bodega implementation notes](docs/bodega.md). |
| `KCensus` | KCensus prototype in [kcensus/](kcensus/). |

## Requirements

- Go 1.20 or newer for building.
- Linux for running the cross-compiled cluster binary.
- The system `ping` command on experiment nodes.
- Python 3, SSH/SCP, and Slurm for the Jed benchmark workflow.

The Linux cross-build uses `CGO_ENABLED=0` and produces a standalone executable;
no container runtime is required.

## Build

For a standalone checkout and native build:

```bash
git clone https://github.com/hongzicong/ConsensusArena.git
cd ConsensusArena
go build -trimpath -o consensusarena .
```

When using the Laputa checkout, initialize its submodules from the Laputa root
with `git submodule update --init --recursive`, then work in `ConsensusArena/`.

To cross-compile Linux x86-64 from Windows PowerShell:

```powershell
$artifact = powershell -NoProfile -ExecutionPolicy Bypass -File .\slurm\build-linux.ps1 | ConvertFrom-Json
$artifact | Format-List
```

The build helper creates `consensusarena-linux-amd64`, freezes a copy under
`results/binaries/<sha256>/`, and returns `FrozenPath` and `SHA256`. It restores
the previous Go environment variables after building. Use the frozen copy and
its hash for benchmarks.

## Run participants

A deployment has three participant roles:

| Role | Responsibility |
| --- | --- |
| Master | Register replicas and publish membership information. |
| Replica | Run the selected protocol and state machine. |
| Client | Generate requests and record completion latency. |

Prepare a deployment configuration with participant aliases, reachable
endpoints, protocol settings, and topology inputs. The benchmark parameter
template `slurm/workload.conf` needs generated membership before it can be used
as a deployment configuration.

On Linux, inspect startup placement before launching participants:

```bash
./consensusarena -run plan -config deployment.conf
```

Start the master, then the configured replicas, then clients in separate
processes. Replace the example replica/client aliases with aliases from the
same deployment configuration:

```bash
./consensusarena -run master -config deployment.conf -alias m0
./consensusarena -run replica -config deployment.conf -alias replica-name
./consensusarena -run client -config deployment.conf -alias client-name
```

When using the Windows cross-build, replace `./consensusarena` with
`./consensusarena-linux-amd64` on the Linux host.

| Option | Meaning |
| --- | --- |
| `-config` | Required deployment/workload configuration file. |
| `-run` | `master`, `replica` (`server` is an alias), `client`, or `plan` for placement inspection. |
| `-alias` | Participant alias from the configuration; not needed for `plan`. |
| `-protocol` | Override the configuration's protocol for this process. |
| `-log` | Application log path. |

To select a protocol in the deployment configuration:

```text
protocol: EPaxos
```

For a direct-run override, pass `-protocol epaxos` consistently to every
participant. For benchmark runs, use `CONSENSUSARENA_PROTOCOL=epaxos` as described
in the benchmarking skill; the launcher applies it to all participants.

## Development and benchmarks

| Topic | Documentation |
| --- | --- |
| Replica event interface and output ordering | [protocol/README.md](protocol/README.md) |
| New protocol package skeleton | [templates/README.md](templates/README.md) |
| Bodega settings, behavior, diagnostics, and limitations | [docs/bodega.md](docs/bodega.md) |

In a Laputa checkout, use the
[prototype development skill](../.agents/skills/develop-consensus-prototype/SKILL.md)
for implementation, review, repair, and ordinary runs. Use the
[prototype benchmarking skill](../.agents/skills/benchmark-consensus-prototype/SKILL.md)
for workload/network configuration, Jed deployment, latency and crash-recovery
experiments, result validation, archival, and plotting. These skill paths belong
to the parent Laputa repository.

## License

See [LICENSE](LICENSE).
