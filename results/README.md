# Paper evaluation results

This folder is the entry point for the three evaluations intended for the paper.

| Experiment | What it establishes | Current status |
| --- | --- | --- |
| [Regular evaluation](regular/README.md) | Mean and p99 latency for six deployments and workloads A/B, each topology shown separately | Final single-CPU results pending |
| [Scalability and resources](scalability/README.md) | Latency, throughput, traffic, messages, CPU and peak memory as replicas increase | Final single-CPU results pending |
| [Throughput](throughput/README.md) | Achieved throughput and latency as offered load rises toward saturation | Final single-CPU results pending |

The final campaign will run every replica with `GOMAXPROCS=1` and `taskset` on
one logical CPU. Each participant task reserves two CPU slots; a replica's
proxy and supervisor share its other slot. Clients and master each have two
CPU slots. Historical measurements use different CPU settings and protocol
revisions; they are references, not the final paper dataset. No final data or
figures have been published here yet.

Each experiment will contain its reviewed CSV data in `data/`, the code and
dependencies to reproduce its figures in `plotting/`, and the paper figures in
`figures/`. Its `manifest.json` will identify the executable, input configurations,
measurement policy, validation and raw archives. Record the source commit,
Go version, CPU/memory allocation, workload seeds and configuration checksums
alongside the binary hash. Raw logs, individual latency observations and
throughput snapshots will be preserved separately and referenced by checksum.

CSV rows must retain their protocol, topology, replica count, workload and run
identity. Keep missing statistics blank. The plotting code must work from these
portable tables locally, without SSH or machine-specific paths, and reproduce
both vector figures and PNG previews.

The experiment READMEs describe their metrics and inclusion rules. Historical
reports remain under [Laputa-main/report](../../report/).

Build products, caches, downloaded tools, development checks and old launch
scripts belong in the ignored `.local-artifacts/` directory beside this folder.
The previous contents were preserved there with a move manifest and file hashes
under `archive/20261112-results-cleanup/`.

Agreement hot/cold ablations are deferred unless the paper needs them to answer
a specific question. They are outside the three main evaluations.
