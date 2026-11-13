# Scalability and resources

Every replica ran with **`GOMAXPROCS=1` + taskset on one logical CPU**.
AWS-random, odd replica counts 3–31, workload B, 20,000 offered requests/s.
84/90 runs plotted: SwiftPaxos timed out at 21–27 replicas and hit startup OOM at 29/31; exclusions remain in the CSVs.

| Location | Contents |
| --- | --- |
| [data/scalability.csv](data/scalability.csv) | Latency, throughput, average/min/max replica resources and run status |
| [data/resources-by-replica.csv](data/resources-by-replica.csv) | Individual replica measurements |
| [data/operation-latency.csv](data/operation-latency.csv) | Read/update latency |
| [data/topologies.csv](data/topologies.csv) | Frozen region selection at each size |
| [data/agreement-vs-best.csv](data/agreement-vs-best.csv) | Agreement versus the best eligible alternative per size/metric |
| [figures/](figures/) | Mean/p99 latency, bytes, messages, CPU and linear-scale peak memory, PDF/PNG |
| [manifest.json](manifest.json) | Measurement definitions, provenance and raw archive references |

Replot: `python results/plot.py scalability` from ConsensusArena; add `--full-range` for uncapped traffic/message axes. [Comparison tables](../COMPARISONS.md).
