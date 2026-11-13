# Regular evaluation

Every replica ran with **`GOMAXPROCS=1` + taskset on one logical CPU**.
Six deployments, workloads A (50% writes) / B (5% writes), 20,000 offered requests/s. All 72 runs validated and fully drained.

| Location | Contents |
| --- | --- |
| [data/latency.csv](data/latency.csv) | Overall latency, achieved throughput and run status |
| [data/operation-latency.csv](data/operation-latency.csv) | Read/update latency |
| [data/topologies.csv](data/topologies.csv) | Replica counts and regions |
| [data/agreement-vs-best.csv](data/agreement-vs-best.csv) | Agreement versus the best eligible alternative per configuration/metric |
| [figures/](figures/) | Mean and p99 latency, PDF/PNG |
| [manifest.json](manifest.json) | Measurement definitions, provenance and raw archive references |

Replot: `python results/plot.py regular` from ConsensusArena. [Comparison tables](../COMPARISONS.md).
