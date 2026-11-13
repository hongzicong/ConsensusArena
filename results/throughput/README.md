# Throughput

Every replica ran with **`GOMAXPROCS=1` + taskset on one logical CPU**.
Ring 7, workload B. All six adaptive searches completed: 110 selected points, 133 total attempts.

| Location | Contents |
| --- | --- |
| [data/points.csv](data/points.csv) | Search-selected offered-load, achieved-throughput and latency points |
| [data/attempts.csv](data/attempts.csv) | Every attempt, including retries and incomplete cohorts |
| [data/capacity.csv](data/capacity.csv) | Finite-window capacity brackets |
| [data/agreement-vs-best.csv](data/agreement-vs-best.csv) | Matched-load comparisons and capacity comparisons |
| [figures/](figures/) | Throughput and latency curves, PDF/PNG |
| [manifest.json](manifest.json) | Search rules, measurement definitions, provenance and raw archive references |

Replot: `python results/plot.py throughput` from ConsensusArena. [Comparison tables](../COMPARISONS.md).
