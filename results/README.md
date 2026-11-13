# Paper results

Initial campaign only: 2026-11-12. Every replica ran with **`GOMAXPROCS=1` + taskset on one logical CPU**.

| Experiment | Data and figures |
| --- | --- |
| [Regular](regular/README.md) | Six deployments, workloads A/B; mean and p99 latency |
| [Scalability](scalability/README.md) | AWS-random, 3–31 replicas, workload B; latency and resources |
| [Throughput](throughput/README.md) | Ring 7, workload B; offered load, achieved throughput and latency |

**[Agreement versus the best alternative](COMPARISONS.md)** has the numbers and percentage differences. Each experiment also has a filterable `data/agreement-vs-best.csv`.

Inside each experiment: `data/` contains CSVs, `figures/` contains PDF/PNG plots, and `manifest.json` records measurement definitions, provenance and raw archive locations.

From ConsensusArena, using Python 3.11+:

```sh
python -m pip install -r results/requirements.txt
python results/plot.py
python results/compare.py
```

Both scripts calculate from CSVs. Plot one experiment with `python results/plot.py regular` (or `scalability` / `throughput`); add `--full-range` to remove traffic/message axis caps.
