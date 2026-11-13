# Agreement versus the best alternative

Best means the lowest latency/resource cost or highest throughput among eligible alternatives. **Positive gain favors Agreement**: lower-is-better `(best − Agreement) / best × 100`; higher-is-better `(Agreement − best) / best × 100`. Rankings use unrounded CSV values.

The tables below show overall latency, resources and capacity. Filter the experiment CSVs for read/update latency, achieved throughput and comparisons at exactly matching offered loads. No averaging across topologies or interpolation between loads.

[Regular CSV](regular/data/agreement-vs-best.csv) · [Scalability CSV](scalability/data/agreement-vs-best.csv) · [Throughput CSV](throughput/data/agreement-vs-best.csv)

Only eligible measurements compete. `Alternatives` shows coverage out of five; exclusions are listed in the CSV. A blank gain means no valid comparison. The CSV advantage factor is above one when Agreement is better. Capacity refers to the finite-window search, not a matched-load latency comparison.

## Regular latency (ms)

| Deployment | Workload | Metric | Agreement | Best alternative | Alternative value | Agreement gain |
| --- | --- | --- | --- | --- | --- | --- |
| ring-7 | A | Mean latency | 128.296 | SwiftPaxos | 150.518 | +14.76% |
| ring-7 | A | p99 latency | 244.846 | SwiftPaxos | 219.975 | -11.31% |
| ring-7 | B | Mean latency | 32.028 | Bodega | 84.549 | +62.12% |
| ring-7 | B | p99 latency | 233.588 | SwiftPaxos | 219.532 | -6.40% |
| europe-7 | A | Mean latency | 15.391 | SwiftPaxos | 18.62 | +17.34% |
| europe-7 | A | p99 latency | 31.523 | Paxos | 34.32 | +8.15% |
| europe-7 | B | Mean latency | 3.091 | Bodega | 6.233 | +50.41% |
| europe-7 | B | p99 latency | 35.565 | KCensus | 32.018 | -11.08% |
| east-asia-7 | A | Mean latency | 41.029 | SwiftPaxos | 49.991 | +17.93% |
| east-asia-7 | A | p99 latency | 85.278 | SwiftPaxos | 75.47 | -13.00% |
| east-asia-7 | B | Mean latency | 9.622 | Bodega | 20.936 | +54.04% |
| east-asia-7 | B | p99 latency | 88.267 | SwiftPaxos | 76.134 | -15.94% |
| north-america-7 | A | Mean latency | 43.673 | SwiftPaxos | 53.853 | +18.90% |
| north-america-7 | A | p99 latency | 86.678 | SwiftPaxos | 78.332 | -10.66% |
| north-america-7 | B | Mean latency | 9.037 | Bodega | 18.774 | +51.86% |
| north-america-7 | B | p99 latency | 84.951 | SwiftPaxos | 78.025 | -8.88% |
| north-america-3 | A | Mean latency | 31.243 | SwiftPaxos | 34.816 | +10.26% |
| north-america-3 | A | p99 latency | 76.963 | SwiftPaxos | 66.501 | -15.73% |
| north-america-3 | B | Mean latency | 7.677 | Bodega | 16.135 | +52.42% |
| north-america-3 | B | p99 latency | 75.732 | SwiftPaxos | 64.915 | -16.66% |
| north-america-5 | A | Mean latency | 39.124 | SwiftPaxos | 39.924 | +2.00% |
| north-america-5 | A | p99 latency | 86.54 | SwiftPaxos | 73.39 | -17.92% |
| north-america-5 | B | Mean latency | 8.364 | Bodega | 20.189 | +58.57% |
| north-america-5 | B | p99 latency | 80.645 | SwiftPaxos | 73.367 | -9.92% |

## Scalability (AWS-random, workload B)

### Mean latency (ms)

| Replicas | Agreement | Best alternative | Alternative value | Agreement gain | Alternatives |
| --- | --- | --- | --- | --- | --- |
| 3 | 36.917 | Bodega | 57.035 | +35.27% | 5/5 |
| 5 | 36.915 | Bodega | 79.74 | +53.71% | 5/5 |
| 7 | 36.639 | Bodega | 93.374 | +60.76% | 5/5 |
| 9 | 35.615 | Bodega | 88.875 | +59.93% | 5/5 |
| 11 | 38.502 | Bodega | 100.933 | +61.85% | 5/5 |
| 13 | 38.445 | Bodega | 99.196 | +61.24% | 5/5 |
| 15 | 53.193 | Bodega | 106.879 | +50.23% | 5/5 |
| 17 | 54.053 | Bodega | 104.151 | +48.10% | 5/5 |
| 19 | 57.168 | Bodega | 110.803 | +48.41% | 5/5 |
| 21 | 70.095 | Bodega | 112.576 | +37.74% | 4/5 |
| 23 | 75.005 | Bodega | 117.952 | +36.41% | 4/5 |
| 25 | 70.95 | Bodega | 118.114 | +39.93% | 4/5 |
| 27 | 72.252 | Bodega | 117.475 | +38.50% | 4/5 |
| 29 | 72.338 | Bodega | 110.799 | +34.71% | 4/5 |
| 31 | 67.553 | Bodega | 110.978 | +39.13% | 4/5 |

### p99 latency (ms)

| Replicas | Agreement | Best alternative | Alternative value | Agreement gain | Alternatives |
| --- | --- | --- | --- | --- | --- |
| 3 | 256.748 | SwiftPaxos | 180.91 | -41.92% | 5/5 |
| 5 | 265.994 | SwiftPaxos | 238.48 | -11.54% | 5/5 |
| 7 | 249.314 | SwiftPaxos | 257.166 | +3.05% | 5/5 |
| 9 | 267.152 | SwiftPaxos | 269.418 | +0.84% | 5/5 |
| 11 | 262.379 | KCensus | 335.21 | +21.73% | 5/5 |
| 13 | 272.816 | Paxos | 332.786 | +18.02% | 5/5 |
| 15 | 299.5 | KCensus | 343.847 | +12.90% | 5/5 |
| 17 | 308.084 | KCensus | 360.631 | +14.57% | 5/5 |
| 19 | 307.072 | KCensus | 360.726 | +14.87% | 5/5 |
| 21 | 303.103 | KCensus | 392.809 | +22.84% | 4/5 |
| 23 | 314.879 | KCensus | 407.899 | +22.80% | 4/5 |
| 25 | 318.43 | KCensus | 400.016 | +20.40% | 4/5 |
| 27 | 320.929 | KCensus | 376.589 | +14.78% | 4/5 |
| 29 | 349.914 | KCensus | 393.037 | +10.97% | 4/5 |
| 31 | 334.695 | KCensus | 384.906 | +13.05% | 4/5 |

### Traffic (bytes/request/replica)

| Replicas | Agreement | Best alternative | Alternative value | Agreement gain | Alternatives |
| --- | --- | --- | --- | --- | --- |
| 3 | 118.058 | Bodega | 46.487 | -153.96% | 5/5 |
| 5 | 154.52 | Bodega | 43.017 | -259.20% | 5/5 |
| 7 | 178.665 | Bodega | 42.764 | -317.79% | 5/5 |
| 9 | 220.929 | Bodega | 40.534 | -445.05% | 5/5 |
| 11 | 254.705 | Bodega | 41.443 | -514.59% | 5/5 |
| 13 | 283.863 | Bodega | 40.646 | -598.38% | 5/5 |
| 15 | 273.863 | Bodega | 41.42 | -561.18% | 5/5 |
| 17 | 278.729 | Bodega | 41.404 | -573.19% | 5/5 |
| 19 | 310.314 | Bodega | 42.678 | -627.11% | 5/5 |
| 21 | 380.984 | Bodega | 42.67 | -792.86% | 4/5 |
| 23 | 405.89 | Bodega | 43.416 | -834.88% | 4/5 |
| 25 | 422.277 | Bodega | 43.794 | -864.23% | 4/5 |
| 27 | 429.641 | Bodega | 43.069 | -897.56% | 4/5 |
| 29 | 476.832 | Bodega | 43.08 | -1006.84% | 4/5 |
| 31 | 467.708 | Bodega | 43.19 | -982.91% | 4/5 |

### Messages (messages/request/replica)

| Replicas | Agreement | Best alternative | Alternative value | Agreement gain | Alternatives |
| --- | --- | --- | --- | --- | --- |
| 3 | 1.186 | Paxos | 0.775 | -52.95% | 5/5 |
| 5 | 1.207 | Paxos | 0.529 | -128.08% | 5/5 |
| 7 | 1.206 | Paxos | 0.425 | -183.56% | 5/5 |
| 9 | 1.394 | Paxos | 0.365 | -282.11% | 5/5 |
| 11 | 1.554 | Paxos | 0.33 | -370.91% | 5/5 |
| 13 | 1.696 | Paxos | 0.302 | -460.70% | 5/5 |
| 15 | 1.577 | Paxos | 0.283 | -456.44% | 5/5 |
| 17 | 1.588 | Paxos | 0.269 | -489.83% | 5/5 |
| 19 | 1.777 | Paxos | 0.257 | -592.42% | 5/5 |
| 21 | 2.153 | Paxos | 0.246 | -774.68% | 4/5 |
| 23 | 2.222 | Paxos | 0.236 | -843.62% | 4/5 |
| 25 | 2.287 | Paxos | 0.225 | -914.08% | 4/5 |
| 27 | 2.387 | Paxos | 0.217 | -1001.18% | 4/5 |
| 29 | 2.584 | Paxos | 0.209 | -1137.72% | 4/5 |
| 31 | 2.586 | Paxos | 0.199 | -1198.57% | 4/5 |

### CPU per replica (% of one logical CPU)

| Replicas | Agreement | Best alternative | Alternative value | Agreement gain | Alternatives |
| --- | --- | --- | --- | --- | --- |
| 3 | 14.756 | Bodega | 11.541 | -27.86% | 5/5 |
| 5 | 14.484 | Bodega | 9.147 | -58.34% | 5/5 |
| 7 | 15.063 | Bodega | 8.429 | -78.71% | 5/5 |
| 9 | 17.305 | Bodega | 8.047 | -115.06% | 5/5 |
| 11 | 19.835 | Bodega | 8.249 | -140.46% | 5/5 |
| 13 | 21.478 | Bodega | 8.082 | -165.75% | 5/5 |
| 15 | 21.777 | Bodega | 8.178 | -166.28% | 5/5 |
| 17 | 20.752 | Bodega | 7.403 | -180.30% | 5/5 |
| 19 | 22.2 | Bodega | 7.58 | -192.90% | 5/5 |
| 21 | 27.552 | Bodega | 7.546 | -265.10% | 4/5 |
| 23 | 28.983 | Bodega | 7.866 | -268.48% | 4/5 |
| 25 | 30.028 | Bodega | 7.722 | -288.84% | 4/5 |
| 27 | 31.289 | PQL | 7.683 | -307.27% | 4/5 |
| 29 | 34.228 | PQL | 7.431 | -360.59% | 4/5 |
| 31 | 35.009 | PQL | 7.095 | -393.43% | 4/5 |

### Peak memory per replica (MiB)

| Replicas | Agreement | Best alternative | Alternative value | Agreement gain | Alternatives |
| --- | --- | --- | --- | --- | --- |
| 3 | 73.199 | Bodega | 67.271 | -8.81% | 5/5 |
| 5 | 70.951 | Bodega | 72.538 | +2.19% | 5/5 |
| 7 | 70.379 | Bodega | 63.916 | -10.11% | 5/5 |
| 9 | 70.907 | PQL | 70.102 | -1.15% | 5/5 |
| 11 | 75.594 | PQL | 64.94 | -16.40% | 5/5 |
| 13 | 114.927 | Bodega | 68.721 | -67.24% | 5/5 |
| 15 | 90.914 | PQL | 68.247 | -33.21% | 5/5 |
| 17 | 93.978 | PQL | 70.707 | -32.91% | 5/5 |
| 19 | 74.937 | PQL | 67.73 | -10.64% | 5/5 |
| 21 | 95.477 | PQL | 68.217 | -39.96% | 4/5 |
| 23 | 96.851 | PQL | 73.4 | -31.95% | 4/5 |
| 25 | 98.145 | PQL | 78.9 | -24.39% | 4/5 |
| 27 | 99.106 | Bodega | 82.613 | -19.96% | 4/5 |
| 29 | 101.392 | Bodega | 75.115 | -34.98% | 4/5 |
| 31 | 102.528 | Bodega | 73.158 | -40.15% | 4/5 |

## Throughput capacity (ring 7, workload B)

| Metric | Agreement | Best alternative | Alternative value | Agreement gain |
| --- | --- | --- | --- | --- |
| Achieved at highest sustained load (requests/s) | 474,547 | Bodega | 335,153.5 | +41.59% |
| Highest sustained offered load (requests/s) | 476,000 | Bodega | 336,000 | +41.67% |

Rebuild from the measured CSVs: `python results/compare.py`.
