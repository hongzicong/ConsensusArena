# Regular evaluation

**Status: final single-CPU campaign pending.** No historical measurement has
been promoted as a final paper result.

Compare Agreement, Paxos, SwiftPaxos, Bodega, KCensus and PQLFrontier (displayed
as PQL) across six deployments, using workloads A/B at 20,000 total offered
requests/s, one run per deployment and profile. The complete matrix has 72 scenes.

| Deployment | Replicas | Regions |
| --- | --- | --- |
| Worldwide ring | 7 | Tokyo, Mumbai, Singapore, Canada Central, Milan, Ireland, Oregon |
| Europe | 7 | Frankfurt, Zurich, Milan, Spain, Ireland, London, Paris |
| East Asia | 7 | Hong Kong, Taipei, Tokyo, Seoul, Osaka, Singapore, Malaysia |
| North America | 7 | Canada Central, Calgary, Mexico Central, Northern Virginia, Ohio, Northern California, Oregon |
| North America small | 3 | Northern Virginia, Northern California, Oregon |
| North America medium | 5 | Canada Central, Northern Virginia, Ohio, Northern California, Oregon |

The two smaller deployments use exact submatrices of the frozen NA-7 source.
A has 50% writes; B has 5% writes. Read-only C is excluded. Report each deployment
and workload separately, with no averaging across topologies. If the main-body
figure needs to be smaller, A on ring 7 can be presented separately while
retaining all collected data.

The paper will show mean and p99 request latency, using complete, fully drained
cohorts. Retain achieved throughput, request counts and completion status in the
data so readers can check that the comparisons are valid. Separate operation
statistics and raw latency observations support further analysis.

Final contents will be `data/`, `plotting/`, `figures/` and `manifest.json`, with
the CPU policy, binary and input checksums recorded. Replica resource observation
is disabled for this suite.

Inputs and launch instructions: [KCensus suite](../../slurm/kcensus-suite/README.md).
Historical reference: [the evaluation with PQLFrontier](../../../report/kcensus-evaluation/20261029-n7-20krps-pqlfrontier/README.txt).
That report reuses earlier runs and is not the new single-CPU campaign.
