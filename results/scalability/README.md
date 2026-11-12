# Scalability and resources

**Status: final single-CPU campaign pending.** No historical measurement has
been promoted as a final paper result.

Compare the same six protocols on the frozen aws-random deployment matrices at
every odd replica count from 3 through 31, using workload B and 20,000 total
offered requests/s. One colocated client per replica and one run per size give
90 scenes and 1,530 replica resource rows.

The paper will show mean and p99 latency, bytes/request/replica,
messages/request/replica, CPU use and peak memory against replica count. Retain
achieved throughput, separate send/receive counters and completion status in
the data. Omit incomplete or timed-out cohorts from the comparison curves;
dotted connectors may span gaps without inventing values. Keep KCensus values
in the CSV even when a figure clips its extreme values. Exclude storage-fix
variants from the final comparison.

CPU and traffic use counter differences between seconds 5 and 15. Memory is a
kernel peak through measurement end, including startup and warmup; its plot is
linear. The observer runs within the replica's single logical CPU budget.
Validate observation overhead before final measurements.

Final contents will be `data/`, `plotting/`, `figures/` and `manifest.json`.
Inputs and launch instructions: [scalability suite](../../slurm/scalability-suite/README.md).
Historical runs and implementation audits remain in
[the scalability report area](../../../report/scalability-evaluation/).
