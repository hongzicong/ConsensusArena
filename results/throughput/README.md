# Throughput and saturation

**Status: final single-CPU campaign pending.** No historical measurement has
been promoted as a final paper result.

Compare the same six protocols on ring 7, workload B, using the existing
adaptive offered-load search. Plot offered versus achieved requests/s and
latency versus offered load to show where each protocol stops sustaining the
workload. Replica resource observation is disabled for this suite.
Clients and master have two CPU slots each. Check the recorded offered and
issued request counts and sender-queue delays near the measured throughput
boundary before attributing a limit specifically to replicas.

Preserve every attempt, including timeouts, invalid executions and retries,
alongside the selected points and capacity summary. Preserve the original
search and acceptance rules, including the 400 ms mean-latency cap. Do not
silently discard nonmonotone observations or treat timeout latency as a
complete-cohort result. The capacity estimate describes this finite workload
window, rather than proving indefinite steady-state capacity.

Final contents will be `data/`, `plotting/`, `figures/` and `manifest.json`.
The adaptive specification is
[experiment.toml](../../slurm/throughput-suite/experiment.toml), run using
[launch.ps1](../../slurm/throughput-suite/launch.ps1).

Historical adaptive runs, extra knee sweeps and the Agreement CPU-mode
diagnostic remain in [the throughput report area](../../../report/throughput-evaluation/).
They do not form the final common single-CPU comparison.
