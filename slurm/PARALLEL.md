# Parallel benchmark suites

From `ConsensusArena/` on Jed, with the usual binary, checksum, protocol and
Toxiproxy environment variables configured:

```sh
python3 slurm/benchmark-suite.py submit latency --account dcl
python3 slurm/benchmark-suite.py submit fault --account dcl
```

Each command submits a Slurm job array and an `afterany` collector job. The
default array contains 27 independent scenes: 5/9/13 replicas, three topologies,
and A/B/C. Every scene reserves three exclusive nodes, 24 tasks, and eight CPUs
per task. The scenarios may run concurrently as resources and account limits
permit; all 27 running at once require 81 nodes. `--max-parallel 6` limits the
array to six simultaneous scenes. Benchmark protocol behavior, workloads and
the recovery 5/20/40-second schedule are unchanged.

`CONSENSUSARENA_REPLICAS`, `CONSENSUSARENA_YCSB_PROFILES`, and
`CONSENSUSARENA_CASES` (e.g. `5-1:13-3`) select a smaller suite. Use `plan latency`
or `plan fault` to inspect the scene list. The submitter also accepts the existing
`--export ALL,NAME=value,...` convention and sizes the array to the selection.

```powershell
ssh zihong@jed.hpc.epfl.ch "cd ~/ConsensusArena && python3 slurm/benchmark-suite.py submit latency --account dcl --export=$exports"
```

The submission prints the array ID, collector ID and root output directory.
Monitor both IDs with `squeue -j ARRAY_ID,COLLECTOR_ID`. Array logs are
`slurm-ARRAY_ID_TASK_ID.out`; the collector log is `slurm-COLLECTOR_ID.out`.
Wait for the collector to finish before consuming the aggregate CSVs.

Default suite roots are `$SCRATCH/consensusarena-ARRAY_ID` and
`$SCRATCH/consensusarena-fault-ARRAY_ID`, falling back to `/scratch/$USER` when
`SCRATCH` is unset. `CONSENSUSARENA_RUN_DIR` overrides the root and must be unique
to the submission. Each scene writes under
`scenes/replicas-N-topology-T-ycsb-X/`, with the original harness layout inside.
No scene shares mutable configurations, binaries, status files or logs.

`suite.json` records the expected scenes. `suite-status.json` records scene
completeness and errors. Latency collection combines `summary.csv`,
`repetition-summaries.csv`, and `baseline-latency.csv`. Fault collection reruns
the existing observation validator and combines `phase-summary.csv`,
`timeseries.csv`, `request-cohorts.csv`, and `validation.json`. Failed or missing
scenes produce an incomplete suite and collector exit code 2; available evidence
is retained, and absent samples are never synthesized. Validity and sustained
recovery must still be checked before promotion.

For a manual collection or recollection:

```sh
python3 slurm/benchmark-suite.py collect latency /scratch/USER/consensusarena-ARRAY_ID
python3 slurm/benchmark-suite.py collect fault /scratch/USER/consensusarena-fault-ARRAY_ID
python3 slurm/collect-fault.py ARRAY_ID
```

The last command archives the default fault root as before (for a custom root,
pass it as the optional second argument). Direct `sbatch slurm/run-latency.sbatch`
or `sbatch slurm/run-fault.sbatch` also defaults to a 27-element array, but does
not submit a collector; collect manually in that case. Direct arrays with subset
overrides skip unused indices. Executing either sbatch script with `bash` inside
an existing allocation without an array task ID retains the serial sweep.

Slurm array and dependency semantics are documented at
https://slurm.schedmd.com/job_array.html.
