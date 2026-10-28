#!/usr/bin/env bash
# Submit the four imported deployments using the unchanged suite runner.
set -euo pipefail
suite_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$suite_dir/../.." && pwd)
protocol=${1:-agreement}
if (( $# > 0 )); then shift; fi

# Inherited diagnostic selections must not change this suite's workload.
unset CONSENSUSARENA_CASE CONSENSUSARENA_CASE_SHA256 CONSENSUSARENA_CASES
unset CONSENSUSARENA_ARRIVAL_RATE CONSENSUSARENA_PLAN_SHA256
export CONSENSUSARENA_REPLICAS=7
export CONSENSUSARENA_YCSB_PROFILES=A:B:C
export CONSENSUSARENA_WORKLOAD_SEED=1
export CONSENSUSARENA_LATENCY_JITTER_MS=0
export CONSENSUSARENA_BANDWIDTH_KBPS=125000
cd "$repo_root"
exec "${CONSENSUSARENA_PYTHON:-python3}" slurm/benchmark-suite.py submit latency \
    --suite generated --generated-root "$suite_dir" --account dcl \
    --export "ALL,CONSENSUSARENA_PROTOCOL=$protocol" "$@"
