#!/usr/bin/env bash
# Submit four original n7 deployments plus NA n3/n5, each with workloads A/B.
set -euo pipefail
suite_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$suite_dir/../.." && pwd)
protocol=${1:-agreement}
if (( $# > 0 )); then shift; fi
case "${protocol,,}" in
    agreement|paxos|swiftpaxos|bodega|kcensus|pqlfrontier) ;;
    *) echo 'Use agreement, paxos, swiftpaxos, bodega, kcensus, or pqlfrontier' >&2; exit 1 ;;
esac

# Inherited diagnostic selections must not change this suite's workload.
unset CONSENSUSARENA_CASE CONSENSUSARENA_CASE_SHA256 CONSENSUSARENA_CASES
unset CONSENSUSARENA_ARRIVAL_RATE CONSENSUSARENA_PLAN_SHA256
export CONSENSUSARENA_REPLICAS=3:5:7
export CONSENSUSARENA_YCSB_PROFILES=A:B
export CONSENSUSARENA_WORKLOAD_SEED=1
export CONSENSUSARENA_LATENCY_JITTER_MS=0
export CONSENSUSARENA_BANDWIDTH_KBPS=125000
export CONSENSUSARENA_RUNTIME_POLICY=replica-single-cpu-v2
export CONSENSUSARENA_RESOURCES=0
cd "$repo_root"
exec "${CONSENSUSARENA_PYTHON:-python3}" slurm/benchmark-suite.py submit latency \
    --suite generated --generated-root "$suite_dir" --account dcl \
    --export "ALL,CONSENSUSARENA_PROTOCOL=$protocol" "$@"
