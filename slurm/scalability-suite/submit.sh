#!/usr/bin/env bash
set -euo pipefail
suite_dir=$(cd "$(dirname "$0")" && pwd)
protocol=${1:-agreement}
if (( $# > 0 )); then shift; fi
case "${protocol,,}" in
    agreement|paxos|swiftpaxos|bodega|kcensus|pqlfrontier) ;;
    *) echo 'Use agreement, paxos, swiftpaxos, bodega, kcensus, or pqlfrontier' >&2; exit 1 ;;
esac
unset CONSENSUSARENA_CASE CONSENSUSARENA_CASE_SHA256 CONSENSUSARENA_CASES
unset CONSENSUSARENA_ARRIVAL_RATE CONSENSUSARENA_PLAN_SHA256 CONSENSUSARENA_RUN_DIR
unset CONSENSUSARENA_REPLICAS
export CONSENSUSARENA_YCSB_PROFILES=B CONSENSUSARENA_WORKLOAD_SEED=1
export CONSENSUSARENA_LATENCY_JITTER_MS=0 CONSENSUSARENA_BANDWIDTH_KBPS=125000
export CONSENSUSARENA_RESOURCES=1
cd "$suite_dir/../.."
exec "${CONSENSUSARENA_PYTHON:-python3}" slurm/benchmark-suite.py submit latency \
    --suite generated --generated-root "$suite_dir/generated" --account dcl \
    --export "ALL,CONSENSUSARENA_PROTOCOL=$protocol" "$@"
