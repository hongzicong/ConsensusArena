#!/usr/bin/env bash
#SBATCH --job-name=paxos-latency
#SBATCH --partition=standard
#SBATCH --qos=parallel
#SBATCH --nodes=3
#SBATCH --ntasks=24
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=1G
#SBATCH --exclusive
#SBATCH --time=02:00:00
#SBATCH --output=slurm-%j.out
set -euo pipefail
export LC_ALL=C
repo_root=${CONSENSUSARENA_ROOT:-${SLURM_SUBMIT_DIR:?}}
if [[ ${CONSENSUSARENA_SCENE_WORKER:-0} == 1 ]]; then
    exec python3 "$repo_root/slurm/upstream_harness.py" run latency
elif [[ -n ${SLURM_ARRAY_TASK_ID:-} ]]; then
    exec python3 "$repo_root/slurm/benchmark-suite.py" task latency "$repo_root"
else
    exec python3 "$repo_root/slurm/benchmark-suite.py" serial latency "$repo_root"
fi
