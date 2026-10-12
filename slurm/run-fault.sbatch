#!/usr/bin/env bash
#SBATCH --job-name=consensusarena-fault
#SBATCH --partition=standard
#SBATCH --qos=parallel
#SBATCH --nodes=3
#SBATCH --ntasks=24
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=1G
#SBATCH --exclusive
#SBATCH --array=0-26
#SBATCH --time=02:00:00
#SBATCH --output=slurm-%A_%a.out
set -euo pipefail
export LC_ALL=C
repo_root=${CONSENSUSARENA_ROOT:-$SLURM_SUBMIT_DIR}
if [[ -n "${SLURM_ARRAY_TASK_ID:-}" && "${CONSENSUSARENA_SCENE_WORKER:-0}" != 1 ]]; then
    exec python3 "$repo_root/slurm/benchmark-suite.py" task fault "$repo_root"
fi
export CONSENSUSARENA_REPLICAS=${CONSENSUSARENA_REPLICAS:-5:9:13}
if [[ "${CONSENSUSARENA_SCENE_WORKER:-0}" == 1 ]]; then
    exec python3 "$repo_root/slurm/fault-harness.py" coordinator-one "$repo_root"
fi
exec python3 "$repo_root/slurm/fault-harness.py" coordinator "$repo_root"
