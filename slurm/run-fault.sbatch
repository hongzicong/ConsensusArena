#!/usr/bin/env bash
#SBATCH --job-name=consensusarena-fault
#SBATCH --partition=standard
#SBATCH --qos=parallel
#SBATCH --nodes=3
#SBATCH --ntasks=24
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=1G
#SBATCH --time=02:00:00
#SBATCH --output=slurm-%j.out
set -euo pipefail
export LC_ALL=C
repo_root=${CONSENSUSARENA_ROOT:-$SLURM_SUBMIT_DIR}
export CONSENSUSARENA_REPLICAS=${CONSENSUSARENA_REPLICAS:-5:9:13}
exec python3 "$repo_root/slurm/fault-harness.py" coordinator "$repo_root"
