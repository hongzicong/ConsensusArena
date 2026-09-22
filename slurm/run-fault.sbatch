#!/usr/bin/env bash
#SBATCH --job-name=consensusarena-fault
#SBATCH --partition=standard
#SBATCH --qos=parallel
#SBATCH --nodes=2
#SBATCH --ntasks=16
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=1G
#SBATCH --time=01:00:00
#SBATCH --output=slurm-%j.out
set -euo pipefail
export LC_ALL=C
repo_root=${CONSENSUSARENA_ROOT:-$SLURM_SUBMIT_DIR}
export CONSENSUSARENA_REPLICAS=5
exec python3 "$repo_root/slurm/fault-harness.py" coordinator "$repo_root"
