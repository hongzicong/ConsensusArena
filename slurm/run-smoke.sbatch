#!/usr/bin/env bash
#SBATCH --job-name=paxos-smoke
#SBATCH --partition=standard
#SBATCH --qos=serial
#SBATCH --nodes=1
#SBATCH --ntasks=16
#SBATCH --ntasks-per-node=16
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=1G
#SBATCH --time=00:20:00
#SBATCH --output=slurm-%j.out
set -euo pipefail
export LC_ALL=C
repo_root=${CONSENSUSARENA_ROOT:-${SLURM_SUBMIT_DIR:?}}
exec python3 "$repo_root/slurm/upstream_harness.py" run smoke
