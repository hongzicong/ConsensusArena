#!/usr/bin/env bash
#SBATCH --job-name=throughput-collect
#SBATCH --partition=standard
#SBATCH --qos=serial
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --time=02:00:00
set -euo pipefail
root=${CONSENSUSARENA_ROOT:?CONSENSUSARENA_ROOT is required}
exec python3 "$root/slurm/throughput-suite/runner.py" collect "$1"
