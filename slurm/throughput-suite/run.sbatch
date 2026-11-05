#!/usr/bin/env bash
#SBATCH --job-name=consensusarena-throughput
#SBATCH --partition=standard
#SBATCH --qos=parallel
#SBATCH --nodes=2
#SBATCH --ntasks=15
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=1G
#SBATCH --exclusive
#SBATCH --time=02:00:00
set -euo pipefail
root=${CONSENSUSARENA_ROOT:?CONSENSUSARENA_ROOT is required}
exec python3 "$root/slurm/throughput-suite/runner.py" worker "$1"
