#!/usr/bin/env bash
#SBATCH --job-name=consensusarena-collect
#SBATCH --partition=standard
#SBATCH --qos=parallel
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=1G
#SBATCH --time=02:00:00
#SBATCH --output=slurm-%j.out
set -euo pipefail
exec python3 "$1/slurm/benchmark-suite.py" collect "$2" "$3"
