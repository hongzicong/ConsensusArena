#!/usr/bin/env bash
#SBATCH --job-name=consensusarena-serial
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
exec "${CONSENSUSARENA_PYTHON:-python3}" "$1/slurm/benchmark-suite.py" serial "$2" "$1"
