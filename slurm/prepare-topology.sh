#!/usr/bin/env bash
# Generate membership and RTT inputs; protocols choose their own plans.
set -euo pipefail
script_dir=$(cd "$(dirname "$0")" && pwd)
output=${1:?Usage: prepare-topology.sh OUTPUT_DIRECTORY}
exec "${CONSENSUSARENA_PYTHON:-python3}" "$script_dir/topology.py" prepare "$output"
