#!/usr/bin/env bash
# Generate deployment config and RTT inputs from topologies.json/workload.conf.
set -euo pipefail
script_dir=$(cd "$(dirname "$0")" && pwd)
output=${1:?Usage: prepare-topology.sh OUTPUT_DIRECTORY}
exec "${CONSENSUSARENA_PYTHON:-python3}" "$script_dir/topology.py" prepare "$output"
