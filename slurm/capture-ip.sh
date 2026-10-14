#!/usr/bin/env bash
set -euo pipefail

output_dir=$1
mkdir -p "$output_dir"

node_ip=$(getent ahostsv4 "$(hostname -s)" 2>/dev/null | awk 'NR == 1 { print $1 }' || true)
if [[ -z "$node_ip" ]]; then
    node_ip=$(hostname -I 2>/dev/null | awk '{ print $1 }' || true)
fi
if [[ -z "$node_ip" ]]; then
    echo "Could not determine an IPv4 address for $(hostname)" >&2
    exit 1
fi

printf '%s\n' "$node_ip" > "$output_dir/${SLURM_PROCID}.ip"
printf '%s %s %s\n' "$SLURM_PROCID" "$(hostname -s)" "$node_ip"
