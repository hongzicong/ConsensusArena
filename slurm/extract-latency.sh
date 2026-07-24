#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

run_dir=$1
results_dir="$run_dir/results"
config="$run_dir/config/cluster.conf"
raw_dir="$run_dir/raw-latency"
raw_file="$raw_dir/overall-ALL"
mkdir -p "$raw_dir"

regions=(
    ap-east-1 ap-northeast-1 ap-southeast-2 eu-west-1 ca-central-1
    sa-east-1 us-east-1 us-east-2 us-west-1 us-west-2
)
clones=$(awk '$1 == "clones:" { print $2; exit }' "$config")
expected_files=$((clones + 1))

: > "$raw_file"
shopt -s nullglob
for region in "${regions[@]}"; do
    files=("$results_dir/${region}-client-"*)
    if [[ ${#files[@]} != "$expected_files" ]]; then
        echo "Expected $expected_files client log files for $region but found ${#files[@]}" >&2
        exit 1
    fi
    awk '/latency / && ($(NF - 1) == "READ" || $(NF - 1) == "UPDATE") { print $NF }' \
        "${files[@]}" >> "$raw_file"
done

count=$(wc -l < "$raw_file" | tr -d ' ')
if [[ "$count" == 0 ]]; then
    echo "No latency samples found under $run_dir" >&2
    exit 1
fi

printf 'Extracted %s latency samples: %s\n' "$count" "$raw_file"
