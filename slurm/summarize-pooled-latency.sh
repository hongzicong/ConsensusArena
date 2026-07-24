#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

run_dir=$1
summary="$run_dir/summary.csv"
pooled_raw="$run_dir/.pooled-latency-ALL"
pooled_sorted="$pooled_raw.sorted"

shopt -s nullglob
profile_dirs=("$run_dir"/ycsb-*)
if [[ ${#profile_dirs[@]} == 0 ]]; then
    echo "No YCSB profile latency samples found under $run_dir" >&2
    exit 1
fi

percentile() {
    local sorted_file=$1
    local count=$2
    local percent=$3
    local index
    index=$(awk -v n="$count" -v p="$percent" 'BEGIN { i = int(n * p); if (i < n * p) i++; if (i < 1) i = 1; print i }')
    sed -n "${index}p" "$sorted_file" | awk '{printf "%.3f\n", $1}'
}

write_row() {
    local scope=$1
    local sorted_file=$2
    local count mean minimum maximum p50 p95 p99
    count=$(wc -l < "$sorted_file" | tr -d ' ')
    if [[ "$count" == 0 ]]; then
        echo "Pooled latency input is empty for $scope" >&2
        exit 1
    fi
    read -r mean minimum maximum < <(
        awk 'NR == 1 { min = max = $1 } { sum += $1; if ($1 < min) min = $1; if ($1 > max) max = $1 } END { printf "%.3f %.3f %.3f\n", sum / NR, min, max }' "$sorted_file"
    )
    p50=$(percentile "$sorted_file" "$count" 0.50)
    p95=$(percentile "$sorted_file" "$count" 0.95)
    p99=$(percentile "$sorted_file" "$count" 0.99)
    printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$scope" "$count" "$mean" "$p50" "$p95" "$p99" "$minimum" "$maximum" >> "$summary"
}

: > "$pooled_raw"
printf 'Scope,Count,MeanMs,P50Ms,P95Ms,P99Ms,MinMs,MaxMs\n' > "$summary"
for profile_dir in "${profile_dirs[@]}"; do
    files=("$profile_dir"/repetition-*/raw-latency/overall-ALL)
    if [[ ${#files[@]} == 0 ]]; then
        echo "No repetition latency samples found under $profile_dir" >&2
        exit 1
    fi
    profile=$(basename "$profile_dir")
    profile=${profile#ycsb-}
    profile_raw="$run_dir/.latency-ycsb-$profile"
    profile_sorted="$profile_raw.sorted"
    cat "${files[@]}" > "$profile_raw"
    cat "$profile_raw" >> "$pooled_raw"
    sort -n "$profile_raw" > "$profile_sorted"
    write_row "$profile" "$profile_sorted"
    rm -f "$profile_raw" "$profile_sorted"
done

sort -n "$pooled_raw" > "$pooled_sorted"
write_row ALL "$pooled_sorted"
rm -f "$pooled_raw" "$pooled_sorted"
cat "$summary"
printf 'Pooled latency summary: %s\n' "$summary"
