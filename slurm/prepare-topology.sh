#!/usr/bin/env bash
# Generate logical inputs; this also runs locally without Slurm or a Go binary.
set -euo pipefail
script_dir=$(cd "$(dirname "$0")" && pwd)
repo_root=$(cd "$script_dir/.." && pwd)
source "$script_dir/topology.sh"
output=${1:?Usage: prepare-topology.sh OUTPUT_DIRECTORY}
mkdir -p "$output"

if (( replica_count == 5 )); then
    cp "$script_dir/workload.conf" "$output/workload.conf"
    cp "$repo_root/latency.conf" "$output/latency.conf"
else
    # Add sites already represented by clients in the original measured matrix.
    awk -v n="$replica_count" '
        BEGIN {
            added[6]="eu-west-1"; added[7]="us-east-1"
            added[8]="ap-southeast-2"; added[9]="sa-east-1"
            added[10]="ap-east-1"; added[11]="ca-central-1"
            added[12]="us-east-2"; added[13]="us-west-2"
        }
        { sub(/\r$/, "") }
        $0 == "-- Clients --" {
            for (i=6; i<=n; i++) print added[i], "0.0.0." i
            print ""
        }
        { print }
    ' "$script_dir/workload.conf" > "$output/workload.conf"

    # Expand both directed axes; no new geographic latency is invented.
    awk -v n="$replica_count" '
        BEGIN {
            origin["0.0.0.6"]="0.0.1.4"; origin["0.0.0.7"]="0.0.1.7"
            origin["0.0.0.8"]="0.0.1.3"; origin["0.0.0.9"]="0.0.1.6"
            origin["0.0.0.10"]="0.0.1.1"; origin["0.0.0.11"]="0.0.1.5"
            origin["0.0.0.12"]="0.0.1.8"; origin["0.0.0.13"]="0.0.1.10"
            for (i=1; i<=n; i++) endpoint[++count]="0.0.0." i
            for (i=1; i<=10; i++) endpoint[++count]="0.0.1." i
        }
        { sub(/\r$/, ""); rtt[$1 SUBSEP $2]=$3 }
        END {
            for (i=1; i<=count; i++) for (j=1; j<=count; j++) {
                a=endpoint[i]; b=endpoint[j]
                from=(a in origin) ? origin[a] : a
                to=(b in origin) ? origin[b] : b
                if (!((from SUBSEP to) in rtt)) {
                    print "Missing source latency: " from " " to > "/dev/stderr"
                    exit 1
                }
                print a, b, rtt[from SUBSEP to]
            }
        }
    ' "$repo_root/latency.conf" > "$output/latency.conf"
fi
printf 'replica_count=%s\nclient_count=%s\nfault_budget=%s\nmajority_size=%s\nlatency_source=latency.conf\n' \
    "$replica_count" "$client_count" "$((replica_count / 2))" "$((replica_count / 2 + 1))" \
    > "$output/topology-metadata.txt"
printf 'placement=protocol-plan.go\n' >> "$output/topology-metadata.txt"
