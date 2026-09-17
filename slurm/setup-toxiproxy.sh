#!/usr/bin/env bash
set -euo pipefail

run_dir=$1
rank=$2
server_binary=$3
script_dir=$(cd "$(dirname "$0")" && pwd)
source "$script_dir/topology.sh"
address_map="$run_dir/config/address-map.txt"
latency_matrix="$run_dir/config/latency.conf"
control_port=$((18000 + rank))
proxy_port_base=$((20000 + rank * 16))
dial_map="$run_dir/config/dial-map-$rank.txt"
pid_file="$run_dir/status/toxiproxy-$rank.pid"
proxy_snapshot="$run_dir/config/toxiproxy-$rank.json"
latency_jitter_ms=${CONSENSUSARENA_LATENCY_JITTER_MS:-10}
bandwidth_kbps=${CONSENSUSARENA_BANDWIDTH_KBPS:-125000}

[[ "$latency_jitter_ms" =~ ^[0-9]+$ ]] || {
    echo "Toxiproxy latency jitter must be an integer number of milliseconds: $latency_jitter_ms" >&2
    exit 1
}
[[ "$bandwidth_kbps" =~ ^[1-9][0-9]*$ ]] || {
    echo "Toxiproxy bandwidth must be a positive integer number of KB/s: $bandwidth_kbps" >&2
    exit 1
}

source_logical=$(source_logical_for_rank "$rank")

lookup_endpoint() {
    local logical=$1
    awk -v logical="$logical" '$1 == logical { print $2; found = 1; exit } END { if (!found) exit 1 }' "$address_map"
}

lookup_rtt_ms() {
    local from=$1
    local to=$2
    local duration
    duration=$(awk -v from="$from" -v to="$to" \
        '$1 == from && $2 == to { print $3; found = 1; exit } END { if (!found) exit 1 }' \
        "$latency_matrix")
    case "$duration" in
        *ms) duration=${duration%ms} ;;
        *s) duration=$(( ${duration%s} * 1000 )) ;;
        *) echo "Toxiproxy requires an integer ms or s latency, got $duration" >&2; return 1 ;;
    esac
    [[ "$duration" =~ ^[0-9]+$ ]] || {
        echo "Toxiproxy requires an integer latency, got $duration" >&2
        return 1
    }
    printf '%s\n' "$duration"
}

post_json() {
    local path=$1
    local payload=$2
    curl --fail --silent --show-error \
        -H 'Content-Type: application/json' \
        -X POST --data "$payload" "http://127.0.0.1:$control_port$path" >/dev/null
}

source_endpoint=$(lookup_endpoint "$source_logical")
: > "$dial_map"

LOG_LEVEL=warn "$server_binary" -host 127.0.0.1 -port "$control_port" \
    > "$run_dir/stdout/toxiproxy-$rank.out" 2>&1 &
server_pid=$!
printf '%s\n' "$server_pid" > "$pid_file"

cleanup_failed_setup() {
    local status=$?
    trap - EXIT
    if (( status != 0 )) && kill -0 "$server_pid" 2>/dev/null; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    exit "$status"
}
trap cleanup_failed_setup EXIT

for _ in $(seq 1 100); do
    if curl --fail --silent "http://127.0.0.1:$control_port/version" >/dev/null 2>&1; then
        break
    fi
    if ! kill -0 "$server_pid" 2>/dev/null; then
        echo "Toxiproxy rank $rank exited during startup" >&2
        exit 1
    fi
    sleep 0.05
done
curl --fail --silent "http://127.0.0.1:$control_port/version" >/dev/null

for ((target_index=0; target_index<replica_count; target_index++)); do
    target_logical="0.0.0.$((target_index + 1))"
    target_endpoint=$(lookup_endpoint "$target_logical")
    proxy_port=$((proxy_port_base + target_index))
    proxy_name="rank-${rank}-replica-${target_index}"
    listen="127.0.0.1:$proxy_port"
    upstream_ms=$(lookup_rtt_ms "$source_endpoint" "$target_endpoint")
    downstream_ms=$(lookup_rtt_ms "$target_endpoint" "$source_endpoint")
    upstream_ms=$(( (upstream_ms + 1) / 2 ))
    downstream_ms=$(( (downstream_ms + 1) / 2 ))

    post_json /proxies \
        "{\"name\":\"$proxy_name\",\"listen\":\"$listen\",\"upstream\":\"$target_endpoint\",\"enabled\":true}"
    if (( upstream_ms > 0 )); then
        post_json "/proxies/$proxy_name/toxics" \
            "{\"name\":\"latency-upstream\",\"type\":\"latency\",\"stream\":\"upstream\",\"toxicity\":1.0,\"attributes\":{\"latency\":$upstream_ms,\"jitter\":$latency_jitter_ms}}"
    fi
    if (( downstream_ms > 0 )); then
        post_json "/proxies/$proxy_name/toxics" \
            "{\"name\":\"latency-downstream\",\"type\":\"latency\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"latency\":$downstream_ms,\"jitter\":$latency_jitter_ms}}"
    fi
    post_json "/proxies/$proxy_name/toxics" \
        "{\"name\":\"bandwidth-upstream\",\"type\":\"bandwidth\",\"stream\":\"upstream\",\"toxicity\":1.0,\"attributes\":{\"rate\":$bandwidth_kbps}}"
    post_json "/proxies/$proxy_name/toxics" \
        "{\"name\":\"bandwidth-downstream\",\"type\":\"bandwidth\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"rate\":$bandwidth_kbps}}"
    printf '%s %s\n' "$target_endpoint" "$listen" >> "$dial_map"
done

curl --fail --silent "http://127.0.0.1:$control_port/proxies" > "$proxy_snapshot"
trap - EXIT
printf '%s\n' "$dial_map"
