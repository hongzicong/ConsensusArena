#!/usr/bin/env bash
set -euo pipefail

run_dir=$1
binary=$2
toxiproxy_server=$3
rank=${SLURM_PROCID:?SLURM_PROCID is not set}
config="$run_dir/config/cluster.conf"
quorum="$run_dir/config/quorum.conf"
script_dir=$(cd "$(dirname "$0")" && pwd)

source "$script_dir/topology.sh"
replica_alias=
client_alias=
if (( rank >= 0 && rank < replica_count )); then
    replica_alias=${replicas[$rank]}
elif (( rank >= replica_count && rank < master_rank )); then
    client_alias=${clients[$((rank - replica_count))]}
elif (( rank != master_rank )); then
    echo "Unexpected Slurm rank: $rank" >&2
    exit 1
fi

pids=()
client_exit=0

cleanup() {
    for pid in "${pids[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
        fi
    done
    for pid in "${pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT INT TERM

run_app() {
    # One resource record per role, outside the request path (GNU time).
    GOMAXPROCS=${SLURM_CPUS_PER_TASK:-8} /usr/bin/time \
        -o "$run_dir/stdout/resources-rank-$rank.txt" \
        -f 'elapsed_seconds=%e\nuser_seconds=%U\nsystem_seconds=%S\nmax_rss_kb=%M\nvoluntary_context_switches=%w\ninvoluntary_context_switches=%c\nexit_status=%x' \
        "$binary" "$@"
}

if [[ "$rank" != "$master_rank" ]]; then
    dial_map=$(bash "$script_dir/setup-toxiproxy.sh" "$run_dir" "$rank" "$toxiproxy_server")
    toxiproxy_pid=$(tr -d '[:space:]' < "$run_dir/status/toxiproxy-$rank.pid")
    pids+=("$toxiproxy_pid")
    export CONSENSUSARENA_DIAL_MAP="$dial_map"
fi

if [[ "$rank" == "$master_rank" ]]; then
    run_app -run master -config "$config" -alias m0 \
        -log "$run_dir/logs/master.log" \
        > "$run_dir/stdout/master.out" 2>&1 &
    pids+=("$!")
    sleep 3
    if ! kill -0 "${pids[0]}" 2>/dev/null; then
        echo "Master exited before replicas started" >&2
        touch "$run_dir/status/node-failed"
        exit 1
    fi
    touch "$run_dir/status/master.ready"
else
    while [[ ! -f "$run_dir/status/master.ready" ]]; do
        [[ ! -f "$run_dir/status/stop" ]] || exit 1
        sleep 1
    done
fi

if [[ -n "$replica_alias" ]]; then
    run_app -run replica -config "$config" \
        -alias "$replica_alias" -quorum "$quorum" \
        -log "$run_dir/logs/${replica_alias}-replica.log" \
        > "$run_dir/stdout/${replica_alias}-replica.out" 2>&1 &
    pids+=("$!")
fi

if [[ -n "$client_alias" ]]; then
    while [[ ! -f "$run_dir/status/replicas.ready" ]]; do
        [[ ! -f "$run_dir/status/stop" ]] || exit 1
        sleep 1
    done

    set +e
    run_app -run client -config "$config" \
        -alias "$client_alias" -log "$run_dir/results/${client_alias}-client-" \
        > "$run_dir/stdout/${client_alias}-client.out" 2>&1
    client_exit=$?
    set -e
    printf '%s\n' "$client_exit" > "$run_dir/status/client-${client_alias}.exit"
    touch "$run_dir/status/client-${client_alias}.done"
fi

while [[ ! -f "$run_dir/status/stop" ]]; do
    for pid in "${pids[@]}"; do
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "A server process exited early on rank $rank" >&2
            touch "$run_dir/status/node-failed"
            exit 1
        fi
    done
    sleep 1
done

exit "$client_exit"
