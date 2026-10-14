#!/usr/bin/env bash
set -euo pipefail
run_dir=$1
script_dir=$(cd "$(dirname "$0")" && pwd)
source "$script_dir/topology.sh"
rank=${SLURM_PROCID:?}
config="$run_dir/config/cluster.conf"
quorum="$run_dir/config/quorum.conf"
binary="$run_dir/binary/consensusarena-linux-amd64"
pids=()
cleanup() {
    for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
    for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM
trap 'touch "$run_dir/status/failed-$rank"' ERR
wait_marker() {
    while [[ ! -f "$run_dir/status/$1" ]]; do
        [[ ! -f "$run_dir/status/stop" ]] || return 1
        sleep 0.2
    done
}
run_app() {
    GOMAXPROCS=${SLURM_CPUS_PER_TASK:-8} /usr/bin/time \
      -o "$run_dir/stdout/resources-rank-$rank.txt" \
      -f 'elapsed_seconds=%e\nuser_seconds=%U\nsystem_seconds=%S\nmax_rss_kb=%M\nvoluntary_context_switches=%w\ninvoluntary_context_switches=%c\nexit_status=%x' \
      "$binary" "$@"
}
if (( rank == master_rank )); then
    run_app -run master -config "$config" -alias m0 -log "$run_dir/logs/master.log" \
      > "$run_dir/stdout/master.out" 2>&1 &
    pids+=("$!")
    sleep 3
    kill -0 "${pids[0]}"
    touch "$run_dir/status/master.ready"
else
    dial_map=$(bash "$script_dir/setup-toxiproxy.sh" "$run_dir" "$rank" "$run_dir/binary/toxiproxy-server-linux-amd64")
    pids+=("$(cat "$run_dir/status/toxiproxy-$rank.pid")")
    export CONSENSUSARENA_DIAL_MAP="$dial_map"
    wait_marker master.ready
    if (( rank < replica_count )); then
        alias=${replicas[$rank]}
        export CONSENSUSARENA_PEER_LISTENERS_READY="$run_dir/status/peer-listeners.ready"
        run_app -run server -config "$config" -quorum "$quorum" -alias "$alias" \
          -log "$run_dir/logs/$alias-replica.log" > "$run_dir/stdout/$alias-replica.out" 2>&1 &
        pids+=("$!")
        python3 "$script_dir/listener-ready.py" "$((7070+rank))" "$run_dir/status/listening-$rank"
    else
        alias=${clients[$((rank-replica_count))]}
        wait_marker replicas.ready
        run_app -run client -config "$config" -quorum "$quorum" -alias "$alias" \
          -log "$run_dir/results/$alias-client.log" > "$run_dir/stdout/$alias-client.out" 2>&1 &
        client_pid=$!
        pids+=("$client_pid")
        set +e
        wait "$client_pid"
        client_exit=$?
        set -e
        # The client has completed; do not classify this owned process as an early server exit.
        pids=("${pids[0]}")
        printf '%s\n' "$client_exit" > "$run_dir/status/$alias.exit"
    fi
fi
while [[ ! -f "$run_dir/status/stop" ]]; do
    for pid in "${pids[@]}"; do
        if ! kill -0 "$pid" 2>/dev/null; then
            touch "$run_dir/status/failed-$rank"
            exit 1
        fi
    done
    sleep 0.2
done
