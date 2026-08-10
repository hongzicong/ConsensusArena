#!/usr/bin/env bash
set -euo pipefail

run_dir=$1
binary=$2
toxiproxy_server=$3
rank=${SLURM_PROCID:?SLURM_PROCID is not set}
config="$run_dir/config/cluster.conf"
quorum="$run_dir/config/quorum.conf"
script_dir=$(cd "$(dirname "$0")" && pwd)

replica_alias=
client_alias=
case "$rank" in
    0) replica_alias=ap-south-1 ;;
    1) replica_alias=ap-northeast-1 ;;
    2) replica_alias=eu-west-3 ;;
    3) replica_alias=us-west-1 ;;
    4) replica_alias=af-south-1 ;;
    5) client_alias=ap-east-1 ;;
    6) client_alias=ap-northeast-1 ;;
    7) client_alias=ap-southeast-2 ;;
    8) client_alias=eu-west-1 ;;
    9) client_alias=ca-central-1 ;;
    10) client_alias=sa-east-1 ;;
    11) client_alias=us-east-1 ;;
    12) client_alias=us-east-2 ;;
    13) client_alias=us-west-1 ;;
    14) client_alias=us-west-2 ;;
    15) ;;
    *) echo "Unexpected Slurm rank: $rank" >&2; exit 1 ;;
esac

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
    GOMAXPROCS=${SLURM_CPUS_PER_TASK:-8} "$binary" "$@"
}

if [[ "$rank" != 15 ]]; then
    dial_map=$(bash "$script_dir/setup-toxiproxy.sh" "$run_dir" "$rank" "$toxiproxy_server")
    toxiproxy_pid=$(tr -d '[:space:]' < "$run_dir/status/toxiproxy-$rank.pid")
    pids+=("$toxiproxy_pid")
    export CONSENSUSARENA_DIAL_MAP="$dial_map"
fi

if [[ "$rank" == 15 ]]; then
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
