#!/usr/bin/env bash
set -euo pipefail

run_dir=$1
binary=$2
toxiproxy_server=$3
rank=${SLURM_PROCID:?SLURM_PROCID is not set}
config="$run_dir/config/cluster.conf"
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

if [[ -n "${CONSENSUSARENA_RUNTIME_POLICY:-}" ]]; then
    [[ "$CONSENSUSARENA_RUNTIME_POLICY" == replica-single-cpu-v2 ]] || { echo 'Unknown CPU policy' >&2; exit 1; }
    [[ "${CONSENSUSARENA_REPLICA_GOMAXPROCS:-}" == 1 && "${CONSENSUSARENA_REPLICA_TASKSET:-}" == 1 &&
       "${CONSENSUSARENA_CLIENT_GOMAXPROCS:-}" == 2 && "${CONSENSUSARENA_MASTER_GOMAXPROCS:-}" == 2 &&
       "${CONSENSUSARENA_AFFINITY_AFTER_MEASUREMENT:-}" == 1 &&
       "${CONSENSUSARENA_TASK_CPUS:-}" == 2 && "${CONSENSUSARENA_TASKS_PER_NODE:-}" == 32 ]] || { echo 'CPU policy controls missing or inconsistent' >&2; exit 1; }
fi

snapshot_after_measurement() {
    if [[ -n "${pinned_cpu:-}" && "${CONSENSUSARENA_AFFINITY_AFTER_MEASUREMENT:-0}" == 1 && "${affinity_measurement_checked:-0}" == 0 && -f "$run_dir/status/start-unix-ns" ]]; then
        local epoch now_ns
        read -r epoch < "$run_dir/status/start-unix-ns"
        now_ns=$(date +%s%N)
        if (( now_ns >= epoch + 15000000000 )); then
            python3 "$script_dir/runtime_policy.py" snapshot "$run_dir" "$rank" "$pinned_cpu" measurement_end
            affinity_measurement_checked=1
        fi
    fi
}

cleanup() {
    snapshot_after_measurement || true
    if [[ -n "${pinned_cpu:-}" && -f "$run_dir/status/replica-$rank.pid" ]]; then
        python3 "$script_dir/runtime_policy.py" snapshot "$run_dir" "$rank" "$pinned_cpu" end || true
    fi
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
    local role_gomax=${CONSENSUSARENA_MASTER_GOMAXPROCS:-${SLURM_CPUS_PER_TASK:-8}}
    if [[ -n "$replica_alias" ]]; then
        role_gomax=${CONSENSUSARENA_REPLICA_GOMAXPROCS:-${SLURM_CPUS_PER_TASK:-8}}
    elif [[ -n "$client_alias" ]]; then
        role_gomax=${CONSENSUSARENA_CLIENT_GOMAXPROCS:-8}
    fi
    [[ "$role_gomax" =~ ^[1-9][0-9]*$ ]] || { echo 'Invalid role GOMAXPROCS' >&2; return 1; }
    (( role_gomax <= ${SLURM_CPUS_PER_TASK:-8} )) || { echo 'GOMAXPROCS exceeds the unchanged CPU allocation' >&2; return 1; }
    # One resource record per role, outside the request path (GNU time).
    local -a command=("$binary" "$@")
    if [[ -n "${pinned_cpu:-}" ]]; then
        # Set affinity before Go starts; new threads inherit this single CPU.
        command=(taskset --cpu-list "$pinned_cpu" bash -c
            'printf "%s\n" "$$" > "$1"; shift; exec "$@"'
            pinned-replica "$run_dir/status/replica-$rank.pid" "$binary" "$@")
    fi
    GOMAXPROCS=$role_gomax /usr/bin/time \
        -o "$run_dir/stdout/resources-rank-$rank.txt" \
        -f 'elapsed_seconds=%e\nuser_seconds=%U\nsystem_seconds=%S\nmax_rss_kb=%M\nvoluntary_context_switches=%w\ninvoluntary_context_switches=%c\nexit_status=%x' \
        "${command[@]}"
}

pinned_cpu=
affinity_checked=0
affinity_measurement_checked=0
if [[ -n "$replica_alias" && "${CONSENSUSARENA_REPLICA_TASKSET:-0}" == 1 ]]; then
    [[ "${CONSENSUSARENA_REPLICA_GOMAXPROCS:-1}" == 1 ]] || { echo 'Pinned trial requires GOMAXPROCS=1' >&2; exit 1; }
    read -r pinned_cpu observer_cpus < <(python3 "$script_dir/runtime_policy.py" select "$run_dir" "$rank")
    [[ "$pinned_cpu" =~ ^[0-9]+$ && -n "$observer_cpus" ]] || exit 1
    # Restrict infrastructure before launching it: Toxiproxy and all of its
    # threads inherit this mask and cannot compete on the replica's CPU.
    taskset --pid --cpu-list "$observer_cpus" "$BASHPID" > "$run_dir/stdout/observer-affinity-$rank.txt"
fi

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
    if [[ "${CONSENSUSARENA_RESOURCES:-0}" == 1 ]]; then
        export CONSENSUSARENA_RESOURCE_RUN="$run_dir"
        if [[ "${CONSENSUSARENA_RUNTIME_POLICY:-}" == replica-single-cpu-v2 ]]; then
            export CONSENSUSARENA_RESOURCE_MODE=boundary-cpu-traffic-peak-memory-v2
        fi
    else
        unset CONSENSUSARENA_RESOURCE_RUN
    fi
    run_app -run replica -config "$config" \
        -alias "$replica_alias" \
        -log "$run_dir/logs/${replica_alias}-replica.log" \
        > "$run_dir/stdout/${replica_alias}-replica.out" 2>&1 &
    pids+=("$!")
fi

if [[ -n "$client_alias" ]]; then
    while [[ ! -f "$run_dir/status/replicas.ready" ]]; do
        [[ ! -f "$run_dir/status/stop" ]] || exit 1
        sleep 1
    done

    export CONSENSUSARENA_LATENCY_RUN="$run_dir"
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
    snapshot_after_measurement
    if [[ -n "$pinned_cpu" && "$affinity_checked" == 0 && -f "$run_dir/status/replicas.ready" ]]; then
        python3 "$script_dir/runtime_policy.py" snapshot "$run_dir" "$rank" "$pinned_cpu" ready
        affinity_checked=1
    fi
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
