#!/usr/bin/env bash
#SBATCH --job-name=consensusarena-latency
#SBATCH --partition=standard
#SBATCH --qos=parallel
#SBATCH --nodes=3
#SBATCH --ntasks=24
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=8
#SBATCH --mem-per-cpu=1G
#SBATCH --time=01:00:00
#SBATCH --output=slurm-%j.out

set -euo pipefail
export LC_ALL=C

repo_root=${CONSENSUSARENA_ROOT:-${SLURM_SUBMIT_DIR:?SLURM_SUBMIT_DIR is not set}}
repo_root=$(cd "$repo_root" && pwd)
script_dir="$repo_root/slurm"
replica_counts_spec=${CONSENSUSARENA_REPLICAS:-5:9:13}
if [[ ! "$replica_counts_spec" =~ ^(5|9|13)(:(5|9|13))*$ ]]; then
    echo "Invalid replica sizes: $replica_counts_spec (expected 5, 9, or 13 separated by colons)" >&2
    exit 1
fi
IFS=: read -r -a requested_replica_counts <<< "$replica_counts_spec"
declare -A seen_replica_counts=()
max_replica_count=0
for count in "${requested_replica_counts[@]}"; do
    if [[ -n "${seen_replica_counts[$count]:-}" ]]; then
        echo "Duplicate replica count: $count" >&2
        exit 1
    fi
    seen_replica_counts[$count]=1
    if (( count > max_replica_count )); then max_replica_count=$count; fi
done
required_tasks=$((max_replica_count + 11))
required_nodes=$(((required_tasks + 7) / 8))
if (( ${SLURM_NTASKS:-0} < required_tasks || ${SLURM_JOB_NUM_NODES:-0} < required_nodes || ${SLURM_CPUS_PER_TASK:-8} < 8 )); then
    echo "Replica sizes $replica_counts_spec require at least $required_tasks tasks on $required_nodes nodes and 8 CPUs/task; use run-latency.sbatch with its default allocation" >&2
    exit 1
fi
binary=${CONSENSUSARENA_BINARY:-$repo_root/consensusarena-linux-amd64}
expected_binary_sha256=${CONSENSUSARENA_BINARY_SHA256:-}
toxiproxy_source=${CONSENSUSARENA_TOXIPROXY_SERVER:-$repo_root/toxiproxy-server-linux-amd64}
expected_toxiproxy_sha256=${CONSENSUSARENA_TOXIPROXY_SHA256:-556d891134a3c582dc1e1a3f7335fd55142e5965769855a00b944e13e48302fc}
latency_jitter_ms=${CONSENSUSARENA_LATENCY_JITTER_MS:-10}
bandwidth_kbps=${CONSENSUSARENA_BANDWIDTH_KBPS:-125000}
scratch_root=${SCRATCH:-/scratch/$USER}
run_dir=${CONSENSUSARENA_RUN_DIR:-$scratch_root/consensusarena-$SLURM_JOB_ID}
replica_ready_timeout=${REPLICA_READY_TIMEOUT_SECONDS:-900}
client_timeout=${CLIENT_TIMEOUT_SECONDS:-300}
ycsb_profiles_spec=${CONSENSUSARENA_YCSB_PROFILES:-A:B:C}
runner_pid=
active_run_dir=

[[ "$latency_jitter_ms" =~ ^[0-9]+$ ]] || {
    echo "Invalid latency jitter in milliseconds: $latency_jitter_ms" >&2
    exit 1
}
[[ "$bandwidth_kbps" =~ ^[1-9][0-9]*$ ]] || {
    echo "Invalid bandwidth in KB/s: $bandwidth_kbps" >&2
    exit 1
}
export CONSENSUSARENA_LATENCY_JITTER_MS=$latency_jitter_ms
export CONSENSUSARENA_BANDWIDTH_KBPS=$bandwidth_kbps

IFS=: read -r -a requested_profiles <<< "$ycsb_profiles_spec"
if [[ ${#requested_profiles[@]} == 0 ]]; then
    echo "At least one YCSB profile is required" >&2
    exit 1
fi
declare -a ycsb_profiles=()
declare -a write_ratios=()
declare -A seen_ycsb_profiles=()
for requested_profile in "${requested_profiles[@]}"; do
    profile=${requested_profile//[[:space:]]/}
    profile=${profile^^}
    case "$profile" in
        A) write_ratio=50 ;;
        B) write_ratio=5 ;;
        C) write_ratio=0 ;;
        *) echo "Unsupported YCSB profile: $requested_profile (expected A, B, or C)" >&2; exit 1 ;;
    esac
    if [[ -n "${seen_ycsb_profiles[$profile]:-}" ]]; then
        echo "Duplicate YCSB profile: $profile" >&2
        exit 1
    fi
    seen_ycsb_profiles[$profile]=1
    ycsb_profiles+=("$profile")
    write_ratios+=("$write_ratio")
done

cleanup() {
    if [[ -n "$active_run_dir" ]]; then
        touch "$active_run_dir/status/stop" 2>/dev/null || true
    fi
    if [[ -n "$runner_pid" ]] && kill -0 "$runner_pid" 2>/dev/null; then
        kill "$runner_pid" 2>/dev/null || true
        wait "$runner_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

if [[ ! -x "$binary" ]]; then
    echo "Native ConsensusArena binary is missing or not executable: $binary" >&2
    echo "Build consensusarena-linux-amd64 and run chmod +x before submitting." >&2
    exit 1
fi
source_binary=$(readlink -f "$binary")
if [[ ! -x "$toxiproxy_source" ]]; then
    echo "Toxiproxy server is missing or not executable: $toxiproxy_source" >&2
    echo "Fetch the pinned binary with slurm/fetch-toxiproxy.ps1, upload it, and run chmod +x." >&2
    exit 1
fi
source_toxiproxy=$(readlink -f "$toxiproxy_source")

mkdir -p "$run_dir/binary"
binary="$run_dir/binary/consensusarena-linux-amd64"
toxiproxy_binary="$run_dir/binary/toxiproxy-server-linux-amd64"
cp "$source_binary" "$binary"
cp "$source_toxiproxy" "$toxiproxy_binary"
chmod +x "$binary"
chmod +x "$toxiproxy_binary"
binary_sha256=$(sha256sum "$binary" | awk '{ print $1 }')
if [[ -n "$expected_binary_sha256" && "${binary_sha256,,}" != "${expected_binary_sha256,,}" ]]; then
    echo "Binary SHA-256 mismatch: got $binary_sha256, expected $expected_binary_sha256" >&2
    exit 1
fi
toxiproxy_sha256=$(sha256sum "$toxiproxy_binary" | awk '{ print $1 }')
if [[ "${toxiproxy_sha256,,}" != "${expected_toxiproxy_sha256,,}" ]]; then
    echo "Toxiproxy SHA-256 mismatch: got $toxiproxy_sha256, expected $expected_toxiproxy_sha256" >&2
    exit 1
fi
toxiproxy_version=$($toxiproxy_binary -version 2>&1 | tr -d '\r' | head -n 1)

printf 'job_id=%s\nsubmitted_at=%s\nbinary=%s\nbinary_source=%s\nbinary_sha256=%s\nrepo=%s\n' \
    "$SLURM_JOB_ID" "$(date --iso-8601=seconds)" "$binary" "$source_binary" "$binary_sha256" "$repo_root" \
    > "$run_dir/metadata.txt"
printf 'latency_backend=toxiproxy\ntoxiproxy_binary=%s\ntoxiproxy_source=%s\ntoxiproxy_sha256=%s\ntoxiproxy_version=%s\nlatency_rounding=%s\nlatency_jitter_ms=%s\nbandwidth_kbps=%s\nbandwidth_bits_per_second=%s\nbandwidth_scope=%s\n' \
    "$toxiproxy_binary" "$source_toxiproxy" "$toxiproxy_sha256" "$toxiproxy_version" \
    'ceil(rtt_ms/2) per stream direction' "$latency_jitter_ms" "$bandwidth_kbps" \
    "$((bandwidth_kbps * 8000))" 'per TCP connection per stream direction' >> "$run_dir/metadata.txt"
printf 'ycsb_profiles=%s\nwrite_ratios_percent=%s\n' \
    "$(IFS=:; echo "${ycsb_profiles[*]}")" \
    "$(IFS=:; echo "${write_ratios[*]}")" >> "$run_dir/metadata.txt"
printf '%s\n' \
    'ycsb_semantics=ratio-only; READ is mapped to GET; UPDATE is mapped to PUT' \
    'request_distribution=zipfian' >> "$run_dir/metadata.txt"
printf 'resource_observer=GNU time per role; exit-time CPU, RSS, context switches\n' >> "$run_dir/metadata.txt"

run_scale() {
    local run_dir="$sweep_dir/replicas-$1"
    local combined_summary combined_details
    export CONSENSUSARENA_REPLICAS=$1
    source "$script_dir/topology.sh"
    # Pin both steps to the same nodes when this size uses only part of the allocation.
    local -a scale_node_names
    local scale_nodes
    mapfile -t scale_node_names < <(scontrol show hostnames "$SLURM_JOB_NODELIST")
    scale_nodes=$(IFS=,; echo "${scale_node_names[*]:0:node_count}")
    mkdir -p "$run_dir"/{config,ranks}
    cp "$sweep_dir/metadata.txt" "$run_dir/metadata.txt"
    bash "$script_dir/prepare-topology.sh" "$run_dir/config/logical"
    cat "$run_dir/config/logical/topology-metadata.txt" >> "$run_dir/metadata.txt"
    printf 'slurm_nodes=%s\nslurm_tasks=%s\ncpus_per_task=8\n' "$node_count" "$task_count" >> "$run_dir/metadata.txt"
    echo "Capturing addresses from $node_count allocated nodes for $replica_count replicas..."
    srun --nodelist="$scale_nodes" --nodes="$node_count" --ntasks="$task_count" --ntasks-per-node="$tasks_per_node" --cpus-per-task=1 --distribution=cyclic \
        bash "$script_dir/capture-ip.sh" "$run_dir/ranks"

    rank_ip() {
        tr -d '[:space:]' < "$run_dir/ranks/$1.ip"
    }

    {
        printf '0.0.2.1 %s\n' "$(rank_ip "$master_rank")"
        for ((rank=0; rank<replica_count; rank++)); do
            printf '0.0.0.%s %s:%s\n' "$((rank + 1))" "$(rank_ip "$rank")" "$((7070 + rank))"
        done
        for ((client=0; client<client_count; client++)); do
            printf '0.0.1.%s %s:%s\n' "$((client + 1))" "$(rank_ip "$((replica_count + client))")" "$((17000 + client))"
        done
    } > "$run_dir/config/address-map.txt"

    awk -f "$script_dir/remap-addresses.awk" \
        "$run_dir/config/address-map.txt" "$run_dir/config/logical/workload.conf" \
        > "$run_dir/config/cluster.conf"
    if [[ -n "${CONSENSUSARENA_WORKLOAD_SEED:-}" ]]; then
        if [[ ! "$CONSENSUSARENA_WORKLOAD_SEED" =~ ^[0-9]+$ ]]; then
            echo "Invalid workload seed: $CONSENSUSARENA_WORKLOAD_SEED" >&2
            exit 1
        fi
        awk -v workload_seed="$CONSENSUSARENA_WORKLOAD_SEED" \
            '$1 == "workloadSeed:" { $2 = workload_seed } { print }' \
            "$run_dir/config/cluster.conf" > "$run_dir/config/cluster.conf.tmp"
        mv "$run_dir/config/cluster.conf.tmp" "$run_dir/config/cluster.conf"
    fi
    if [[ -n "${CONSENSUSARENA_PROTOCOL:-}" ]]; then
        case "${CONSENSUSARENA_PROTOCOL,,}" in
            swiftpaxos|curp|fastpaxos|n2paxos|paxos|epaxos) ;;
            *) echo "Unsupported protocol: $CONSENSUSARENA_PROTOCOL" >&2; exit 1 ;;
        esac
        awk -v protocol="$CONSENSUSARENA_PROTOCOL" \
            '$1 == "protocol:" { $2 = protocol } { print }' \
            "$run_dir/config/cluster.conf" > "$run_dir/config/cluster.conf.tmp"
        mv "$run_dir/config/cluster.conf.tmp" "$run_dir/config/cluster.conf"
    fi
    awk -f "$script_dir/remap-addresses.awk" \
        "$run_dir/config/address-map.txt" "$run_dir/config/logical/latency.conf" \
        > "$run_dir/config/latency.conf"
    cp "$run_dir/config/logical/quorum.conf" "$run_dir/config/quorum.conf"

    repetitions=$(awk '$1 == "repetitions:" { print $2; exit }' "$run_dir/config/cluster.conf" | tr -d '\r')
    if [[ ! "$repetitions" =~ ^[1-9][0-9]*$ ]]; then
        echo "Invalid repetitions value: $repetitions" >&2
        exit 1
    fi
    preload_enabled=$(awk 'tolower($1) == "preload:" { print tolower($2); exit }' "$run_dir/config/cluster.conf" | tr -d '\r')
    preload_enabled=${preload_enabled:-false}
    if [[ "$preload_enabled" != "true" && "$preload_enabled" != "false" ]]; then
        echo "Invalid preload value: $preload_enabled" >&2
        exit 1
    fi
    preload_records=$(awk 'tolower($1) == "keycount:" { print $2; exit }' "$run_dir/config/cluster.conf" | tr -d '\r')
    preload_value_size=$(awk 'tolower($1) == "commandsize:" { print $2; exit }' "$run_dir/config/cluster.conf" | tr -d '\r')
    preload_seed=$(awk 'tolower($1) == "preloadseed:" { print $2; exit }' "$run_dir/config/cluster.conf" | tr -d '\r')
    preload_seed=${preload_seed:-1}
    workload_seed=$(awk 'tolower($1) == "workloadseed:" { print $2; exit }' "$run_dir/config/cluster.conf" | tr -d '\r')
    workload_seed=${workload_seed:-1}
    if [[ ! "$workload_seed" =~ ^[0-9]+$ ]]; then
        echo "Invalid workload seed: $workload_seed" >&2
        exit 1
    fi
    if [[ "$preload_enabled" == "true" ]]; then
        if [[ ! "$preload_records" =~ ^[1-9][0-9]*$ ]]; then
            echo "Invalid preload record count: $preload_records" >&2
            exit 1
        fi
        if [[ ! "$preload_value_size" =~ ^[1-9][0-9]*$ ]]; then
            echo "Invalid preload value size: $preload_value_size" >&2
            exit 1
        fi
        if [[ ! "$preload_seed" =~ ^[0-9]+$ ]]; then
            echo "Invalid preload seed: $preload_seed" >&2
            exit 1
        fi
    fi
    printf 'repetitions=%s\nworkload_seed=%s\npreload_enabled=%s\npreload_records=%s\npreload_value_size=%s\npreload_seed=%s\n' \
        "$repetitions" "$workload_seed" "$preload_enabled" "$preload_records" "$preload_value_size" "$preload_seed" \
        >> "$run_dir/metadata.txt"

    run_repetition() {
        local profile_dir=$1
        local repetition=$2
        local profile=$3
        local write_ratio=$4
        local repetition_name repetition_dir deadline ready alias replica_log done_count status_file runner_exit digest preload_digest
        repetition_name=$(printf 'repetition-%02d' "$repetition")
        repetition_dir="$profile_dir/$repetition_name"
        active_run_dir=$repetition_dir
        mkdir -p "$repetition_dir"/{config,logs,results,status,stdout}
        cp "$profile_dir/config/cluster.conf" "$repetition_dir/config/cluster.conf"
        cp "$profile_dir/config/address-map.txt" "$repetition_dir/config/address-map.txt"
        cp "$profile_dir/config/latency.conf" "$repetition_dir/config/latency.conf"
        cp "$profile_dir/config/quorum.conf" "$repetition_dir/config/quorum.conf"
        printf 'ycsb_profile=%s\nwrite_ratio_percent=%s\nworkload_seed=%s\npreload_enabled=%s\n' \
            "$profile" "$write_ratio" "$workload_seed" "$preload_enabled" > "$repetition_dir/metadata.txt"

        echo "Starting YCSB profile $profile (${write_ratio}% writes) $repetition_name/$repetitions through Toxiproxy..."
        srun --nodelist="$scale_nodes" --nodes="$node_count" --ntasks="$task_count" --ntasks-per-node="$tasks_per_node" --cpus-per-task=8 --distribution=cyclic \
            --kill-on-bad-exit=1 bash "$script_dir/node-runner.sh" \
            "$repetition_dir" "$binary" "$toxiproxy_binary" &
        runner_pid=$!

        deadline=$((SECONDS + replica_ready_timeout))
        while true; do
            ready=0
            for alias in "${replicas[@]}"; do
                replica_log="$repetition_dir/logs/${alias}-replica.log"
                if [[ -f "$replica_log" ]] && grep -q 'Waiting for client connections' "$replica_log"; then
                    ready=$((ready + 1))
                fi
            done
            printf '%s replica readiness: %s/%s\r' "$repetition_name" "$ready" "$replica_count"
            if [[ "$ready" == "$replica_count" ]]; then
                printf '\n'
                if [[ "$preload_enabled" == "true" ]]; then
                    preload_digest=
                    for alias in "${replicas[@]}"; do
                        replica_log="$repetition_dir/logs/${alias}-replica.log"
                        digest=$(awk '/PRELOAD_COMPLETE/ { for (field = 1; field <= NF; field++) if ($field ~ /^digest=/) { sub(/^digest=/, "", $field); value = $field } } END { print value }' "$replica_log")
                        if [[ -z "$digest" ]]; then
                            echo "Replica $alias did not report a preload digest" >&2
                            return 1
                        fi
                        if [[ -z "$preload_digest" ]]; then
                            preload_digest=$digest
                        elif [[ "$digest" != "$preload_digest" ]]; then
                            echo "Preload digest mismatch: replica $alias reported $digest, expected $preload_digest" >&2
                            return 1
                        fi
                    done
                    printf 'preload_digest=%s\n' "$preload_digest" >> "$repetition_dir/metadata.txt"
                    echo "Verified preload digest $preload_digest across $replica_count replicas"
                fi
                touch "$repetition_dir/status/replicas.ready"
                break
            fi
            if [[ -f "$repetition_dir/status/node-failed" ]] || ! kill -0 "$runner_pid" 2>/dev/null; then
                echo "A master or replica failed; inspect $repetition_dir/stdout" >&2
                return 1
            fi
            if (( SECONDS >= deadline )); then
                echo "Replicas did not become ready within $replica_ready_timeout seconds" >&2
                return 1
            fi
            sleep 2
        done

        deadline=$((SECONDS + client_timeout))
        while true; do
            done_count=$(find "$repetition_dir/status" -maxdepth 1 -name 'client-*.done' | wc -l | tr -d ' ')
            printf '%s clients completed: %s/%s\r' "$repetition_name" "$done_count" "$client_count"
            if [[ "$done_count" == "$client_count" ]]; then
                printf '\n'
                break
            fi
            if [[ -f "$repetition_dir/status/node-failed" ]] || ! kill -0 "$runner_pid" 2>/dev/null; then
                echo "A workload process failed; inspect $repetition_dir/stdout" >&2
                return 1
            fi
            if (( SECONDS >= deadline )); then
                echo "Clients did not finish within $client_timeout seconds" >&2
                return 1
            fi
            sleep 3
        done

        for status_file in "$repetition_dir"/status/client-*.exit; do
            if [[ $(tr -d '[:space:]' < "$status_file") != 0 ]]; then
                echo "Client failed according to $status_file" >&2
                return 1
            fi
        done

        bash "$script_dir/summarize-latency.sh" "$repetition_dir"
        touch "$repetition_dir/status/stop"
        set +e
        wait "$runner_pid"
        runner_exit=$?
        set -e
        runner_pid=
        active_run_dir=
        if [[ "$runner_exit" != 0 ]]; then
            echo "$repetition_name runner failed with exit code $runner_exit" >&2
            return 1
        fi
    }

    protocol=$(awk '$1 == "protocol:" { print $2; exit }' "$run_dir/config/cluster.conf" | tr -d '\r')
    printf 'protocol=%s\n' "$protocol" >> "$run_dir/metadata.txt"
    combined_summary="$run_dir/summary.csv"
    combined_details="$run_dir/repetition-summaries.csv"
    : > "$combined_summary"
    : > "$combined_details"

    for index in "${!ycsb_profiles[@]}"; do
        profile=${ycsb_profiles[$index]}
        write_ratio=${write_ratios[$index]}
        read_ratio=$((100 - write_ratio))
        profile_dir="$run_dir/ycsb-$profile"
        mkdir -p "$profile_dir/config"
        awk -v writes="$write_ratio" \
            '$1 == "writes:" { $2 = writes } { print }' \
            "$run_dir/config/cluster.conf" > "$profile_dir/config/cluster.conf"
        cp "$run_dir/config/address-map.txt" "$profile_dir/config/address-map.txt"
        cp "$run_dir/config/latency.conf" "$profile_dir/config/latency.conf"
        cp "$run_dir/config/quorum.conf" "$profile_dir/config/quorum.conf"
        printf 'ycsb_profile=%s\nread_ratio_percent=%s\nwrite_ratio_percent=%s\nrepetitions=%s\nworkload_seed=%s\npreload_enabled=%s\npreload_records=%s\npreload_value_size=%s\npreload_seed=%s\n' \
            "$profile" "$read_ratio" "$write_ratio" "$repetitions" "$workload_seed" "$preload_enabled" \
            "$preload_records" "$preload_value_size" "$preload_seed" > "$profile_dir/metadata.txt"

        for repetition in $(seq 1 "$repetitions"); do
            run_repetition "$profile_dir" "$repetition" "$profile" "$write_ratio"
        done

        bash "$script_dir/summarize-repetitions.sh" "$profile_dir" "$repetitions"
        if [[ ! -s "$combined_summary" ]]; then
            awk 'NR == 1 { print "ReplicaCount,Protocol,YCSBProfile,ReadRatioPercent,WriteRatioPercent," $0 }' "$profile_dir/summary.csv" > "$combined_summary"
            awk 'NR == 1 { print "ReplicaCount,Protocol,YCSBProfile,ReadRatioPercent,WriteRatioPercent," $0 }' "$profile_dir/repetition-summaries.csv" > "$combined_details"
        fi
        awk -v replica_count="$replica_count" -v protocol="$protocol" -v profile="$profile" -v read_ratio="$read_ratio" -v write_ratio="$write_ratio" \
            'NR > 1 { print replica_count "," protocol "," profile "," read_ratio "," write_ratio "," $0 }' \
            "$profile_dir/summary.csv" >> "$combined_summary"
        awk -v replica_count="$replica_count" -v protocol="$protocol" -v profile="$profile" -v read_ratio="$read_ratio" -v write_ratio="$write_ratio" \
            'NR > 1 { print replica_count "," protocol "," profile "," read_ratio "," write_ratio "," $0 }' \
            "$profile_dir/repetition-summaries.csv" >> "$combined_details"
    done

    printf 'completed_at=%s\n' "$(date --iso-8601=seconds)" >> "$run_dir/metadata.txt"
    echo "Replica size $replica_count completed: $run_dir"
}

sweep_dir=$run_dir
printf 'replica_counts=%s\nallocated_nodes=%s\nallocated_tasks=%s\ncpus_per_task=%s\n' \
    "$replica_counts_spec" "$SLURM_JOB_NUM_NODES" "$SLURM_NTASKS" "${SLURM_CPUS_PER_TASK:-8}" >> "$run_dir/metadata.txt"
for filename in summary.csv repetition-summaries.csv; do
    : > "$run_dir/$filename"
done
for requested_count in "${requested_replica_counts[@]}"; do
    run_scale "$requested_count"
    for filename in summary.csv repetition-summaries.csv; do
        source_csv="$run_dir/replicas-$requested_count/$filename"
        if [[ ! -s "$run_dir/$filename" ]]; then
            head -n 1 "$source_csv" > "$run_dir/$filename"
        fi
        tail -n +2 "$source_csv" >> "$run_dir/$filename"
    done
done
combined_summary="$run_dir/summary.csv"
combined_details="$run_dir/repetition-summaries.csv"
printf 'protocol=%s\n' "$protocol" >> "$run_dir/metadata.txt"
printf 'completed_at=%s\n' "$(date --iso-8601=seconds)" >> "$run_dir/metadata.txt"
cat "$combined_summary"
printf 'YCSB profile details: %s\n' "$combined_details"
printf 'YCSB profile summary: %s\n' "$combined_summary"
echo "Run completed successfully: $run_dir"
