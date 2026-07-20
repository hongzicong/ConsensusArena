#!/usr/bin/env bash
#SBATCH --job-name=consensusarena-latency
#SBATCH --partition=standard
#SBATCH --qos=parallel
#SBATCH --nodes=2
#SBATCH --ntasks=16
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
binary=${CONSENSUSARENA_BINARY:-$repo_root/consensusarena-linux-amd64}
scratch_root=${SCRATCH:-/scratch/$USER}
run_dir=${CONSENSUSARENA_RUN_DIR:-$scratch_root/consensusarena-$SLURM_JOB_ID}
replica_ready_timeout=${REPLICA_READY_TIMEOUT_SECONDS:-180}
client_timeout=${CLIENT_TIMEOUT_SECONDS:-300}
write_ratios_spec=${CONSENSUSARENA_WRITE_RATIOS:-0:50:100}
runner_pid=
active_run_dir=

IFS=: read -r -a write_ratios <<< "$write_ratios_spec"
if [[ ${#write_ratios[@]} == 0 ]]; then
    echo "At least one write ratio is required" >&2
    exit 1
fi
declare -A seen_write_ratios=()
for index in "${!write_ratios[@]}"; do
    write_ratio=${write_ratios[$index]//[[:space:]]/}
    if [[ ! "$write_ratio" =~ ^(0|[1-9][0-9]{0,2})$ ]] || (( write_ratio > 100 )); then
        echo "Invalid write ratio: ${write_ratios[$index]} (expected an integer from 0 to 100)" >&2
        exit 1
    fi
    if [[ -n "${seen_write_ratios[$write_ratio]:-}" ]]; then
        echo "Duplicate write ratio: $write_ratio" >&2
        exit 1
    fi
    seen_write_ratios[$write_ratio]=1
    write_ratios[$index]=$write_ratio
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
binary=$(readlink -f "$binary")

mkdir -p "$run_dir"/{config,ranks}
printf 'job_id=%s\nsubmitted_at=%s\nbinary=%s\nrepo=%s\n' \
    "$SLURM_JOB_ID" "$(date --iso-8601=seconds)" "$binary" "$repo_root" \
    > "$run_dir/metadata.txt"
printf 'write_ratios_percent=%s\n' "$(IFS=:; echo "${write_ratios[*]}")" >> "$run_dir/metadata.txt"

echo "Capturing addresses from the two allocated nodes..."
srun --nodes=2 --ntasks=16 --ntasks-per-node=8 --cpus-per-task=1 --distribution=cyclic \
    bash "$script_dir/capture-ip.sh" "$run_dir/ranks"

rank_ip() {
    tr -d '[:space:]' < "$run_dir/ranks/$1.ip"
}

cat > "$run_dir/config/address-map.txt" <<EOF
0.0.2.1 $(rank_ip 15)
0.0.0.1 $(rank_ip 0):7070
0.0.0.2 $(rank_ip 1):7071
0.0.0.3 $(rank_ip 2):7072
0.0.0.4 $(rank_ip 3):7073
0.0.0.5 $(rank_ip 4):7074
0.0.1.1 $(rank_ip 5):17000
0.0.1.2 $(rank_ip 6):17001
0.0.1.3 $(rank_ip 7):17002
0.0.1.4 $(rank_ip 8):17003
0.0.1.5 $(rank_ip 9):17004
0.0.1.6 $(rank_ip 10):17005
0.0.1.7 $(rank_ip 11):17006
0.0.1.8 $(rank_ip 12):17007
0.0.1.9 $(rank_ip 13):17008
0.0.1.10 $(rank_ip 14):17009
EOF

awk -f "$script_dir/remap-addresses.awk" \
    "$run_dir/config/address-map.txt" "$script_dir/workload.conf" \
    > "$run_dir/config/cluster.conf"
if [[ -n "${CONSENSUSARENA_PROTOCOL:-}" ]]; then
    case "${CONSENSUSARENA_PROTOCOL,,}" in
        swiftpaxos|curp|xpaxos|fastpaxos|n2paxos|paxos|epaxos) ;;
        *) echo "Unsupported protocol: $CONSENSUSARENA_PROTOCOL" >&2; exit 1 ;;
    esac
    awk -v protocol="$CONSENSUSARENA_PROTOCOL" \
        '$1 == "protocol:" { $2 = protocol } { print }' \
        "$run_dir/config/cluster.conf" > "$run_dir/config/cluster.conf.tmp"
    mv "$run_dir/config/cluster.conf.tmp" "$run_dir/config/cluster.conf"
fi
awk -f "$script_dir/remap-addresses.awk" \
    "$run_dir/config/address-map.txt" "$repo_root/latency.conf" \
    > "$run_dir/config/latency.conf"
cp "$repo_root/quorum.conf" "$run_dir/config/quorum.conf"

replicas=(ap-south-1 ap-northeast-1 eu-west-3 us-west-1 af-south-1)
repetitions=$(awk '$1 == "repetitions:" { print $2; exit }' "$run_dir/config/cluster.conf" | tr -d '\r')
if [[ ! "$repetitions" =~ ^[1-9][0-9]*$ ]]; then
    echo "Invalid repetitions value: $repetitions" >&2
    exit 1
fi
printf 'repetitions=%s\n' "$repetitions" >> "$run_dir/metadata.txt"

run_repetition() {
    local ratio_dir=$1
    local repetition=$2
    local write_ratio=$3
    local repetition_name repetition_dir deadline ready alias replica_log done_count status_file runner_exit
    repetition_name=$(printf 'repetition-%02d' "$repetition")
    repetition_dir="$ratio_dir/$repetition_name"
    active_run_dir=$repetition_dir
    mkdir -p "$repetition_dir"/{config,logs,results,status,stdout}
    cp "$ratio_dir/config/cluster.conf" "$repetition_dir/config/cluster.conf"
    cp "$ratio_dir/config/latency.conf" "$repetition_dir/config/latency.conf"
    cp "$ratio_dir/config/quorum.conf" "$repetition_dir/config/quorum.conf"

    echo "Starting write ratio ${write_ratio}% $repetition_name/$repetitions: master, five replicas, and ten clients..."
    srun --nodes=2 --ntasks=16 --ntasks-per-node=8 --cpus-per-task=8 --distribution=cyclic \
        --kill-on-bad-exit=1 bash "$script_dir/node-runner.sh" "$repetition_dir" "$binary" &
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
        printf '%s replica readiness: %s/5\r' "$repetition_name" "$ready"
        if [[ "$ready" == 5 ]]; then
            printf '\n'
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
        printf '%s clients completed: %s/10\r' "$repetition_name" "$done_count"
        if [[ "$done_count" == 10 ]]; then
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

combined_summary="$run_dir/summary.csv"
combined_details="$run_dir/repetition-summaries.csv"
: > "$combined_summary"
: > "$combined_details"

for write_ratio in "${write_ratios[@]}"; do
    ratio_dir="$run_dir/write-ratio-$write_ratio"
    mkdir -p "$ratio_dir/config"
    awk -v writes="$write_ratio" \
        '$1 == "writes:" { $2 = writes } { print }' \
        "$run_dir/config/cluster.conf" > "$ratio_dir/config/cluster.conf"
    cp "$run_dir/config/latency.conf" "$ratio_dir/config/latency.conf"
    cp "$run_dir/config/quorum.conf" "$ratio_dir/config/quorum.conf"
    printf 'write_ratio_percent=%s\nrepetitions=%s\n' \
        "$write_ratio" "$repetitions" > "$ratio_dir/metadata.txt"

    for repetition in $(seq 1 "$repetitions"); do
        run_repetition "$ratio_dir" "$repetition" "$write_ratio"
    done

    bash "$script_dir/summarize-repetitions.sh" "$ratio_dir" "$repetitions"
    if [[ ! -s "$combined_summary" ]]; then
        awk 'NR == 1 { print "WriteRatioPercent," $0 }' "$ratio_dir/summary.csv" > "$combined_summary"
        awk 'NR == 1 { print "WriteRatioPercent," $0 }' "$ratio_dir/repetition-summaries.csv" > "$combined_details"
    fi
    awk -v ratio="$write_ratio" 'NR > 1 { print ratio "," $0 }' \
        "$ratio_dir/summary.csv" >> "$combined_summary"
    awk -v ratio="$write_ratio" 'NR > 1 { print ratio "," $0 }' \
        "$ratio_dir/repetition-summaries.csv" >> "$combined_details"
done

printf 'completed_at=%s\n' "$(date --iso-8601=seconds)" >> "$run_dir/metadata.txt"
cat "$combined_summary"
printf 'Write-ratio details: %s\n' "$combined_details"
printf 'Write-ratio summary: %s\n' "$combined_summary"
echo "Run completed successfully: $run_dir"
