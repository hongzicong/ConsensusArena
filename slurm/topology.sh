#!/usr/bin/env bash
# Shared rank layout. Source this file before using the topology arrays.
replica_count=${CONSENSUSARENA_REPLICAS:-5}
case "$replica_count" in
    5|9|13) ;;
    *) echo "Unsupported replica count: $replica_count (expected 5, 9, or 13)" >&2; return 1 ;;
esac
replicas=(ap-south-1 ap-northeast-1 eu-west-3 us-west-1 af-south-1
          eu-west-1 us-east-1 ap-southeast-2 sa-east-1
          ap-east-1 ca-central-1 us-east-2 us-west-2)
replicas=("${replicas[@]:0:replica_count}")
clients=(ap-east-1 ap-northeast-1 ap-southeast-2 eu-west-1 ca-central-1
         sa-east-1 us-east-1 us-east-2 us-west-1 us-west-2)
client_count=${#clients[@]}
master_rank=$((replica_count + client_count))
task_count=$((master_rank + 1))
node_count=$(((task_count + 7) / 8))
# Match the cyclic srun layout so its last node has enough allocated CPUs.
tasks_per_node=$(((task_count + node_count - 1) / node_count))

source_logical_for_rank() {
    local source_rank=$1
    if (( source_rank >= 0 && source_rank < replica_count )); then
        printf '0.0.0.%s\n' "$((source_rank + 1))"
    elif (( source_rank >= replica_count && source_rank < master_rank )); then
        printf '0.0.1.%s\n' "$((source_rank - replica_count + 1))"
    else
        echo "Rank $source_rank does not originate benchmark data connections" >&2
        return 1
    fi
}
