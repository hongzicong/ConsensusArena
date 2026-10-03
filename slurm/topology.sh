#!/usr/bin/env bash
# One catalog is shared by the latency/fault harnesses and their rank workers.
topology_script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
topology_shell=$("${CONSENSUSARENA_PYTHON:-python3}" "$topology_script_dir/topology.py" shell) || return 1
eval "$topology_shell"
unset topology_shell topology_script_dir

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
