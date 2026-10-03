#!/usr/bin/env python3
"""Shared experiment layouts and frozen CloudPing 1Y/P50 RTT inputs."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import shlex

ROOT = Path(__file__).resolve().parent
COUNTS = (5, 9, 13)
TOPOLOGIES = (1, 2, 3)


def load_layout(count=None, topology=None):
    count = int(os.environ.get('CONSENSUSARENA_REPLICAS', '5').split(':')[0] if count is None else count)
    topology = int(os.environ.get('CONSENSUSARENA_TOPOLOGY', '1') if topology is None else topology)
    if count not in COUNTS or topology not in TOPOLOGIES:
        raise ValueError('expected 5/9/13 replicas and topology 1/2/3')
    catalog = json.loads((ROOT / 'topologies.json').read_text())
    layout = catalog['topologies'][str(topology)]
    replicas, clients = layout['replicas'][:count], layout['clients']
    if len(replicas) != count or len(set(replicas)) != count or len(clients) != 10 or len(set(clients)) != 10:
        raise ValueError('invalid layout membership')
    return dict(topology_id=topology, replica_count=count, replicas=replicas, clients=clients)


def load_matrix():
    path = ROOT / 'cloudping-1y-p50.json'
    snapshot = json.loads(path.read_text())
    meta = snapshot['metadata']
    if (meta['percentile'], meta['timeframe'], meta['unit']) != ('p_50', '1Y', 'milliseconds'):
        raise ValueError('expected CloudPing 1Y P50 in milliseconds')
    matrix = snapshot['data']
    regions = set(matrix)
    if len(regions) != 35:
        raise ValueError('the frozen CloudPing snapshot must contain 35 regions')
    for source, row in matrix.items():
        if set(row) != regions or any(not isinstance(v, (int, float)) or not math.isfinite(v) or v < 0 for v in row.values()):
            raise ValueError('incomplete/invalid matrix row: ' + source)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    provenance = json.loads((ROOT / 'cloudping-1y-p50.metadata.json').read_text())
    if digest != provenance['raw_sha256']:
        raise ValueError('CloudPing snapshot does not match recorded SHA256')
    return matrix, digest


def write_text(path, text):
    with path.open('w', encoding='utf-8', newline='\n') as output:
        output.write(text)


def prepare(output, count=None, topology=None):
    layout = load_layout(count, topology)
    matrix, source_hash = load_matrix()
    replicas, clients = layout['replicas'], layout['clients']
    if not set(replicas + clients).issubset(matrix):
        raise ValueError('layout refers to a region absent from the snapshot')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    # Deployment policies stay in each protocol's plan.go. This generates only
    # membership and latency inputs, never leader/quorum/responder choices.
    template = (ROOT / 'workload.conf').read_text()
    options = template.split('-- Master --', 1)[1].split('-- Proxy --', 1)[0].strip()
    workload = '-- Replicas --\n' + ''.join(f'{region} 0.0.0.{i+1}\n' for i, region in enumerate(replicas))
    workload += '\n-- Clients --\n' + ''.join(f'{region} 0.0.1.{i+1}\n' for i, region in enumerate(clients))
    workload += '\n-- Master --\n' + options + '\n\n-- Proxy --\n---\n'
    write_text(output / 'workload.conf', workload)
    endpoints = [(f'0.0.0.{i+1}', region) for i, region in enumerate(replicas)]
    endpoints += [(f'0.0.1.{i+1}', region) for i, region in enumerate(clients)]
    lines = []
    for source, source_region in endpoints:
        for target, target_region in endpoints:
            # One process cannot have network latency to itself. Preserve the
            # measured regional diagonal for distinct colocated processes.
            rtt = 0 if source == target else matrix[source_region][target_region]
            lines.append(f'{source} {target} {rtt}ms\n')
    write_text(output / 'latency.conf', ''.join(lines))
    provenance = json.loads((ROOT / 'cloudping-1y-p50.metadata.json').read_text())
    layout.update(latency_source=provenance['source_url'], latency_timeframe='1Y', latency_percentile='P50',
                  latency_unit='RTT milliseconds', latency_snapshot_sha256=source_hash,
                  latency_fetched_at_utc=provenance['fetched_at_utc'], region_count=35,
                  topology_sha256=hashlib.sha256((output/'latency.conf').read_bytes()).hexdigest(),
                  repetitions=1, experiment_design='three_topologies_one_run_each',
                  fault_budget=len(replicas)//2, majority_size=len(replicas)//2+1, placement='protocol-plan.go')
    write_text(output / 'topology.json', json.dumps(layout, indent=2)+'\n')
    write_text(output / 'topology-metadata.txt', ''.join(f'{key}={json.dumps(value) if isinstance(value,list) else value}\n' for key,value in layout.items()))
    return layout


def shell(layout):
    count = layout['replica_count']
    master = count + len(layout['clients'])
    tasks = master + 1
    nodes = (tasks + 7)//8
    values = dict(replica_count=count, topology_id=layout['topology_id'], client_count=len(layout['clients']),
                  master_rank=master, task_count=tasks, node_count=nodes, tasks_per_node=(tasks+nodes-1)//nodes)
    for name, value in values.items():
        print(f'{name}={value}')
    for name in ('replicas', 'clients'):
        print(name + '=(' + ' '.join(shlex.quote(v) for v in layout[name]) + ')')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('shell', 'prepare', 'inspect'))
    parser.add_argument('output', nargs='?')
    parser.add_argument('--replicas', type=int)
    parser.add_argument('--topology', type=int)
    args = parser.parse_args()
    layout = load_layout(args.replicas, args.topology)
    if args.mode == 'shell':
        shell(layout)
    elif args.mode == 'prepare':
        if not args.output:
            parser.error('prepare requires an output directory')
        prepare(args.output, args.replicas, args.topology)
    else:
        print(json.dumps(layout, indent=2))


if __name__ == '__main__':
    main()
