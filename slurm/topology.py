#!/usr/bin/env python3
"""Shared generated layouts and the fixed CloudPing 1Y/P50 RTT inputs."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shlex
import shutil

ROOT = Path(__file__).resolve().parent
COUNTS = (5, 9, 13)
TOPOLOGIES = (1, 2, 3)


def generated_layout(case):
    """Load raw generated inputs; deployment decisions belong to the protocol."""
    case = Path(case).resolve()
    match = re.fullmatch(r'n(\d+)-c(\d+)-topology-(\d+)', case.name)
    if not match:
        raise ValueError('invalid generated case name: ' + case.name)
    names = ('case.json', 'workload.conf', 'latency.conf')
    digest = hashlib.sha256()
    for name in names:
        data = (case / name).read_bytes()
        digest.update(name.encode() + b'\0' + data + b'\0')
    metadata = json.loads((case / 'case.json').read_text())
    # Preserve configuration order: it determines replica IDs and Slurm ranks.
    section, members = None, {'replicas': [], 'clients': []}
    for line in (case / 'workload.conf').read_text().splitlines():
        fields = line.lower().split('//', 1)[0].split()
        if not fields:
            continue
        if fields[0] == '--':
            if len(fields) < 2:
                raise ValueError('invalid generated membership section: ' + case.name)
            section = fields[1]
        elif fields[0].endswith(':'):
            section = None
        elif section in members:
            if len(fields) != 2:
                raise ValueError('invalid generated membership entry: ' + case.name)
            members[section].append(tuple(fields))
    replicas = [alias for alias, address in members['replicas']]
    clients = [alias for alias, address in members['clients']]
    n, c, index = map(int, match.groups())
    if (not 3 <= n <= 17 or n % 2 == 0 or c < 1 or n + c > 63
            or len(replicas) != n or len(clients) != c
            or len(set(replicas + clients)) != n + c):
        raise ValueError('invalid generated membership: ' + case.name)
    # The launcher maps these logical endpoints to participant ranks by index.
    for group, prefix in (('replicas', '0.0.0'), ('clients', '0.0.1')):
        if any(address != f'{prefix}.{i+1}' for i, (alias, address) in enumerate(members[group])):
            raise ValueError('invalid generated membership address/order: ' + case.name)
    network = metadata.get('network')
    if not isinstance(network, dict):
        raise ValueError('invalid generated network controls')
    latency_jitter_ms = network.get('latency_jitter_ms')
    bandwidth_kbps = network.get('bandwidth_kbps')
    if (type(latency_jitter_ms) is not int or latency_jitter_ms < 0
            or type(bandwidth_kbps) is not int or bandwidth_kbps <= 0):
        raise ValueError('invalid generated network controls')
    return dict(topology_id=index + 1, replica_count=n, replicas=replicas, clients=clients,
                case=case.name, case_source=str(case), case_sha256=digest.hexdigest(),
                latency_jitter_ms=latency_jitter_ms, bandwidth_kbps=bandwidth_kbps)


def generated_layouts(root):
    root = Path(root).resolve()
    manifest = json.loads((root / 'manifest.json').read_text())
    names = manifest['topologies']
    if not names or len(set(names)) != len(names):
        raise ValueError('empty or duplicate generated manifest cases')
    layouts = []
    for name in names:
        case = (root / name).resolve()
        if case.parent != root:
            raise ValueError('generated case must be directly inside its suite')
        layouts.append(generated_layout(case))
    return layouts


def load_layout(count=None, topology=None):
    if os.environ.get('CONSENSUSARENA_CASE'):
        layout = generated_layout(os.environ['CONSENSUSARENA_CASE'])
        expected = os.environ.get('CONSENSUSARENA_CASE_SHA256')
        if expected and layout['case_sha256'] != expected:
            raise ValueError('generated inputs changed after suite planning')
        if count is not None and int(count) != layout['replica_count']:
            raise ValueError('generated replica count mismatch')
        if topology is not None and int(topology) != layout['topology_id']:
            raise ValueError('generated topology ID mismatch')
        return layout
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
    if 'case' in layout:
        source, output = Path(layout['case_source']), Path(output)
        if source == output.resolve() or source in output.resolve().parents:
            raise ValueError('runtime output must not overwrite generated inputs')
        output.mkdir(parents=True, exist_ok=True)
        options = (source / 'workload.conf').read_text()
        options = re.sub(r'^(?:requests|leader|fastQuorum|warmup|duration|repetitions):.*\n?', '', options, flags=re.M | re.I)
        options = options.rstrip() + '\nwarmup: 5s\nduration: 10s\nrepetitions: 1\n'
        write_text(output / 'workload.conf', options)
        shutil.copyfile(source / 'workload.conf', output / 'input-workload.conf')
        for name in ('latency.conf', 'case.json'):
            shutil.copyfile(source / name, output / name)
        layout.update(latency_source='generated', latency_unit='RTT milliseconds',
                      topology_sha256=hashlib.sha256((source / 'latency.conf').read_bytes()).hexdigest(),
                      repetitions=1, experiment_design='generated_one_run_each',
                      warmup_s=5, measurement_s=10, fault_budget=len(layout['replicas'])//2,
                      majority_size=len(layout['replicas'])//2+1)
        write_text(output / 'topology.json', json.dumps(layout, indent=2)+'\n')
        write_text(output / 'topology-metadata.txt', ''.join(f'{key}={json.dumps(value) if isinstance(value,list) else value}\n' for key,value in layout.items()))
        return layout
    matrix, source_hash = load_matrix()
    replicas, clients = layout['replicas'], layout['clients']
    if not set(replicas + clients).issubset(matrix):
        raise ValueError('layout refers to a region absent from the snapshot')
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    # Deployment policies stay in each protocol's plan.go. This generates only
    # membership and latency inputs, never leader/quorum/responder choices.
    options = (ROOT / 'workload.conf').read_text().strip()
    # Each topology has its own reproducible hotspot layout, shared across
    # protocols, profiles, replica counts and all clients within that topology.
    key_mapping_seed = layout['topology_id']
    workload = '-- Replicas --\n' + ''.join(f'{region} 0.0.0.{i+1}\n' for i, region in enumerate(replicas))
    workload += '\n-- Clients --\n' + ''.join(f'{region} 0.0.1.{i+1}\n' for i, region in enumerate(clients))
    workload += '\n-- Master --\nm0 0.0.2.1\n\n' + options
    workload += f'\n\nkeyMappingSeed: {key_mapping_seed}\n\n-- Proxy --\n---\n'
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
                  key_mapping_seed=key_mapping_seed, key_mapping_seed_policy='topology_id',
                  fault_budget=len(replicas)//2, majority_size=len(replicas)//2+1)
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
    if 'case' in layout:
        values.update(case_latency_jitter_ms=layout['latency_jitter_ms'], case_bandwidth_kbps=layout['bandwidth_kbps'])
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
