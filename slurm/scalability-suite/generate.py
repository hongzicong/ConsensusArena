#!/usr/bin/env python3
"""Freeze KCensus AWS scalability matrices into the existing generated-suite format."""
import argparse
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import tomllib

ROOT = Path(__file__).resolve().parent


def generate(output, deployments=('aws-random',)):
    if output.exists() and any(output.iterdir()):
        raise FileExistsError('refusing to replace nonempty output: ' + str(output))
    template = (ROOT.parent / 'kcensus-suite/workload-template.conf').read_text()
    cases, mapping = [], {}
    for n in range(3, 32, 2):
        for index, deployment in enumerate(deployments):
            source = ROOT / 'sources' / deployment / f'{n}.toml'
            raw = source.read_bytes()
            cfg = tomllib.loads(raw.decode(), parse_float=Decimal)
            regions, matrix = cfg['regions'], cfg['raw_latencies']
            if (len(regions) != n or len(set(regions)) != n or len(matrix) != n
                    or any(len(row) != n for row in matrix)
                    or any(not Decimal(v).is_finite() or v < 0 for row in matrix for v in row)
                    or any(matrix[i][i] != 0 for i in range(n))):
                raise ValueError('invalid AWS source: ' + str(source))
            replicas = [f'replica-{r}' for r in regions]
            clients = [f'client-{r}' for r in regions]
            workload = template
            for key, value in dict(writes=5, commandSize=16, clones=0,
                    arrivalRate=format(20000/n, '.15g'), warmup='5s', duration='10s',
                    repetitions=1, keyCount=100000, zipfSkew=0.99, shuffleKeys='true',
                    workloadSeed=1, preload='true', preloadSeed=1).items():
                workload, count = re.subn(r'^'+key+r':.*$', f'{key}: {value}', workload, flags=re.M)
                if count != 1:
                    raise ValueError('template requires exactly one ' + key)
            lines = ['-- Replicas --'] + [f'{a} 0.0.0.{i+1}' for i, a in enumerate(replicas)]
            lines += ['', '-- Clients --'] + [f'{a} 0.0.1.{i+1}' for i, a in enumerate(clients)]
            lines += ['', '-- Master --', 'm0 0.0.2.1', '', workload.rstrip(),
                      '', 'keyMappingSeed: 1', '', '-- Proxy --']
            for replica, client in zip(replicas, clients):
                lines += [f'server_alias {replica}', f'{client} (local)', '']
            lines += ['---', '']
            endpoints = [(f'0.0.{group}.{i+1}', i) for group in (0, 1) for i in range(n)]
            latency = ''.join(f'{a} {b} {Decimal(matrix[i][j])*2:f}ms\n'
                              for a, i in endpoints for b, j in endpoints)
            name = f'n{n}-c{n}-topology-{index:02d}'
            folder = output / name
            folder.mkdir(parents=True)
            metadata = dict(deployment=deployment, regions=regions, seed=1,
                topology_model='kcensus_aws_snapshot',
                replica_regions=dict(zip(replicas, regions)), client_regions=dict(zip(clients, regions)),
                source=dict(path=f'configs/{deployment}/{n}.toml',
                    bundled_path=f'../sources/{deployment}/{n}.toml',
                    sha256=hashlib.sha256(raw).hexdigest(),
                    conversion='2 * directed one-way milliseconds; emulator halves and rounds up'),
                network=dict(latency_jitter_ms=0, bandwidth_kbps=125000),
                workload=dict(profiles=['B'], total_arrival_rate=20000,
                    arrival_rate_per_client=20000/n, key_count=100000, zipf_skew=0.99,
                    command_size_bytes=16, workload_seed=1, key_mapping_seed=1),
                resources=dict(enabled=True, window_seconds=[5, 15]))
            for filename, text in [('workload.conf', '\n'.join(lines)), ('latency.conf', latency),
                    ('case.json', json.dumps(metadata, indent=2)+'\n')]:
                (folder / filename).write_text(text, encoding='utf-8', newline='\n')
            cases.append(name)
            mapping[name] = deployment
    (output / 'manifest.json').write_text(json.dumps(dict(
        name='scalability-B-zipf099-20krps', topologies=cases, deployments=mapping), indent=2)+'\n',
        encoding='utf-8', newline='\n')
    print(f'Generated {len(cases)} workload-B scenes in {output}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('output', type=Path, nargs='?', default=ROOT / 'generated')
    parser.add_argument('--include-paris', action='store_true', help='opt in to the deferred second deployment')
    args = parser.parse_args()
    generate(args.output.resolve(), ('aws-random', 'aws-from-paris') if args.include_paris else ('aws-random',))
