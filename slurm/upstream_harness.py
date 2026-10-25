#!/usr/bin/env python3
"""Paxos scene adapter with synchronized latency and fixed-window fault observations."""
import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import struct

from topology import load_layout, load_matrix
from master_placement import plan as master_plan

ROOT = Path(__file__).resolve().parent.parent
UPSTREAM = '35c69365f1c7737a08e237bfbaf828ee68897080'
BENCHMARK_REFERENCE_COMMIT = 'd93237aac869d897fb4370d7b4852e2b79535d26'
PROTOCOLS = ('paxos',)
TIMEOUT = 3600
WRITES = {'A': 50, 'B': 5, 'C': 0}


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8')


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def selection(env):
    counts = [int(n) for n in env.get('CONSENSUSARENA_REPLICAS', '5:9:13').split(':')]
    profiles = env.get('CONSENSUSARENA_YCSB_PROFILES', 'A:B:C').upper().split(':')
    if len(set(counts)) != len(counts) or any(n not in (5, 9, 13) for n in counts):
        raise ValueError('expected distinct replica counts from 5:9:13')
    if len(set(profiles)) != len(profiles) or any(p not in WRITES for p in profiles):
        raise ValueError('expected distinct ratio profiles from A:B:C')
    cases = {(n, t) for n in counts for t in (1, 2, 3)}
    if env.get('CONSENSUSARENA_CASES'):
        entries = [tuple(map(int, item.split('-'))) for item in env['CONSENSUSARENA_CASES'].split(':')]
        if len(set(entries)) != len(entries) or any(item not in cases for item in entries):
            raise ValueError('invalid, duplicate, or unrequested size-topology cases')
        cases = set(entries)
    return [dict(replicas=n, topology_id=t, profile=p) for n in counts
            for t in (1, 2, 3) if (n, t) in cases for p in profiles]


def settings(env, kind='latency'):
    protocol = env.get('CONSENSUSARENA_PROTOCOL', 'paxos').lower()
    if protocol not in PROTOCOLS:
        raise ValueError('unsupported upstream protocol: ' + protocol)
    if kind not in ('latency', 'fault'):
        raise ValueError('expected latency or fault suite')
    if env.get('CONSENSUSARENA_SUITE', 'fixed') != 'fixed':
        raise ValueError('this adapter supports only the fixed topology suite')
    mode = env.get('CONSENSUSARENA_BENCHMARK_MODE', 'matched')
    if mode != 'matched':
        raise ValueError('synchronized latency/fault suites require benchmark mode=matched')
    if env.get('UPSTREAM_SMOKE_ONLY'):
        raise ValueError('single-node smoke mode has been removed')
    if env.get('CONSENSUSARENA_CLIENT_WEIGHTS'):
        raise ValueError('custom client weights are not supported by this adapter')
    if any(key.startswith('UPSTREAM_') and key in ('UPSTREAM_REQUESTS', 'UPSTREAM_COMMAND_SIZE',
                                                 'UPSTREAM_PENDING', 'UPSTREAM_CONFLICTS') for key in env):
        raise ValueError('legacy UPSTREAM_* workload exports are not supported by synchronized suites')
    defaults = dict(re.findall(r'^(\w+):\s*(\S+)', (ROOT / 'slurm/workload.conf').read_text(), re.M))
    options = dict(protocol=protocol, benchmark_mode=mode, workload_kind='matched-master-poisson-zipf',
                   seed_policy='master-alias-clone-derived', clones=0, preload=True,
                   shuffle_keys=True, warmup_kind='shared-epoch-linear-ramp' if kind == 'latency' else 'shared-epoch-constant-rate',
                   diagnostic_only=False, fault_support=True, suite_kind=kind,
                   workload_model='synchronized-ramp-sustain-v1' if kind == 'latency' else 'fixed-window-crash-stop-v1',
                   latency_origin='scheduled_arrival',
                   upstream_commit=UPSTREAM, placement_objective=env.get('CONSENSUSARENA_QUORUM_OBJECTIVE', 'slow-first'))
    for field, key, variable, convert in (
        ('arrival_rate', 'arrivalRate', 'CONSENSUSARENA_ARRIVAL_RATE', float),
        ('command_size', 'commandSize', 'CONSENSUSARENA_COMMAND_SIZE', int),
        ('key_count', 'keyCount', 'CONSENSUSARENA_KEY_COUNT', int),
        ('zipf_skew', 'zipfSkew', 'CONSENSUSARENA_ZIPF_SKEW', float),
        ('workload_seed', 'workloadSeed', 'CONSENSUSARENA_WORKLOAD_SEED', int),
        ('preload_seed', 'preloadSeed', 'CONSENSUSARENA_PRELOAD_SEED', int)):
        options[field] = convert(env.get(variable, defaults[key]))
    for field, key, variable in (('warmup', 'warmup', 'CONSENSUSARENA_WARMUP'),
                                 ('duration', 'duration', 'CONSENSUSARENA_DURATION')):
        options[field] = env.get(variable, defaults[key])
    if (not math.isfinite(options['arrival_rate']) or options['arrival_rate'] <= 0
            or not math.isfinite(options['zipf_skew']) or options['zipf_skew'] < 0
            or min(options['command_size'], options['key_count']) <= 0
            or min(options['workload_seed'], options['preload_seed']) < 0
            or duration_seconds(options['warmup']) < 0 or duration_seconds(options['duration']) <= 0):
        raise ValueError('invalid matched workload setting')
    if options['placement_objective'] not in ('slow-first', 'fast-first'):
        raise ValueError('invalid placement objective')
    options['benchmark_reference_commit'] = BENCHMARK_REFERENCE_COMMIT
    if kind == 'fault':
        if int(env.get('FAULT_REPETITIONS', '1')) != 1:
            raise ValueError('fixed topology suites require FAULT_REPETITIONS=1')
        options.update(warmup='5s', duration='35s',
                       arrival_rate=float(env.get('FAULT_ARRIVAL_RATE', options['arrival_rate'])))
        if not math.isfinite(options['arrival_rate']) or options['arrival_rate'] <= 0:
            raise ValueError('invalid fault arrival rate')
    elif (options['warmup'], options['duration']) != ('5s', '10s'):
        raise ValueError('latency suites require 5s warmup and 10s measurement')
    frozen = env.get('CONSENSUSARENA_SETTINGS_JSON')
    if frozen and json.loads(frozen) != options:
        raise ValueError('workload settings changed after submission')
    return options


def prepare(base, scene, addresses, options):
    layout = load_layout(scene['replicas'], scene['topology_id'])
    matrix, source_hash = load_matrix()
    replicas, clients = layout['replicas'], layout['clients']
    n = len(replicas)
    if len(addresses) != n + len(clients) + 1:
        raise ValueError('wrong rank/address count')
    endpoints = [f'{addresses[i]}:{7070+i}' for i in range(n)]
    client_endpoints = [f'{addresses[n+i]}:{17000+i}' for i in range(len(clients))]
    if len(set(endpoints + client_endpoints)) != n + len(clients):
        raise ValueError('participant endpoints must be unique')
    config_dir = Path(base) / 'config'
    config_dir.mkdir(parents=True, exist_ok=True)
    text = '-- Replicas --\n' + ''.join(f'{alias} {endpoint}\n' for alias, endpoint in zip(replicas, endpoints))
    text += '\n-- Clients --\n' + ''.join(f'{alias} {endpoint}\n' for alias, endpoint in zip(clients, client_endpoints))
    text += f'\n-- Master --\nm0 {addresses[-1]}\n\nmasterPort: 7087\nport: 7070\n'
    policy = master_plan(options['protocol'], replicas, clients, matrix, options['placement_objective'])
    text += f'protocol: {options["protocol"]}\n'
    if policy['leader'] is not None:
        text += f'leader: {endpoints[replicas.index(policy["leader"])]}\n'
    text += 'noop: false\nleaderless: false\nfast: false\nwaitClosest: false\n'
    text += f'commandSize: {options["command_size"]}\nclones: 0\nwrites: {WRITES[scene["profile"]]}\n'
    text += 'benchmark: true\nrepetitions: 1\n'
    for key, field in (('arrivalRate', 'arrival_rate'), ('keyCount', 'key_count'), ('zipfSkew', 'zipf_skew'),
                       ('workloadSeed', 'workload_seed'), ('preloadSeed', 'preload_seed'),
                       ('warmup', 'warmup'), ('duration', 'duration')):
        text += f'{key}: {options[field]}\n'
    text += f'shuffleKeys: true\npreload: true\nkeyMappingSeed: {scene["topology_id"]}\n'
    for i, alias in enumerate(replicas):
        text += f'\n-- Apply to {alias} --\nport: {7070+i}\n-- Stop --\n'
    ingresses = {a: replicas.index(policy['client_routes'][a]) for a in clients}
    text += '\n-- Proxy --\n'
    for i, alias in enumerate(replicas):
        text += f'server_alias {alias}\n'
        text += ''.join(client + '\n' for client in clients if ingresses[client] == i)
    text += '---\n'
    (config_dir / 'cluster.conf').write_text(text, encoding='utf-8')
    quorum = ([f'l {policy["leader"]}'] + [a for a in policy['fast_quorum'] if a != policy['leader']]
              if policy['fast_quorum'] else [])
    (config_dir / 'quorum.conf').write_text('\n'.join(quorum) + ('\n' if quorum else ''), encoding='utf-8')
    logical = [f'0.0.0.{i+1}' for i in range(n)] + [f'0.0.1.{i+1}' for i in range(len(clients))]
    physical = endpoints + client_endpoints
    regions = replicas + clients
    (config_dir / 'address-map.txt').write_text(''.join(f'{a} {b}\n' for a, b in zip(logical, physical)), encoding='utf-8')
    latency = ''.join(f'{physical[i]} {physical[j]} {0 if i == j else matrix[a][b]}ms\n'
                      for i, a in enumerate(regions) for j, b in enumerate(regions))
    (config_dir / 'latency.conf').write_text(latency, encoding='utf-8')
    layout.update(**options, profile=scene['profile'], writes=WRITES[scene['profile']],
                  latency_snapshot_sha256=source_hash, topology_sha256=sha256(config_dir / 'latency.conf'),
                  replica_endpoints=endpoints, client_endpoints=client_endpoints,
                  initial_leader_alias=policy['leader'], fast_quorum_aliases=policy['fast_quorum'],
                  placement_plan=policy, key_mapping_seed=scene['topology_id'],
                  ingress_by_client={a: replicas[i] for a, i in ingresses.items()},
                  replica_id_policy='upstream-master-registration-order')
    write_json(Path(base) / 'metadata.json', layout)
    write_json(Path(base) / 'plan.json', policy)
    return layout


def duration_seconds(text):
    factors = {'h': 3600, 'm': 60, 's': 1, 'ms': .001, 'us': .000001, '\u00b5s': .000001, 'ns': .000000001}
    tokens = re.findall(r'(\d+(?:\.\d+)?)(ns|\u00b5s|us|ms|s|m|h)', text)
    if not tokens or ''.join(number + unit for number, unit in tokens) != text:
        raise ValueError('invalid Go duration: ' + text)
    return sum(float(number) * factors[unit] for number, unit in tokens)


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def client_seed(base, alias, clone=0):
    value = 14695981039346656037
    for byte in struct.pack('<QQ', base, clone) + alias.encode():
        value = ((value ^ byte) * 1099511628211) & ((1 << 64)-1)
    return value & ((1 << 63)-1)


def preload_evidence(base, metadata):
    evidence = {}
    for alias in metadata['replicas']:
        path = Path(base) / 'logs' / (alias + '-replica.log')
        text = path.read_text(errors='replace') if path.exists() else ''
        records = re.findall(r'PRELOAD_COMPLETE records=(\d+) value_size=(\d+) seed=(\d+) digest=([0-9a-f]{64})', text)
        if len(records) != 1:
            raise ValueError('missing or duplicate preload evidence: ' + alias)
        count, size, seed, digest = records[0]
        if (int(count), int(size), int(seed)) != (metadata['key_count'], metadata['command_size'], metadata['preload_seed']):
            raise ValueError('preload configuration mismatch: ' + alias)
        evidence[alias] = digest
    if len(set(evidence.values())) != 1:
        raise ValueError('preload digest mismatch across replicas')
    return evidence


def resource_records(base, metadata):
    resources = []
    if metadata.get('resource_observer') == 'gnu-time':
        for rank in range(metadata['replica_count'] + len(metadata['clients']) + 1):
            path = base / 'stdout' / f'resources-rank-{rank}.txt'
            text = path.read_text() if path.exists() else ''
            fields = dict(line.split('=', 1) for line in text.splitlines() if '=' in line)
            keys = ('elapsed_seconds', 'user_seconds', 'system_seconds', 'max_rss_kb',
                    'voluntary_context_switches', 'involuntary_context_switches', 'exit_status')
            try:
                complete = all(math.isfinite(float(fields[key])) and float(fields[key]) >= 0 for key in keys)
                complete = complete and float(fields['max_rss_kb']) > 0
            except (KeyError, ValueError):
                complete = False
            resources.append(dict(rank=rank, complete=complete, fields=fields))
    return resources


def summarize(base):
    base = Path(base)
    metadata = json.loads((base / 'metadata.json').read_text())
    rows = []
    pooled = {'READ': [], 'UPDATE': [], 'ALL': []}
    valid = True
    preload_error = None
    workload_error = None
    try:
        barrier_module().validate(base)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        valid, workload_error = False, str(exc)
    try:
        digests = preload_evidence(base, metadata)
        if digests != metadata.get('preload_digests'):
            raise ValueError('preload evidence differs from validated startup metadata')
    except ValueError as exc:
        valid, preload_error = False, str(exc)
    for alias in metadata['clients']:
        log = base / 'results' / (alias + '-client.log')
        text = log.read_text(errors='replace') if log.exists() else ''
        samples = re.findall(r'\blatency (READ|UPDATE) (\S+)', text)
        try:
            samples = [(op, float(v)) for op, v in samples]
        except ValueError:
            samples = [('ALL', float('nan'))]
        values = [v for _, v in samples]
        elapsed = re.findall(r'Test took ([^\s]+)', text)
        status = base / 'status' / (alias + '.exit')
        try:
            seconds = duration_seconds(elapsed[-1]) if len(elapsed) == 1 else 0
            exit_code = int(status.read_text()) if status.exists() else None
        except (ValueError, OSError):
            seconds, exit_code = 0, None
        counts = re.findall(r'Measured requests (\d+)', text)
        warmups = re.findall(r'Warm-up requests (\d+)', text)
        expected = int(counts[0]) if len(counts) == 1 and len(warmups) == 1 else None
        complete = (expected is not None and expected > 0 and len(values) == expected and seconds > 0 and exit_code == 0
                    and all(math.isfinite(v) and v >= 0 for v in values))
        rates = re.findall(r'Open-loop Poisson arrival rate (\S+) requests/second', text)
        distributions = re.findall(r'Zipfian key distribution: keyCount=(\d+) skew=(\S+)', text)
        windows = re.findall(r'Warm-up (\S+), measurement duration (\S+)', text)
        seeds = re.findall(r'Workload seed (\d+)', text)
        try:
            settings_valid = (len(rates) == len(distributions) == len(windows) == len(seeds) == 1
                and float(rates[0]) == metadata['arrival_rate']
                and (int(distributions[0][0]), float(distributions[0][1])) == (metadata['key_count'], metadata['zipf_skew'])
                and tuple(map(duration_seconds, windows[0])) == (duration_seconds(metadata['warmup']), duration_seconds(metadata['duration']))
                and int(seeds[0]) == client_seed(metadata['workload_seed'], alias))
        except (ValueError, IndexError):
            settings_valid = False
        mapping = f'WORKLOAD_KEY_MAPPING mode=shuffled algorithm=rand-perm-v1 seed={metadata["key_mapping_seed"]} keyCount={metadata["key_count"]}'
        complete = complete and settings_valid and mapping in text
        complete = complete and not (metadata['profile'] == 'C' and any(op == 'UPDATE' for op, _ in samples))
        valid = valid and complete
        identity = dict(protocol=metadata['protocol'], replicas=metadata['replica_count'],
                        topology_id=metadata['topology_id'], profile=metadata['profile'], repetition=1,
                        region=alias, operation='ALL', workload_kind=metadata['workload_kind'],
                        diagnostic_only=metadata['diagnostic_only'],
                        completed=len(values), expected=expected, valid=complete)
        identity.update(mean_ms=sum(values)/len(values) if values else '',
                        p50_ms=percentile(values, .5) if values else '',
                        p99_ms=percentile(values, .99) if values else '',
                        completed_per_second=len(values)/seconds if seconds else '')
        rows.append(identity)
        pooled['ALL'].extend(values)
        for operation in ('READ', 'UPDATE'):
            selected = [v for op, v in samples if op == operation]
            pooled[operation].extend(selected)
            if selected:
                rows.append(dict(identity, operation=operation, completed=len(selected), expected='',
                                 mean_ms=sum(selected)/len(selected), p50_ms=percentile(selected, .5),
                                 p99_ms=percentile(selected, .99), completed_per_second=len(selected)/seconds if seconds else ''))
    resources = resource_records(base, metadata)
    valid = valid and all(row['complete'] for row in resources)
    all_values = pooled['ALL']
    aggregate = dict(protocol=metadata['protocol'], replicas=metadata['replica_count'],
                     topology_id=metadata['topology_id'], profile=metadata['profile'], repetition=1,
                     workload_kind=metadata['workload_kind'], operation='ALL', valid=valid, workload_model=metadata['workload_model'], latency_origin='scheduled_arrival',
                     diagnostic_only=metadata['diagnostic_only'],
                     completed=len(all_values), mean_ms=sum(all_values)/len(all_values) if all_values else '',
                     p99_ms=percentile(all_values, .99) if all_values else '',
                     binary_sha256=metadata['binary_sha256'], upstream_commit=UPSTREAM,
                     topology_sha256=metadata['topology_sha256'],
                     latency_snapshot_sha256=metadata['latency_snapshot_sha256'])
    aggregates = [aggregate]
    aggregate.update(benchmark_reference_commit=metadata['benchmark_reference_commit'],
                     arrival_rate=metadata['arrival_rate'], warmup=metadata['warmup'], duration=metadata['duration'],
                     preload_digest=next(iter(metadata.get('preload_digests', {}).values()), ''),
                     leader_alias=metadata['initial_leader_alias'],
                     fast_quorum_aliases=':'.join(metadata['fast_quorum_aliases']))
    for operation in ('READ', 'UPDATE'):
        selected = pooled[operation]
        if selected:
            aggregates.append(dict(aggregate, operation=operation, completed=len(selected),
                                   mean_ms=sum(selected)/len(selected), p99_ms=percentile(selected, .99)))
    for name, records in (('summary.csv', rows), ('baseline-latency.csv', aggregates)):
        with (base / name).open('w', newline='', encoding='utf-8') as output:
            writer = csv.DictWriter(output, fieldnames=list(records[0]))
            writer.writeheader()
            writer.writerows(records)
    claim = 'synchronized planned-arrival latency; upstream-derived Paxos with framing and retry-slot fixes'
    write_json(base / 'validation.json', dict(valid=valid, clients=rows, resources=resources,
                                            preload_error=preload_error, workload_error=workload_error,
                                            diagnostic_only=metadata['diagnostic_only'], claim=claim))
    return valid


def scene(base, specification, env):
    base = Path(base)
    base.mkdir(parents=True, exist_ok=False)
    for directory in ('config', 'results', 'stdout', 'logs', 'status', 'ranks', 'binary'):
        (base / directory).mkdir()
    n = specification['replicas']
    tasks = n + 11
    kind = env.get('CONSENSUSARENA_SCENE_KIND', 'latency')
    nodes = (tasks + 7) // 8
    cpus = 8
    allocated = subprocess.check_output(['scontrol', 'show', 'hostnames', env['SLURM_JOB_NODELIST']], text=True, timeout=TIMEOUT).split()
    if len(allocated) < nodes or int(env.get('SLURM_NTASKS', '0')) < tasks or int(env.get('SLURM_CPUS_PER_TASK', '0')) < cpus:
        raise ValueError('allocation lacks required nodes, tasks, or CPUs')
    selected_nodes = ','.join(allocated[:nodes])
    prefix = ['srun', '--nodelist=' + selected_nodes, '--nodes=' + str(nodes), '--ntasks=' + str(tasks),
              '--ntasks-per-node=' + '8', '--distribution=cyclic']
    subprocess.run(prefix + ['--cpus-per-task=1', 'bash', str(ROOT / 'slurm/capture-ip.sh'), str(base / 'ranks')],
                   env=env, check=True, timeout=TIMEOUT)
    addresses = [(base / 'ranks' / (str(i) + '.ip')).read_text().strip() for i in range(tasks)]
    metadata = prepare(base, specification, addresses, settings(env, kind))
    for name, variable, hash_variable in (
            ('consensusarena-linux-amd64', 'CONSENSUSARENA_BINARY', 'CONSENSUSARENA_BINARY_SHA256'),
            ('toxiproxy-server-linux-amd64', 'CONSENSUSARENA_TOXIPROXY_SERVER', 'CONSENSUSARENA_TOXIPROXY_SHA256')):
        source = Path(env.get(variable, str(ROOT / name)))
        expected = env.get(hash_variable)
        if not expected or sha256(source) != expected.lower():
            raise ValueError('missing or mismatched dependency checksum: ' + variable)
        shutil.copy2(source, base / 'binary' / name)
        (base / 'binary' / name).chmod(0o755)
        metadata['binary_sha256' if name.startswith('consensusarena') else 'toxiproxy_sha256'] = expected.lower()
    metadata.update(job_id=env['SLURM_JOB_ID'], allocation_nodes=allocated, scene_nodes=allocated[:nodes],
                    partition=env.get('SLURM_JOB_PARTITION', 'unrecorded'),
                    qos=env.get('SLURM_JOB_QOS', 'unrecorded'),
                    resource_observer='gnu-time',
                    cpus_per_task=cpus, latency_backend='toxiproxy', latency_rounding='ceil(RTT/2) per stream direction',
                    adapter_revision=env.get('UPSTREAM_ADAPTER_REVISION', 'unrecorded'),
                    harness_sha256=sha256(ROOT / 'slurm/upstream_harness.py'),
                    latency_jitter_ms=env.get('CONSENSUSARENA_LATENCY_JITTER_MS', '0'),
                    bandwidth_kbps=env.get('CONSENSUSARENA_BANDWIDTH_KBPS', '125000'))
    write_json(base / 'metadata.json', metadata)
    worker_env = dict(env, CONSENSUSARENA_REPLICAS=str(n), CONSENSUSARENA_TOPOLOGY=str(specification['topology_id']))
    runner_output = (base / 'stdout' / 'srun.out').open('w')
    if kind == 'fault':
        metadata.update(replica_aliases=metadata['replicas'], replicas=n, repetition=1, timeline_origin='run_start', warmup_s=5,
                        observation_s=40, measurement_s=35, crash_s=5, max_crash_s=20,
                        experiment_design='three_topologies_one_run_each')
        write_json(base / 'metadata.json', metadata)
        return fault_module().run_scene(base, prefix, worker_env, runner_output, metadata)
    runner = subprocess.Popen(prefix + ['--cpus-per-task=' + str(cpus), 'bash', str(ROOT / 'slurm/node-runner.sh'), str(base)],
                              env=worker_env, stdout=runner_output, stderr=subprocess.STDOUT)
    error = None
    try:
        deadline = time.monotonic() + int(env.get('REPLICA_READY_TIMEOUT_SECONDS', '900'))
        while True:
            if len(list((base / 'status').glob('listening-*'))) == n:
                (base / 'status' / 'peer-listeners.ready').touch()
            ready = 0
            for alias in metadata['replicas']:
                log = base / 'logs' / (alias + '-replica.log')
                text = log.read_text(errors='replace') if log.exists() else ''
                ready += 'done connecting to peers' in text.lower()
            if ready == n:
                metadata['preload_digests'] = preload_evidence(base, metadata)
                write_json(base / 'metadata.json', metadata)
                break
            if runner.poll() is not None or list((base / 'status').glob('failed-*')):
                raise RuntimeError('participant failed before peer readiness')
            if time.monotonic() >= deadline:
                raise TimeoutError(f'replica readiness timeout: {ready}/{n}')
            time.sleep(1)
        (base / 'status' / 'replicas.ready').touch()
        barrier_module().coordinate(base, runner.pid,
                                    int(env.get('REPLICA_READY_TIMEOUT_SECONDS', '900')),
                                    int(env.get('CLIENT_TIMEOUT_SECONDS', '300')))
        deadline = time.monotonic() + int(env.get('CLIENT_TIMEOUT_SECONDS', '300'))
        while len(list((base / 'status').glob('*.exit'))) != len(metadata['clients']):
            if runner.poll() is not None or list((base / 'status').glob('failed-*')):
                raise RuntimeError('participant failed during workload')
            if time.monotonic() >= deadline:
                raise TimeoutError('client completion timeout')
            time.sleep(1)
    except Exception as exc:
        error = str(exc)
    finally:
        (base / 'status' / 'stop').touch()
        try:
            code = runner.wait(timeout=30)
        except subprocess.TimeoutExpired:
            runner.terminate()
            try:
                code = runner.wait(timeout=30)
            except subprocess.TimeoutExpired:
                runner.kill()
                code = runner.wait(timeout=30)
        runner_output.close()
    valid = summarize(base)
    write_json(base / 'outcome.json', dict(valid=valid, error=error, runner_exit=code))
    return 0 if valid and error is None and code == 0 else 1


def load_module(filename, name):
    import importlib.util
    spec = importlib.util.spec_from_file_location(name, ROOT / 'slurm' / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def barrier_module():
    return load_module('latency-barrier.py', 'latency_barrier')


def fault_module():
    return load_module('fault-harness.py', 'fault_harness')


def scene_name(item):
    return 'replicas-{replicas}-topology-{topology_id}-ycsb-{profile}'.format(**item)


def collect_suite(base, kind):
    base = Path(base)
    manifest = json.loads((base / 'suite.json').read_text())
    if manifest['kind'] != kind:
        raise ValueError('suite kind mismatch')
    scenes = manifest['scenes']
    names = [scene_name(item) for item in scenes]
    if not scenes or len(set(names)) != len(names):
        raise ValueError('empty or duplicate scene manifest')
    filenames = (('summary.csv', 'baseline-latency.csv') if kind == 'latency'
                 else ('phase-summary.csv', 'timeseries.csv', 'request-cohorts.csv'))
    files = {name: [] for name in filenames}
    statuses, validations = [], []
    for item, name in zip(scenes, names):
        path = base / 'scenes' / name
        record = dict(**item, complete=False)
        try:
            metadata = json.loads((path / 'metadata.json').read_text())
            identity = (metadata['replica_count'], metadata['topology_id'], metadata['profile'])
            if identity != (item['replicas'], item['topology_id'], item['profile']):
                raise ValueError('scene identity differs from suite manifest')
            if metadata.get('suite_kind') != kind or metadata.get('diagnostic_only') is not False:
                raise ValueError('scene kind or diagnostic flag differs from suite')
            for key, expected in manifest['settings'].items():
                if metadata.get(key) != expected:
                    raise ValueError('scene setting differs from suite manifest: ' + key)
            status = json.loads((base / 'scene-status' / (name + '.json')).read_text())
            if any(status.get(key) != value for key, value in item.items()) or status['returncode'] != 0:
                raise ValueError('scene worker failed or identity changed')
            outcome = json.loads((path / 'outcome.json').read_text())
            if kind == 'latency':
                valid = summarize(path)
                validation = json.loads((path / 'validation.json').read_text())
                complete = valid and outcome.get('error') is None and outcome.get('runner_exit') == 0
            else:
                fault_module().validate_evidence(path, metadata, outcome)
                validation = load_module('summarize-fault.py', 'summarize_fault').summarize(path)
                validations.extend(validation)
                complete = len(validation) == 1 and validation[0]['valid']
            if not complete:
                raise ValueError('invalid or incomplete observations')
            scene_rows = {}
            for filename in files:
                with (path / filename).open(newline='') as source:
                    scene_rows[filename] = list(csv.DictReader(source))
                if not scene_rows[filename]:
                    raise ValueError('empty ' + filename)
            for filename, rows in scene_rows.items():
                files[filename].extend(rows)
            record['complete'] = True
        except (OSError, ValueError, KeyError, TypeError) as exc:
            record['error'] = str(exc)
        statuses.append(record)
    for name, rows in files.items():
        output = base / name
        if rows:
            fields = list(rows[0])
            if any(list(row) != fields for row in rows):
                raise ValueError('incompatible CSV headers: ' + name)
            with output.open('w', newline='') as target:
                writer = csv.DictWriter(target, fieldnames=fields)
                writer.writeheader()
                writer.writerows(rows)
        else:
            output.unlink(missing_ok=True)
    if kind == 'fault':
        write_json(base / 'validation.json', validations)
    complete = all(item['complete'] for item in statuses) and not (base / 'serial-error.json').exists()
    write_json(base / 'suite-status.json', dict(complete=complete, expected_scenes=len(statuses),
                                              completed_scenes=sum(item['complete'] for item in statuses), scenes=statuses))
    print('{} suite {}: {}'.format(kind.title(), 'completed successfully' if complete else 'INCOMPLETE', base), flush=True)
    return 0 if complete else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('run',))
    parser.add_argument('kind', choices=('latency', 'fault'))
    args = parser.parse_args()
    env = dict(os.environ, CONSENSUSARENA_SCENE_KIND=args.kind)
    plan = selection(env)
    if len(plan) != 1:
        raise ValueError('scene adapter requires exactly one selected scene')
    settings(env, args.kind)
    return scene(Path(env['CONSENSUSARENA_RUN_DIR']), plan[0], env)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print(str(error), file=sys.stderr)
        sys.exit(2)
