#!/usr/bin/env python3
"""Slurm adapter with matched-master benchmarks and opt-in upstream workloads."""
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
PROTOCOLS = ('swiftpaxos', 'paxos', 'n2paxos', 'curp', 'fastpaxos', 'epaxos')
TIMEOUT = 7200
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


def settings(env):
    protocol = env.get('CONSENSUSARENA_PROTOCOL', 'swiftpaxos').lower()
    if protocol not in PROTOCOLS:
        raise ValueError('unsupported upstream protocol: ' + protocol)
    mode = env.get('CONSENSUSARENA_BENCHMARK_MODE', 'matched')
    if mode not in ('matched', 'native'):
        raise ValueError('benchmark mode must be matched or native')
    if mode == 'matched':
        if protocol == 'n2paxos':
            raise ValueError('n2paxos has no master benchmark counterpart; use native mode')
        if env.get('CONSENSUSARENA_CLIENT_WEIGHTS'):
            raise ValueError('custom client weights are not supported by this adapter')
        if any(key.startswith('UPSTREAM_') and key in ('UPSTREAM_REQUESTS', 'UPSTREAM_COMMAND_SIZE',
                                                     'UPSTREAM_PENDING', 'UPSTREAM_CONFLICTS') for key in env):
            raise ValueError('native workload exports require benchmark mode=native')
        defaults = dict(re.findall(r'^(\w+):\s*(\S+)', (ROOT / 'slurm/workload.conf').read_text(), re.M))
        options = dict(protocol=protocol, benchmark_mode=mode, workload_kind='matched-master-poisson-zipf',
                       seed_policy='master-alias-clone-derived', clones=0, preload=True,
                       shuffle_keys=True, warmup_kind='bootstrap-then-timed-warmup',
                       diagnostic_only=env.get('UPSTREAM_SMOKE_ONLY') == '1', fault_support=False,
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
        reference = json.loads((ROOT / 'upstream-provenance.json').read_text())
        options['benchmark_reference_commit'] = reference['benchmark_reference_commit']
        return options
    requests = int(env.get('UPSTREAM_REQUESTS', '1000'))
    payload = int(env.get('UPSTREAM_COMMAND_SIZE', '1000'))
    conflicts = int(env.get('UPSTREAM_CONFLICTS', '2'))
    pending = int(env.get('UPSTREAM_PENDING', '128'))
    if requests <= 0 or payload <= 0 or not 0 <= conflicts <= 100 or pending <= 0:
        raise ValueError('invalid upstream workload settings')
    return dict(protocol=protocol, requests=requests, command_size=payload,
                benchmark_mode=mode,
                conflicts=conflicts, pending=pending, clones=0,
                workload_kind='upstream-request-count', seed_policy='upstream-random',
                preload=False, warmup_kind='one-unmeasured-bootstrap-request',
                diagnostic_only=env.get('UPSTREAM_SMOKE_ONLY') == '1',
                pipeline=True, fault_support=False, upstream_commit=UPSTREAM)


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
    matched = options['benchmark_mode'] == 'matched'
    policy = (master_plan(options['protocol'], replicas, clients, matrix, options['placement_objective'])
              if matched else dict(leader=replicas[0], fast_quorum=replicas[:n//2+1],
                                  client_routes={a: replicas[min(range(n), key=lambda i: (matrix[a][replicas[i]], i))]
                                                 for a in clients}, model='upstream-fixed-first-majority'))
    text += f'protocol: {options["protocol"]}\n'
    if policy['leader'] is not None:
        text += f'leader: {endpoints[replicas.index(policy["leader"])]}\n'
    text += 'noop: false\nthrifty: false\noptread: false\nleaderless: false\nfast: true\nwaitClosest: true\n'
    text += f'commandSize: {options["command_size"]}\nclones: 0\nwrites: {WRITES[scene["profile"]]}\n'
    if matched:
        text += 'benchmark: true\n'
        for key, field in (('arrivalRate', 'arrival_rate'), ('keyCount', 'key_count'), ('zipfSkew', 'zipf_skew'),
                           ('workloadSeed', 'workload_seed'), ('preloadSeed', 'preload_seed'),
                           ('warmup', 'warmup'), ('duration', 'duration')):
            text += f'{key}: {options[field]}\n'
        text += f'shuffleKeys: true\npreload: true\nkeyMappingSeed: {scene["topology_id"]}\n'
    else:
        text += f'reqs: {options["requests"]}\nrunTime: none\nconflicts: {options["conflicts"]}\nkey: 42\n'
        text += f'pipeline: true\npendings: {options["pending"]}\n'
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
    factors = {'h': 3600, 'm': 60, 's': 1, 'ms': .001, 'us': .000001, 'µs': .000001, 'ns': .000000001}
    tokens = re.findall(r'(\d+(?:\.\d+)?)(ns|µs|us|ms|s|m|h)', text)
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


def summarize(base):
    base = Path(base)
    metadata = json.loads((base / 'metadata.json').read_text())
    rows = []
    pooled = {'READ': [], 'UPDATE': [], 'ALL': []}
    valid = True
    matched = metadata.get('benchmark_mode') == 'matched'
    preload_error = None
    if matched:
        try:
            digests = preload_evidence(base, metadata)
            if digests != metadata.get('preload_digests'):
                raise ValueError('preload evidence differs from validated startup metadata')
        except ValueError as exc:
            valid, preload_error = False, str(exc)
    for alias in metadata['clients']:
        log = base / 'results' / (alias + '-client.log')
        text = log.read_text(errors='replace') if log.exists() else ''
        samples = re.findall(r'\blatency (READ|UPDATE) (\S+)', text) if matched else [
            ('ALL', v) for v in re.findall(r'\blatency (\S+)', text)]
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
        expected = metadata.get('requests')
        if matched:
            counts = re.findall(r'Measured requests (\d+)', text)
            warmups = re.findall(r'Warm-up requests (\d+)', text)
            expected = int(counts[0]) if len(counts) == 1 and len(warmups) == 1 else None
        complete = (expected is not None and expected > 0 and len(values) == expected and seconds > 0 and exit_code == 0
                    and all(math.isfinite(v) and v >= 0 for v in values))
        if matched:
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
        if matched:
            for operation in ('READ', 'UPDATE'):
                selected = [v for op, v in samples if op == operation]
                pooled[operation].extend(selected)
                if selected:
                    rows.append(dict(identity, operation=operation, completed=len(selected), expected='',
                                     mean_ms=sum(selected)/len(selected), p50_ms=percentile(selected, .5),
                                     p99_ms=percentile(selected, .99), completed_per_second=len(selected)/seconds if seconds else ''))
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
            valid = valid and complete
    all_values = pooled['ALL']
    aggregate = dict(protocol=metadata['protocol'], replicas=metadata['replica_count'],
                     topology_id=metadata['topology_id'], profile=metadata['profile'], repetition=1,
                     workload_kind=metadata['workload_kind'], operation='ALL', valid=valid,
                     diagnostic_only=metadata['diagnostic_only'],
                     completed=len(all_values), mean_ms=sum(all_values)/len(all_values) if all_values else '',
                     p99_ms=percentile(all_values, .99) if all_values else '',
                     binary_sha256=metadata['binary_sha256'], upstream_commit=UPSTREAM,
                     topology_sha256=metadata['topology_sha256'],
                     latency_snapshot_sha256=metadata['latency_snapshot_sha256'])
    aggregates = [aggregate]
    if matched:
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
    claim = ('diagnostic integration only; no comparative performance evidence' if metadata['diagnostic_only']
             else ('matched master workload, preload, clocks and placement; protocol implementation still upstream'
                   if matched else 'upstream-native latency only; not matched Poisson or recovery evidence'))
    write_json(base / 'validation.json', dict(valid=valid, clients=rows, resources=resources,
                                            preload_error=preload_error,
                                            diagnostic_only=metadata['diagnostic_only'], claim=claim))
    return valid


def scene(base, specification, env):
    base = Path(base)
    base.mkdir(parents=True, exist_ok=False)
    for directory in ('config', 'results', 'stdout', 'logs', 'status', 'ranks', 'binary'):
        (base / directory).mkdir()
    n = specification['replicas']
    tasks = n + 11
    diagnostic = env.get('UPSTREAM_SMOKE_ONLY') == '1'
    nodes = 1 if diagnostic else (tasks + 7) // 8
    cpus = 1 if diagnostic else 8
    allocated = subprocess.check_output(['scontrol', 'show', 'hostnames', env['SLURM_JOB_NODELIST']], text=True, timeout=TIMEOUT).split()
    if len(allocated) < nodes or int(env.get('SLURM_NTASKS', '0')) < tasks or int(env.get('SLURM_CPUS_PER_TASK', '0')) < cpus:
        raise ValueError('allocation lacks required nodes, tasks, or CPUs')
    selected_nodes = ','.join(allocated[:nodes])
    prefix = ['srun', '--nodelist=' + selected_nodes, '--nodes=' + str(nodes), '--ntasks=' + str(tasks),
              '--ntasks-per-node=' + str(16 if diagnostic else 8), '--distribution=cyclic']
    subprocess.run(prefix + ['--cpus-per-task=1', 'bash', str(ROOT / 'slurm/capture-ip.sh'), str(base / 'ranks')],
                   env=env, check=True, timeout=TIMEOUT)
    addresses = [(base / 'ranks' / (str(i) + '.ip')).read_text().strip() for i in range(tasks)]
    metadata = prepare(base, specification, addresses, settings(env))
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
                if metadata['benchmark_mode'] == 'matched':
                    metadata['preload_digests'] = preload_evidence(base, metadata)
                    write_json(base / 'metadata.json', metadata)
                break
            if runner.poll() is not None or list((base / 'status').glob('failed-*')):
                raise RuntimeError('participant failed before peer readiness')
            if time.monotonic() >= deadline:
                raise TimeoutError(f'replica readiness timeout: {ready}/{n}')
            time.sleep(1)
        (base / 'status' / 'replicas.ready').touch()
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


def collect(base):
    base = Path(base)
    manifest = json.loads((base / 'suite.json').read_text())
    status = []
    files = {'summary.csv': [], 'baseline-latency.csv': []}
    for item in manifest['scenes']:
        path = base / 'scenes' / scene_name(item)
        try:
            metadata = json.loads((path / 'metadata.json').read_text())
            if metadata['diagnostic_only'] != (manifest.get('kind', 'latency') == 'smoke'):
                raise ValueError('diagnostic flag differs from suite kind')
            if (metadata['replica_count'], metadata['topology_id'], metadata['profile']) != (item['replicas'], item['topology_id'], item['profile']):
                raise ValueError('scene identity differs from suite manifest')
            for key, expected in manifest.get('settings', {}).items():
                if metadata.get(key) != expected:
                    raise ValueError('scene setting differs from suite manifest: ' + key)
            outcome = json.loads((path / 'outcome.json').read_text())
            valid = summarize(path)
            complete = valid and outcome.get('error') is None and outcome.get('runner_exit') == 0
            record = dict(**item, complete=complete, error=outcome.get('error'))
            for name in files:
                with (path / name).open(newline='') as source:
                    files[name].extend(csv.DictReader(source))
        except (OSError, ValueError, KeyError) as exc:
            record = dict(**item, complete=False, error=str(exc))
        status.append(record)
    for name, rows in files.items():
        if rows:
            with (base / name).open('w', newline='') as output:
                writer = csv.DictWriter(output, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
    complete = all(item['complete'] for item in status)
    write_json(base / 'suite-status.json', dict(complete=complete, expected_scenes=len(status),
                                              completed_scenes=sum(item['complete'] for item in status), scenes=status))
    print('{} suite {}: {}'.format(manifest.get('kind', 'latency').title(),
                                  'completed successfully' if complete else 'INCOMPLETE', base), flush=True)
    return 0 if complete else 2


def scene_name(item):
    return 'replicas-{replicas}-topology-{topology_id}-ycsb-{profile}'.format(**item)


def smoke_defaults(env):
    if env.get('CONSENSUSARENA_BENCHMARK_MODE', 'matched') == 'native':
        env.setdefault('UPSTREAM_REQUESTS', '32')
    else:
        # Small diagnostic dataset/load fits one CPU and 1 GiB per participant.
        # Keep the standard 5s warmup and 10s measurement clock.
        env.setdefault('CONSENSUSARENA_KEY_COUNT', '10000')
        env.setdefault('CONSENSUSARENA_ARRIVAL_RATE', '50')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    for command in ('plan', 'submit', 'run', 'collect'):
        child = sub.add_parser(command)
        child.add_argument('kind', choices=('latency', 'smoke'), help='latency or diagnostic smoke only; fault recovery is not adapted')
        if command == 'submit':
            child.add_argument('--account', default='dcl')
            child.add_argument('--export', default='ALL')
            child.add_argument('--time')
            child.add_argument('--mode', choices=('serial',), default='serial')
        if command == 'collect':
            child.add_argument('path', type=Path)
    args = parser.parse_args()
    env = dict(os.environ)
    if args.command == 'submit':
        seen = set()
        for entry in args.export.split(','):
            if entry == 'ALL':
                continue
            key, separator, value = entry.partition('=')
            if not separator or not key.isidentifier() or key in seen:
                raise ValueError('invalid or duplicate export: ' + entry)
            seen.add(key)
            env[key] = value
        if args.kind == 'smoke':
            for key, value in {'CONSENSUSARENA_REPLICAS': '5', 'CONSENSUSARENA_CASES': '5-1', 'CONSENSUSARENA_YCSB_PROFILES': 'A'}.items():
                if key in env and env[key] != value:
                    raise ValueError('smoke mode requires ' + key + '=' + value)
                env[key] = value
            env['UPSTREAM_SMOKE_ONLY'] = '1'
            smoke_defaults(env)
        elif env.get('UPSTREAM_SMOKE_ONLY'):
            raise ValueError('use submit smoke for a diagnostic allocation')
        plan = selection(env)
        settings(env)
        env['CONSENSUSARENA_ROOT'] = str(ROOT)
        cmd = ['sbatch', '--parsable', '--account=' + args.account, '--export=ALL']
        if args.time:
            cmd.append('--time=' + args.time)
        cmd.append(str(ROOT / 'slurm' / ('run-smoke.sbatch' if args.kind == 'smoke' else 'run-latency.sbatch')))
        job = subprocess.check_output(cmd, env=env, cwd=ROOT, text=True, timeout=TIMEOUT).strip().split(';')[0]
        if not job.isdigit():
            raise ValueError('unexpected Slurm job ID: ' + job)
        root = env.get('CONSENSUSARENA_RUN_DIR', '/scratch/' + env['USER'] + '/swiftpaxos-upstream-' + job)
        print(f'Serial job: {job} | scenes: {len(plan)} | run directory: {root}', flush=True)
        return 0
    if args.command == 'collect':
        return collect(args.path)
    if args.kind == 'smoke':
        env.update(CONSENSUSARENA_REPLICAS='5', CONSENSUSARENA_CASES='5-1', CONSENSUSARENA_YCSB_PROFILES='A', UPSTREAM_SMOKE_ONLY='1')
        smoke_defaults(env)
    elif env.get('UPSTREAM_SMOKE_ONLY'):
        raise ValueError('diagnostic mode cannot be labeled latency')
    plan = selection(env)
    options = settings(env)
    if args.command == 'plan':
        print(json.dumps(dict(scenes=plan, settings=options), indent=2))
        return 0
    if env.get('SLURM_ARRAY_TASK_ID'):
        raise ValueError('job arrays are unsupported')
    root = Path(env.get('CONSENSUSARENA_RUN_DIR', '/scratch/' + env['USER'] + '/swiftpaxos-upstream-' + env['SLURM_JOB_ID']))
    root.mkdir(parents=True, exist_ok=False)
    (root / 'scenes').mkdir()
    write_json(root / 'suite.json', dict(kind=args.kind, scenes=plan, settings=options, execution='serial-slurm',
                                       upstream_commit=UPSTREAM, adapter_revision=env.get('UPSTREAM_ADAPTER_REVISION', 'unrecorded')))
    for item in plan:
        try:
            code = scene(root / 'scenes' / scene_name(item), item, env)
            print(f'Scene {scene_name(item)}: exit={code}', flush=True)
        except Exception as exc:
            print(f'Scene {scene_name(item)} failed: {exc}', flush=True)
    return collect(root)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except Exception as error:
        print(str(error), file=sys.stderr)
        sys.exit(2)
