#!/usr/bin/env python3
"""Run workload-B adaptive searches or repeated fixed sweeps in isolated Slurm jobs.

plan: inspect experiment.toml without submitting or writing files.
submit: requires frozen CONSENSUSARENA_BINARY{,_SHA256} and
CONSENSUSARENA_TOXIPROXY_SERVER environment variables. Prefer launch.ps1 from
Windows to build/freeze/upload before submitting. Only the six registered
comparisons in experiment.toml are supported; original PQL is rejected.

Every load point restarts all participants using the existing latency runner:
5s warmup, 10s measurement, 2s sustain, 19s drain deadline. Rate-dependent
placement uses the existing independent heat-sample workflow before measurement.
Valid drained runs with mean <=400ms sustain a rate. Timeout observations retry
up to the configured attempt count; over-cap completed runs are final, as in
KCensus. Invalid evidence cannot establish a capacity wall. Unlike KCensus,
no speculative disturbance filter silently retries or removes slow observations.

Raw evidence is in series/PROTOCOL/points/rate-N/attempt-NN. The afterany
collector revalidates it and exports attempts.csv (all attempts), points.csv
(one chosen observation per rate), resources.csv (per replica/attempt), and
suite-status.json (completion plus rate brackets). The Slurm time limit covers
an entire search; interrupted searches retain evidence but remain incomplete.
Never relaunch into an old result directory. plot.py refuses incomplete searches
unless --allow-partial is explicitly selected. Archive the root and frozen launch
directory from persistent storage before scratch cleanup, as for latency runs.

A [sweep] specification runs every configured rate for every repetition, in
seeded randomized blocks. attempt-NN then identifies repetition NN, not a retry.
Timeouts and over-cap observations never stop the sweep or trigger retries.
points.csv retains every repetition; collection requires the complete matrix.
"""
import argparse
import csv
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tomllib
import uuid

from search import Settings, SweepSettings, adaptive, sweep_schedule

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
sys.path.insert(0, str(HERE.parent))
from topology import generated_layout

PROTOCOLS = ('agreement', 'paxos', 'swiftpaxos', 'bodega', 'kcensus', 'pqlfrontier')
TIMEOUT = 7200
PROXY_HASH = '556d891134a3c582dc1e1a3f7335fd55142e5965769855a00b944e13e48302fc'


class CleanupError(RuntimeError):
    pass


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024*1024), b''):
            h.update(block)
    return h.hexdigest()


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix+'.tmp')
    temp.write_text(json.dumps(value, indent=2, allow_nan=False)+'\n', encoding='utf-8', newline='\n')
    temp.replace(path)


def write_csv(path, rows):
    if not rows:
        return
    temp = path.with_suffix('.csv.tmp')
    with temp.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    temp.replace(path)


def specification(path):
    spec = tomllib.loads(path.read_text(encoding='utf-8'))
    if ('search' in spec) == ('sweep' in spec):
        raise ValueError('specify exactly one of search or sweep')
    settings = SweepSettings(**spec['sweep']) if 'sweep' in spec else Settings(**spec['search'])
    settings.validate()
    protocols = spec['protocols']
    if not protocols or len(protocols) != len(set(protocols)) or not set(protocols) <= set(PROTOCOLS):
        raise ValueError('select distinct protocols; PQL must use pqlfrontier')
    if spec['profile'] != 'B' or type(spec['resources']) is not bool:
        raise ValueError('this suite requires workload B and a boolean resources setting')
    template = (path.parent/spec['template']).resolve()
    layout = generated_layout(template)
    if layout['replica_count'] != 7 or len(layout['clients']) != 7:
        raise ValueError('load experiment requires seven replicas and seven clients')
    case = json.loads((template/'case.json').read_text())
    if case['deployment'] != 'ring-7':
        raise ValueError('expected frozen ring-7 deployment')
    return spec, settings, template, layout


def set_field(text, key, value):
    result, count = re.subn(r'^'+re.escape(key)+r':.*$', f'{key}: {value}', text, flags=re.M)
    if count != 1:
        raise ValueError('expected exactly one config field '+key)
    return result


def prepare(template, output, rate, protocol):
    """One immutable generated case per offered rate and attempt; preserve raw topology."""
    if type(rate) is not int or rate < 1 or protocol not in PROTOCOLS:
        raise ValueError('invalid offered rate or protocol')
    output.mkdir(parents=True, exist_ok=False)
    text = (template/'workload.conf').read_text()
    for key, value in dict(protocol=protocol, writes=5, commandSize=16, clones=0,
            arrivalRate=format(rate/7, '.15g'), keyCount=100000, zipfSkew=.99,
            warmup='5s', duration='10s', repetitions=1, shuffleKeys='true',
            workloadSeed=1, preload='true', preloadSeed=1).items():
        text = set_field(text, key, value)
    (output/'workload.conf').write_text(text, encoding='utf-8', newline='\n')
    shutil.copyfile(template/'latency.conf', output/'latency.conf')
    case = json.loads((template/'case.json').read_text())
    case['workload'].update(total_arrival_rate=rate, arrival_rate_per_client=rate/7, profiles=['B'],
        offered_load_source='throughput experiment, requests/s total', key_count=100000,
        zipf_skew=.99, command_size_bytes=16)
    case['throughput_search'] = dict(total_offered_rps=rate, profile='B', parent_case_sha256=generated_layout(template)['case_sha256'])
    write_json(output/'case.json', case)
    return generated_layout(output)


def harness():
    spec = importlib.util.spec_from_file_location('latency_suite', HERE.parent/'benchmark-suite.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def classify(row, cap):
    if row['workload_model'] != 'synchronized-ramp-fixed-sustain-v2' or row['latency_sample_policy'] != 'completed_measured_requests':
        raise ValueError('incompatible measurement semantics')
    achieved = float(row['completed_rps'])
    mean = float(row['mean_ms']) if row['mean_ms'] else None
    if not math.isfinite(achieved) or achieved < 0 or (mean is not None and (not math.isfinite(mean) or mean < 0)):
        raise ValueError('invalid latency or throughput')
    timeout = row['timed_out'] == 'True'
    drained = row['all_requests_drained'] == 'True'
    if row['throughput_valid'] != 'True' or row['timed_out'] not in ('True','False') or row['all_requests_drained'] not in ('True','False'):
        raise ValueError('invalid completion/window flags')
    if (int(row['count']) > 0) != (mean is not None):
        raise ValueError('inconsistent latency sample count')
    if not timeout and not drained:
        raise ValueError('normal run failed to drain')
    if not timeout and row['cohort_complete'] != 'True':
        raise ValueError('normal run has incomplete measured cohort')
    if timeout:
        verdict = 'timeout'
    elif mean is None:
        verdict = 'no_samples'
    elif mean > cap:
        verdict = 'over_latency_cap'
    else:
        verdict = 'sustained'
    return dict(valid=True, verdict=verdict, sustained=verdict == 'sustained',
        mean_ms=mean, p99_ms=float(row['p99_ms']) if row['p99_ms'] else None,
        achieved_rps=achieved, count=int(row['count']), timed_out=timeout,
        all_requests_drained=drained, cohort_complete=row['cohort_complete'])


def trial(suite, root, template, protocol, rate, attempt, settings, resource_enabled):
    attempt_root = root/'points'/f'rate-{rate}'/f'attempt-{attempt:02d}'
    case_root = attempt_root/'inputs'/template.name
    layout = prepare(template, case_root, rate, protocol)
    scene = dict(replicas=7, clients=7, topology_id=layout['topology_id'], profile='B',
                 case=layout['case'], case_sha256=layout['case_sha256'])
    env = dict(os.environ)
    # Do not let unrelated diagnostic overrides alter a point or its workload.
    for key in list(env):
        if key.startswith('CONSENSUSARENA_') and key not in (
                'CONSENSUSARENA_BINARY', 'CONSENSUSARENA_BINARY_SHA256',
                'CONSENSUSARENA_TOXIPROXY_SERVER', 'CONSENSUSARENA_TOXIPROXY_SHA256'):
            del env[key]
    env.update(CONSENSUSARENA_PROTOCOL=protocol, CONSENSUSARENA_SUITE='generated',
        CONSENSUSARENA_GENERATED_ROOT=str(case_root.parent), CONSENSUSARENA_RESOURCES=str(int(resource_enabled)),
        CONSENSUSARENA_WORKLOAD_SEED='1', CONSENSUSARENA_LATENCY_JITTER_MS='0',
        CONSENSUSARENA_BANDWIDTH_KBPS='125000')
    suite.publish_manifest(attempt_root, dict(kind='latency', scenes=[scene], execution='adaptive_throughput_point'))
    code = suite.run_scene('latency', REPO, attempt_root, [scene], 0, env)
    # Cleanup is required before another point can reuse the allocation.
    try:
        suite.clean_steps(os.environ['SLURM_JOB_ID'])
    except Exception as exc:
        raise CleanupError('allocation cleanup failed; stop before reusing it') from exc
    if suite.collect('latency', attempt_root) != 0 or code != 0:
        raise ValueError('invalid execution; inspect '+str(attempt_root/'suite-status.json'))
    with (attempt_root/'baseline-latency.csv').open(newline='') as f:
        rows = list(csv.DictReader(f))
    if len(rows) != 1 or rows[0]['protocol'] != protocol or rows[0]['profile'] != 'B':
        raise ValueError('point measurement identity mismatch')
    return classify(rows[0], settings.latency_cap_ms), attempt_root


def worker(root, index):
    manifest = json.loads((root/'suite.json').read_text())
    protocol = manifest['protocols'][index]
    if fingerprint(Path(manifest['spec_path'])) != manifest['fingerprint']:
        raise ValueError('frozen experiment inputs changed after submission')
    spec, settings, template, _ = specification(Path(manifest['spec_path']))
    binary_inputs(manifest['binary'], manifest['proxy'], manifest['binary_sha256'], manifest['proxy_sha256'])
    os.environ.update(CONSENSUSARENA_BINARY=manifest['binary'],
        CONSENSUSARENA_BINARY_SHA256=manifest['binary_sha256'],
        CONSENSUSARENA_TOXIPROXY_SERVER=manifest['proxy'],
        CONSENSUSARENA_TOXIPROXY_SHA256=manifest['proxy_sha256'])
    series = root/'series'/protocol
    series.mkdir(parents=True, exist_ok=False)
    suite = harness()
    rows, selected = [], {}
    common = dict(protocol=protocol, binary_sha256=manifest['binary_sha256'], profile='B')
    write_json(series/'status.json', dict(**common, complete=False, status='running'))

    def observe(rate, phase, attempt):
        location = series/'points'/f'rate-{rate}'/f'attempt-{attempt:02d}'
        try:
            result, _ = trial(suite, series, template, protocol, rate, attempt, settings, spec['resources'])
            error = ''
        except (OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
            result = dict(valid=False, verdict='invalid', sustained=False, mean_ms=None, p99_ms=None,
                achieved_rps=None, count=None, timed_out=None, all_requests_drained=None, cohort_complete=None)
            error = str(exc)
        row = dict(**common, offered_rps=rate, phase=phase, attempt=attempt, **result,
                   selected=False, path=str(location.relative_to(root)), error=error)
        rows.append(row)
        write_json(location/'decision.json', row)
        write_csv(series/'attempts.csv', rows)
        print(f'{protocol}: offered={rate} attempt={attempt} verdict={row["verdict"]} achieved={row["achieved_rps"]} mean={row["mean_ms"]}', flush=True)
        return result, row, location

    def evaluate(rate, phase):
        for attempt in range(1, settings.attempts+1):
            result, row, location = observe(rate, phase, attempt)
            if result['verdict'] in ('sustained', 'over_latency_cap') or attempt == settings.attempts:
                if not result['valid']:
                    raise RuntimeError('invalid point exhausted retries; no capacity conclusion: '+row['error'])
                row['selected'] = True
                selected[rate] = row
                write_json(location/'decision.json', row)
                write_csv(series/'attempts.csv', rows)
                write_csv(series/'points.csv', [selected[k] for k in sorted(selected)])
                return result
        raise AssertionError('unreachable')

    try:
        if isinstance(settings, SweepSettings):
            for rate, repetition in sweep_schedule(settings):
                result, row, location = observe(rate, 'fixed_sweep', repetition)
                if not result['valid']:
                    raise RuntimeError('invalid repetition; sweep incomplete: '+row['error'])
                # Every planned repetition is retained, including timeouts and slow runs.
                row['selected'] = True
                selected[rate, repetition] = row
                write_json(location/'decision.json', row)
                write_csv(series/'attempts.csv', rows)
                write_csv(series/'points.csv', [selected[k] for k in sorted(selected)])
            write_json(series/'status.json', dict(**common, complete=True, status='complete',
                outcome='sweep_complete', rates=len(settings.rates), repetitions=settings.repetitions,
                points=len(selected), attempts=len(rows)))
            return 0
        outcome = adaptive(settings, evaluate)
        best = selected.get(outcome['highest_sustained_offered_rps'])
        outcome['achieved_rps_at_highest_sustained_load'] = best['achieved_rps'] if best else None
        outcome['mean_ms_at_highest_sustained_load'] = best['mean_ms'] if best else None
        write_json(series/'status.json', dict(**common, complete=True, status='complete', **outcome,
            points=len(selected), attempts=len(rows)))
        return 0
    except Exception as exc:
        write_json(series/'status.json', dict(**common, complete=False, status='failed', error=str(exc)))
        raise


def fingerprint(path):
    _, _, template, layout = specification(path)
    files = [path, HERE/'runner.py', HERE/'search.py', HERE/'plot.py', HERE/'run.sbatch', HERE/'collect.sbatch']
    files += sorted(p for p in HERE.parent.glob('*') if p.is_file() and p.suffix in ('.py','.sh','.sbatch','.awk'))
    return dict(template_case_sha256=layout['case_sha256'],
                files={str(p.relative_to(REPO)):digest(p) for p in files})


def binary_inputs(binary, proxy, binary_hash, proxy_hash):
    if not re.fullmatch('[0-9a-f]{64}', binary_hash) or proxy_hash != PROXY_HASH:
        raise ValueError('frozen binary SHA256 and pinned proxy hash required')
    if digest(binary) != binary_hash or digest(proxy) != proxy_hash:
        raise ValueError('binary/proxy hash mismatch')


def collect(root):
    manifest = json.loads((root/'suite.json').read_text())
    statuses, attempts, points = [], [], []
    resources = []
    suite = harness()
    for protocol in manifest['protocols']:
        series = root/'series'/protocol
        path = series/'status.json'
        status = json.loads(path.read_text()) if path.exists() else dict(protocol=protocol, complete=False, status='missing')
        protocol_attempts, protocol_points = [], []
        for name, target in [('attempts.csv',attempts),('points.csv',points)]:
            if (series/name).exists():
                with (series/name).open(newline='') as f:
                    records = list(csv.DictReader(f))
                target.extend(records)
                if name == 'attempts.csv':
                    protocol_attempts = records
                else:
                    protocol_points = records
        try:
            if not protocol_points:
                raise ValueError('no selected valid points')
            keys = set()
            for row in protocol_attempts:
                if row['protocol'] != protocol or row['binary_sha256'] != manifest['binary_sha256'] or row['profile'] != 'B':
                    raise ValueError('point identity mismatch')
                key = int(row['offered_rps']), int(row['attempt'])
                if key in keys:
                    raise ValueError('duplicate attempt identity')
                keys.add(key)
                attempt_root = (root/row['path']).resolve()
                if not attempt_root.is_relative_to(series.resolve()):
                    raise ValueError('attempt escaped its series')
                decision = json.loads((attempt_root/'decision.json').read_text())
                for k, value in decision.items():
                    if row[k] != ('' if value is None else str(value)):
                        raise ValueError('decision differs from exported attempt')
                if row['valid'] != 'True':
                    continue
                if suite.collect('latency', attempt_root) != 0:
                    raise ValueError('raw point failed revalidation')
                with (attempt_root/'baseline-latency.csv').open(newline='') as f:
                    measured = list(csv.DictReader(f))
                if len(measured) != 1:
                    raise ValueError('missing point measurement')
                controls = manifest['spec'].get('sweep') or manifest['spec']['search']
                verdict = classify(measured[0], controls['latency_cap_ms'])
                for k, value in verdict.items():
                    if row[k] != ('' if value is None else str(value)):
                        raise ValueError('raw metrics differ from decision: '+k)
                if (attempt_root/'resources.csv').exists():
                    with (attempt_root/'resources.csv').open(newline='') as f:
                        for replica in csv.DictReader(f):
                            resources.append(dict(offered_rps=row['offered_rps'], attempt=row['attempt'],
                                selected=row['selected'], **replica))
            chosen = [r for r in protocol_attempts if r['selected']=='True']
            if sorted(chosen,key=lambda r:(int(r['offered_rps']),int(r['attempt']))) != protocol_points:
                raise ValueError('selected points differ from attempt records')
            if 'sweep' in manifest['spec']:
                controls = SweepSettings(**manifest['spec']['sweep'])
                expected = set(sweep_schedule(controls))
                if keys != expected or len(chosen) != len(expected) or any(r['valid'] != 'True' for r in chosen):
                    raise ValueError('missing or invalid fixed-sweep repetitions')
            elif len({r['offered_rps'] for r in chosen}) != len(chosen):
                raise ValueError('duplicate selected offered rate')
        except (OSError, ValueError, KeyError, subprocess.SubprocessError) as exc:
            status = dict(status, complete=False, validation_error=str(exc))
        statuses.append(status)
    write_csv(root/'attempts.csv', attempts)
    write_csv(root/'points.csv', points)
    write_csv(root/'resources.csv', resources)
    complete = all(s['complete'] for s in statuses)
    write_json(root/'suite-status.json', dict(complete=complete, protocols=statuses,
        selected_points=len(points), attempts=len(attempts)))
    print('Throughput suite '+('completed: ' if complete else 'INCOMPLETE: ')+str(root), flush=True)
    return 0 if complete else 2


def submit(args):
    path = args.spec.resolve()
    spec, _, _, _ = specification(path)
    binary = Path(os.environ['CONSENSUSARENA_BINARY']).resolve()
    proxy = Path(os.environ['CONSENSUSARENA_TOXIPROXY_SERVER']).resolve()
    binary_hash = os.environ['CONSENSUSARENA_BINARY_SHA256']
    binary_inputs(binary, proxy, binary_hash, PROXY_HASH)
    root = Path(os.environ.get('SCRATCH','/scratch/'+os.environ['USER']))/('consensusarena-throughput-'+uuid.uuid4().hex[:12])
    root.mkdir(parents=True, exist_ok=False)
    manifest = dict(kind='fixed_throughput_sweep' if 'sweep' in spec else 'adaptive_throughput', spec_path=str(path), spec=spec,
        protocols=spec['protocols'], fingerprint=fingerprint(path), binary=str(binary),
        proxy=str(proxy), binary_sha256=binary_hash, proxy_sha256=PROXY_HASH)
    write_json(root/'suite.json', manifest)
    env = dict(os.environ, CONSENSUSARENA_ROOT=str(REPO))
    # Neither inherited sbatch dependencies nor array settings should serialize algorithms.
    for key in ('SBATCH_DEPENDENCY','SBATCH_ARRAY_INX'):
        env.pop(key, None)
    command = ['sbatch','--parsable','--account='+args.account,'--export=ALL',
        '--nodes=2','--ntasks=15','--ntasks-per-node=8','--cpus-per-task=8','--mem-per-cpu=1G','--exclusive',
        '--array=0-{}%{}'.format(len(spec['protocols'])-1,args.max_parallel),
        '--time='+args.time,'--output=slurm-throughput-%A_%a.out',str(HERE/'run.sbatch'),str(root)]
    job = subprocess.check_output(command, cwd=REPO, env=env, text=True, timeout=TIMEOUT).strip().split(';')[0]
    if not job.isdigit():
        raise ValueError('unexpected array job ID: '+job)
    write_json(root/'submission.json', dict(array_job=job, root=str(root)))
    print(f'Throughput array: {job} | protocols: {len(spec["protocols"])} | root: {root}', flush=True)
    try:
        collector = subprocess.check_output(['sbatch','--parsable','--account='+args.account,'--export=ALL',
            '--dependency=afterany:'+job,str(HERE/'collect.sbatch'),str(root)],
            cwd=REPO, env=env, text=True, timeout=TIMEOUT).strip().split(';')[0]
        if not collector.isdigit():
            raise ValueError('unexpected collector ID')
    except Exception:
        print(f'Array already submitted. Collect manually: python3 {HERE}/runner.py collect {root}', flush=True)
        raise
    write_json(root/'submission.json', dict(array_job=job, collector_job=collector, root=str(root)))
    print('Collector: '+collector, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('plan','submit'):
        cmd = commands.add_parser(name)
        cmd.add_argument('--spec', type=Path, default=HERE/'experiment.toml')
        if name == 'submit':
            cmd.add_argument('--account', default='dcl')
            cmd.add_argument('--max-parallel', type=int, choices=range(1,7), default=6)
            cmd.add_argument('--time', default='02:00:00')
    for name in ('worker','collect'):
        cmd = commands.add_parser(name)
        cmd.add_argument('root', type=Path)
    args = parser.parse_args()
    if args.command == 'plan':
        spec, _, _, layout = specification(args.spec.resolve())
        print(json.dumps(dict(spec=spec, template_sha256=layout['case_sha256'],
            allocation=dict(nodes=2,tasks=15,cpus_per_task=8), fingerprint=fingerprint(args.spec.resolve())), indent=2))
    elif args.command == 'submit':
        submit(args)
    elif args.command == 'worker':
        return worker(args.root.resolve(), int(os.environ['SLURM_ARRAY_TASK_ID']))
    else:
        return collect(args.root.resolve())


if __name__ == '__main__':
    sys.exit(main())
