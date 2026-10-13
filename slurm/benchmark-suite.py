#!/usr/bin/env python3
"""Submit array or serial Slurm scenes and collect complete or partial evidence.

Run from ConsensusArena: python3 slurm/benchmark-suite.py submit latency|fault
Environment overrides are shared with the existing single-scene harnesses.
"""
import argparse
import csv
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

TIMEOUT = 7200


def scenes(env):
    counts = [int(n) for n in env.get('CONSENSUSARENA_REPLICAS', '5:9:13').split(':')]
    profiles = [p.strip().upper() for p in env.get('CONSENSUSARENA_YCSB_PROFILES', 'A:B:C').split(':')]
    if not counts or len(set(counts)) != len(counts) or any(n not in (5, 9, 13) for n in counts):
        raise ValueError('expected distinct replica counts from 5:9:13')
    if not profiles or len(set(profiles)) != len(profiles) or any(p not in ('A', 'B', 'C') for p in profiles):
        raise ValueError('expected distinct YCSB profiles from A:B:C')
    cases = env.get('CONSENSUSARENA_CASES', '')
    selected = None
    if cases:
        pairs = [tuple(map(int, case.split('-'))) for case in cases.split(':')]
        if len(set(pairs)) != len(pairs) or any(len(pair) != 2 or pair[0] not in counts or pair[1] not in (1, 2, 3) for pair in pairs):
            raise ValueError('invalid, duplicate or unrequested size-topology cases')
        selected = set(pairs)
    return [dict(replicas=n, topology_id=t, profile=p)
            for n in counts for t in (1, 2, 3) for p in profiles
            if selected is None or (n, t) in selected]


def scene_name(scene):
    return 'replicas-{replicas}-topology-{topology_id}-ycsb-{profile}'.format(**scene)


def suite_root(kind, job, env):
    scratch = Path(env.get('SCRATCH', '/scratch/' + env['USER']))
    prefix = 'consensusarena' if kind == 'latency' else 'consensusarena-fault'
    return Path(env.get('CONSENSUSARENA_RUN_DIR', str(scratch / (prefix + '-' + job))))


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    temp.write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8')
    temp.replace(path)


def run_scene(kind, repo, root, plan, index, env, serial=False):
    env = dict(os.environ if env is None else env)
    scene = plan[index]
    base = root / 'scenes' / scene_name(scene)
    base.parent.mkdir(parents=True, exist_ok=True)
    if base.exists():
        raise FileExistsError('refusing to overwrite scene evidence: ' + str(base))
    env.update(CONSENSUSARENA_SCENE_WORKER='1',
               CONSENSUSARENA_REPLICAS=str(scene['replicas']),
               CONSENSUSARENA_TOPOLOGY=str(scene['topology_id']),
               CONSENSUSARENA_YCSB_PROFILES=scene['profile'],
               CONSENSUSARENA_CASES='{replicas}-{topology_id}'.format(**scene),
               CONSENSUSARENA_RUN_DIR=str(base))
    print('{} scene {}: {} -> {}'.format('Serial' if serial else 'Array', index, scene_name(scene), base), flush=True)
    command = ['bash', str(repo / 'slurm' / ('run-' + kind + '.sbatch'))]
    outcome = dict(**scene, array_task_id=None if serial else index, scene_index=index)
    try:
        if serial:
            log = root / 'scene-logs' / (scene_name(scene) + '.out')
            log.parent.mkdir(parents=True, exist_ok=True)
            with log.open('w') as output:
                completed = subprocess.run(command, env=env, timeout=TIMEOUT,
                                           stdout=output, stderr=subprocess.STDOUT)
        else:
            completed = subprocess.run(command, env=env, timeout=TIMEOUT)
        outcome['returncode'] = completed.returncode
    except (OSError, subprocess.TimeoutExpired) as exc:
        outcome.update(returncode=124 if isinstance(exc, subprocess.TimeoutExpired) else 1,
                       error=str(exc))
    write_json(root / 'scene-status' / (scene_name(scene) + '.json'),
               outcome)
    return outcome['returncode']


def publish_manifest(root, manifest):
    if (root / 'suite.json').exists() and json.loads((root / 'suite.json').read_text()) != manifest:
        raise ValueError('run directory belongs to a different suite')
    write_json(root / 'suite.json', manifest)


def task(kind, repo, env=None):
    env = dict(os.environ if env is None else env)
    plan = scenes(env)
    index = int(env['SLURM_ARRAY_TASK_ID'])
    # Direct sbatch keeps the default 27 indices even when overrides select fewer.
    if index < 0:
        raise ValueError('negative array index')
    if index >= len(plan):
        print('No selected scene for array index ' + str(index), flush=True)
        return 0
    root = suite_root(kind, env['SLURM_ARRAY_JOB_ID'], env)
    publish_manifest(root, dict(kind=kind, array_job_id=env['SLURM_ARRAY_JOB_ID'], scenes=plan,
                                execution='isolated_slurm_array', nodes_per_scene=3))
    return run_scene(kind, repo, root, plan, index, env)


def clean_steps(job):
    """Drain leftover workload steps before reusing the allocation; keep batch/extern."""
    if not job.isdigit():
        raise ValueError('serial mode requires a non-array Slurm job ID')
    deadline = time.monotonic() + 60
    cancelled = set()
    while True:
        output = subprocess.check_output(
            ['squeue', '--steps', '--noheader', '--jobs=' + job, '--format=%i'],
            text=True, timeout=TIMEOUT)
        steps = [step.strip() for step in output.splitlines()
                 if step.strip().startswith(job + '.') and step.strip().split('.')[-1].isdigit()]
        if not steps:
            return
        new = set(steps) - cancelled
        if new:
            # A step may finish between the query and cancellation. The next query
            # verifies cleanup even if scancel reports that it already disappeared.
            subprocess.run(['scancel', '--signal=KILL', *sorted(new)], timeout=TIMEOUT)
            cancelled.update(new)
        if time.monotonic() >= deadline:
            raise RuntimeError('workload steps did not terminate: ' + ', '.join(steps))
        time.sleep(1)


def serial(kind, repo, env=None):
    env = dict(os.environ if env is None else env)
    if env.get('SLURM_ARRAY_TASK_ID'):
        raise ValueError('serial mode must run in a non-array allocation')
    job = env['SLURM_JOB_ID']
    if not job.isdigit():
        raise ValueError('invalid Slurm job ID')
    plan = scenes(env)
    root = suite_root(kind, job, env)
    if root.exists():
        raise FileExistsError('refusing to overwrite suite evidence: ' + str(root))
    publish_manifest(root, dict(kind=kind, job_id=job, scenes=plan,
                                execution='serial_slurm_allocation', nodes_per_scene=3))
    print('Serial suite: {} | scenes: {} | run directory: {}'.format(job, len(plan), root), flush=True)
    try:
        for index, scene in enumerate(plan):
            code = run_scene(kind, repo, root, plan, index, env, serial=True)
            clean_steps(job)
            print('Scene {}/{}: {} | exit={}'.format(index + 1, len(plan), scene_name(scene), code), flush=True)
    except Exception as exc:
        write_json(root / 'serial-error.json', dict(error=str(exc)))
        collect(kind, root)
        raise
    return collect(kind, root)


def submit(kind, repo, account, concurrency=None, exports='ALL', mode='serial', walltime=None):
    if mode == 'serial' and concurrency is not None:
        raise ValueError('--max-parallel applies only to array mode')
    env = dict(os.environ)
    for entry in exports.split(','):
        if entry == 'ALL':
            continue
        key, separator, value = entry.partition('=')
        if not separator or not key.isidentifier():
            raise ValueError('--export accepts ALL and NAME=value entries')
        env[key] = value
    plan = scenes(env)
    array = '0-' + str(len(plan) - 1)
    if concurrency is not None:
        if concurrency < 1:
            raise ValueError('concurrency must be positive')
        array += '%' + str(concurrency)
    worker = repo / 'slurm' / ('run-serial.sbatch' if mode == 'serial' else 'run-' + kind + '.sbatch')
    command = ['sbatch', '--parsable', '--account=' + account, '--export=ALL']
    if walltime is not None:
        command.append('--time=' + walltime)
    if mode == 'array':
        command.append('--array=' + array)
    command.append(str(worker))
    if mode == 'serial':
        command.extend([str(repo), kind])
    output = subprocess.check_output(command, cwd=repo, env=env, text=True, timeout=TIMEOUT)
    job = output.strip().split(';')[0]
    if not job.isdigit():
        raise ValueError('unexpected sbatch job ID: ' + output)
    root = suite_root(kind, job, env)
    print('{} job: {} | scenes: {} | run directory: {}'.format(mode.title(), job, len(plan), root), flush=True)
    if mode == 'serial':
        print('Results are collected inside the same allocation; log: slurm-{}.out'.format(job), flush=True)
        return
    try:
        collector = subprocess.check_output(
            ['sbatch', '--parsable', '--account=' + account, '--dependency=afterany:' + job,
             str(repo / 'slurm/collect-suite.sbatch'), str(repo), kind, str(root)],
            cwd=repo, env=env, text=True, timeout=TIMEOUT).strip().split(';')[0]
    except subprocess.CalledProcessError:
        print('Collector submission failed; array is still submitted. Collect manually with:\n'
              'python3 slurm/benchmark-suite.py collect {} {}'.format(kind, root), file=sys.stderr)
        raise
    print('Collector job: ' + collector, flush=True)


def merge_csv(paths, output):
    fields = None
    rows = []
    for path in paths:
        with path.open(newline='') as source:
            reader = csv.DictReader(source)
            if not reader.fieldnames:
                raise ValueError('missing CSV header: ' + str(path))
            if fields is None:
                fields = reader.fieldnames
            elif fields != reader.fieldnames:
                raise ValueError('incompatible CSV headers: ' + str(path))
            rows.extend(reader)
    if fields:
        temp = output.with_name(output.name + '.tmp')
        with temp.open('w', newline='') as target:
            writer = csv.DictWriter(target, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)
        temp.replace(output)
    else:
        output.unlink(missing_ok=True)
    return rows


def collect(kind, root):
    manifest = json.loads((root / 'suite.json').read_text())
    if manifest['kind'] != kind:
        raise ValueError('suite kind mismatch')
    plan = manifest['scenes']
    names = [scene_name(scene) for scene in plan]
    if not names or len(set(names)) != len(names):
        raise ValueError('empty or duplicate manifest scenes')
    files = {name: [] for name in (('summary.csv', 'repetition-summaries.csv', 'baseline-latency.csv')
                                 if kind == 'latency' else ('phase-summary.csv', 'timeseries.csv', 'request-cohorts.csv'))}
    statuses = []
    validations = []
    for scene, name in zip(plan, names):
        base = root / 'scenes' / name
        status = dict(**scene, complete=False)
        try:
            outcome = json.loads((root / 'scene-status' / (name + '.json')).read_text())
            if any(outcome[key] != scene[key] for key in scene):
                raise ValueError('scene status identity mismatch')
            status['returncode'] = outcome['returncode']
            if kind == 'fault' and (base / 'metadata.json').exists():
                subprocess.run([sys.executable, str(Path(__file__).with_name('summarize-fault.py')), str(base)],
                               check=True, timeout=TIMEOUT)
                validation = json.loads((base / 'validation.json').read_text())
                validations.extend(validation)
                if len(validation) != 1 or any(not row['valid'] for row in validation):
                    status['error'] = 'invalid fault measurements'
                elif any(str(validation[0].get(key)) != str(scene[key]) for key in scene):
                    raise ValueError('fault measurement identity mismatch')
            for filename, paths in files.items():
                if (base / filename).exists():
                    paths.append(base / filename)
                elif filename != 'request-cohorts.csv':
                    status['error'] = 'missing ' + filename
            if kind == 'latency' and (base / 'baseline-latency.csv').exists():
                with (base / 'baseline-latency.csv').open(newline='') as source:
                    rows = list(csv.DictReader(source))
                if len(rows) != 1 or any(str(rows[0][key]) != str(scene[key]) for key in scene):
                    raise ValueError('latency measurement identity mismatch')
            status['complete'] = outcome['returncode'] == 0 and 'error' not in status
        except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as exc:
            status['error'] = str(exc)
        statuses.append(status)
    for filename, paths in files.items():
        merge_csv(paths, root / filename)
    if kind == 'fault':
        write_json(root / 'validation.json', validations)
    complete = all(status['complete'] for status in statuses)
    suite_status = dict(complete=complete, expected_scenes=len(plan),
                        completed_scenes=sum(s['complete'] for s in statuses), scenes=statuses)
    if (root / 'serial-error.json').exists():
        complete = False
        suite_status.update(complete=False, error=json.loads((root / 'serial-error.json').read_text())['error'])
    write_json(root / 'suite-status.json', suite_status)
    print('{} suite {}: {}'.format(kind.title(), 'completed successfully' if complete else 'INCOMPLETE', root), flush=True)
    return 0 if complete else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('plan', 'submit', 'task', 'serial', 'collect'):
        command = commands.add_parser(name)
        command.add_argument('kind', choices=('latency', 'fault'))
        if name in ('task', 'serial', 'collect'):
            command.add_argument('path', type=Path)
        if name == 'submit':
            command.add_argument('--account', default='dcl')
            command.add_argument('--export', default='ALL', help='ALL and comma-separated NAME=value overrides')
            command.add_argument('--max-parallel', type=int, help='limit simultaneously running scenes')
            command.add_argument('--mode', choices=('array', 'serial'), default='serial',
                                 help='reuse one allocation sequentially (default: serial) or array scenes')
            command.add_argument('--time', help='Slurm wall-time limit (serial: entire suite; array: each scene)')
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent.parent
    if args.command == 'plan':
        print(json.dumps(scenes(os.environ), indent=2))
    elif args.command == 'submit':
        submit(args.kind, repo, args.account, args.max_parallel, args.export, args.mode, args.time)
    elif args.command == 'task':
        return task(args.kind, args.path)
    elif args.command == 'serial':
        return serial(args.kind, args.path)
    else:
        return collect(args.kind, args.path)
    return 0


if __name__ == '__main__':
    sys.exit(main())
