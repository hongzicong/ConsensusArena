#!/usr/bin/env python3
"""Submit isolated Slurm array scenes (default) or serial scenes and collect evidence.

Run from ConsensusArena: python3 slurm/benchmark-suite.py submit latency|fault
Environment overrides are shared with the existing single-scene harnesses.
Latency and fault use the fixed 27-scene topology/workload matrix.
"""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from upstream_harness import selection, settings

TIMEOUT = 3600
DEFAULT_MAX_PARALLEL = 6
ROOT = Path(__file__).resolve().parent.parent


def scenes(env, kind='latency'):
    settings(env, kind)
    return selection(env)


def scene_name(scene):
    return 'replicas-{replicas}-topology-{topology_id}-ycsb-{profile}'.format(**scene)


def allocation(plan):
    return 3, 24


def plan_sha256(plan):
    return hashlib.sha256(json.dumps(plan, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def check_plan(env, plan):
    expected = env.get('CONSENSUSARENA_PLAN_SHA256')
    if expected and expected != plan_sha256(plan):
        raise ValueError('suite inputs or scene selection changed after submission')


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


def run_scene(kind, repo, root, plan, index, env, array=False):
    env = dict(os.environ if env is None else env)
    scene = plan[index]
    base = root / 'scenes' / scene_name(scene)
    base.parent.mkdir(parents=True, exist_ok=True)
    if base.exists():
        raise FileExistsError('refusing to overwrite scene evidence: ' + str(base))
    env.update(CONSENSUSARENA_ROOT=str(repo), CONSENSUSARENA_SCENE_WORKER='1',
               CONSENSUSARENA_REPLICAS=str(scene['replicas']),
               CONSENSUSARENA_TOPOLOGY=str(scene['topology_id']),
               CONSENSUSARENA_YCSB_PROFILES=scene['profile'],
               CONSENSUSARENA_CASES='{replicas}-{topology_id}'.format(**scene),
               CONSENSUSARENA_RUN_DIR=str(base))
    env.pop('CONSENSUSARENA_CASE', None)
    env.pop('CONSENSUSARENA_CASE_SHA256', None)
    print('{} scene {}: {} -> {}'.format('Array' if array else 'Serial', index, scene_name(scene), base), flush=True)
    command = ['bash', str(repo / 'slurm' / ('run-' + kind + '.sbatch'))]
    outcome = dict(**scene, scene_index=index)
    if array:
        outcome['array_task_id'] = index
    try:
        log = root / 'scene-logs' / (scene_name(scene) + '.out')
        log.parent.mkdir(parents=True, exist_ok=True)
        with log.open('w') as output:
            completed = subprocess.run(command, env=env, timeout=TIMEOUT,
                                       stdout=output, stderr=subprocess.STDOUT)
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
    """Run one selected scene in this array element's own allocation."""
    env = dict(os.environ if env is None else env)
    plan = scenes(env, kind)
    check_plan(env, plan)
    index = int(env['SLURM_ARRAY_TASK_ID'])
    job = env['SLURM_ARRAY_JOB_ID']
    if not job.isdigit() or index < 0:
        raise ValueError('invalid Slurm array identity')
    # Direct sbatch arrays may retain 27 indices after selecting fewer scenes.
    if index >= len(plan):
        print('No selected scene for array index ' + str(index), flush=True)
        return 0
    root = suite_root(kind, job, env)
    publish_manifest(root, dict(kind=kind, array_job_id=job, scenes=plan,
                                execution='isolated_slurm_array', nodes_per_scene=allocation(plan)[0], settings=settings(env, kind)))
    return run_scene(kind, repo, root, plan, index, env, array=True)


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
    plan = scenes(env, kind)
    check_plan(env, plan)
    root = suite_root(kind, job, env)
    if root.exists():
        raise FileExistsError('refusing to overwrite suite evidence: ' + str(root))
    publish_manifest(root, dict(kind=kind, job_id=job, scenes=plan,
                                execution='serial_slurm_allocation', nodes_per_scene=allocation(plan)[0], settings=settings(env, kind)))
    print('Serial suite: {} | scenes: {} | run directory: {}'.format(job, len(plan), root), flush=True)
    try:
        for index, scene in enumerate(plan):
            code = run_scene(kind, repo, root, plan, index, env)
            clean_steps(job)
            print('Scene {}/{}: {} | exit={}'.format(index + 1, len(plan), scene_name(scene), code), flush=True)
    except Exception as exc:
        write_json(root / 'serial-error.json', dict(error=str(exc)))
        collect(kind, root)
        raise
    return collect(kind, root)


def submit(kind, repo, account, exports='ALL', walltime=None, mode='array', max_parallel=None):
    if mode not in ('array', 'serial'):
        raise ValueError('expected array or serial mode')
    if max_parallel is not None and (mode != 'array' or max_parallel < 1):
        raise ValueError('--max-parallel requires array mode and a positive limit')
    env = dict(os.environ)
    seen = set()
    for entry in exports.split(','):
        if entry == 'ALL':
            continue
        key, separator, value = entry.partition('=')
        if not separator or not key.isidentifier() or key in seen:
            raise ValueError('--export accepts ALL and NAME=value entries')
        seen.add(key)
        env[key] = value
    plan = scenes(env, kind)
    nodes, tasks = allocation(plan)
    env['CONSENSUSARENA_PLAN_SHA256'] = plan_sha256(plan)
    env['CONSENSUSARENA_SETTINGS_JSON'] = json.dumps(settings(env, kind), sort_keys=True)
    worker = repo / 'slurm' / ('run-' + kind + '.sbatch')
    env['CONSENSUSARENA_ROOT'] = str(repo)
    env['CONSENSUSARENA_SCENE_WORKER'] = '0'
    command = ['sbatch', '--parsable', '--account=' + account, '--export=ALL',
               '--nodes=' + str(nodes), '--ntasks=' + str(tasks)]
    if walltime is not None:
        command.append('--time=' + walltime)
    if mode == 'array':
        # Retain the existing default budget of at most 18 simultaneous nodes.
        limit = min(DEFAULT_MAX_PARALLEL, max(1, 18 // nodes)) if max_parallel is None else max_parallel
        command.extend(['--array=0-{}%{}'.format(len(plan) - 1, limit),
                        '--output=slurm-%A_%a.out'])
    command.append(str(worker))
    output = subprocess.check_output(command, cwd=repo, env=env, text=True, timeout=TIMEOUT)
    job = output.strip().split(';')[0]
    if not job.isdigit():
        raise ValueError('unexpected sbatch job ID: ' + output)
    root = suite_root(kind, job, env)
    print('{} job: {} | scenes: {} | nodes/allocation: {} | tasks/allocation: {} | run directory: {}'.format(
        mode.title(), job, len(plan), nodes, tasks, root), flush=True)
    if mode == 'serial':
        print('Results are collected inside the same allocation; log: slurm-{}.out'.format(job), flush=True)
        return
    try:
        collector = subprocess.check_output(
            ['sbatch', '--parsable', '--account=' + account, '--export=ALL',
             '--dependency=afterany:' + job, str(repo / 'slurm/collect-suite.sbatch'),
             str(repo), kind, str(root)], cwd=repo, env=env, text=True, timeout=TIMEOUT).strip().split(';')[0]
        if not collector.isdigit():
            raise ValueError('unexpected collector job ID: ' + collector)
    except (OSError, subprocess.SubprocessError, ValueError):
        print('Array is submitted, but collector submission failed. Collect manually with:\n'
              'python3 slurm/benchmark-suite.py collect {} {}'.format(kind, root), file=sys.stderr)
        raise
    print('Collector job: {} | log: slurm-{}.out'.format(collector, collector), flush=True)


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
    from upstream_harness import collect_suite
    return collect_suite(root, kind)


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
            command.add_argument('--mode', choices=('array', 'serial'), default='array',
                                 help='parallel array (default) or one serial allocation')
            command.add_argument('--max-parallel', type=int,
                                 help='maximum simultaneous array scenes (default: up to 6 within 18 nodes)')
            command.add_argument('--time', help='Slurm wall-time limit per array scene or entire serial suite')
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent.parent
    if args.command == 'plan':
        env = dict(os.environ)
        print(json.dumps(scenes(env, args.kind), indent=2))
    elif args.command == 'submit':
        submit(args.kind, repo, args.account, args.export, args.time, args.mode, args.max_parallel)
    elif args.command == 'task':
        return task(args.kind, args.path)
    elif args.command == 'serial':
        return serial(args.kind, args.path)
    else:
        return collect(args.kind, args.path)
    return 0


if __name__ == '__main__':
    sys.exit(main())
