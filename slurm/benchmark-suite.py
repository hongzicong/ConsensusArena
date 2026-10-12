#!/usr/bin/env python3
"""Submit isolated Slurm array scenes and collect complete or partial evidence.

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
    manifest = dict(kind=kind, array_job_id=env['SLURM_ARRAY_JOB_ID'], scenes=plan,
                    execution='isolated_slurm_array', nodes_per_scene=3)
    if (root / 'suite.json').exists() and json.loads((root / 'suite.json').read_text()) != manifest:
        raise ValueError('run directory belongs to a different suite')
    # All array elements publish the same manifest, with atomic replacement.
    write_json(root / 'suite.json', manifest)
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
    print('Array scene {}: {} -> {}'.format(index, scene_name(scene), base), flush=True)
    completed = subprocess.run(['bash', str(repo / 'slurm' / ('run-' + ('latency' if kind == 'latency' else 'fault') + '.sbatch'))],
                               env=env, timeout=TIMEOUT)
    write_json(root / 'scene-status' / (scene_name(scene) + '.json'),
               dict(**scene, array_task_id=index, returncode=completed.returncode))
    return completed.returncode


def submit(kind, repo, account, concurrency=None, exports='ALL'):
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
    worker = repo / 'slurm' / ('run-' + ('latency' if kind == 'latency' else 'fault') + '.sbatch')
    output = subprocess.check_output(['sbatch', '--parsable', '--account=' + account,
                                     '--export=ALL', '--array=' + array, str(worker)], cwd=repo, env=env, text=True, timeout=TIMEOUT)
    job = output.strip().split(';')[0]
    if not job.isdigit():
        raise ValueError('unexpected sbatch job ID: ' + output)
    root = suite_root(kind, job, env)
    print('Array job: {} | scenes: {} | run directory: {}'.format(job, len(plan), root), flush=True)
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
    write_json(root / 'suite-status.json', dict(complete=complete, expected_scenes=len(plan),
               completed_scenes=sum(s['complete'] for s in statuses), scenes=statuses))
    print('{} suite {}: {}'.format(kind.title(), 'completed successfully' if complete else 'INCOMPLETE', root), flush=True)
    return 0 if complete else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('plan', 'submit', 'task', 'collect'):
        command = commands.add_parser(name)
        command.add_argument('kind', choices=('latency', 'fault'))
        if name in ('task', 'collect'):
            command.add_argument('path', type=Path)
        if name == 'submit':
            command.add_argument('--account', default='dcl')
            command.add_argument('--export', default='ALL', help='ALL and comma-separated NAME=value overrides')
            command.add_argument('--max-parallel', type=int, help='limit simultaneously running scenes')
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent.parent
    if args.command == 'plan':
        print(json.dumps(scenes(os.environ), indent=2))
    elif args.command == 'submit':
        submit(args.kind, repo, args.account, args.max_parallel, args.export)
    elif args.command == 'task':
        return task(args.kind, args.path)
    else:
        return collect(args.kind, args.path)
    return 0


if __name__ == '__main__':
    sys.exit(main())
