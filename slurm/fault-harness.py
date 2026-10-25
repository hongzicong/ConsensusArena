#!/usr/bin/env python3
"""Crash-stop scene worker: preserve WAN proxies, kill at 5/20s, observe to 40s."""
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

FIRST_CRASH_S = 5
MAX_CRASH_S = 20
OBSERVATION_S = 40
TIMEOUT = 3600

def additional_crash_ranks(count, first, latest=None, policy='leader-first'):
    """Keep the cumulative injected failure count at f, never add f new failures."""
    if count not in (5,9,13) or first not in range(count):
        raise ValueError('invalid replica count or first target')
    if policy not in ('leader-first','preserve-leader'):
        raise ValueError('unknown additional target policy')
    eligible=[rank for rank in range(count) if rank != first]
    if latest in eligible:
        eligible.remove(latest)
        if policy=='leader-first': eligible.insert(0,latest)
        else: eligible.append(latest)
    return eligible[:(count-1)//2-1]

def write_json(path, value):
    path = Path(path)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(value, indent=2) + '\n')
    tmp.replace(path)
def rank_runner(repo, run, binary, toxi):
    metadata = json.loads((run/'metadata.json').read_text())
    REPLICA_COUNT = metadata['replica_count']
    REPLICAS, CLIENTS = metadata['replica_aliases'], metadata['clients']
    MASTER = REPLICA_COUNT + len(CLIENTS)
    rank = int(os.environ['SLURM_PROCID'])
    status = run/'status'
    app = None
    proxy_pid = None
    app_pid = None
    killed = False
    failed = False
    env = dict(os.environ, GOMAXPROCS=os.environ.get('SLURM_CPUS_PER_TASK','8'))
    events = open(run/'results'/('events-rank-%d.jsonl' % rank), 'w', buffering=1)
    def event(kind, **kw):
        events.write(json.dumps(dict(type=kind, unix_ns=time.time_ns(), rank=rank, **kw))+'\n')
    try:
        if rank != MASTER:
            dialmap = subprocess.check_output(['bash',str(repo/'slurm/setup-toxiproxy.sh'),str(run),str(rank),str(toxi)],text=True, timeout=TIMEOUT).strip()
            env['CONSENSUSARENA_DIAL_MAP'] = dialmap
            proxy_pid = int((status/('toxiproxy-%d.pid' % rank)).read_text())
        if rank == MASTER:
            alias, role = 'm0', 'master'
        elif rank < REPLICA_COUNT:
            alias, role = REPLICAS[rank], 'replica'
            env['CONSENSUSARENA_PEER_LISTENERS_READY'] = str(status/'peer-listeners.ready')
            while not (status/'master.ready').exists():
                if (status/'stop').exists(): return
                time.sleep(.1)
        else:
            alias, role = CLIENTS[rank-REPLICA_COUNT], 'client'
            while not (status/'replicas.ready').exists():
                if (status/'stop').exists(): return
                time.sleep(.1)
            env.pop('CONSENSUSARENA_LATENCY_RUN', None)
            env['CONSENSUSARENA_FAULT_RUN'] = str(run)
        args = [str(binary),'-run',role,'-alias',alias,'-config',str(run/'config/cluster.conf')]
        args += ['-log',str(run/'logs'/('master.log' if role == 'master' else alias+'-'+role+'.log'))]
        pid_file = status/('app-pid-%d' % rank)
        resource_file = run/'stdout'/('resources-rank-%d.txt' % rank)
        fields = 'elapsed_seconds=%e\nuser_seconds=%U\nsystem_seconds=%S\nmax_rss_kb=%M\nvoluntary_context_switches=%w\ninvoluntary_context_switches=%c\nexit_status=%x'
        args = ['/usr/bin/time', '-o', str(resource_file), '-f', fields,
                'bash', '-c', 'echo "$$" > "$1"; shift; exec "$@"', 'participant', str(pid_file)] + args
        with open(run/'stdout'/(alias+'-'+role+'.out'),'w') as out:
            app = subprocess.Popen(args, stdout=out, stderr=subprocess.STDOUT, env=env)
        limit = time.monotonic() + 10
        while not pid_file.exists():
            if app.poll() is not None or time.monotonic() > limit:
                raise RuntimeError('participant failed to publish PID')
            time.sleep(.01)
        app_pid = int(pid_file.read_text())
        if role == 'replica':
            subprocess.run([sys.executable, str(repo/'slurm/listener-ready.py'),
                            str(7070+rank), str(status/('listening-%d'%rank))], check=True, timeout=75)
        write_json(status/('process-%d.json'%rank), {'pid':app_pid,'alias':alias,'role':role,'hostname':os.uname().nodename})
        if role == 'master':
            time.sleep(3)
            if app.poll() is not None: raise RuntimeError('master exited')
            (status/'master.ready').touch()
        start = None
        while not (status/'stop').exists():
            if start is None and (status/'start-unix-ns').exists():
                start = int((status/'start-unix-ns').read_text())/1e9
            elapsed = time.time()-start if start else -999
            if rank < REPLICA_COUNT and elapsed >= FIRST_CRASH_S and not killed and (status/'crash-target.json').exists():
                target = json.loads((status/'crash-target.json').read_text())
                if target['rank'] == rank:
                    if app.poll() is not None: raise RuntimeError('target died before injection')
                    event('crash_requested', pid=app_pid, target=target)
                    os.kill(app_pid, signal.SIGKILL)
                    code = app.wait(timeout=5)
                    killed = True
                    event('crash_confirmed', pid=app_pid, returncode=code)
                    (status/('expected-crash-%d'%rank)).touch()
            if rank < REPLICA_COUNT and elapsed >= MAX_CRASH_S and not killed and (status/'max-crash-targets.json').exists():
                target=json.loads((status/'max-crash-targets.json').read_text())
                if rank in target['additional_ranks']:
                    if app.poll() is not None: raise RuntimeError('additional target died before injection')
                    event('max_crash_requested', pid=app_pid, requested_s=MAX_CRASH_S, target=target)
                    os.kill(app_pid, signal.SIGKILL)
                    code=app.wait(timeout=5)
                    killed=True
                    event('max_crash_confirmed', pid=app_pid, requested_s=MAX_CRASH_S, returncode=code)
                    (status/('expected-crash-%d'%rank)).touch()
            code = app.poll()
            if code is not None and not killed:
                if role == 'client':
                    (status/('client-'+alias+'.done')).write_text(str(code))
                    if code: (status/'node-failed').write_text('client '+alias+' exit '+str(code))
                    return
                raise RuntimeError('%s %s exited unexpectedly: %s'%(role,alias,code))
            time.sleep(.02)
    except Exception as exc:
        failed = True
        event('harness_error', error=repr(exc))
        (status/'node-failed').write_text('rank %d: %r'%(rank,exc))
    finally:
        if app is not None and app.poll() is None:
            if app_pid is None:
                app.terminate()
            else:
                try: os.kill(app_pid, signal.SIGTERM)
                except ProcessLookupError: pass
            try: app.wait(timeout=3)
            except subprocess.TimeoutExpired: os.kill(app_pid, signal.SIGKILL); app.wait(timeout=5)
        if proxy_pid:
            try: os.kill(proxy_pid,signal.SIGTERM)
            except ProcessLookupError: pass
        events.close()
    if failed: sys.exit(1)

def choose_target(run, metadata):
    """Translate registration-order replica IDs to deployment ranks by endpoint."""
    text = (run/'logs/master.log').read_text(errors='replace')
    leaders = re.findall(r'replica (\d+) is the new leader', text)
    registered = dict(re.findall(r'node (\d+) \[([^\]]+)\]', text))
    if not leaders or leaders[-1] not in registered:
        raise RuntimeError('missing master leader/registration evidence')
    endpoint = registered[leaders[-1]]
    rank = metadata['replica_endpoints'].index(endpoint)
    return dict(rank=rank, replica_id=int(leaders[-1]), endpoint=endpoint,
                kind='master_last_elected_leader', selected_ns=time.time_ns())


def wait_until(predicate, run, runner, timeout=900):
    deadline = time.monotonic() + timeout
    while not predicate():
        if (run/'status/node-failed').exists():
            raise RuntimeError((run/'status/node-failed').read_text())
        if runner.poll() is not None:
            raise RuntimeError('srun ended before scene completion')
        if time.monotonic() > deadline:
            raise TimeoutError('scene readiness deadline')
        time.sleep(.05)


def validate_evidence(run, metadata, outcome):
    from upstream_harness import preload_evidence, resource_records
    aliases = metadata['replica_aliases']
    digests = preload_evidence(run, dict(metadata, replicas=aliases))
    if digests != metadata.get('preload_digests'):
        raise ValueError('fault preload evidence changed')
    resources = resource_records(run, metadata)
    if not resources or not all(row['complete'] for row in resources):
        raise ValueError('incomplete fault participant resources')
    first = outcome['crash_target']['rank']
    expected = outcome['max_crash_target']['cumulative_ranks']
    confirmed = sorted(int(p.name.rsplit('-', 1)[1]) for p in (run/'status').glob('expected-crash-*'))
    if confirmed != sorted(expected) or len(set(confirmed)) != metadata['replica_count']//2 or first not in confirmed:
        raise ValueError('fault injection identity/count mismatch')
    for rank in confirmed:
        kind = 'crash_confirmed' if rank == first else 'max_crash_confirmed'
        records = [json.loads(line) for line in (run/'results'/('events-rank-%d.jsonl'%rank)).read_text().splitlines()]
        events = [row for row in records if row['type'] == kind]
        if len(events) != 1 or events[0]['returncode'] != 128 + signal.SIGKILL:
            raise ValueError('missing or unsuccessful SIGKILL evidence')
        elapsed = (events[0]['unix_ns'] - outcome['epoch_ns'])/1e9
        if elapsed < (FIRST_CRASH_S if rank == first else MAX_CRASH_S):
            raise ValueError('crash preceded its scheduled injection')


def run_scene(run, prefix, env, output, metadata):
    from upstream_harness import ROOT, preload_evidence, load_module, resource_records
    n = metadata['replica_count']
    clients = metadata['clients']
    binary = run/'binary/consensusarena-linux-amd64'
    toxi = run/'binary/toxiproxy-server-linux-amd64'
    args = prefix + ['--cpus-per-task=8', sys.executable, str(ROOT/'slurm/fault-harness.py'),
                     'rank', str(ROOT), str(run), str(binary), str(toxi)]
    runner = subprocess.Popen(args, env=env, stdout=output, stderr=subprocess.STDOUT)
    result = dict(valid=False, protocol=metadata['protocol'], profile=metadata['profile'], repetition=1)
    try:
        def replicas_ready():
            if len(list((run/'status').glob('listening-*'))) == n:
                (run/'status/peer-listeners.ready').touch()
            return all((run/'logs'/(a+'-replica.log')).exists() and
                       'done connecting to peers' in (run/'logs'/(a+'-replica.log')).read_text(errors='replace').lower()
                       for a in metadata['replica_aliases'])
        wait_until(replicas_ready, run, runner, int(env.get('REPLICA_READY_TIMEOUT_SECONDS', '900')))
        preload_meta = dict(metadata, replicas=metadata['replica_aliases'])
        metadata['preload_digests'] = preload_evidence(run, preload_meta)
        write_json(run/'metadata.json', metadata)
        result['preload_digest'] = next(iter(metadata['preload_digests'].values()))
        (run/'status/replicas.ready').touch()
        wait_until(lambda: all((run/'status'/('client-'+a+'.ready')).exists() for a in clients), run, runner)
        epoch_ns = time.time_ns() + 3_000_000_000
        temporary = run/'status/start.tmp'
        temporary.write_text(str(epoch_ns))
        temporary.replace(run/'status/start-unix-ns')
        result['epoch_ns'] = epoch_ns
        epoch = epoch_ns/1e9
        target = maximum = None
        while time.time() < epoch + OBSERVATION_S + 5:
            if (run/'status/node-failed').exists():
                raise RuntimeError((run/'status/node-failed').read_text())
            if runner.poll() is not None:
                raise RuntimeError('srun exited unexpectedly')
            elapsed = time.time() - epoch
            if elapsed >= FIRST_CRASH_S-1 and target is None:
                target = choose_target(run, metadata)
                write_json(run/'status/crash-target.json', target)
                result['crash_target'] = target
            if elapsed >= MAX_CRASH_S-1 and maximum is None:
                if target is None or not (run/'status'/('expected-crash-%d'%target['rank'])).exists():
                    raise RuntimeError('first crash unconfirmed before maximum stage')
                latest = choose_target(run, metadata)
                policy = env.get('FAULT_MAX_TARGET_POLICY', 'leader-first')
                extra = additional_crash_ranks(n, target['rank'], latest['rank'], policy)
                maximum = dict(additional_ranks=extra, cumulative_ranks=sorted([target['rank']]+extra),
                               cumulative_failure_count=(n-1)//2, requested_s=MAX_CRASH_S,
                               policy=policy, latest_leader_evidence=latest, selected_ns=time.time_ns())
                write_json(run/'status/max-crash-targets.json', maximum)
                result['max_crash_target'] = maximum
            if all((run/'status'/('client-'+a+'.done')).exists() for a in clients):
                break
            time.sleep(.05)
        if not all((run/'status'/('client-'+a+'.done')).exists() for a in clients):
            raise TimeoutError('clients did not end fixed observation')
        confirmed = {int(p.name.rsplit('-',1)[1]) for p in (run/'status').glob('expected-crash-*')}
        if maximum is None or confirmed != set(maximum['cumulative_ranks']):
            raise RuntimeError('injected crash identity/count mismatch')
        policies = {}
        for alias in clients:
            if (run/'status'/('client-'+alias+'.done')).read_text() != '0':
                raise RuntimeError('client failed: '+alias)
            records = [json.loads(line) for line in (run/'results'/(alias+'-fault.jsonl')).read_text().splitlines()]
            if not records or records[-1]['type'] != 'final' or records[0]['type'] != 'start':
                raise RuntimeError('missing client start/final observation')
            if records[0]['epoch_ns'] != epoch_ns:
                raise RuntimeError('client used a different epoch')
            policies[alias] = records[0]['retry_policy']
        result.update(valid=True, confirmed_crash_ranks=sorted(confirmed), max_crash_injected=True,
                      client_retry=policies, completed_ns=time.time_ns())
        metadata['client_retry'] = policies
        write_json(run/'metadata.json', metadata)
    except Exception as exc:
        result['error'] = str(exc)
    finally:
        (run/'status/stop').touch()
        try:
            code = runner.wait(timeout=15)
        except subprocess.TimeoutExpired:
            runner.terminate()
            try:
                code = runner.wait(timeout=10)
            except subprocess.TimeoutExpired:
                runner.kill()
                code = runner.wait(timeout=10)
        output.close()
        resources = resource_records(run, metadata)
        result.update(srun_returncode=code, resources=resources)
        result['valid'] = result['valid'] and code == 0 and all(row['complete'] for row in resources)
        if result['valid']:
            try:
                validate_evidence(run, metadata, result)
            except (OSError, ValueError, KeyError) as exc:
                result.update(valid=False, error=str(exc))
        write_json(run/'outcome.json', result)
    observations = load_module('summarize-fault.py', 'summarize_fault').summarize(run)
    return 0 if len(observations) == 1 and observations[0]['valid'] else 1


if __name__ == '__main__':
    if len(sys.argv) != 6 or sys.argv[1] != 'rank':
        raise SystemExit('usage: fault-harness.py rank REPO RUN BINARY TOXIPROXY')
    rank_runner(*(Path(value) for value in sys.argv[2:]))
