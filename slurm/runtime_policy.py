"""Single logical CPU per replica: launch policy and independently checked evidence."""
import argparse
import json
import os
from pathlib import Path
import re
import socket
import time

POLICY = 'replica-single-cpu-v2'
SELECTOR = 'CONSENSUSARENA_RUNTIME_POLICY'
CONTROLS = dict(CONSENSUSARENA_REPLICA_GOMAXPROCS='1',
    CONSENSUSARENA_CLIENT_GOMAXPROCS='2', CONSENSUSARENA_MASTER_GOMAXPROCS='2',
    CONSENSUSARENA_REPLICA_TASKSET='1', CONSENSUSARENA_AFFINITY_AFTER_MEASUREMENT='1',
    CONSENSUSARENA_TASK_CPUS='2', CONSENSUSARENA_TASKS_PER_NODE='32')


def configure(env, kind=None):
    env = dict(env)
    policy = env.get(SELECTOR, '')
    if not policy:
        return env
    if policy != POLICY:
        raise ValueError('unknown replica runtime policy: '+policy)
    if kind not in (None, 'latency'):
        raise ValueError('single-CPU policy supports latency runs only; fault-suite pinning is not implemented')
    for key, value in CONTROLS.items():
        if key in env and env[key] != value:
            raise ValueError('single-CPU policy conflicts with '+key+'='+env[key])
        env[key] = value
    return env


def manifest(env):
    env = configure(env)
    return dict(runtime_policy=POLICY) if env.get(SELECTOR) else {}


def save(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix+'.tmp')
    temporary.write_text(json.dumps(value, indent=2)+'\n')
    temporary.replace(path)


def select(root, rank):
    allowed = sorted(os.sched_getaffinity(0))
    if len(allowed) != 2:
        raise ValueError('expected a two-CPU Slurm task binding')
    cpu, other = allowed[0], allowed[1:]
    topology = Path(f'/sys/devices/system/cpu/cpu{cpu}/topology')
    record = dict(host=socket.gethostname(), rank=rank, selected_cpu=cpu,
        originally_allowed_cpus=allowed, supervisor_cpus=other, pin_before_exec=True, proxy_pin_before_exec=True,
        resources_enabled=os.environ.get('CONSENSUSARENA_RESOURCES', '0') == '1')
    for name in ['physical_package_id', 'core_id', 'thread_siblings_list']:
        record[name] = (topology/name).read_text().strip()
    save(root/'stdout'/f'affinity-rank-{rank}-policy.json', record)
    print(cpu, ','.join(map(str, other)))


def process_pid(root, rank, role, expected_executable):
    pid = int((root/'status'/f'{role}-{rank}.pid').read_text())
    proc = Path('/proc')/str(pid)
    executable = os.readlink(proc/'exe')
    if Path(executable).name != expected_executable:
        raise ValueError('recorded PID is not the '+role+' executable: '+executable)
    return pid, proc, executable


def thread_snapshot(proc):
    threads = []
    for task in sorted((proc/'task').iterdir()):
        try:
            allowed = sorted(os.sched_getaffinity(int(task.name)))
            stat = (task/'stat').read_text().rsplit(')', 1)[1].split()
        except (ProcessLookupError, FileNotFoundError):
            continue
        threads.append(dict(tid=int(task.name), allowed_cpus=allowed,
            last_cpu=int(stat[36]), user_ticks=int(stat[11]), system_ticks=int(stat[12])))
    return threads


def snapshot(root, rank, cpu, phase):
    pid, proc, executable = process_pid(root, rank, 'replica', 'consensusarena-linux-amd64')
    threads = thread_snapshot(proc)
    proxy_pid, proxy_proc, proxy_executable = process_pid(root, rank, 'toxiproxy', 'toxiproxy-server-linux-amd64')
    proxy_threads = thread_snapshot(proxy_proc)
    infrastructure_cpus = sorted(os.sched_getaffinity(0))
    epoch_file = root/'status/start-unix-ns'
    record = dict(host=socket.gethostname(), rank=rank, pid=pid, phase=phase,
        snapshot_ns=time.time_ns(), epoch_ns=int(epoch_file.read_text()) if epoch_file.exists() else None,
        executable=executable, selected_cpu=cpu, observer_allowed_cpus=infrastructure_cpus,
        proxy=dict(pid=proxy_pid, executable=proxy_executable, threads=proxy_threads,
            all_threads_isolated=bool(proxy_threads) and all(t['allowed_cpus'] == infrastructure_cpus for t in proxy_threads)),
        threads=threads, all_threads_pinned=bool(threads) and all(t['allowed_cpus'] == [cpu] for t in threads))
    save(root/'stdout'/f'affinity-rank-{rank}-{phase}.json', record)
    if not record['all_threads_pinned'] or cpu in infrastructure_cpus or not record['proxy']['all_threads_isolated']:
        raise ValueError('single-CPU affinity or infrastructure isolation failed')


def validate_run(base, run, scene):
    (base/'runtime-validation.json').unlink(missing_ok=True)
    settings = json.loads((base/'runtime-settings.json').read_text())
    if settings.get('runtime_policy') != POLICY:
        raise ValueError('missing single-CPU launch settings')
    env = settings['environment']
    if any(env.get(k) != v for k, v in CONTROLS.items()):
        raise ValueError('incorrect recorded CPU controls')
    metadata = dict(line.split('=', 1) for line in (base/'metadata.txt').read_text().splitlines() if '=' in line)
    if metadata.get('runtime_policy') != POLICY or metadata.get('replica_gomaxprocs') != '1' or metadata.get('replica_logical_cpus') != '1':
        raise ValueError('CPU policy metadata differs from the required policy')
    if metadata.get('resources_enabled') not in ('0', '1'):
        raise ValueError('missing resource observation policy')
    expected_mode = 'boundary-cpu-traffic-peak-memory-v2' if metadata['resources_enabled'] == '1' else 'disabled'
    if metadata.get('resource_sampling_mode') != expected_mode:
        raise ValueError('resource observation mode differs from policy')
    topology = json.loads((run/'topology.json').read_text())
    n, clients = scene['replicas'], len(topology['clients'])
    epoch = int((run/'status/start-unix-ns').read_text())
    expression = re.compile(r'RUNTIME_CONFIG role=(\S+) alias=(\S+) gomaxprocs=(\d+) num_cpu=(\d+)')
    startups = 0
    for role, count, maximum, cpus in [('replica', n, 1, 1), ('client', clients, 2, 2), ('master', 1, 2, 2)]:
        logs = list((run/'stdout').glob('*-'+role+'.out')) if role != 'master' else [run/'stdout/master.out']
        if len(logs) != count:
            raise ValueError('runtime log count mismatch: '+role)
        aliases = set()
        for log in logs:
            found = expression.findall(log.read_text(errors='replace'))
            if len(found) != 1 or found[0][0] != role or (int(found[0][2]), int(found[0][3])) != (maximum, cpus):
                raise ValueError('effective runtime settings mismatch: '+str(log))
            aliases.add(found[0][1])
            startups += 1
        if len(aliases) != count:
            raise ValueError('duplicate runtime identities: '+role)
        expected_aliases = set(topology['replicas'] if role == 'replica' else topology['clients']) if role != 'master' else {'m0'}
        if aliases != expected_aliases:
            raise ValueError('runtime aliases differ from topology: '+role)
    identities, snapshots, thread_entries, proxy_thread_entries = set(), 0, 0, 0
    for rank in range(n):
        prefix = run/'stdout'/f'affinity-rank-{rank}'
        policy = json.loads(Path(str(prefix)+'-policy.json').read_text())
        cpu = policy['selected_cpu']
        identity = policy['host'], cpu
        allowed = policy['originally_allowed_cpus']
        if (identity in identities or len(set(allowed)) != 2 or cpu not in allowed or
                policy['rank'] != rank or not policy['pin_before_exec'] or not policy['proxy_pin_before_exec'] or
                set(policy['supervisor_cpus']) != set(allowed)-{cpu}):
            raise ValueError('replicas share a CPU or task binding changed')
        if policy['resources_enabled'] != (metadata.get('resources_enabled') == '1'):
            raise ValueError('resource sampling policy differs from launch metadata')
        identities.add(identity)
        ready_pid = None
        ready_proxy_pid = None
        for phase in ['ready', 'measurement_end']:
            snap = json.loads(Path(str(prefix)+'-'+phase+'.json').read_text())
            if (snap['host'], snap['selected_cpu'], snap['rank'], snap['phase']) != (policy['host'], cpu, rank, phase):
                raise ValueError('affinity snapshot identity mismatch')
            if (not snap['all_threads_pinned'] or not snap['threads'] or
                    set(snap['observer_allowed_cpus']) != set(policy['supervisor_cpus'])):
                raise ValueError('affinity or observer isolation mismatch')
            if (snap['pid'] <= 0 or Path(snap['executable']).name != 'consensusarena-linux-amd64' or
                    snap['epoch_ns'] not in (None, epoch) or
                    (phase == 'measurement_end' and snap['epoch_ns'] != epoch)):
                raise ValueError('affinity snapshot process or epoch mismatch')
            if any(t['allowed_cpus'] != [cpu] or t['last_cpu'] != cpu for t in snap['threads']):
                raise ValueError('replica thread escaped its CPU')
            proxy = snap['proxy']
            if (proxy['pid'] <= 0 or Path(proxy['executable']).name != 'toxiproxy-server-linux-amd64' or
                    not proxy['all_threads_isolated'] or not proxy['threads'] or
                    any(set(t['allowed_cpus']) != set(policy['supervisor_cpus']) or
                        t['last_cpu'] not in policy['supervisor_cpus'] for t in proxy['threads'])):
                raise ValueError('Toxiproxy affinity or infrastructure isolation mismatch')
            if ready_proxy_pid is None:
                ready_proxy_pid = proxy['pid']
            elif ready_proxy_pid != proxy['pid']:
                raise ValueError('Toxiproxy process changed during measurement')
            if ready_pid is None:
                ready_pid = snap['pid']
            elif ready_pid != snap['pid']:
                raise ValueError('replica process changed during measurement')
            if phase == 'ready':
                if snap['snapshot_ns'] >= epoch+5*10**9:
                    raise ValueError('initial affinity check was not before measurement')
            elif not epoch+15*10**9 <= snap['snapshot_ns'] < epoch+19*10**9:
                raise ValueError('postmeasurement affinity check missed its interval')
            snapshots += 1
            thread_entries += len(snap['threads'])
            proxy_thread_entries += len(proxy['threads'])
        alias = topology['replicas'][rank]
        resource_file = run/'results'/(alias+'-resources.json')
        if metadata.get('resources_enabled') == '1':
            resources = json.loads(resource_file.read_text())
            if resources.get('schema') != 'replica-resources-v2' or resources.get('pid') != ready_pid or resources.get('observer') != 'inside-pinned-replica':
                raise ValueError('resource observer is not inside the verified replica process')
        elif resource_file.exists():
            raise ValueError('unexpected resource observer in sampling-disabled run')
    result = dict(runtime_policy=POLICY, valid=True, replica_gomaxprocs=1, replica_logical_cpus=1,
        replicas=n, clients=clients, startups_verified=startups,
        affinity_snapshots=snapshots, affinity_thread_entries=thread_entries,
        proxy_affinity_thread_entries=proxy_thread_entries)
    save(base/'runtime-validation.json', result)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['select', 'snapshot'])
    parser.add_argument('root', type=Path)
    parser.add_argument('rank', type=int)
    parser.add_argument('arguments', nargs='*')
    args = parser.parse_args()
    if args.command == 'select':
        select(args.root, args.rank)
    else:
        snapshot(args.root, args.rank, int(args.arguments[0]), args.arguments[1])
