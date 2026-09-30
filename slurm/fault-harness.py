#!/usr/bin/env python3
"""Fixed-window, process-crash fault experiment. No protocol modifications.

All controls run on the allocated compute nodes. Loopback Toxiproxy APIs are
accessed by their owning Slurm rank; no SSH from compute nodes is required.
After 10 s warmup: first crash at 10 s, cumulative f crashes at 35 s,
and observation ends at 60 s. The configured WAN latency stays unchanged.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time

REPLICA_COUNT = int(os.environ.get('CONSENSUSARENA_REPLICAS', '5').split(':')[0])
if REPLICA_COUNT not in (5, 9, 13):
    raise ValueError('replica count must be 5, 9 or 13')
REPLICAS = ['ap-south-1', 'ap-northeast-1', 'eu-west-3', 'us-west-1', 'af-south-1',
            'eu-west-1', 'us-east-1', 'ap-southeast-2', 'sa-east-1',
            'ap-east-1', 'ca-central-1', 'us-east-2', 'us-west-2'][:REPLICA_COUNT]
CLIENTS = ['ap-east-1', 'ap-northeast-1', 'ap-southeast-2', 'eu-west-1',
           'ca-central-1', 'sa-east-1', 'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2']
MASTER = REPLICA_COUNT + len(CLIENTS)
TASKS = MASTER + 1
NODES = (TASKS + 7) // 8
TASKS_PER_NODE = (TASKS + NODES - 1) // NODES
TOXI_HASH = '556d891134a3c582dc1e1a3f7335fd55142e5965769855a00b944e13e48302fc'

FIRST_CRASH_S = 10
MAX_CRASH_S = 35
MEASUREMENT_S = 60

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

def sha(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(1024*1024), b''): h.update(chunk)
    return h.hexdigest()

def fields(path):
    result = {}
    for line in Path(path).read_text().splitlines():
        m = re.match(r'^(\w+):\s+(\S+)', line)
        if m: result[m[1]] = m[2]
    return result

def amend(text, values):
    for key, value in values.items():
        text, n = re.subn(r'^' + re.escape(key) + r':.*$', key + ': ' + str(value), text, flags=re.M)
        if n != 1: raise ValueError('missing/nonunique config field ' + key)
    return text

def rank_runner(repo, run, binary, toxi):
    rank = int(os.environ['SLURM_PROCID'])
    status = run/'status'
    app = None
    proxy_pid = None
    killed = False
    failed = False
    env = dict(os.environ, GOMAXPROCS=os.environ.get('SLURM_CPUS_PER_TASK','8'))
    events = open(run/'results'/('events-rank-%d.jsonl' % rank), 'w', buffering=1)
    def event(kind, **kw):
        events.write(json.dumps(dict(type=kind, unix_ns=time.time_ns(), rank=rank, **kw))+'\n')
    try:
        if rank != MASTER:
            dialmap = subprocess.check_output(['bash',str(repo/'slurm/setup-toxiproxy.sh'),str(run),str(rank),str(toxi)],text=True).strip()
            env['CONSENSUSARENA_DIAL_MAP'] = dialmap
            proxy_pid = int((status/('toxiproxy-%d.pid' % rank)).read_text())
        if rank == MASTER:
            alias, role = 'm0', 'master'
        elif rank < REPLICA_COUNT:
            alias, role = REPLICAS[rank], 'replica'
            while not (status/'master.ready').exists():
                if (status/'stop').exists(): return
                time.sleep(.1)
        else:
            alias, role = CLIENTS[rank-REPLICA_COUNT], 'client'
            while not (status/'replicas.ready').exists():
                if (status/'stop').exists(): return
                time.sleep(.1)
            env['CONSENSUSARENA_FAULT_RUN'] = str(run)
        args = [str(binary),'-run',role,'-alias',alias,'-config',str(run/'config/cluster.conf')]
        if role == 'replica': args += ['-quorum',str(run/'config/quorum.conf')]
        args += ['-log',str(run/'logs'/(alias+'-'+role+'.log'))]
        with open(run/'stdout'/(alias+'-'+role+'.out'),'w') as out:
            app = subprocess.Popen(args, stdout=out, stderr=subprocess.STDOUT, env=env)
        write_json(status/('process-%d.json'%rank), {'pid':app.pid,'alias':alias,'role':role,'hostname':os.uname().nodename})
        if role == 'master':
            time.sleep(3)
            if app.poll() is not None: raise RuntimeError('master exited')
            (status/'master.ready').touch()
        start = None
        warmup = float(fields(run/'config/cluster.conf')['warmup'].rstrip('s'))
        while not (status/'stop').exists():
            if start is None and (status/'start-unix-ns').exists():
                start = int((status/'start-unix-ns').read_text())/1e9 + warmup
            elapsed = time.time()-start if start else -999
            if rank < REPLICA_COUNT and elapsed >= FIRST_CRASH_S and not killed and (status/'crash-target.json').exists():
                target = json.loads((status/'crash-target.json').read_text())
                if target['rank'] == rank:
                    if app.poll() is not None: raise RuntimeError('target died before injection')
                    event('crash_requested', pid=app.pid, target=target)
                    app.kill()
                    code = app.wait(timeout=5)
                    killed = True
                    event('crash_confirmed', pid=app.pid, returncode=code)
                    (status/('expected-crash-%d'%rank)).touch()
            if rank < REPLICA_COUNT and elapsed >= MAX_CRASH_S and not killed and (status/'max-crash-targets.json').exists():
                target=json.loads((status/'max-crash-targets.json').read_text())
                if rank in target['additional_ranks']:
                    if app.poll() is not None: raise RuntimeError('additional target died before injection')
                    event('max_crash_requested', pid=app.pid, requested_s=MAX_CRASH_S, target=target)
                    app.kill()
                    code=app.wait(timeout=5)
                    killed=True
                    event('max_crash_confirmed', pid=app.pid, requested_s=MAX_CRASH_S, returncode=code)
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
            app.terminate()
            try: app.wait(timeout=3)
            except subprocess.TimeoutExpired: app.kill(); app.wait()
        if proxy_pid:
            try: os.kill(proxy_pid,signal.SIGTERM)
            except ProcessLookupError: pass
        events.close()
    if failed: sys.exit(1)

def choose_target(run, protocol):
    if protocol == 'bodega':
        # Bodega elects independently of the advisory master. Require agreement
        # in the latest installed roster records before choosing its leader.
        logs = list((run/'logs').glob('*-replica.log'))
        counts = {}
        for path in logs:
            records = re.findall(r'BODEGA_ROSTER replica=\d+ ballot=(\d+) leader=(\d+)',
                                 path.read_text(errors='replace'))
            if records:
                roster = tuple(map(int, records[-1]))
                counts[roster] = counts.get(roster, 0) + 1
        agreed = [(ballot, leader, count) for (ballot, leader), count in counts.items()
                  if count >= len(logs)//2 + 1]
        if not agreed:
            raise RuntimeError('no majority installed Bodega roster evidence')
        ballot, leader, count = max(agreed)
        return dict(rank=leader, kind='bodega_majority_installed_roster',
                    ballot=ballot, agreeing_replicas=count, selected_ns=time.time_ns())
    if protocol in ('epaxos','fastpaxos'):
        return dict(rank=3,kind='designated_replica_no_global_leader',selected_ns=time.time_ns())
    if protocol in ('curp','n2paxos'):
        return dict(rank=3,kind='configured_fixed_leader',selected_ns=time.time_ns())
    text = (run/'logs/m0-master.log').read_text(errors='replace')
    leaders = re.findall(r'replica (\d+) is the new leader', text)
    if not leaders: raise RuntimeError('no current master leader evidence')
    return dict(rank=int(leaders[-1]),kind='master_last_elected_leader',selected_ns=time.time_ns())

def wait_until(predicate, run, runner, timeout=900):
    limit = time.monotonic()+timeout
    while not predicate():
        if (run/'status/node-failed').exists(): raise RuntimeError((run/'status/node-failed').read_text())
        if runner.poll() is not None: raise RuntimeError('srun ended before run completion')
        if time.monotonic()>limit: raise TimeoutError('readiness deadline')
        time.sleep(.2)

def step_args(cpus=8):
    return ['srun', '--nodes='+str(NODES), '--ntasks='+str(TASKS),
            '--ntasks-per-node='+str(TASKS_PER_NODE), '--cpus-per-task='+str(cpus),
            '--nodelist='+os.environ['FAULT_STEP_NODELIST'], '--distribution=cyclic']

def run_one(repo, base, config_dir, protocol, profile, repetition, binary, toxi, options):
    run = base/('ycsb-'+profile)/('repetition-%02d'%repetition)
    for d in ('config','logs','results','stdout','status'): (run/d).mkdir(parents=True,exist_ok=True)
    for p in config_dir.iterdir():
        if p.is_file(): shutil.copy2(p,run/'config'/p.name)
    workload = amend((run/'config/cluster.conf').read_text(),dict(protocol=protocol,writes={'A':50,'B':5,'C':0}[profile],warmup='10s',duration=str(MEASUREMENT_S)+'s',repetitions=1,**options))
    (run/'config/cluster.conf').write_text(workload)
    write_json(run/'metadata.json',dict(protocol=protocol,profile=profile,repetition=repetition,replicas=REPLICA_COUNT,binary_sha256=sha(binary),options=options,measurement_s=MEASUREMENT_S,crash_s=FIRST_CRASH_S,max_crash_s=MAX_CRASH_S))
    args = step_args() + ['--kill-on-bad-exit=1',
            'python3',str(repo/'slurm/fault-harness.py'),'rank',str(repo),str(run),str(binary),str(toxi)]
    with open(run/'stdout/srun.out','w') as out: runner = subprocess.Popen(args,stdout=out,stderr=subprocess.STDOUT)
    result = dict(valid=False,protocol=protocol,profile=profile,repetition=repetition)
    try:
        def replicas_ready():
            return all((run/'logs'/(a+'-replica.log')).exists() and 'Waiting for client connections' in (run/'logs'/(a+'-replica.log')).read_text(errors='replace') for a in REPLICAS)
        wait_until(replicas_ready,run,runner)
        digests=[]
        for a in REPLICAS:
            matches=re.findall(r'PRELOAD_COMPLETE[^\n]*digest=([0-9a-f]+)',(run/'logs'/(a+'-replica.log')).read_text())
            if not matches: raise RuntimeError('missing preload digest '+a)
            digests.append(matches[-1])
        if len(set(digests))!=1: raise RuntimeError('preload mismatch')
        result['preload_digest']=digests[0]
        (run/'status/replicas.ready').touch()
        wait_until(lambda:all((run/'status'/('client-'+a+'.ready')).exists() for a in CLIENTS),run,runner)
        start_ns=time.time_ns()+3_000_000_000
        tmp=run/'status/start.tmp'; tmp.write_text(str(start_ns)); tmp.replace(run/'status/start-unix-ns')
        result['epoch_ns']=start_ns
        measurement=start_ns/1e9+10
        target=None
        maximum_target=None
        while time.time()<measurement+MEASUREMENT_S+5:
            if (run/'status/node-failed').exists(): raise RuntimeError((run/'status/node-failed').read_text())
            if runner.poll() is not None: raise RuntimeError('srun exited unexpectedly')
            elapsed=time.time()-measurement
            if elapsed>=FIRST_CRASH_S-1 and target is None:
                target=choose_target(run,protocol); write_json(run/'status/crash-target.json',target)
                result['crash_target']=target
            if elapsed>=MAX_CRASH_S-1 and maximum_target is None:
                if target is None or not (run/'status'/('expected-crash-%d'%target['rank'])).exists():
                    raise RuntimeError('first injected crash unconfirmed before maximum stage')
                latest=choose_target(run,protocol)
                policy=os.environ.get('FAULT_MAX_TARGET_POLICY','leader-first')
                extra=additional_crash_ranks(REPLICA_COUNT,target['rank'],latest['rank'],policy)
                maximum_target=dict(additional_ranks=extra,cumulative_ranks=sorted([target['rank']]+extra),
                                    cumulative_failure_count=(REPLICA_COUNT-1)//2,requested_s=MAX_CRASH_S,
                                    policy=policy,latest_leader_evidence=latest,
                                    latest_leader_already_dead=latest['rank']==target['rank'],
                                    selection_rule='latest advertised live leader first, then ascending replica IDs' if policy=='leader-first' else 'ascending IDs with latest advertised live leader last',
                                    selected_ns=time.time_ns())
                write_json(run/'status/max-crash-targets.json',maximum_target)
                result['max_crash_target']=maximum_target
            if all((run/'status'/('client-'+a+'.done')).exists() for a in CLIENTS): break
            time.sleep(.1)
        if not all((run/'status'/('client-'+a+'.done')).exists() for a in CLIENTS): raise TimeoutError('clients did not end fixed observation')
        if target is None or not (run/'status'/('expected-crash-%d'%target['rank'])).exists(): raise RuntimeError('crash not confirmed')
        if maximum_target is None: raise RuntimeError('maximum crash targets not selected')
        confirmed={int(f.name.rsplit('-',1)[1]) for f in (run/'status').glob('expected-crash-*')}
        if confirmed != set(maximum_target['cumulative_ranks']): raise RuntimeError('maximum crash count/identity mismatch')
        result['confirmed_crash_ranks']=sorted(confirmed)
        result['max_crash_injected']=True
        for a in CLIENTS:
            if (run/'status'/('client-'+a+'.done')).read_text()!='0': raise RuntimeError('client failed '+a)
            records=[json.loads(s) for s in (run/'results'/(a+'-fault.jsonl')).read_text().splitlines()]
            if not records or records[-1]['type']!='final': raise RuntimeError('missing client final record')
        for rank in range(MASTER):
            events=[json.loads(s) for s in (run/'results'/('events-rank-%d.jsonl'%rank)).read_text().splitlines()]
            kinds={e['type'] for e in events}
            if rank == target['rank'] and 'crash_confirmed' not in kinds: raise RuntimeError('missing first crash event')
            if rank in maximum_target['additional_ranks'] and 'max_crash_confirmed' not in kinds: raise RuntimeError('missing additional crash event')
        result.update(valid=True,crash_target=target,completed_ns=time.time_ns())
    except Exception as exc:
        result['error']=repr(exc)
    finally:
        (run/'status/stop').touch()
        try: runner.wait(timeout=15)
        except subprocess.TimeoutExpired: runner.terminate(); runner.wait(timeout=10)
        result['srun_returncode']=runner.returncode
        if runner.returncode: result['valid']=False
        write_json(run/'outcome.json',result)
    print(json.dumps(result),flush=True)
    return result

def coordinator_one(repo):
    protocol=os.environ['CONSENSUSARENA_PROTOCOL'].lower()
    base=Path(os.environ.get('CONSENSUSARENA_RUN_DIR','/scratch/%s/consensusarena-fault-%s'%(os.environ['USER'],os.environ['SLURM_JOB_ID'])))
    base.mkdir(parents=True,exist_ok=False)
    (base/'binary').mkdir(); (base/'config').mkdir(); (base/'ranks').mkdir()
    binary=base/'binary/consensusarena'; toxi=base/'binary/toxiproxy'
    shutil.copy2(os.environ['CONSENSUSARENA_BINARY'],binary)
    shutil.copy2(os.environ['CONSENSUSARENA_TOXIPROXY_SERVER'],toxi)
    binary.chmod(0o755); toxi.chmod(0o755)
    assert sha(binary)==os.environ['CONSENSUSARENA_BINARY_SHA256']
    assert sha(toxi)==TOXI_HASH
    subprocess.run(['bash',str(repo/'slurm/prepare-topology.sh'),str(base/'logical')],check=True)
    hosts=subprocess.check_output(['scontrol','show','hostnames',os.environ['SLURM_JOB_NODELIST']],text=True).splitlines()
    if len(hosts)<NODES: raise RuntimeError('insufficient allocated nodes')
    os.environ['FAULT_STEP_NODELIST']=','.join(hosts[:NODES])
    subprocess.run(step_args(1)+['bash',str(repo/'slurm/capture-ip.sh'),str(base/'ranks')],check=True)
    ips=[(base/'ranks'/('%d.ip'%i)).read_text().strip() for i in range(TASKS)]
    mapping=['0.0.2.1 '+ips[MASTER]]+['0.0.0.%d %s:%d'%(i+1,ips[i],7070+i) for i in range(REPLICA_COUNT)]+['0.0.1.%d %s:%d'%(i+1,ips[i+REPLICA_COUNT],17000+i) for i in range(len(CLIENTS))]
    address=base/'config/address-map.txt'; address.write_text('\n'.join(mapping)+'\n')
    for src,dst in [('workload.conf','cluster.conf'),('latency.conf','latency.conf')]:
        with open(base/'config'/dst,'w') as out: subprocess.run(['awk','-f',str(repo/'slurm/remap-addresses.awk'),str(address),str(base/'logical'/src)],stdout=out,check=True)
    # Otherwise the legacy master chooses Paxos' initial leader by tiny physical
    # cluster ICMP differences, changing which logical WAN role is the leader.
    config_path=base/'config/cluster.conf'
    config_text=config_path.read_text()
    config_text=re.sub(r'^(protocol:.*)$',r'\1\nleader: '+ips[3]+':7073',config_text,flags=re.M)
    config_path.write_text(config_text)
    shutil.copy2(base/'logical/quorum.conf',base/'config/quorum.conf')
    options=json.loads(os.environ.get('FAULT_WORKLOAD_OVERRIDES','{}'))
    if 'FAULT_ARRIVAL_RATE' in os.environ:
        options['arrivalRate']=float(os.environ['FAULT_ARRIVAL_RATE'])
    repetitions=int(os.environ.get('FAULT_REPETITIONS','3'))
    profiles=os.environ.get('CONSENSUSARENA_YCSB_PROFILES','A:B:C').split(':')
    write_json(base/'metadata.json',dict(job_id=os.environ['SLURM_JOB_ID'],protocol=protocol,binary_sha256=sha(binary),toxiproxy_sha256=sha(toxi),
             replicas=REPLICA_COUNT,profiles=profiles,repetitions=repetitions,
             initial_leader_site='us-west-1',
             warmup_s=10,crash_s=FIRST_CRASH_S,measurement_s=MEASUREMENT_S,
             max_crash_s=MAX_CRASH_S,max_total_failures=(REPLICA_COUNT-1)//2,
             max_target_policy=os.environ.get('FAULT_MAX_TARGET_POLICY','leader-first'),
             cpus_per_task=8,mem_per_cpu='1G',client_retry='none; unresolved retained in counts',options=options))
    outcomes=[]
    for profile in profiles:
        for repetition in range(1,repetitions+1):
            outcomes.append(run_one(repo,base,base/'config',protocol,profile,repetition,binary,toxi,options))
            write_json(base/'outcomes.json',outcomes)
    subprocess.run(['python3',str(repo/'slurm/summarize-fault.py'),str(base)],check=True)
    print('Fault run completed: '+str(base),flush=True)
    if not all(o['valid'] for o in outcomes): sys.exit(2)

def coordinator(repo):
    counts=[int(n) for n in os.environ.get('CONSENSUSARENA_REPLICAS','5:9:13').split(':')]
    if not counts or len(set(counts))!=len(counts) or any(n not in (5,9,13) for n in counts):
        raise ValueError('expected distinct replica counts chosen from 5:9:13')
    required=max(counts)+len(CLIENTS)+1
    if int(os.environ.get('SLURM_NTASKS','0'))<required or int(os.environ.get('SLURM_CPUS_PER_TASK','0'))<8:
        raise RuntimeError('insufficient Slurm allocation for requested fault sweep')
    base=Path(os.environ.get('CONSENSUSARENA_RUN_DIR','/scratch/%s/consensusarena-fault-%s'%(os.environ['USER'],os.environ['SLURM_JOB_ID'])))
    base.mkdir(parents=True,exist_ok=False)
    write_json(base/'metadata.json',dict(job_id=os.environ['SLURM_JOB_ID'],protocol=os.environ['CONSENSUSARENA_PROTOCOL'].lower(),replica_counts=counts,crash_s=FIRST_CRASH_S,max_crash_s=MAX_CRASH_S,measurement_s=MEASUREMENT_S))
    outcomes=[]
    for count in counts:
        env=dict(os.environ,CONSENSUSARENA_REPLICAS=str(count),CONSENSUSARENA_RUN_DIR=str(base/('replicas-'+str(count))))
        completed=subprocess.run([sys.executable,str(repo/'slurm/fault-harness.py'),'coordinator-one',str(repo)],env=env)
        outcomes.append(dict(replicas=count,returncode=completed.returncode))
        write_json(base/'size-outcomes.json',outcomes)
    subprocess.run([sys.executable,str(repo/'slurm/summarize-fault.py'),str(base)],check=True)
    print('Fault sweep completed: '+str(base),flush=True)
    if any(o['returncode'] for o in outcomes): sys.exit(2)

if __name__=='__main__':
    if sys.argv[1]=='rank': rank_runner(*(Path(p) for p in sys.argv[2:]))
    elif sys.argv[1]=='coordinator': coordinator(Path(sys.argv[2]))
    elif sys.argv[1]=='coordinator-one': coordinator_one(Path(sys.argv[2]))
    else: raise SystemExit('expected coordinator, coordinator-one or rank')
