#!/usr/bin/env python3
"""Fixed-window, process-crash fault experiment. No protocol modifications.

All controls run on the allocated compute nodes. Loopback Toxiproxy APIs are
accessed by their owning Slurm rank; no SSH from compute nodes is required.
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
import urllib.request

REPLICAS = ['ap-south-1', 'ap-northeast-1', 'eu-west-3', 'us-west-1', 'af-south-1']
CLIENTS = ['ap-east-1', 'ap-northeast-1', 'ap-southeast-2', 'eu-west-1',
           'ca-central-1', 'sa-east-1', 'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2']
MASTER = 15
TOXI_HASH = '556d891134a3c582dc1e1a3f7335fd55142e5965769855a00b944e13e48302fc'

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

def api(rank, path='', data=None, method=None):
    body = None if data is None else json.dumps(data).encode()
    request = urllib.request.Request('http://127.0.0.1:%d%s' % (18000+rank, path),
                                     data=body, method=method,
                                     headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(request, timeout=3) as response:
        raw = response.read()
        return json.loads(raw) if raw else None

def affected_proxies(rank, slow):
    source_site = REPLICAS[rank] if rank < 5 else CLIENTS[rank-5]
    return ['rank-%d-replica-%d' % (rank, target) for target in range(5)
            if source_site != REPLICAS[target]
            and (source_site == REPLICAS[slow] or target == slow)]

def delay(rank, slow, enable):
    names = affected_proxies(rank, slow)
    for name in names:
        for stream in ['upstream', 'downstream']:
            toxic = 'fault-extra-' + stream
            path = '/proxies/' + name + '/toxics'
            if enable:
                api(rank, path, {'name':toxic, 'type':'latency', 'stream':stream,
                                'toxicity':1.0, 'attributes':{'latency':100, 'jitter':0}})
            else:
                api(rank, path+'/'+toxic, method='DELETE')
    snapshot = api(rank, '/proxies')
    for name in names:
        present = {t['name'] for t in snapshot[name]['toxics']}
        assert ('fault-extra-upstream' in present) == enable
        assert ('fault-extra-downstream' in present) == enable
    return names, snapshot

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
        elif rank < 5:
            alias, role = REPLICAS[rank], 'replica'
            while not (status/'master.ready').exists():
                if (status/'stop').exists(): return
                time.sleep(.1)
        else:
            alias, role = CLIENTS[rank-5], 'client'
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
        applied = removed = False
        slow = int(os.environ.get('FAULT_SLOW_REPLICA','1'))
        warmup = float(fields(run/'config/cluster.conf')['warmup'].rstrip('s'))
        while not (status/'stop').exists():
            if start is None and (status/'start-unix-ns').exists():
                start = int((status/'start-unix-ns').read_text())/1e9 + warmup
            elapsed = time.time()-start if start else -999
            if rank != MASTER and elapsed >= 20 and not applied:
                begin = time.time_ns()
                names,snapshot = delay(rank,slow,True)
                write_json(run/'config'/('slow-proxies-%d.json'%rank),snapshot)
                event('delay_on', requested_s=20, started_ns=begin, proxies=names, extra_rtt_ms=200)
                applied = True
            if rank != MASTER and elapsed >= 40 and not removed:
                begin = time.time_ns()
                names,snapshot = delay(rank,slow,False)
                write_json(run/'config'/('restored-proxies-%d.json'%rank),snapshot)
                event('delay_off', requested_s=40, started_ns=begin, proxies=names)
                removed = True
            if rank < 5 and elapsed >= 60 and not killed and (status/'crash-target.json').exists():
                target = json.loads((status/'crash-target.json').read_text())
                if target['rank'] == rank:
                    if app.poll() is not None: raise RuntimeError('target died before injection')
                    event('crash_requested', pid=app.pid, target=target)
                    app.kill()
                    code = app.wait(timeout=5)
                    killed = True
                    event('crash_confirmed', pid=app.pid, returncode=code)
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

def run_one(repo, base, config_dir, protocol, profile, repetition, binary, toxi, options):
    run = base/('ycsb-'+profile)/('repetition-%02d'%repetition)
    for d in ('config','logs','results','stdout','status'): (run/d).mkdir(parents=True,exist_ok=True)
    for p in config_dir.iterdir():
        if p.is_file(): shutil.copy2(p,run/'config'/p.name)
    workload = amend((run/'config/cluster.conf').read_text(),dict(protocol=protocol,writes={'A':50,'B':5,'C':0}[profile],warmup='10s',duration='100s',repetitions=1,**options))
    (run/'config/cluster.conf').write_text(workload)
    write_json(run/'metadata.json',dict(protocol=protocol,profile=profile,repetition=repetition,replicas=5,binary_sha256=sha(binary),options=options))
    args = ['srun','--nodes=2','--ntasks=16','--ntasks-per-node=8','--cpus-per-task=8','--distribution=cyclic','--kill-on-bad-exit=1',
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
        while time.time()<measurement+105:
            if (run/'status/node-failed').exists(): raise RuntimeError((run/'status/node-failed').read_text())
            if runner.poll() is not None: raise RuntimeError('srun exited unexpectedly')
            elapsed=time.time()-measurement
            if elapsed>=59 and target is None:
                target=choose_target(run,protocol); write_json(run/'status/crash-target.json',target)
            if all((run/'status'/('client-'+a+'.done')).exists() for a in CLIENTS): break
            time.sleep(.1)
        if not all((run/'status'/('client-'+a+'.done')).exists() for a in CLIENTS): raise TimeoutError('clients did not end fixed observation')
        if target is None or not (run/'status'/('expected-crash-%d'%target['rank'])).exists(): raise RuntimeError('crash not confirmed')
        for a in CLIENTS:
            if (run/'status'/('client-'+a+'.done')).read_text()!='0': raise RuntimeError('client failed '+a)
            records=[json.loads(s) for s in (run/'results'/(a+'-fault.jsonl')).read_text().splitlines()]
            if not records or records[-1]['type']!='final': raise RuntimeError('missing client final record')
        for rank in range(MASTER):
            events=[json.loads(s) for s in (run/'results'/('events-rank-%d.jsonl'%rank)).read_text().splitlines()]
            if not {'delay_on','delay_off'} <= {e['type'] for e in events}: raise RuntimeError('missing network phase event')
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

def coordinator(repo):
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
    subprocess.run(['srun','--nodes=2','--ntasks=16','--ntasks-per-node=8','--cpus-per-task=1','--distribution=cyclic','bash',str(repo/'slurm/capture-ip.sh'),str(base/'ranks')],check=True)
    ips=[(base/'ranks'/('%d.ip'%i)).read_text().strip() for i in range(16)]
    mapping=['0.0.2.1 '+ips[15]]+['0.0.0.%d %s:%d'%(i+1,ips[i],7070+i) for i in range(5)]+['0.0.1.%d %s:%d'%(i+1,ips[i+5],17000+i) for i in range(10)]
    address=base/'config/address-map.txt'; address.write_text('\n'.join(mapping)+'\n')
    for src,dst in [('workload.conf','cluster.conf'),('latency.conf','latency.conf')]:
        with open(base/'config'/dst,'w') as out: subprocess.run(['awk','-f',str(repo/'slurm/remap-addresses.awk'),str(address),str(base/'logical'/src)],stdout=out,check=True)
    # Otherwise the legacy master chooses Paxos' initial leader by tiny physical
    # cluster ICMP differences, changing which logical WAN role is slowed.
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
             replicas=5,profiles=profiles,repetitions=repetitions,slow_site=REPLICAS[int(os.environ.get('FAULT_SLOW_REPLICA','1'))],
             initial_leader_site='us-west-1',
             extra_rtt_ms=200,extra_one_way_ms=100,warmup_s=10,delay_on_s=20,delay_off_s=40,crash_s=60,measurement_s=100,
             cpus_per_task=8,mem_per_cpu='1G',client_retry='none; unresolved retained in counts',options=options))
    outcomes=[]
    for profile in profiles:
        for repetition in range(1,repetitions+1):
            outcomes.append(run_one(repo,base,base/'config',protocol,profile,repetition,binary,toxi,options))
            write_json(base/'outcomes.json',outcomes)
    subprocess.run(['python3',str(repo/'slurm/summarize-fault.py'),str(base)],check=True)
    print('Fault run completed: '+str(base),flush=True)
    if not all(o['valid'] for o in outcomes): sys.exit(2)

if __name__=='__main__':
    if sys.argv[1]=='rank': rank_runner(*(Path(p) for p in sys.argv[2:]))
    elif sys.argv[1]=='coordinator': coordinator(Path(sys.argv[2]))
    else: raise SystemExit('expected coordinator or rank')
