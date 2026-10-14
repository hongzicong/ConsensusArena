#!/usr/bin/env python3
"""Check adapters against actual Go source at the pinned master reference.

Builds a temporary oracle under ignored results/, never checks out another branch.
The oracle contains the donor config/placement packages and ONLY its plan.go
policies, not protocol state machines. Runs all nine size/topology cases for
five shared protocols, with both placement objectives.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'slurm'))
from upstream_harness import prepare, settings


def function(source, name):
    match = re.search(r'^func (?:\([^\n]*\) )?' + re.escape(name) + r'\([^\n]*[\s\S]*?^}\s*', source, re.M)
    if not match:
        raise ValueError('missing function ' + name)
    return match.group().strip() + '\n'


def main():
    manifest = json.loads((ROOT / 'upstream-provenance.json').read_text())
    reference = manifest['benchmark_reference_commit']

    def donor(path):
        return subprocess.check_output(['git', 'show', reference + ':' + path], cwd=ROOT).decode()

    for path, expected in manifest['benchmark_reference_sha256_lf'].items():
        if hashlib.sha256(donor(path).replace('\r\n', '\n').encode()).hexdigest() != expected:
            raise ValueError('reference digest mismatch: ' + path)
    normalize = lambda source: subprocess.check_output(['gofmt'], input=('package parity\n' + source).encode())
    source = donor('client/buffer.go')
    adapted = (ROOT / 'client/benchmark.go').read_text()
    names = ('DeriveWorkloadSeed', 'mixWorkloadSeed', 'loopOpen', 'poissonInterval', 'sendScheduledRequest',
             'nextUpdateValue', 'genGetKey', 'sharedKeyMapping', 'sharedZipfCDF', 'randomTrue')
    for name in names:
        original = function(source, name).replace('*BufferClient', '*BenchmarkClient')
        if normalize(original) != normalize(function(adapted, name)):
            raise ValueError('sampling/clock function differs from master: ' + name)
    for name in ('Preload', 'fillPreloadedValue'):
        if normalize(function(donor('state/state.go'), name)) != normalize(function((ROOT / 'state/preload.go').read_text(), name)):
            raise ValueError('preload function differs from master: ' + name)
    if (ROOT / 'slurm/workload.conf').read_text() != donor('slurm/workload.conf'):
        raise ValueError('workload template differs from master')

    (ROOT / 'results').mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=ROOT / 'results', prefix='master-parity-') as directory:
        temporary = Path(directory)
        for package in ('config', 'placement', 'replicaset'):
            paths = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', reference, package], cwd=ROOT).decode().splitlines()
            for path in paths:
                if path.endswith('_test.go'):
                    continue
                destination = temporary / path
                destination.parent.mkdir(exist_ok=True)
                destination.write_text(donor(path), encoding='utf-8')
        (temporary / 'go.mod').write_text('module github.com/hongzicong/ConsensusArena\n\ngo 1.23\n')
        protocols = ('swiftpaxos', 'paxos', 'curp', 'fastpaxos', 'epaxos')
        packages = ('swift', 'paxos', 'curp', 'fastpaxos', 'epaxos')
        body = 'package main\nimport("encoding/json";"os";"fmt";"math";"slices";"github.com/hongzicong/ConsensusArena/config";"github.com/hongzicong/ConsensusArena/placement")\n'
        for protocol, package in zip(protocols, packages):
            body += function(donor(package + '/plan.go'), 'PlanDeployment').replace('PlanDeployment', 'plan_' + protocol, 1)
        body += 'func fastQuorumSize(n int)int{return 3*n/4+1}\n'
        body += '''func main(){
 c,err:=config.Read(os.Args[1],"");if err!=nil{panic(err)}
 t,err:=placement.Load(c,os.Args[1]);if err!=nil{panic(err)}
 var p config.DeploymentPlan
 switch os.Args[2]{
'''
        body += ''.join('case "'+p+'":p,err=plan_'+p+'(c,t)\n' for p in protocols)
        body += '};if err!=nil{panic(err)};p.Protocol=os.Args[2];if err=json.NewEncoder(os.Stdout).Encode(p);err!=nil{panic(fmt.Sprint(err))}}\n'
        (temporary / 'main.go').write_text(body, encoding='utf-8')
        binary = temporary / ('oracle.exe' if os.name == 'nt' else 'oracle')
        subprocess.run(['go', 'build', '-o', str(binary), '.'], cwd=temporary, check=True, timeout=7200)
        checked = 0
        for count in (5, 9, 13):
            for topology in (1, 2, 3):
                for protocol in protocols:
                    for objective in ('slow-first', 'fast-first'):
                        base = temporary / f'{count}-{topology}-{protocol}-{objective}'
                        options = settings(dict(CONSENSUSARENA_PROTOCOL=protocol, CONSENSUSARENA_QUORUM_OBJECTIVE=objective))
                        actual = prepare(base, dict(replicas=count, topology_id=topology, profile='A'),
                                         ['10.0.0.1'] * (count+11), options)['placement_plan']
                        env = dict(os.environ, CONSENSUSARENA_QUORUM_OBJECTIVE=objective)
                        env.pop('CONSENSUSARENA_CLIENT_WEIGHTS', None)
                        expected = json.loads(subprocess.check_output([str(binary), str(base / 'config/cluster.conf'), protocol], env=env))
                        leader = expected['leader']['alias'] if expected['leader'] else None
                        quorum = [r['alias'] for r in expected['fast_quorum'] or []]
                        if (leader, quorum, expected['client_routes'], expected['model']) != (
                                actual['leader'], actual['fast_quorum'], actual['client_routes'], actual['model']):
                            raise ValueError(f'placement mismatch: {base.name}')
                        for field in ('fast_mean_ms', 'slow_mean_ms'):
                            if abs(expected.get(field, 0)-actual[field]) > 1e-10:
                                raise ValueError(f'cost mismatch: {base.name}, {field}: {expected.get(field, 0)} != {actual[field]}')
                        checked += 1
    print(f'PASS: {len(names)} sampling/clock functions, 2 preload functions, template, {checked} placement cases match master {reference}.')


if __name__ == '__main__':
    main()
