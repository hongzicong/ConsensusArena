#!/usr/bin/env python3
"""Export one latency sweep in the report's explicit topology CSV format."""
import csv
import json
from pathlib import Path
import sys


def export(base, cases=None):
    base = Path(base)
    if cases is None and (base/'metadata.txt').exists():
        metadata = dict(line.split('=',1) for line in (base/'metadata.txt').read_text().splitlines() if '=' in line)
        cases = metadata.get('selected_cases')
    selected = {tuple(map(int, case.split('-'))) for case in cases.split(':')} if cases else None
    if selected and any(n not in (5,9,13) or t not in (1,2,3) for n,t in selected):
        raise ValueError('invalid selected size/topology cases')
    rows = []
    for path in sorted(base.rglob('topology.json')):
        run = path.parent
        if run.name != 'repetition-01':
            continue
        topology = json.loads(path.read_text())
        if selected and (topology['replica_count'],topology['topology_id']) not in selected:
            continue
        plan = json.loads((run/'plan.json').read_text())
        with (run/'results/summary.csv').open(newline='') as source:
            overall = [row for row in csv.DictReader(source) if row['Region']=='OVERALL' and row['Operation']=='ALL']
        if len(overall) != 1 or int(overall[0]['Count']) <= 0:
            raise ValueError('missing overall latency measurement: '+str(run))
        row = overall[0]
        rows.append(dict(replicas=topology['replica_count'],protocol=plan['protocol'].lower(),
                         profile=run.parent.name.removeprefix('ycsb-'),topology_id=topology['topology_id'],
                         repetition=1,status='complete',
                         count=row['Count'],mean_ms=row['MeanMs'],p99_ms=row['P99Ms']))
    if not rows:
        raise ValueError('no completed topology runs under '+str(base))
    keys = {(r['replicas'],r['protocol'],r['profile'],r['topology_id']) for r in rows}
    if len(keys) != len(rows):
        raise ValueError('duplicate topology measurements')
    groups = {key[:3] for key in keys}
    if any({key[3] for key in keys if key[:3] == group} !=
           ({t for n,t in selected if n == group[0]} if selected else {1,2,3}) for group in groups):
        raise ValueError('each size/profile must contain all selected topologies')
    if selected and {(key[0],key[3]) for key in keys} != selected:
        raise ValueError('missing selected size/topology cases')
    path=base/'baseline-latency.csv'
    with path.open('w',newline='') as output:
        writer=csv.DictWriter(output,fieldnames=list(rows[0]))
        writer.writeheader();writer.writerows(rows)
    return path


if __name__ == '__main__':
    print(export(sys.argv[1],sys.argv[2] if len(sys.argv)>2 else None))
