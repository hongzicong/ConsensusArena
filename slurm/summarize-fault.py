#!/usr/bin/env python3
"""Aggregate client completion windows; retain censored operation cohorts."""
import collections
import csv
import json
import math
from pathlib import Path
import sys

def percentile(hist, fraction):
    total=sum(hist.values())
    if not total: return None
    target=math.ceil(total*fraction); count=0
    for k in sorted(hist):
        count+=hist[k]
        if count>=target: return k

def bucket():
    return dict(completed=0,offered=0,issued=0,dropped=0,latency_sum_ms=0.,send_latency_sum_ms=0.,hist=collections.Counter(),pending=0,queue=0)

def add(target, stats):
    for key in ('completed','offered','issued','dropped','latency_sum_ms','send_latency_sum_ms'):
        target[key]+=stats.get(key,0)
    target['hist'].update({int(k):v for k,v in (stats.get('latency_histogram_ms_ceil') or {}).items()})

def export(path, rows):
    if not rows: return
    with Path(path).open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)

def summarize(base):
    metadata=json.loads((base/'metadata.json').read_text())
    protocol=metadata['protocol']
    summaries=[]; series=[]; cohorts=[]; outcomes=[]
    for run in sorted(base.glob('ycsb-*/repetition-*')):
        outcome=json.loads((run/'outcome.json').read_text())
        meta=json.loads((run/'metadata.json').read_text())
        identity=dict(protocol=protocol,profile=meta['profile'],repetition=meta['repetition'],valid=outcome['valid'])
        seconds=collections.defaultdict(bucket)
        cohort=collections.defaultdict(bucket)
        unresolved=collections.Counter()
        finals=0; accounting_errors=[]; send_errors=0
        for file in sorted((run/'results').glob('*-fault.jsonl')):
            rows=[json.loads(line) for line in file.read_text().splitlines()]
            for row in rows:
                if row['type']=='start': continue
                # Samples share an absolute measurement epoch. Assign the tiny
                # scheduling jitter to the nearest one-second interval.
                sec=math.floor((row['start_s']+row['end_s'])/2)
                if 0<=sec<100:
                    b=seconds[sec]; add(b,row['stats']); b['pending']+=row['pending']; b['queue']+=row['queue']
                if row['type']=='final':
                    finals+=1; send_errors+=row['send_errors']
                    if row['total_issued']!=row['total_completed']+row['pending']:
                        accounting_errors.append(file.name+': issued accounting')
                    if row['total_offered']!=row['total_issued']+row['total_dropped']+row.get('unissued',row['queue']):
                        accounting_errors.append(file.name+': offered accounting')
                    for k,stats in row['cohorts'].items(): add(cohort[k],stats)
                    unresolved.update(row['unresolved'])
        if finals!=10: accounting_errors.append('expected 10 final client records; got '+str(finals))
        valid=bool(outcome['valid'] and not accounting_errors)
        identity['valid']=valid
        pre=sum(seconds[s]['completed'] for s in range(10,20))/10
        pre_offered=sum(seconds[s]['offered'] for s in range(10,20))/10
        stable=pre_offered>0 and .9*pre_offered<=pre<=1.1*pre_offered
        recovery=None
        if valid and stable and pre>0:
            for s in range(60,98):
                if all(seconds[t]['completed']>=.9*pre for t in range(s,s+3)):
                    recovery=s-60; break
        outcomes.append(dict(**identity,accounting_errors=accounting_errors,baseline_last10_rps=pre,baseline_last10_offered_rps=pre_offered,baseline_stable=stable,
                             recovery_90pct_3seconds_s=recovery,send_errors=send_errors,
                             target=outcome.get('crash_target'),error=outcome.get('error')))
        for sec in range(100):
            b=seconds[sec]; n=b['completed']
            series.append(dict(**identity,second=sec,completed=n,offered=b['offered'],issued=b['issued'],dropped=b['dropped'],
                               mean_ms=b['latency_sum_ms']/n if n else None,p99_ms=percentile(b['hist'],.99),pending=b['pending'],queue=b['queue']))
        for phase,lo,hi in [('normal',0,20),('slow',20,40),('restored',40,60),('crashed',60,100)]:
            b=bucket()
            for sec in range(lo,hi):
                src=seconds[sec]
                for k in ('completed','offered','issued','dropped','latency_sum_ms','send_latency_sum_ms'): b[k]+=src[k]
                b['hist'].update(src['hist'])
            n=b['completed']
            summaries.append(dict(**identity,phase=phase,duration_s=hi-lo,completed=n,completion_rps=n/(hi-lo),
                                  mean_ms=b['latency_sum_ms']/n if n else None,p99_ms=percentile(b['hist'],.99),
                                  offered=b['offered'],issued=b['issued'],dropped=b['dropped'],pending_at_end=seconds[hi-1]['pending'],
                                  recovery_90pct_3seconds_s=recovery if phase=='crashed' else None))
        for key,b in sorted(cohort.items()):
            phase,operation=key.split('/')
            cohorts.append(dict(**identity,issue_phase=phase,operation=operation,offered=b['offered'],issued=b['issued'],
                                completed_by_end=b['completed'],unresolved_at_end=unresolved[key],dropped=b['dropped'],
                                completion_fraction=b['completed']/b['issued'] if b['issued'] else None,
                                completed_mean_ms=b['latency_sum_ms']/b['completed'] if b['completed'] else None,
                                completed_p99_ms=percentile(b['hist'],.99)))
    export(base/'phase-summary.csv',summaries)
    export(base/'timeseries.csv',series)
    export(base/'request-cohorts.csv',cohorts)
    (base/'validation.json').write_text(json.dumps(outcomes,indent=2)+'\n')
    return outcomes

if __name__=='__main__': summarize(Path(sys.argv[1]))
