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
        w=csv.DictWriter(f,fieldnames=list(rows[0])); w.writeheader()
        w.writerows({key:str(value).lower() if isinstance(value,bool) else value for key,value in row.items()} for row in rows)

def recovery_window(seconds, start, end, reference, valid):
    """Completion-window observations; not a claim that new requests committed."""
    first=sustained=positive=None
    if valid:
        positive=next((s-start for s in range(start,end-2)
                       if all(seconds[t]['completed']>0 for t in range(s,s+3))),None)
    if valid and reference>0:
        first=next((s-start for s in range(start,end-2)
                    if all(seconds[t]['completed']>=.9*reference for t in range(s,s+3))),None)
        sustained=next((s-start for s in range(start,end-2)
                        if all(seconds[t]['completed']>=.9*reference for t in range(s,end))),None)
    return dict(first_s=first,sustained_from_s=sustained,positive_3seconds_s=positive,
                stays_after_first=(all(seconds[t]['completed']>=.9*reference for t in range(start+first,end)) if first is not None else None))

def summarize(base):
    metadata=json.loads((base/'metadata.json').read_text())
    if 'replica_counts' in metadata:
        sizes=[base/('replicas-'+str(n)) for n in metadata['replica_counts']]
        outcomes=[]
        combined={name:[] for name in ('phase-summary.csv','timeseries.csv','request-cohorts.csv')}
        for size in sizes:
            if not (size/'metadata.json').exists():
                outcomes.append(dict(replicas=int(size.name.split('-')[1]),valid=False,error='missing size metadata'))
                continue
            outcomes.extend(summarize(size))
            for name,rows in combined.items():
                if (size/name).exists():
                    with (size/name).open(newline='') as f: rows.extend(csv.DictReader(f))
        for name,rows in combined.items(): export(base/name,rows)
        (base/'validation.json').write_text(json.dumps(outcomes,indent=2)+'\n')
        return outcomes
    protocol=metadata['protocol']
    origin=metadata.get('timeline_origin','measurement_start')
    warmup=int(metadata.get('warmup_s',0)) if origin=='run_start' else 0
    duration=int(metadata.get('observation_s',metadata.get('measurement_s',60)))
    maximum=metadata.get('max_crash_s',35)
    first=int(metadata.get('crash_s',10))
    if (first,maximum,duration) not in ((5,20,40),(10,35,60)):
        raise ValueError('expected crash-only schedule 5/20/40 or historical 10/35/60')
    if origin not in ('run_start','measurement_start') or (origin=='run_start' and
            ((first,maximum,duration)!=(5,20,40) or warmup!=5 or metadata.get('measurement_s')!=35)):
        raise ValueError('new schedule requires 5 s warmup within 40 s total, with 35 s measurement')
    leader_end=maximum if maximum is not None else duration
    summaries=[]; series=[]; cohorts=[]; outcomes=[]
    for run in sorted(base.glob('ycsb-*/repetition-*')):
        if not (run/'metadata.json').exists() or not (run/'outcome.json').exists():
            outcomes.append(dict(protocol=protocol,replicas=metadata.get('replicas'),valid=False,run=str(run),error='missing run metadata/outcome'))
            continue
        outcome=json.loads((run/'outcome.json').read_text())
        meta=json.loads((run/'metadata.json').read_text())
        if (meta.get('crash_s',first),meta.get('max_crash_s',maximum),meta.get('observation_s',meta.get('measurement_s',duration)),
                meta.get('timeline_origin','measurement_start'),meta.get('warmup_s',0) if origin=='run_start' else 0)!=(first,maximum,duration,origin,warmup):
            raise ValueError('run schedule differs from experiment metadata: '+str(run))
        identity=dict(protocol=protocol,replicas=meta['replicas'],profile=meta['profile'],repetition=meta['repetition'],valid=outcome['valid'],
                      timeline_origin=origin,warmup_s=warmup,observation_s=duration,
                      crash_s=first,max_crash_s=maximum,measurement_s=duration-warmup)
        seconds=collections.defaultdict(bucket)
        cohort=collections.defaultdict(bucket)
        unresolved=collections.Counter()
        coverage=collections.defaultdict(set)
        finals=0; accounting_errors=[]; send_errors=0
        for file in sorted((run/'results').glob('*-fault.jsonl')):
            rows=[json.loads(line) for line in file.read_text().splitlines()]
            for row in rows:
                if row['type']=='start':
                    if row.get('timeline_origin','measurement_start')!=origin:
                        accounting_errors.append(file.name+': client timeline differs from metadata; rebuild executable')
                    if origin=='run_start' and (row.get('warmup_s'),row.get('duration_s'),row.get('observation_s'))!=(warmup,duration-warmup,duration):
                        accounting_errors.append(file.name+': client timing differs from metadata')
                    continue
                # Samples share the configured timeline origin. Assign the tiny
                # scheduling jitter to the nearest one-second interval.
                sec=math.floor((row['start_s']+row['end_s'])/2)
                if 0<=sec<duration:
                    coverage[sec].add(file.name)
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
        missing_seconds=[s for s in range(duration) if len(coverage[s])!=10]
        if missing_seconds: accounting_errors.append('incomplete client coverage in seconds '+str(missing_seconds))
        valid=bool(outcome['valid'] and not accounting_errors)
        identity['valid']=valid
        reference_s=min(10,first)
        pre=sum(seconds[s]['completed'] for s in range(first-reference_s,first))/reference_s
        pre_offered=sum(seconds[s]['offered'] for s in range(first-reference_s,first))/reference_s
        stable=pre_offered>0 and .9*pre_offered<=pre<=1.1*pre_offered
        leader=recovery_window(seconds,first,leader_end,pre,valid and stable)
        recovery=leader['first_s'];sustained_from=leader['sustained_from_s']
        maximum_metrics={}
        if maximum is not None:
            local_ref=sum(seconds[s]['completed'] for s in range(maximum-10,maximum))/10
            local_offered=sum(seconds[s]['offered'] for s in range(maximum-10,maximum))/10
            local_stable=local_offered>0 and .9*local_offered<=local_ref<=1.1*local_offered
            maximum_metrics=dict(injected=bool(outcome.get('max_crash_injected')),target=outcome.get('max_crash_target'),
                                 confirmed_crash_ranks=outcome.get('confirmed_crash_ranks'),
                                 versus_original_reference=recovery_window(seconds,maximum,duration,pre,valid and stable),
                                 versus_premaximum_reference=recovery_window(seconds,maximum,duration,local_ref,valid and local_stable),
                                 positive_completion_3seconds_s=recovery_window(seconds,maximum,duration,0,valid)['positive_3seconds_s'],
                                 premaximum_last10_rps=local_ref,premaximum_stable=local_stable,
                                 final20_mean_rps=(sum(seconds[s]['completed'] for s in range(duration-20,duration))/20 if valid else None),
                                 observation_end_s=duration)
        outcomes.append(dict(**identity,accounting_errors=accounting_errors,baseline_reference_s=reference_s,baseline_rps=pre,baseline_offered_rps=pre_offered,baseline_stable=stable,
                             recovery_90pct_3seconds_s=recovery,send_errors=send_errors,
                             sustained_recovery=leader['stays_after_first'],
                             sustained_recovery_from_s=sustained_from,
                             leader_observation_end_s=leader_end,maximum_failure=maximum_metrics,
                             target=outcome.get('crash_target'),error=outcome.get('error')))
        for sec in range(duration):
            b=seconds[sec]; n=b['completed']
            covered=len(coverage[sec])==10
            series.append(dict(**identity,second=sec,completed=n if covered else None,offered=b['offered'] if covered else None,issued=b['issued'] if covered else None,dropped=b['dropped'] if covered else None,
                               mean_ms=b['latency_sum_ms']/n if n else None,p99_ms=percentile(b['hist'],.99),pending=b['pending'],queue=b['queue']))
        phases=[('warmup' if origin=='run_start' else 'normal',0,first),('crashed',first,leader_end)]
        if maximum is not None: phases.append(('maximum_crashes',maximum,duration))
        for phase,lo,hi in phases:
            b=bucket()
            for sec in range(lo,hi):
                src=seconds[sec]
                for k in ('completed','offered','issued','dropped','latency_sum_ms','send_latency_sum_ms'): b[k]+=src[k]
                b['hist'].update(src['hist'])
            n=b['completed']
            covered=all(len(coverage[s])==10 for s in range(lo,hi))
            summaries.append(dict(**identity,phase=phase,duration_s=hi-lo,completed=n if covered else None,completion_rps=n/(hi-lo) if covered else None,
                                  mean_ms=b['latency_sum_ms']/n if n and covered else None,p99_ms=percentile(b['hist'],.99) if covered else None,
                                  offered=b['offered'] if covered else None,issued=b['issued'] if covered else None,dropped=b['dropped'] if covered else None,pending_at_end=seconds[hi-1]['pending'] if covered else None,
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
