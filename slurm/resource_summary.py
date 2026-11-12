#!/usr/bin/env python3
"""Validate synchronized replica resources and export one row per replica."""
import csv
import json
import math
from pathlib import Path
import sys


def summarize(run):
    topology = json.loads((run / 'topology.json').read_text())
    plan = json.loads((run / 'plan.json').read_text())
    validation = json.loads((run / 'workload-validation.json').read_text())
    if not validation.get('valid') or not validation.get('throughput_valid'):
        raise ValueError('resources require valid matching throughput')
    epoch = int((run / 'status/start-unix-ns').read_text())
    completions = float(validation['completed_rps']) * 10
    if not math.isfinite(completions) or completions < 0:
        raise ValueError('invalid resource denominator')
    aliases = topology['replicas']
    if len(aliases) != topology['replica_count'] or len(set(aliases)) != len(aliases):
        raise ValueError('invalid resource membership')
    counters = ('sent_frames', 'sent_bytes', 'received_frames', 'received_bytes', 'cpu_seconds')
    rows = []
    for alias in aliases:
        data = json.loads((run / 'results' / (alias+'-resources.json')).read_text())
        schema = data['schema']
        checked_counters = counters
        if schema == 'replica-resources-v1':
            samples = data['samples']
            memory = samples
            expected_seconds = list(range(5, 16))
            sampling_mode = 'legacy-in-process-11-samples'
            memory_scope = 'sampled-measurement-window'
        elif schema == 'replica-resources-v2':
            samples = data['counter_samples']
            memory = None
            expected_seconds = [5, 15]
            checked_counters = (*counters, 'peak_rss_bytes')
            sampling_mode = 'boundary-cpu-traffic-peak-memory-v2'
            memory_scope = data['memory_scope']
            if memory_scope != 'process-start-through-measurement-end':
                raise ValueError('invalid peak memory scope: '+alias)
            if data.get('observer') != 'inside-pinned-replica' or not isinstance(data.get('pid'), int) or data['pid'] <= 0:
                raise ValueError('invalid resource observer identity: '+alias)
        else:
            raise ValueError('unknown resource schema: '+str(schema))
        if (data['schema'], data['alias'], data['epoch_ns'], len(samples)) != (
                schema, alias, epoch, len(expected_seconds)):
            raise ValueError('resource identity/window mismatch: '+alias)
        for i, sample in enumerate(samples):
            # Preserve actual timestamps; refuse badly scheduled observations.
            if abs(sample['unix_ns'] - (epoch+expected_seconds[i]*10**9)) > 100_000_000:
                raise ValueError('resource sample missed boundary: '+alias)
            if any(not math.isfinite(sample[k]) or sample[k] < 0 for k in checked_counters):
                raise ValueError('invalid resource sample: '+alias)
            if i and any(sample[k] < samples[i-1][k] for k in checked_counters):
                raise ValueError('resource counter regressed: '+alias)
        if memory is not None and len(memory) != 11:
            raise ValueError('memory samples do not cover the measurement interval: '+alias)
        for i, sample in enumerate(memory or []):
            if abs(sample['unix_ns']-(epoch+(5+i)*10**9)) > 100_000_000:
                raise ValueError('memory sample missed boundary: '+alias)
            if not math.isfinite(sample['rss_bytes']) or sample['rss_bytes'] < 0:
                raise ValueError('invalid memory sample: '+alias)
            if i and sample['unix_ns'] <= memory[i-1]['unix_ns']:
                raise ValueError('memory time did not advance: '+alias)
        first, last = samples[0], samples[-1]
        seconds = (last['unix_ns']-first['unix_ns'])/1e9
        delta = {k: last[k]-first[k] for k in counters}
        # Trapezoidal time average of sampled RSS, including both boundaries.
        rss = (sum((a['rss_bytes']+b['rss_bytes'])/2*(b['unix_ns']-a['unix_ns'])
                   for a, b in zip(memory, memory[1:]))/(memory[-1]['unix_ns']-memory[0]['unix_ns'])
               if memory else None)
        rows.append(dict(replicas=topology['replica_count'], protocol=plan['protocol'].lower(),
            profile=run.parent.name.removeprefix('ycsb-'), topology_id=topology['topology_id'],
            case=topology.get('case', ''), case_sha256=topology.get('case_sha256', ''),
            replica=alias, window_start_ns=first['unix_ns'], window_end_ns=last['unix_ns'],
            completed_requests_in_window=completions, normalization_valid=completions > 0,
            sent_bytes=delta['sent_bytes'], received_bytes=delta['received_bytes'],
            sent_messages=delta['sent_frames'], received_messages=delta['received_frames'],
            traffic_bytes_per_request=(delta['sent_bytes']+delta['received_bytes'])/completions if completions else '',
            messages_per_request=(delta['sent_frames']+delta['received_frames'])/completions if completions else '',
            cpu_percent=100*delta['cpu_seconds']/seconds, mean_rss_mib=rss/2**20 if memory else '',
            sampled_peak_rss_mib=max(s['rss_bytes'] for s in memory)/2**20 if memory else '',
            peak_rss_mib=last['peak_rss_bytes']/2**20 if schema == 'replica-resources-v2' else '',
            memory_window_start_ns=memory[0]['unix_ns'] if memory else '',
            memory_window_end_ns=memory[-1]['unix_ns'] if memory else last['unix_ns'],
            sampling_mode=sampling_mode, memory_scope=memory_scope,
            timed_out=validation['timed_out'], cohort_complete=validation['cohort_complete'],
            schema=schema))
    return rows


def export(base):
    rows = []
    for path in sorted(base.rglob('repetition-01/topology.json')):
        rows.extend(summarize(path.parent))
    if not rows:
        raise ValueError('no resource runs in '+str(base))
    output = base / 'resources.csv'
    temporary = output.with_suffix('.csv.tmp')
    with temporary.open('w', newline='') as target:
        writer = csv.DictWriter(target, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(output)
    return output


if __name__ == '__main__':
    print(export(Path(sys.argv[1])))
