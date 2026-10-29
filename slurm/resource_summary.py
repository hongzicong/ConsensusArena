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
        samples = data['samples']
        if (data['schema'], data['alias'], data['epoch_ns'], len(samples)) != (
                'replica-resources-v1', alias, epoch, 11):
            raise ValueError('resource identity/window mismatch: '+alias)
        for i, sample in enumerate(samples):
            # Preserve actual timestamps; refuse badly scheduled observations.
            if abs(sample['unix_ns'] - (epoch+(5+i)*10**9)) > 100_000_000:
                raise ValueError('resource sample missed boundary: '+alias)
            if any(not math.isfinite(sample[k]) or sample[k] < 0 for k in (*counters, 'rss_bytes')):
                raise ValueError('invalid resource sample: '+alias)
            if i and any(sample[k] < samples[i-1][k] for k in counters):
                raise ValueError('resource counter regressed: '+alias)
        first, last = samples[0], samples[-1]
        seconds = (last['unix_ns']-first['unix_ns'])/1e9
        delta = {k: last[k]-first[k] for k in counters}
        # Trapezoidal time average of sampled RSS, including both boundaries.
        rss = sum((a['rss_bytes']+b['rss_bytes'])/2*(b['unix_ns']-a['unix_ns'])/1e9
                  for a, b in zip(samples, samples[1:]))/seconds
        rows.append(dict(replicas=topology['replica_count'], protocol=plan['protocol'].lower(),
            profile=run.parent.name.removeprefix('ycsb-'), topology_id=topology['topology_id'],
            case=topology.get('case', ''), case_sha256=topology.get('case_sha256', ''),
            replica=alias, window_start_ns=first['unix_ns'], window_end_ns=last['unix_ns'],
            completed_requests_in_window=completions, normalization_valid=completions > 0,
            sent_bytes=delta['sent_bytes'], received_bytes=delta['received_bytes'],
            sent_messages=delta['sent_frames'], received_messages=delta['received_frames'],
            traffic_bytes_per_request=(delta['sent_bytes']+delta['received_bytes'])/completions if completions else '',
            messages_per_request=(delta['sent_frames']+delta['received_frames'])/completions if completions else '',
            cpu_percent=100*delta['cpu_seconds']/seconds, mean_rss_mib=rss/2**20,
            sampled_peak_rss_mib=max(s['rss_bytes'] for s in samples)/2**20,
            timed_out=validation['timed_out'], cohort_complete=validation['cohort_complete'],
            schema='replica-resources-v1'))
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
