#!/usr/bin/env python3
"""Plot validated suite roots; PQL is always the pqlfrontier implementation."""
import argparse
import csv
import json
import math
from pathlib import Path
import statistics

PROTOCOLS = {'agreement': 'Agreement', 'paxos': 'Paxos', 'swiftpaxos': 'SwiftPaxos',
             'bodega': 'Bodega', 'kcensus': 'KCensus', 'pqlfrontier': 'PQL'}


def load(roots):
    latency, resources = {}, {}
    for root in roots:
        status = json.loads((root / 'suite-status.json').read_text())
        if status.get('complete') is not True:
            raise ValueError('incomplete suite: '+str(root))
        with (root / 'baseline-latency.csv').open(newline='') as source:
            for row in csv.DictReader(source):
                key = row['protocol'], int(row['replicas'])
                if key[0] not in PROTOCOLS or row['profile'] != 'B' or row['topology_id'] != '1':
                    raise ValueError('expected aws-random B using the six selected implementations')
                if key in latency:
                    raise ValueError('duplicate latency observation: '+str(key))
                if row['workload_model'] != 'synchronized-ramp-fixed-sustain-v2':
                    raise ValueError('incompatible measurement window')
                latency[key] = row
        with (root / 'resources.csv').open(newline='') as source:
            for row in csv.DictReader(source):
                key = row['protocol'], int(row['replicas'])
                if key not in latency or row['profile'] != 'B' or row['topology_id'] != '1':
                    raise ValueError('resource/latency identity mismatch')
                resources.setdefault(key, []).append(row)
    expected = {(p, n) for p in PROTOCOLS for n in range(3, 32, 2)}
    if set(latency) != expected or set(resources) != expected:
        raise ValueError('need all six protocols at every odd size 3..31 (90 scenes)')
    for key, rows in resources.items():
        if len(rows) != key[1] or len({r['replica'] for r in rows}) != key[1]:
            raise ValueError('missing or duplicate replica resources: '+str(key))
    return latency, resources


def plot(roots, output):
    latency, resources = load(roots)
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    output.mkdir(parents=True, exist_ok=True)
    summaries = []
    for key in sorted(latency):
        row, replicas = latency[key], resources[key]
        summary = dict(protocol=key[0], replicas=key[1], mean_ms=row['mean_ms'],
            timed_out=row['timed_out'], cohort_complete=row['cohort_complete'])
        for metric in ('traffic_bytes_per_request', 'messages_per_request', 'cpu_percent', 'mean_rss_mib', 'sampled_peak_rss_mib'):
            values = [float(r[metric]) for r in replicas if r[metric] != '']
            summary[metric] = statistics.mean(values) if len(values) == len(replicas) else ''
        summaries.append(summary)
    with (output / 'scalability-summary.csv').open('w', newline='') as target:
        writer = csv.DictWriter(target, fieldnames=list(summaries[0]))
        writer.writeheader()
        writer.writerows(summaries)
    charts = [('scalability', [('mean_ms', 'Average latency (ms)')]),
              ('resources', [('traffic_bytes_per_request', 'Traffic (bytes/request/replica)'),
                             ('messages_per_request', 'Messages/request/replica'),
                             ('cpu_percent', 'CPU per replica (% of one core)'),
                             ('mean_rss_mib', 'Mean RSS per replica (MiB)')])]
    for name, metrics in charts:
        fig, axes = plt.subplots(1 if len(metrics) == 1 else 2,
                                 1 if len(metrics) == 1 else 2,
                                 figsize=(6, 3.6) if len(metrics) == 1 else (9, 6), squeeze=False)
        for ax, (metric, label) in zip(axes.flat, metrics):
            for protocol, display in PROTOCOLS.items():
                series = sorted((r for r in summaries if r['protocol'] == protocol), key=lambda r: r['replicas'])
                ys = [float(r[metric]) if r[metric] != '' else math.nan for r in series]
                line, = ax.plot([r['replicas'] for r in series], ys, marker='o', markersize=3,
                    linewidth=2 if protocol == 'agreement' else 1, label=display)
                flagged = [(r['replicas'], y) for r, y in zip(series, ys)
                           if r['timed_out'] == 'True' or r['cohort_complete'] != 'True']
                if flagged:
                    ax.scatter(*zip(*flagged), marker='x', s=50, color=line.get_color())
            ax.set(xlabel='Number of replicas', ylabel=label, xticks=list(range(3, 32, 4)))
            ax.grid(alpha=.25)
        fig.legend(*axes.flat[0].get_legend_handles_labels(), loc='upper center', ncol=6)
        fig.text(.5, .01, 'Workload B · aws-random · × timeout or incomplete cohort; completed-request latency',
                 ha='center', fontsize=8)
        fig.tight_layout(rect=(0, .035, 1, .9))
        for extension in ('png', 'pdf'):
            fig.savefig(output / f'{name}.{extension}', dpi=200)
        plt.close(fig)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('runs', type=Path, nargs='+', help='six collected suite roots')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    plot(args.runs, args.output)
