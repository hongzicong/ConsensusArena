"""Compare Agreement with the best eligible alternative using the published CSVs.

Run from any directory: python results/compare.py
Writes COMPARISONS.md and one agreement-vs-best.csv in each experiment's data/.
Positive gain always favors Agreement. Throughput points are matched by offered
load; capacity brackets are compared separately. No interpolation or averaging
across configurations is performed.
"""
import csv
from collections import defaultdict
from decimal import Decimal
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PROTOCOLS = ('agreement', 'paxos', 'swiftpaxos', 'bodega', 'kcensus', 'pqlfrontier')
LABELS = dict(zip(PROTOCOLS, ('Agreement', 'Paxos', 'SwiftPaxos', 'Bodega', 'KCensus', 'PQL')))
DEPLOYMENTS = ('ring-7', 'europe-7', 'east-asia-7', 'north-america-7', 'north-america-3', 'north-america-5', 'aws-random')
KEYS = ('deployment', 'replicas', 'profile', 'operation', 'offered_rps')
LATENCY = [('mean_ms', 'ms', 'lower'), ('p99_ms', 'ms', 'lower')]
RESOURCE = [('bytes_per_request_per_replica', 'bytes/request/replica', 'lower'),
            ('messages_per_request_per_replica', 'messages/request/replica', 'lower'),
            ('cpu_percent_per_replica', '% of one logical CPU', 'lower'),
            ('peak_memory_mib_per_replica', 'MiB', 'lower')]
METRIC_NAMES = {'mean_ms': 'Mean latency', 'p99_ms': 'p99 latency',
    'bytes_per_request_per_replica': 'Traffic', 'messages_per_request_per_replica': 'Messages',
    'cpu_percent_per_replica': 'CPU per replica', 'peak_memory_mib_per_replica': 'Peak memory per replica',
    'highest_sustained_offered_rps': 'Highest sustained offered load (requests/s)',
    'achieved_rps_at_highest_sustained_load': 'Achieved at highest sustained load (requests/s)'}


def read(suite, filename):
    with (ROOT/suite/'data'/filename).open(newline='', encoding='utf-8') as source:
        return list(csv.DictReader(source))


def compare(rows, metrics, eligibility):
    groups = defaultdict(dict)
    for row in rows:
        protocol = row['protocol']
        if protocol not in PROTOCOLS:
            raise ValueError('Unexpected protocol: '+protocol)
        key = tuple(str(row[k]) for k in KEYS)
        if protocol in groups[key]:
            raise ValueError('Duplicate observation: '+str((key, protocol)))
        groups[key][protocol] = row
    output = []
    for key, group in groups.items():
        if 'agreement' not in group:
            continue
        for metric, unit, direction in metrics:
            flag = eligibility[metric] if isinstance(eligibility, dict) else eligibility
            values = {}
            excluded = []
            for protocol in PROTOCOLS:
                row = group.get(protocol)
                if row is None:
                    reason = 'not measured at this load'
                elif row.get(flag) != 'True':
                    reason = row.get('exclusion_reason') or row.get('verdict') or 'incomplete or invalid run'
                elif row.get(metric, '') == '':
                    reason = 'metric unavailable'
                else:
                    value = Decimal(row[metric])
                    if not value.is_finite() or value < 0:
                        raise ValueError('Invalid '+metric+': '+str(row))
                    values[protocol] = value
                    continue
                if protocol != 'agreement':
                    excluded.append(protocol+': '+reason)
            candidates = {p: v for p, v in values.items() if p != 'agreement'}
            result = dict(zip(KEYS, key), metric=metric, unit=unit, better=direction,
                agreement_value=group['agreement'][metric] if 'agreement' in values else '',
                best_alternative='', best_alternative_value='', agreement_gain_percent='',
                agreement_advantage_factor='', alternatives_compared=len(candidates),
                excluded_alternatives='; '.join(excluded), comparison_status='')
            if candidates:
                best = (min if direction == 'lower' else max)(candidates.values())
                winners = [p for p in PROTOCOLS if candidates.get(p) == best]
                result.update(best_alternative=';'.join(winners), best_alternative_value=group[winners[0]][metric])
            if 'agreement' not in values:
                result['comparison_status'] = 'agreement_unavailable'
            elif not candidates:
                result['comparison_status'] = 'no_comparable_alternative'
            else:
                a = values['agreement']
                gain = best-a if direction == 'lower' else a-best
                if best != 0:
                    result['agreement_gain_percent'] = format(100*gain/best, '.6f')
                numerator, denominator = (best, a) if direction == 'lower' else (a, best)
                if denominator != 0:
                    result['agreement_advantage_factor'] = format(numerator/denominator, '.6f')
                result['comparison_status'] = 'all_alternatives' if len(candidates) == 5 else 'partial_alternatives'
            output.append(result)
    return sorted(output, key=lambda r: (DEPLOYMENTS.index(r['deployment']), int(r['replicas']),
        r['profile'], r['operation'], Decimal(r['offered_rps'] or 0), r['metric']))


def tables():
    output = {}
    for suite, filename in [('regular', 'latency.csv'), ('scalability', 'scalability.csv')]:
        rows = [dict(row, operation='ALL') for row in read(suite, filename)]
        metrics = LATENCY+[('completed_rps', 'requests/s', 'higher')]
        if suite == 'scalability':
            metrics += RESOURCE
        results = compare(rows, metrics, 'plot_eligible')
        operations = [dict(row, offered_rps='20000') for row in read(suite, 'operation-latency.csv')]
        results += compare(operations, LATENCY, 'plot_eligible')
        output[suite] = results
    points = [dict(row, operation='ALL') for row in read('throughput', 'points.csv')]
    metrics = LATENCY+[('achieved_rps', 'requests/s', 'higher')]
    output['throughput'] = compare(points, metrics,
        dict(mean_ms='latency_plot_eligible', p99_ms='latency_plot_eligible', achieved_rps='throughput_plot_eligible'))
    capacity = [dict(row, deployment='ring-7', replicas='7', operation='ALL', offered_rps='')
                for row in read('throughput', 'capacity.csv')]
    output['throughput'] += compare(capacity,
        [('highest_sustained_offered_rps', 'requests/s', 'higher'),
         ('achieved_rps_at_highest_sustained_load', 'requests/s', 'higher')], 'complete')
    return output


def readable(value):
    return f'{Decimal(value):,.3f}'.rstrip('0').rstrip('.') if value != '' else '—'


def markdown(rows, columns):
    lines = ['| '+' | '.join(columns)+' |', '| '+' | '.join('---' for _ in columns)+' |']
    for row in rows:
        winners = ', '.join(LABELS[p] for p in row['best_alternative'].split(';') if p)
        cells = {
            'Deployment': row['deployment'], 'Replicas': row['replicas'], 'Workload': row['profile'],
            'Metric': METRIC_NAMES.get(row['metric'], row['metric']), 'Agreement': readable(row['agreement_value']),
            'Best alternative': winners or '—', 'Alternative value': readable(row['best_alternative_value']),
            'Agreement gain': f"{Decimal(row['agreement_gain_percent']):+.2f}%" if row['agreement_gain_percent'] != '' else '—',
            'Alternatives': str(row['alternatives_compared'])+'/5'}
        lines.append('| '+' | '.join(str(cells[c]) for c in columns)+' |')
    return '\n'.join(lines)


def main():
    comparisons = tables()
    for suite, rows in comparisons.items():
        path = ROOT/suite/'data/agreement-vs-best.csv'
        with path.open('w', newline='', encoding='utf-8') as target:
            writer = csv.DictWriter(target, fieldnames=list(rows[0]), lineterminator='\n')
            writer.writeheader()
            writer.writerows(rows)
        manifest_path = ROOT/suite/'manifest.json'
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text())
            manifest.setdefault('csv_sha256', {})['data/agreement-vs-best.csv'] = hashlib.sha256(path.read_bytes()).hexdigest()
            manifest['comparison_generator_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
            manifest_path.write_text(json.dumps(manifest, indent=2)+'\n')
        print(suite+': '+str(len(rows))+' comparisons')
    text = ['# Agreement versus the best alternative', '',
        'Best means the lowest latency/resource cost or highest throughput among eligible alternatives. '
        '**Positive gain favors Agreement**: lower-is-better `(best − Agreement) / best × 100`; '
        'higher-is-better `(Agreement − best) / best × 100`. Rankings use unrounded CSV values.', '',
        'The tables below show overall latency, resources and capacity. Filter the experiment CSVs for '
        'read/update latency, achieved throughput and comparisons at exactly matching offered loads. '
        'No averaging across topologies or interpolation between loads.', '',
        '[Regular CSV](regular/data/agreement-vs-best.csv) · '
        '[Scalability CSV](scalability/data/agreement-vs-best.csv) · '
        '[Throughput CSV](throughput/data/agreement-vs-best.csv)', '',
        'Only eligible measurements compete. `Alternatives` shows coverage out of five; exclusions are '
        'listed in the CSV. A blank gain means no valid comparison. The CSV advantage factor is above '
        'one when Agreement is better. Capacity refers to the finite-window search, not a matched-load '
        'latency comparison.', '', '## Regular latency (ms)', '']
    selected = [r for r in comparisons['regular'] if r['operation']=='ALL' and r['metric'] in ('mean_ms','p99_ms')]
    text += [markdown(selected, ['Deployment','Workload','Metric','Agreement','Best alternative','Alternative value','Agreement gain']), '',
             '## Scalability (AWS-random, workload B)', '']
    for metric, unit, _ in LATENCY+RESOURCE:
        text += ['### '+METRIC_NAMES[metric]+' ('+unit+')', '', markdown(
            [r for r in comparisons['scalability'] if r['operation']=='ALL' and r['metric']==metric],
            ['Replicas','Agreement','Best alternative','Alternative value','Agreement gain','Alternatives']), '']
    text += ['## Throughput capacity (ring 7, workload B)', '', markdown(
        [r for r in comparisons['throughput'] if r['offered_rps']==''],
        ['Metric','Agreement','Best alternative','Alternative value','Agreement gain']), '',
        'Rebuild from the measured CSVs: `python results/compare.py`.', '']
    (ROOT/'COMPARISONS.md').write_text('\n'.join(text), encoding='utf-8')


if __name__ == '__main__':
    main()
