#!/usr/bin/env python3
"""Synchronize 5s/10s/2s arrivals and retain completed-request metrics at 19s."""
import argparse
import json
import math
import os
from pathlib import Path
import time

WORKLOAD_MODEL = 'synchronized-ramp-fixed-sustain-v2'
SAMPLE_POLICY = 'completed_measured_requests'


def inputs(run):
    clients, settings, section = [], {}, None
    for line in (run / 'config/cluster.conf').read_text().splitlines():
        fields = line.lower().split('//', 1)[0].split()
        if not fields:
            continue
        if fields[0] == '--':
            section = fields[1]
        elif fields[0].endswith(':'):
            settings[fields[0][:-1]] = fields[1]
            section = None
        elif section == 'clients':
            clients.append(fields[0])
    clones = int(settings.get('clones', '0'))
    if not clients or len(set(clients)) != len(clients) or clones < 0:
        raise ValueError('invalid latency client membership')
    if (settings['warmup'], settings['duration'], settings['repetitions']) != ('5s', '10s', '1'):
        raise ValueError('latency requires 5s warmup, 10s measurement and one run')
    rate = float(settings['arrivalrate'])
    if not math.isfinite(rate) or rate <= 0:
        raise ValueError('invalid arrivalRate')
    return [alias + '-' + str(i) for alias in clients for i in range(clones + 1)], rate


def write(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(value + '\n')
    temporary.replace(path)


def read(path):
    return json.loads(path.read_text())


def coordinate(run, pid, ready_timeout, timeout):
    if timeout != 19:
        raise ValueError('ordinary latency experiments require a 19s workload deadline')
    labels, rate = inputs(run)
    status = run / 'status'

    def wait(suffix, seconds=None, deadline=None):
        if deadline is None:
            deadline = time.monotonic() + seconds
        paths = [status / ('client-' + label + suffix) for label in labels]
        while not all(path.exists() for path in paths):
            if (status / 'stop').exists() or (status / 'node-failed').exists():
                raise RuntimeError('a workload participant failed during barrier')
            if list(status.glob('client-*.exit')):
                raise RuntimeError('a client exited before the shared load-stop epoch')
            os.kill(pid, 0)
            if time.monotonic() >= deadline:
                missing = [path.name for path in paths if not path.exists()]
                raise TimeoutError('latency barrier timed out: ' + ', '.join(missing))
            time.sleep(.02)
        return [read(path) for path in paths]

    ready = wait('.ready', ready_timeout)
    if any((row['warmup_s'], row['measurement_s'], row['arrival_rate']) != (5, 10, rate) for row in ready):
        raise ValueError('client workload settings differ from shared configuration')
    if any((row.get('workload_model'), row.get('sustain_s'), row.get('experiment_timeout_s')) !=
           (WORKLOAD_MODEL, 2, 19) for row in ready):
        raise ValueError('client binary does not implement the fixed-sustain workload')
    # Future epochs allow shared-storage propagation and process scheduling.
    epoch = time.time_ns() + 2_000_000_000
    if max(row['ready_ns'] for row in ready) >= epoch:
        raise ValueError('client clock is ahead of the shared start epoch')
    write(status / 'load-stop-unix-ns', str(epoch + 17_000_000_000))
    write(status / 'start-unix-ns', str(epoch))
    print('All clients READY; shared workload epoch:', epoch, flush=True)
    deadline = time.monotonic() + (epoch - time.time_ns()) / 1e9 + timeout
    try:
        done = wait('.drained', deadline=deadline)
    except (TimeoutError, RuntimeError, ProcessLookupError) as error:
        # A watchdog panic can reach Slurm just before this process observes
        # its own deadline. Other participant failures remain errors.
        watchdog = any('latency experiment timed out: requests did not drain within 19s' in path.read_text(errors='replace') or
                       'latency experiment exceeded its 19s deadline' in path.read_text(errors='replace')
                       for path in (run / 'stdout').glob('*-client.out'))
        if not isinstance(error, TimeoutError) and not watchdog:
            raise
        write(status / 'drain-timeout.json', json.dumps(dict(
            reason='workload_deadline', epoch_ns=epoch,
            deadline_ns=epoch + 19_000_000_000, observed_ns=time.time_ns())))
        print('19s drain deadline reached; preserving throughput and completed-request latency', flush=True)
        return
    if any(not epoch + 17_000_000_000 <= row['done_ns'] < epoch + 19_000_000_000 for row in done):
        raise ValueError('invalid client drain timestamps')
    write(status / 'drain-complete-unix-ns', str(time.time_ns()))
    print('All clients drained; fixed load-stop epoch:', epoch + 17_000_000_000, flush=True)


def validate(run):
    labels, rate = inputs(run)
    epoch = int((run / 'status/start-unix-ns').read_text())
    stop = int((run / 'status/load-stop-unix-ns').read_text())
    timeout_path = run / 'status/drain-timeout.json'
    timed_out = timeout_path.exists()
    if timed_out:
        timeout = read(timeout_path)
        if (timeout['reason'], timeout['epoch_ns'], timeout['deadline_ns']) != (
                'workload_deadline', epoch, epoch + 19_000_000_000):
            raise ValueError('invalid workload deadline record')
    if stop != epoch + 17_000_000_000:
        raise ValueError('invalid fixed load-stop epoch')
    rows = []
    for label in labels:
        ready = read(run / 'status' / ('client-' + label + '.ready'))
        if (ready.get('workload_model'), ready.get('warmup_s'), ready.get('measurement_s'),
                ready.get('arrival_rate'), ready.get('sustain_s'), ready.get('experiment_timeout_s')) != (
                WORKLOAD_MODEL, 5, 10, rate, 2, 19):
            raise ValueError('client readiness used another measurement policy: ' + label)
        alias, clone = label.rsplit('-', 1)
        log = run / 'results' / (alias + '-client-' + (clone if int(clone) else ''))
        header, completed = None, 0
        with log.open() as source:
            for line in source:
                fields = line.split()
                if len(fields) >= 3 and fields[-3] == 'latency' and fields[-2] in ('READ', 'UPDATE'):
                    latency = float(fields[-1])
                    if not math.isfinite(latency) or latency < 0:
                        raise ValueError('invalid latency sample: ' + label)
                    completed += 1
                elif 'LATENCY_WORKLOAD_START ' in line:
                    if header is not None:
                        raise ValueError('duplicate workload start: ' + label)
                    header = dict(field.split('=', 1) for field in fields if '=' in field)
        if not header or header['workload_model'] != WORKLOAD_MODEL:
            raise ValueError('missing fixed-sustain workload start: ' + label)
        if tuple(int(header[key]) for key in ('epoch_ns', 'load_stop_ns')) != (epoch, stop):
            raise ValueError('client used different workload epochs: ' + label)
        if tuple(float(header[key]) for key in ('warmup_s', 'ramp_s', 'measurement_s',
                                              'sustain_s', 'experiment_timeout_s', 'arrival_rate')) != (5, 2.5, 10, 2, 19, rate):
            raise ValueError('client used different workload parameters: ' + label)
        actual_start = int(header['actual_start_ns'])
        if not ready['ready_ns'] < epoch <= actual_start <= epoch + 100_000_000:
            raise ValueError('client missed shared start: ' + label)
        snapshot = read(run / 'results' / (label + '-throughput.json'))
        if (snapshot['workload_model'], snapshot['epoch_ns'], snapshot['warmup_s'],
                snapshot['measurement_s'], snapshot['sustain_s'], snapshot['experiment_timeout_s']) != (
                WORKLOAD_MODEL, epoch, 5, 10, 2, 19):
            raise ValueError('invalid throughput workload: ' + label)
        if snapshot['snapshot_ns'] < epoch + 15_000_000_000:
            raise ValueError('incomplete throughput observation window: ' + label)
        counts = snapshot['completed_per_second']
        if (snapshot['completion_origin'] != 'deduplicated_protocol_authorized_reply' or
                len(counts) != 19 or any(type(n) is not int or n < 0 for n in counts)):
            raise ValueError('invalid completion counters: ' + label)
        report_path = run / 'results' / (label + '-workload.json')
        row = read(report_path) if report_path.exists() else {}
        expected = row.get('measured_requests')
        if row and (row.get('workload_model'), row.get('epoch_ns'), row.get('load_stop_ns'),
                    row.get('measurement_end_ns'), row.get('completed_requests')) != (
                    WORKLOAD_MODEL, epoch, stop, epoch + 15_000_000_000, completed):
            raise ValueError('inconsistent client report: ' + label)
        if expected is not None and (type(expected) is not int or not 0 <= completed <= expected):
            raise ValueError('invalid measured request counts: ' + label)
        if not timed_out:
            done = read(run / 'status' / ('client-' + label + '.drained'))
            release = int((run / 'status/drain-complete-unix-ns').read_text())
            total = sum(row[key] for key in ('warmup_requests', 'measured_requests', 'sustain_requests'))
            if (not stop <= row['drain_done_ns'] == done['done_ns'] < epoch + 19_000_000_000 or
                    release < done['done_ns'] or completed != expected or
                    row['issued_requests'] != row['all_completed_requests'] or row['issued_requests'] != total):
                raise ValueError('invalid normal drain: ' + label)
        row.update(client=label, actual_start_ns=actual_start, measured_requests=expected,
                   completed_requests=completed, cohort_complete=completed == expected if expected is not None else None,
                   completed_in_window=sum(counts[5:15]), throughput_snapshot_ns=snapshot['snapshot_ns'])
        rows.append(row)
    known_counts = all(row['measured_requests'] is not None for row in rows)
    result = dict(valid=True, workload_model=WORKLOAD_MODEL, latency_origin='scheduled_arrival',
                  latency_sample_policy=SAMPLE_POLICY, timed_out=timed_out,
                  status='timeout' if timed_out else 'complete',
                  load_model='Poisson with linear ramp during first half of warmup',
                  epoch_ns=epoch, load_stop_ns=stop, warmup_s=5, ramp_s=2.5,
                  measurement_s=10, repetitions=1,
                  start_spread_ms=(max(row['actual_start_ns'] for row in rows) - min(row['actual_start_ns'] for row in rows)) / 1e6,
                  sustain_s=2, experiment_timeout_s=19,
                  drain_s=None if timed_out else (max(row['drain_done_ns'] for row in rows) - stop) / 1e9,
                  all_requests_drained=not timed_out,
                  measured_requests=sum(row['measured_requests'] for row in rows) if known_counts else None,
                  completed_requests=sum(row['completed_requests'] for row in rows),
                  cohort_complete=all(row['cohort_complete'] for row in rows) if known_counts else None,
                  throughput_valid=True, completed_rps=sum(row['completed_in_window'] for row in rows) / 10,
                  throughput_window_s=[5, 15], clients=rows)
    write(run / 'workload-validation.json', json.dumps(result, indent=2))
    print('Synchronized workload validated:', run, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('run', 'validate'))
    parser.add_argument('run', type=Path)
    parser.add_argument('--runner-pid', type=int)
    parser.add_argument('--ready-timeout', type=float, default=900)
    parser.add_argument('--timeout', type=float, default=19)
    args = parser.parse_args()
    if args.mode == 'run':
        if args.runner_pid is None:
            parser.error('--runner-pid is required')
        coordinate(args.run, args.runner_pid, args.ready_timeout, args.timeout)
    else:
        try:
            validate(args.run)
        except (OSError, ValueError, KeyError, TypeError) as error:
            write(args.run / 'workload-validation.json', json.dumps(dict(valid=False, error=str(error))))
            raise


if __name__ == '__main__':
    main()
