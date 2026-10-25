#!/usr/bin/env python3
"""Synchronize prepared latency clients and sustain load through cohort completion."""
import argparse
import json
import math
import os
from pathlib import Path
import time


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
    labels, rate = inputs(run)
    status = run / 'status'

    def wait(suffix, seconds):
        deadline = time.monotonic() + seconds
        paths = [status / ('client-' + label + suffix) for label in labels]
        while not all(path.exists() for path in paths):
            if (status / 'stop').exists() or (status / 'node-failed').exists() or list(status.glob('failed-*')):
                raise RuntimeError('a workload participant failed during barrier')
            if list(status.glob('*.exit')):
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
    # Future epochs allow shared-storage propagation and process scheduling.
    epoch = time.time_ns() + 2_000_000_000
    if max(row['ready_ns'] for row in ready) >= epoch:
        raise ValueError('client clock is ahead of the shared start epoch')
    write(status / 'start-unix-ns', str(epoch))
    print('All clients READY; shared workload epoch:', epoch, flush=True)
    done = wait('.measured', timeout)
    if any(row['done_ns'] < epoch + 15_000_000_000 for row in done):
        raise ValueError('client declared measurement complete before its window ended')
    stop = time.time_ns() + 2_000_000_000
    if max(row['done_ns'] for row in done) >= stop:
        raise ValueError('client clock is ahead of the shared load-stop epoch')
    write(status / 'load-stop-unix-ns', str(stop))
    print('All measured cohorts complete; shared load-stop epoch:', stop, flush=True)


def validate(run):
    labels, rate = inputs(run)
    epoch = int((run / 'status/start-unix-ns').read_text())
    stop = int((run / 'status/load-stop-unix-ns').read_text())
    rows = []
    for label in labels:
        ready = read(run / 'status' / ('client-' + label + '.ready'))
        done = read(run / 'status' / ('client-' + label + '.measured'))
        row = read(run / 'results' / (label + '-workload.json'))
        if (row['epoch_ns'], row['load_stop_ns'], row['measurement_end_ns']) != (epoch, stop, epoch + 15_000_000_000):
            raise ValueError('client used different workload epochs: ' + label)
        if (row['warmup_s'], row['ramp_s'], row['measurement_s'], row['arrival_rate']) != (5, 2.5, 10, rate):
            raise ValueError('client used different workload parameters: ' + label)
        if not (ready['ready_ns'] < epoch <= row['actual_start_ns'] <= epoch + 100_000_000):
            raise ValueError('client missed shared start: ' + label)
        if not (epoch + 15_000_000_000 <= row['measured_done_ns'] == done['done_ns'] <= stop):
            raise ValueError('load ended before measured cohort completed: ' + label)
        if row['measured_requests'] <= 0 or row['completed_requests'] != row['measured_requests']:
            raise ValueError('incomplete measured cohort: ' + label)
        offered = row['offered_per_second']
        if (sum(offered[:5]), sum(offered[5:15]), sum(offered[15:])) != (
                row['warmup_requests'], row['measured_requests'], row['sustain_requests']):
            raise ValueError('inconsistent arrival accounting: ' + label)
        if any(type(n) is not int or n < 0 for n in offered + row['issued_per_second']):
            raise ValueError('invalid per-second arrival counts: ' + label)
        if not row['measured_requests'] <= sum(row['issued_per_second']) <= sum(offered):
            raise ValueError('inconsistent issued request count: ' + label)
        if any(not math.isfinite(row[key]) or row[key] < 0 for key in ('queue_sum_ms', 'queue_max_ms')):
            raise ValueError('invalid queueing observations: ' + label)
        rows.append(dict(client=label, **row))
    result = dict(valid=True, latency_origin='scheduled_arrival',
                  load_model='Poisson with linear ramp during first half of warmup',
                  epoch_ns=epoch, load_stop_ns=stop, warmup_s=5, ramp_s=2.5,
                  measurement_s=10, repetitions=1,
                  start_spread_ms=(max(row['actual_start_ns'] for row in rows) - min(row['actual_start_ns'] for row in rows)) / 1e6,
                  sustain_s=(stop - epoch - 15_000_000_000) / 1e9, clients=rows)
    write(run / 'workload-validation.json', json.dumps(result, indent=2))
    print('Synchronized workload validated:', run, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('run', 'validate'))
    parser.add_argument('run', type=Path)
    parser.add_argument('--runner-pid', type=int)
    parser.add_argument('--ready-timeout', type=float, default=900)
    parser.add_argument('--timeout', type=float, default=300)
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
