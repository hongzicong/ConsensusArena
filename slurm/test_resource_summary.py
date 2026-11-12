import json
from pathlib import Path
import tempfile
import unittest

from resource_summary import summarize


class ResourceTests(unittest.TestCase):
    def test_boundary_cpu_counters_and_kernel_peak_memory(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory)/'ycsb-B/repetition-01'
            (run/'results').mkdir(parents=True)
            (run/'status').mkdir()
            def put(name, value):
                (run/name).write_text(json.dumps(value))
            put('topology.json', dict(replicas=['r0'], replica_count=1, topology_id=1))
            put('plan.json', dict(protocol='Agreement'))
            put('workload-validation.json', dict(valid=True, throughput_valid=True, completed_rps=10,
                timed_out=False, cohort_complete=True))
            (run/'status/start-unix-ns').write_text(str(10**9))
            counters = [dict(unix_ns=(6+10*i)*10**9, cpu_seconds=2+8*i, peak_rss_bytes=(4+7*i)*2**20,
                sent_frames=100+20*i, received_frames=100+10*i,
                sent_bytes=1000+2000*i, received_bytes=1000+1000*i) for i in range(2)]
            data = dict(schema='replica-resources-v2', alias='r0', epoch_ns=10**9, pid=123,
                observer='inside-pinned-replica', counter_samples=counters,
                memory_scope='process-start-through-measurement-end')
            put('results/r0-resources.json', data)
            row = summarize(run)[0]
            self.assertEqual(row['cpu_percent'], 80)
            self.assertEqual(row['traffic_bytes_per_request'], 30)
            self.assertEqual(row['messages_per_request'], .3)
            self.assertEqual(row['mean_rss_mib'], '')
            self.assertEqual(row['sampled_peak_rss_mib'], '')
            self.assertEqual(row['peak_rss_mib'], 11)
            self.assertEqual(row['memory_window_start_ns'], '')
            self.assertEqual(row['memory_window_end_ns'], counters[-1]['unix_ns'])
            self.assertEqual(row['sampling_mode'], 'boundary-cpu-traffic-peak-memory-v2')
            for key, value in [('observer', 'external-process'), ('pid', 0), ('counter_samples', counters[:1]),
                               ('memory_scope', 'measurement-window-only')]:
                put('results/r0-resources.json', dict(data, **{key:value}))
                with self.subTest(key=key), self.assertRaises(ValueError):
                    summarize(run)
            counters[-1]['cpu_seconds'] = 1
            put('results/r0-resources.json', data)
            with self.assertRaisesRegex(ValueError, 'regressed'):
                summarize(run)
            counters[-1]['cpu_seconds'] = 10
            counters[-1]['unix_ns'] += 200_000_000
            put('results/r0-resources.json', data)
            with self.assertRaisesRegex(ValueError, 'boundary'):
                summarize(run)

    def test_window_normalization_and_missing_evidence(self):
        with tempfile.TemporaryDirectory() as temp:
            run = Path(temp) / 'ycsb-B/repetition-01'
            (run / 'results').mkdir(parents=True)
            (run / 'status').mkdir()
            def put(path, data):
                (run / path).write_text(json.dumps(data))
            aliases = ['r0', 'r1', 'r2']
            put('topology.json', dict(replicas=aliases, replica_count=3, topology_id=1))
            put('plan.json', dict(protocol='Agreement'))
            put('workload-validation.json', dict(valid=True, throughput_valid=True, completed_rps=10,
                timed_out=False, cohort_complete=True))
            (run / 'status/start-unix-ns').write_text('1000000000')
            samples = [dict(unix_ns=(6+i)*10**9, cpu_seconds=i/2, rss_bytes=2**20,
                sent_frames=2*i, received_frames=i, sent_bytes=200*i, received_bytes=100*i) for i in range(11)]
            for alias in aliases:
                put('results/'+alias+'-resources.json', dict(schema='replica-resources-v1', alias=alias,
                    epoch_ns=10**9, samples=samples))
            rows = summarize(run)
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[0]['traffic_bytes_per_request'], 30)
            self.assertEqual(rows[0]['messages_per_request'], .3)
            self.assertEqual(rows[0]['cpu_percent'], 50)
            self.assertEqual(rows[0]['mean_rss_mib'], 1)
            samples[-1]['unix_ns'] += 200_000_000
            put('results/r0-resources.json', dict(schema='replica-resources-v1', alias='r0', epoch_ns=10**9, samples=samples))
            with self.assertRaisesRegex(ValueError, 'boundary'):
                summarize(run)
            (run / 'results/r0-resources.json').unlink()
            with self.assertRaises(FileNotFoundError):
                summarize(run)


if __name__ == '__main__':
    unittest.main()
