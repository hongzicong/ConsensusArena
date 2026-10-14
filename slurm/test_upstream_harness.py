import csv
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import upstream_harness as harness


class HarnessTests(unittest.TestCase):
    def setUp(self):
        (harness.ROOT / 'results').mkdir(exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=harness.ROOT / 'results')
        self.addCleanup(self.temporary.cleanup)
        self.base = Path(self.temporary.name)

    def fixture(self):
        options = harness.settings({'CONSENSUSARENA_BENCHMARK_MODE': 'native', 'UPSTREAM_REQUESTS': '3'})
        metadata = harness.prepare(self.base, dict(replicas=5, topology_id=1, profile='A'),
                                   ['10.0.0.1'] * 16, options)
        metadata['binary_sha256'] = 'test-binary'
        harness.write_json(self.base / 'metadata.json', metadata)
        for directory in ('results', 'status'):
            (self.base / directory).mkdir()
        for alias in metadata['clients']:
            (self.base / 'results' / (alias + '-client.log')).write_text(
                'latency 1\nlatency 2\nlatency 3\nTest took 2s\n')
            (self.base / 'status' / (alias + '.exit')).write_text('0\n')
        return metadata

    def test_complete_matrix_and_subset_validation(self):
        self.assertEqual(len(harness.selection({})), 27)
        selected = harness.selection({'CONSENSUSARENA_REPLICAS': '5',
                                     'CONSENSUSARENA_YCSB_PROFILES': 'A', 'CONSENSUSARENA_CASES': '5-1'})
        self.assertEqual(selected, [dict(replicas=5, topology_id=1, profile='A')])
        for env in ({'CONSENSUSARENA_REPLICAS': '5:5'}, {'CONSENSUSARENA_CASES': '5-4'},
                    {'CONSENSUSARENA_YCSB_PROFILES': 'A:A'}):
            with self.assertRaises(ValueError):
                harness.selection(env)

    def test_colocated_configuration_and_quorum(self):
        metadata = self.fixture()
        self.assertEqual(len(set(metadata['replica_endpoints'])), 5)
        config = (self.base / 'config' / 'cluster.conf').read_text()
        self.assertIn('port: 7074', config)
        self.assertIn('10.0.0.1:7071', config)
        quorum = (self.base / 'config' / 'quorum.conf').read_text().splitlines()
        self.assertEqual(len(quorum), 3)
        self.assertEqual(quorum[0], 'l ' + metadata['replicas'][0])

    def test_complete_client_results_are_valid(self):
        self.fixture()
        self.assertTrue(harness.summarize(self.base))
        with (self.base / 'baseline-latency.csv').open(newline='') as source:
            row = next(csv.DictReader(source))
        self.assertEqual(float(row['mean_ms']), 2)
        self.assertEqual(float(row['p99_ms']), 3)
        self.assertEqual(row['workload_kind'], 'upstream-request-count')

    def test_missing_or_failed_client_cannot_be_valid(self):
        metadata = self.fixture()
        alias = metadata['clients'][0]
        log = self.base / 'results' / (alias + '-client.log')
        log.write_text('latency 1\nlatency 2\nTest took 2s\n')
        self.assertFalse(harness.summarize(self.base))
        log.write_text('latency 1\nlatency 2\nlatency 3\nTest took 2s\n')
        (self.base / 'status' / (alias + '.exit')).write_text('1\n')
        self.assertFalse(harness.summarize(self.base))

    def test_duration_and_partial_suite(self):
        self.assertAlmostEqual(harness.duration_seconds('1m2.5s'), 62.5)
        with self.assertRaises(ValueError):
            harness.duration_seconds('garbage')
        harness.write_json(self.base / 'suite.json', dict(scenes=[dict(replicas=5, topology_id=1, profile='A')]))
        self.assertEqual(harness.collect(self.base), 2)
        self.assertFalse(json.loads((self.base / 'suite-status.json').read_text())['complete'])

    def test_smoke_submission_is_distinct_from_latency(self):
        with patch.dict(harness.os.environ, {'USER': 'test-user'}, clear=True), \
             patch.object(harness.sys, 'argv', ['benchmark-suite.py', 'submit', 'smoke']), \
             patch.object(harness.subprocess, 'check_output', return_value='12345\n') as submit:
            self.assertEqual(harness.main(), 0)
        command = submit.call_args.args[0]
        env = submit.call_args.kwargs['env']
        self.assertTrue(command[-1].endswith('run-smoke.sbatch'))
        self.assertEqual(env['UPSTREAM_SMOKE_ONLY'], '1')
        self.assertEqual(env['CONSENSUSARENA_CASES'], '5-1')
        self.assertEqual(env['CONSENSUSARENA_ARRIVAL_RATE'], '50')
        self.assertEqual(env['CONSENSUSARENA_KEY_COUNT'], '10000')
        with patch.dict(harness.os.environ, {'USER': 'test-user', 'UPSTREAM_SMOKE_ONLY': '1'}, clear=True), \
             patch.object(harness.sys, 'argv', ['benchmark-suite.py', 'submit', 'latency']):
            with self.assertRaises(ValueError):
                harness.main()

    def test_collector_rejects_contaminated_scene_identity(self):
        suite = self.base
        item = dict(replicas=5, topology_id=1, profile='A')
        self.base = suite / 'scenes' / harness.scene_name(item)
        metadata = self.fixture()
        metadata['profile'] = 'C'
        harness.write_json(self.base / 'metadata.json', metadata)
        harness.write_json(self.base / 'outcome.json', dict(valid=True, error=None, runner_exit=0))
        harness.write_json(suite / 'suite.json', dict(scenes=[item]))
        self.assertEqual(harness.collect(suite), 2)
        status = json.loads((suite / 'suite-status.json').read_text())
        self.assertIn('identity', status['scenes'][0]['error'])

    def test_listener_probe_only_accepts_listen_state(self):
        spec = importlib.util.spec_from_file_location('listener_ready', harness.ROOT / 'slurm/listener-ready.py')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        table = self.base / 'tcp'
        table.write_text('header\n0: 00000000:1B9E 00000000:0000 01\n')
        self.assertFalse(module.listening(7070, [table]))
        table.write_text('header\n0: 00000000:1B9E 00000000:0000 0A\n')
        self.assertTrue(module.listening(7070, [table]))
        self.assertFalse(module.listening(7071, [table]))

    def test_missing_participant_resource_evidence_is_rejected(self):
        metadata = self.fixture()
        metadata['resource_observer'] = 'gnu-time'
        harness.write_json(self.base / 'metadata.json', metadata)
        (self.base / 'stdout').mkdir()
        self.assertFalse(harness.summarize(self.base))
        for rank in range(16):
            (self.base / 'stdout' / f'resources-rank-{rank}.txt').write_text(
                'elapsed_seconds=1\nuser_seconds=.1\nsystem_seconds=.1\nmax_rss_kb=100\n'
                'voluntary_context_switches=10\ninvoluntary_context_switches=1\nexit_status=0\n')
        self.assertTrue(harness.summarize(self.base))
        (self.base / 'stdout' / 'resources-rank-15.txt').unlink()
        self.assertFalse(harness.summarize(self.base))

    def matched_fixture(self, profile='A'):
        options = harness.settings({'CONSENSUSARENA_KEY_COUNT': '10000', 'CONSENSUSARENA_ARRIVAL_RATE': '50'})
        metadata = harness.prepare(self.base, dict(replicas=5, topology_id=1, profile=profile),
                                   ['10.0.0.1']*16, options)
        metadata['binary_sha256'] = 'matched-test'
        for directory in ('results', 'status', 'logs'):
            (self.base / directory).mkdir()
        metadata['preload_digests'] = {a: 'a'*64 for a in metadata['replicas']}
        for alias in metadata['replicas']:
            (self.base / 'logs' / (alias+'-replica.log')).write_text(
                f'PRELOAD_COMPLETE records=10000 value_size=1000 seed=1 digest={"a"*64} duration=1s\n')
        harness.write_json(self.base / 'metadata.json', metadata)
        for alias in metadata['clients']:
            (self.base / 'results' / (alias+'-client.log')).write_text(
                f'Workload seed {harness.client_seed(1, alias)}\n'
                'Warm-up 5s, measurement duration 10s\n'
                'Open-loop Poisson arrival rate 50 requests/second\n'
                'Zipfian key distribution: keyCount=10000 skew=0.9\n'
                'WORKLOAD_KEY_MAPPING mode=shuffled algorithm=rand-perm-v1 seed=1 keyCount=10000\n'
                'latency READ 1\nlatency UPDATE 3\nWarm-up requests 2\nMeasured requests 2\nTest took 15s\n')
            (self.base / 'status' / (alias+'.exit')).write_text('0\n')
        return metadata

    def test_matched_defaults_and_invalid_exports(self):
        options = harness.settings({})
        self.assertEqual((options['arrival_rate'], options['key_count'], options['zipf_skew']), (2000, 1000000, .9))
        self.assertEqual((options['warmup'], options['duration']), ('5s', '10s'))
        self.assertTrue(options['preload'])
        for env in ({'UPSTREAM_REQUESTS': '32'}, {'CONSENSUSARENA_PROTOCOL': 'n2paxos'},
                    {'CONSENSUSARENA_ARRIVAL_RATE': 'nan'}, {'CONSENSUSARENA_DURATION': '0s'},
                    {'CONSENSUSARENA_WORKLOAD_SEED': '-1'}):
            with self.assertRaises(ValueError):
                harness.settings(env)

    def test_matched_summary_separates_operations(self):
        self.matched_fixture()
        self.assertTrue(harness.summarize(self.base))
        with (self.base / 'baseline-latency.csv').open() as source:
            rows = list(csv.DictReader(source))
        self.assertEqual({r['operation']: float(r['mean_ms']) for r in rows}, {'ALL': 2, 'READ': 1, 'UPDATE': 3})
        self.assertEqual({r['completed'] for r in rows if r['operation'] == 'ALL'}, {'20'})

    def test_matched_preload_mismatch_is_invalid(self):
        metadata = self.matched_fixture()
        log = self.base / 'logs' / (metadata['replicas'][0]+'-replica.log')
        log.write_text(log.read_text().replace('a'*64, 'b'*64))
        self.assertFalse(harness.summarize(self.base))
        self.assertIn('preload', json.loads((self.base / 'validation.json').read_text())['preload_error'])

    def test_matched_missing_sample_is_invalid(self):
        metadata = self.matched_fixture()
        log = self.base / 'results' / (metadata['clients'][0]+'-client.log')
        log.write_text(log.read_text().replace('latency UPDATE 3\n', ''))
        self.assertFalse(harness.summarize(self.base))

    def test_read_only_profile_rejects_update(self):
        self.matched_fixture(profile='C')
        self.assertFalse(harness.summarize(self.base))

    def test_matched_epaxos_has_no_leader_or_fixed_quorum(self):
        options = harness.settings({'CONSENSUSARENA_PROTOCOL': 'epaxos'})
        metadata = harness.prepare(self.base, dict(replicas=5, topology_id=1, profile='C'), ['10.0.0.1']*16, options)
        self.assertIsNone(metadata['initial_leader_alias'])
        self.assertEqual((self.base / 'config/quorum.conf').read_text(), '')
        self.assertNotIn('leader:', (self.base / 'config/cluster.conf').read_text())



if __name__ == '__main__':
    unittest.main()
