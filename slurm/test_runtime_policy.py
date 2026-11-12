import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import runtime_policy as policy

HERE = Path(__file__).resolve().parent


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def put(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


class LaunchTests(unittest.TestCase):
    def test_fault_policy_rejected_before_launch_submission_or_directory_creation(self):
        suite = module('benchmark_suite_fault_policy_test', HERE/'benchmark-suite.py')
        env = {policy.SELECTOR: policy.POLICY}
        scene = dict(replicas=7, topology_id=1, profile='B')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)/'not-created'
            with patch.object(suite.subprocess, 'run') as launch, patch.object(suite.subprocess, 'check_output') as submit:
                for entry in [lambda: suite.run_scene('fault', HERE.parent, root, [scene], 0, env),
                              lambda: suite.task('fault', HERE.parent, env),
                              lambda: suite.serial('fault', HERE.parent, env)]:
                    with self.assertRaisesRegex(ValueError, 'fault-suite pinning is not implemented'):
                        entry()
                with patch.dict(suite.os.environ, env, clear=True):
                    with self.assertRaisesRegex(ValueError, 'fault-suite pinning is not implemented'):
                        suite.submit('fault', HERE.parent, 'dcl')
                launch.assert_not_called()
                submit.assert_not_called()
                self.assertFalse(root.exists())
            # A legacy fault launch must leave the scene absent for its coordinator.
            def coordinator(command, **kwargs):
                self.assertFalse(Path(kwargs['env']['CONSENSUSARENA_RUN_DIR']).exists())
                return SimpleNamespace(returncode=0)
            with patch.object(suite.subprocess, 'run', side_effect=coordinator):
                self.assertEqual(suite.run_scene('fault', HERE.parent, root, [scene], 0, {}), 0)

    def test_compact_submission_overrides_legacy_batch_reservations(self):
        suite = module('benchmark_suite_compact_submission_test', HERE/'benchmark-suite.py')
        for n, nodes, tasks, qos in [(7,1,15,'serial'),(17,2,37,'parallel'),(31,2,63,'parallel')]:
            with self.subTest(n=n), tempfile.TemporaryDirectory() as directory:
                env = {policy.SELECTOR:policy.POLICY, 'USER':'test', 'SCRATCH':directory}
                plan = [dict(replicas=n,clients=n,case='fixture',profile='B',topology_id=1)]
                with patch.dict(suite.os.environ,env,clear=True), patch.object(suite,'scenes',return_value=plan), \
                     patch.object(suite.subprocess,'check_output',side_effect=['101\n','102\n']) as submit:
                    suite.submit('latency', HERE.parent, 'dcl')
                command = submit.call_args_list[0].args[0]
                for flag in [f'--nodes={nodes}',f'--ntasks={tasks}','--cpus-per-task=2',
                             '--ntasks-per-node=32','--mem-per-cpu=4G','--qos='+qos]:
                    self.assertIn(flag,command)
                self.assertEqual(submit.call_args_list[0].kwargs['env']['CONSENSUSARENA_CLIENT_GOMAXPROCS'],'2')


class EvidenceTests(unittest.TestCase):
    def fixture(self, base, n, resources=False):
        run = base/'ycsb-B/repetition-01'
        replicas = [f'r{i}' for i in range(n)]
        clients = [f'c{i}' for i in range(7)]
        put(base/'runtime-settings.json', dict(runtime_policy=policy.POLICY, environment=policy.CONTROLS))
        (base/'metadata.txt').write_text('runtime_policy='+policy.POLICY+
            '\nreplica_gomaxprocs=1\nreplica_logical_cpus=1\nresources_enabled='+str(int(resources))+
            '\nresource_sampling_mode='+('boundary-cpu-traffic-peak-memory-v2' if resources else 'disabled')+'\n')
        put(run/'topology.json', dict(replicas=replicas, clients=clients))
        (run/'status').mkdir()
        (run/'status/start-unix-ns').write_text(str(10**12))
        (run/'stdout').mkdir()
        for role, aliases, maximum in [('replica', replicas, 1), ('client', clients, 2), ('master', ['m0'], 2)]:
            for alias in aliases:
                name = alias+'-'+role+'.out' if role != 'master' else 'master.out'
                (run/'stdout'/name).write_text(
                    f'RUNTIME_CONFIG role={role} alias={alias} gomaxprocs={maximum} num_cpu={maximum}\n')
        for rank, alias in enumerate(replicas):
            cpu = (rank//2)*8
            host = 'node-'+str(rank % 2)
            prefix = run/'stdout'/f'affinity-rank-{rank}'
            put(Path(str(prefix)+'-policy.json'), dict(rank=rank, host=host, selected_cpu=cpu,
                originally_allowed_cpus=list(range(cpu, cpu+2)), supervisor_cpus=[cpu+1],
                pin_before_exec=True, proxy_pin_before_exec=True, resources_enabled=resources))
            for phase, offset in [('ready', -1), ('measurement_end', 16)]:
                put(Path(str(prefix)+'-'+phase+'.json'), dict(rank=rank, host=host, selected_cpu=cpu,
                    pid=1000+rank, phase=phase, snapshot_ns=10**12+offset*10**9, epoch_ns=10**12,
                    executable='/immutable/consensusarena-linux-amd64',
                    observer_allowed_cpus=[cpu+1], all_threads_pinned=True,
                    proxy=dict(pid=2000+rank, executable='/immutable/toxiproxy-server-linux-amd64',
                        all_threads_isolated=True, threads=[dict(tid=20000+rank*4+t,
                            allowed_cpus=[cpu+1], last_cpu=cpu+1) for t in range(4)]),
                    threads=[dict(tid=10000+rank*4+t, allowed_cpus=[cpu], last_cpu=cpu) for t in range(4)]))
            if resources:
                put(run/'results'/(alias+'-resources.json'), dict(schema='replica-resources-v2',
                    pid=1000+rank, observer='inside-pinned-replica'))
        return run, dict(replicas=n)

    def test_all_threads_and_resource_observer_for_multiple_sizes(self):
        for n in [3, 7, 21, 31]:
            for resources in [False, True]:
                with self.subTest(n=n, resources=resources), tempfile.TemporaryDirectory() as directory:
                    base = Path(directory)
                    run, scene = self.fixture(base, n, resources)
                    result = policy.validate_run(base, run, scene)
                    self.assertEqual(result['affinity_snapshots'], 2*n)
                    self.assertEqual(result['affinity_thread_entries'], 8*n)
                    self.assertEqual(result['proxy_affinity_thread_entries'], 8*n)
                    self.assertEqual(result['startups_verified'], n+8)

    def test_reject_unpinned_thread_wrong_runtime_late_check_and_observer(self):
        changes = [
            ('affinity-rank-0-measurement_end.json', lambda d: d['threads'][0].update(allowed_cpus=[0, 1])),
            ('affinity-rank-0-measurement_end.json', lambda d: d.update(pid=999)),
            ('affinity-rank-0-measurement_end.json', lambda d: d.update(epoch_ns=None)),
            ('affinity-rank-0-ready.json', lambda d: d.update(snapshot_ns=10**12+5*10**9)),
            ('affinity-rank-0-measurement_end.json', lambda d: d.update(snapshot_ns=10**12+19*10**9)),
            ('affinity-rank-0-policy.json', lambda d: d.update(supervisor_cpus=[0, 1, 2, 3, 4, 5, 6])),
            ('affinity-rank-0-policy.json', lambda d: d.update(proxy_pin_before_exec=False)),
            ('affinity-rank-0-ready.json', lambda d: d['proxy']['threads'][0].update(allowed_cpus=list(range(8)))),
            ('affinity-rank-0-measurement_end.json', lambda d: d['proxy']['threads'][0].update(last_cpu=0)),
            ('affinity-rank-0-measurement_end.json', lambda d: d['proxy'].update(pid=999)),
        ]
        for filename, change in changes:
            with self.subTest(filename=filename, change=change), tempfile.TemporaryDirectory() as directory:
                base = Path(directory)
                run, scene = self.fixture(base, 3)
                path = run/'stdout'/filename
                data = json.loads(path.read_text())
                change(data)
                put(path, data)
                with self.assertRaises(ValueError):
                    policy.validate_run(base, run, scene)
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            run, scene = self.fixture(base, 3, True)
            put(run/'results/r0-resources.json', dict(schema='replica-resources-v2', pid=999,
                observer='inside-pinned-replica'))
            with self.assertRaisesRegex(ValueError, 'observer'):
                policy.validate_run(base, run, scene)
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            run, scene = self.fixture(base, 3)
            (run/'stdout/r0-replica.out').write_text('RUNTIME_CONFIG role=replica alias=r0 gomaxprocs=8 num_cpu=8\n')
            with self.assertRaisesRegex(ValueError, 'runtime settings'):
                policy.validate_run(base, run, scene)
            (run/'stdout/affinity-rank-1-measurement_end.json').unlink()
            (run/'stdout/r0-replica.out').write_text('RUNTIME_CONFIG role=replica alias=r0 gomaxprocs=1 num_cpu=1\n')
            with self.assertRaises(FileNotFoundError):
                policy.validate_run(base, run, scene)


if __name__ == '__main__':
    unittest.main()
