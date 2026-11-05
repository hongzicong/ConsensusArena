import csv
import json
import math
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import runner
from search import Settings, SweepSettings, adaptive, sweep_schedule


class SearchTests(unittest.TestCase):
    def test_bracket_and_backfill(self):
        calls = []
        def evaluate(rate, phase):
            calls.append((rate,phase))
            return dict(sustained=rate <= 23750, mean_ms=100+max(0,rate-10000)/50, achieved_rps=min(rate,24000))
        result = adaptive(Settings(),evaluate)
        self.assertEqual([r for r,p in calls if p=='doubling'],[500,1000,2000,4000,8000,16000,32000])
        self.assertEqual(result['outcome'],'bracketed')
        self.assertLessEqual(result['highest_sustained_offered_rps'],23750)
        self.assertGreater(result['first_unsustained_offered_rps'],23750)
        self.assertLessEqual(result['first_unsustained_offered_rps']-result['highest_sustained_offered_rps'],2000)
        self.assertTrue(any(p=='near_knee' for r,p in calls))
        self.assertTrue(any(p=='knee_backfill' for r,p in calls))
        self.assertEqual(len({r for r,p in calls}),len(calls))

    def test_boundaries_and_invalid_evidence(self):
        low = adaptive(Settings(),lambda r,p:dict(sustained=False,mean_ms=500))
        self.assertEqual(low['outcome'],'below_start')
        high = adaptive(Settings(ceiling=2000),lambda r,p:dict(sustained=True,mean_ms=10))
        self.assertEqual(high['outcome'],'ceiling_reached')
        self.assertEqual(high['highest_sustained_offered_rps'],2000)
        with self.assertRaises(RuntimeError):
            adaptive(Settings(),lambda r,p:(_ for _ in ()).throw(RuntimeError('invalid')))
        for settings in (Settings(start=0),Settings(ceiling=1),Settings(knee_factor=1),Settings(latency_cap_ms=math.nan)):
            with self.assertRaises(ValueError):settings.validate()


class MeasurementTests(unittest.TestCase):
    def row(self, **updates):
        row = dict(workload_model='synchronized-ramp-fixed-sustain-v2',latency_sample_policy='completed_measured_requests',
            completed_rps='500',mean_ms='400',p99_ms='800',count='5000',timed_out='False',
            all_requests_drained='True',cohort_complete='True',throughput_valid='True')
        row.update(updates)
        return row

    def test_sustained_timeout_and_cap(self):
        self.assertTrue(runner.classify(self.row(),400)['sustained'])
        self.assertEqual(runner.classify(self.row(mean_ms='400.1'),400)['verdict'],'over_latency_cap')
        result = runner.classify(self.row(mean_ms='10',timed_out='True',all_requests_drained='False',cohort_complete=''),400)
        self.assertFalse(result['sustained'])
        self.assertEqual(result['verdict'],'timeout')
        for row in (self.row(throughput_valid='False'),self.row(mean_ms='nan'),self.row(all_requests_drained='False')):
            with self.assertRaises(ValueError):runner.classify(row,400)

    def test_fractional_rate_and_immutable_topology(self):
        _,_,template,_ = runner.specification(runner.HERE/'experiment.toml')
        with tempfile.TemporaryDirectory() as directory:
            case = Path(directory)/template.name
            layout = runner.prepare(template,case,500,'pqlfrontier')
            text = (case/'workload.conf').read_text()
            self.assertIn('writes: 5\n',text)
            self.assertIn('zipfSkew: 0.99\n',text)
            rate = float(next(s.split(':',1)[1] for s in text.splitlines() if s.startswith('arrivalRate:')))
            self.assertAlmostEqual(rate*7,500)
            self.assertEqual((case/'latency.conf').read_bytes(),(template/'latency.conf').read_bytes())
            self.assertEqual(layout['replica_count'],7)
            self.assertEqual(json.loads((case/'case.json').read_text())['workload']['profiles'],['B'])
            with self.assertRaises(FileExistsError):runner.prepare(template,case,1000,'agreement')
            with self.assertRaises(ValueError):runner.prepare(template,case/'bad',1000,'pql')

    def test_worker_retries_and_preserves_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            spec=json.loads(json.dumps(runner.specification(runner.HERE/'experiment.toml')[0]))
            spec['search']['ceiling']=2000
            manifest=dict(protocols=['agreement'],spec_path=str(runner.HERE/'experiment.toml'),spec=spec,
                fingerprint={},binary='binary',proxy='proxy',binary_sha256='a'*64,proxy_sha256=runner.PROXY_HASH)
            runner.write_json(root/'suite.json',manifest)
            settings=Settings(ceiling=2000)
            original=runner.specification(runner.HERE/'experiment.toml')
            def fake(suite,series,template,protocol,rate,attempt,*args):
                if rate==1000 and attempt==1:
                    return runner.classify(self.row(timed_out='True',all_requests_drained='False'),400),series
                return runner.classify(self.row(completed_rps=str(rate)),400),series
            with patch.object(runner,'fingerprint',return_value={}),patch.object(runner,'binary_inputs'), \
                 patch.object(runner,'specification',return_value=(spec,settings,original[2],original[3])), \
                 patch.object(runner,'trial',side_effect=fake),patch.dict(runner.os.environ,{},clear=False):
                self.assertEqual(runner.worker(root,0),0)
            with (root/'series/agreement/attempts.csv').open() as f:rows=list(csv.DictReader(f))
            self.assertEqual(len(rows),4)
            self.assertEqual([r['selected'] for r in rows if r['offered_rps']=='1000'],['False','True'])
            status=json.loads((root/'series/agreement/status.json').read_text())
            self.assertEqual(status['outcome'],'ceiling_reached')
            self.assertTrue(status['complete'])

    def test_cleanup_failure_is_not_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            spec,settings,template,layout=runner.specification(runner.HERE/'experiment.toml')
            runner.write_json(root/'suite.json',dict(protocols=['agreement'],spec_path=str(runner.HERE/'experiment.toml'),
                spec=spec,fingerprint={},binary='binary',proxy='proxy',binary_sha256='a'*64,proxy_sha256=runner.PROXY_HASH))
            with patch.object(runner,'fingerprint',return_value={}),patch.object(runner,'binary_inputs'), \
                 patch.object(runner,'trial',side_effect=runner.CleanupError('unclosed steps')) as trial, \
                 patch.dict(runner.os.environ,{},clear=False):
                with self.assertRaises(runner.CleanupError):runner.worker(root,0)
            self.assertEqual(trial.call_count,1)
            self.assertFalse(json.loads((root/'series/agreement/status.json').read_text())['complete'])

    def test_fixed_sweep_keeps_all_repetitions_even_when_every_run_times_out(self):
        settings=SweepSettings(rates=[500,1000,2000],repetitions=3)
        schedule=list(sweep_schedule(settings))
        self.assertEqual(set(schedule),{(rate,rep) for rate in settings.rates for rep in (1,2,3)})
        self.assertEqual(len(schedule),9)
        self.assertEqual(schedule,list(sweep_schedule(settings)))
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            spec,_,template,layout=runner.specification(runner.HERE/'knee-B.toml')
            runner.write_json(root/'suite.json',dict(protocols=['agreement'],spec_path=str(runner.HERE/'knee-B.toml'),
                spec=spec,fingerprint={},binary='binary',proxy='proxy',binary_sha256='a'*64,proxy_sha256=runner.PROXY_HASH))
            verdict=runner.classify(self.row(timed_out='True',all_requests_drained='False'),400)
            with patch.object(runner,'fingerprint',return_value={}),patch.object(runner,'binary_inputs'), \
                 patch.object(runner,'specification',return_value=(spec,settings,template,layout)), \
                 patch.object(runner,'trial',return_value=(verdict,root)) as trial:
                self.assertEqual(runner.worker(root,0),0)
            with (root/'series/agreement/points.csv').open() as f:
                rows=list(csv.DictReader(f))
            self.assertEqual(trial.call_count,9)
            self.assertEqual(len(rows),9)
            self.assertTrue(all(r['selected']=='True' and r['sustained']=='False' for r in rows))
            self.assertEqual(json.loads((root/'series/agreement/status.json').read_text())['outcome'],'sweep_complete')

    def test_sweep_collector_requires_every_planned_repetition(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            controls=dict(rates=[500],repetitions=3,latency_cap_ms=400,order_seed=1)
            runner.write_json(root/'suite.json',dict(protocols=['agreement'],binary_sha256='a'*64,spec=dict(sweep=controls)))
            series=root/'series/agreement'
            rows=[]
            for rep in (1,2,3):
                location=series/f'points/rate-500/attempt-{rep:02d}'
                measured=self.row()
                row=dict(protocol='agreement',binary_sha256='a'*64,profile='B',offered_rps=500,
                    phase='fixed_sweep',attempt=rep,**runner.classify(measured,400),selected=True,
                    path=str(location.relative_to(root)),error='')
                runner.write_json(location/'decision.json',row)
                runner.write_csv(location/'baseline-latency.csv',[measured])
                rows.append(row)
            runner.write_json(series/'status.json',dict(protocol='agreement',complete=True))
            runner.write_csv(series/'attempts.csv',rows)
            runner.write_csv(series/'points.csv',rows)
            with patch.object(runner,'harness',return_value=SimpleNamespace(collect=lambda *args:0)):
                self.assertEqual(runner.collect(root),0)
                runner.write_csv(series/'attempts.csv',rows[:2])
                runner.write_csv(series/'points.csv',rows[:2])
                self.assertEqual(runner.collect(root),2)
            status=json.loads((root/'suite-status.json').read_text())
            self.assertIn('fixed-sweep repetitions',status['protocols'][0]['validation_error'])

    def test_invalid_trials_cannot_become_capacity_wall(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            spec,settings,template,layout=runner.specification(runner.HERE/'experiment.toml')
            runner.write_json(root/'suite.json',dict(protocols=['agreement'],spec_path=str(runner.HERE/'experiment.toml'),
                spec=spec,fingerprint={},binary='binary',proxy='proxy',binary_sha256='a'*64,proxy_sha256=runner.PROXY_HASH))
            with patch.object(runner,'fingerprint',return_value={}),patch.object(runner,'binary_inputs'), \
                 patch.object(runner,'trial',side_effect=ValueError('preload mismatch')) as trial, \
                 patch.dict(runner.os.environ,{},clear=False):
                with self.assertRaisesRegex(RuntimeError,'no capacity conclusion'):runner.worker(root,0)
            self.assertEqual(trial.call_count,2)
            self.assertFalse((root/'series/agreement/points.csv').exists())
            with (root/'series/agreement/attempts.csv').open() as f:rows=list(csv.DictReader(f))
            self.assertTrue(all(r['valid']=='False' for r in rows))

    def test_collector_rejects_corrupted_raw_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            runner.write_json(root/'suite.json',dict(protocols=['agreement'],binary_sha256='a'*64,
                spec=dict(search=dict(latency_cap_ms=400))))
            series=root/'series/agreement'
            location=series/'points/rate-500/attempt-01'
            decision=dict(protocol='agreement',binary_sha256='a'*64,profile='B',offered_rps=500,
                phase='doubling',attempt=1,**runner.classify(self.row(mean_ms='100'),400),
                selected=True,path=str(location.relative_to(root)),error='')
            runner.write_json(location/'decision.json',decision)
            runner.write_json(series/'status.json',dict(protocol='agreement',complete=True))
            runner.write_csv(series/'attempts.csv',[decision])
            runner.write_csv(series/'points.csv',[decision])
            # Structurally successful collection, but raw latency was tampered with.
            runner.write_csv(location/'baseline-latency.csv',[self.row(mean_ms='200')])
            with patch.object(runner,'harness',return_value=SimpleNamespace(collect=lambda *args:0)):
                self.assertEqual(runner.collect(root),2)
            status=json.loads((root/'suite-status.json').read_text())
            self.assertFalse(status['complete'])
            self.assertIn('raw metrics differ',status['protocols'][0]['validation_error'])

    def test_submit_allocates_six_independent_workers(self):
        with tempfile.TemporaryDirectory() as directory:
            calls=[]
            def command(args,**kwargs):
                calls.append(args)
                return '101\n' if len(calls)==1 else '102\n'
            args=SimpleNamespace(spec=runner.HERE/'experiment.toml',account='dcl',max_parallel=6,time='02:00:00')
            with patch.dict(runner.os.environ,dict(SCRATCH=directory,USER='test',CONSENSUSARENA_BINARY='binary',
                CONSENSUSARENA_BINARY_SHA256='a'*64,CONSENSUSARENA_TOXIPROXY_SERVER='proxy')), \
                patch.object(runner,'binary_inputs'),patch.object(runner.subprocess,'check_output',side_effect=command):
                runner.submit(args)
            self.assertIn('--array=0-5%6',calls[0])
            self.assertIn('--nodes=2',calls[0])
            self.assertIn('--dependency=afterany:101',calls[1])
            self.assertFalse(any('dependency' in x for x in calls[0]))
            roots=list(Path(directory).glob('consensusarena-throughput-*'))
            self.assertEqual(len(roots),1)
            self.assertEqual(json.loads((roots[0]/'submission.json').read_text())['collector_job'],'102')


class PlotTests(unittest.TestCase):
    def test_sweep_plot_does_not_average_only_successful_repetitions(self):
        try:
            import matplotlib
        except ImportError:
            self.skipTest('matplotlib not available in this interpreter')
        import plot
        with tempfile.TemporaryDirectory(dir=runner.REPO/'results') as directory:
            root=Path(directory)
            runner.write_json(root/'suite.json',dict(protocols=['agreement'],binary_sha256='a'*64,
                spec=dict(sweep=dict(rates=[500,1000],repetitions=3,latency_cap_ms=400))))
            runner.write_json(root/'suite-status.json',dict(complete=True))
            rows=[]
            for rate in (500,1000):
                for rep in (1,2,3):
                    timeout=rate==1000 and rep==3
                    rows.append(dict(protocol='agreement',profile='B',binary_sha256='a'*64,
                        offered_rps=rate,achieved_rps=rate*.9,mean_ms=100*rep,valid=True,
                        selected=True,timed_out=timeout,all_requests_drained=not timeout,cohort_complete=not timeout))
            runner.write_csv(root/'attempts.csv',rows)
            plot.plot(root,root/'figures')
            with (root/'figures/sweep-summary.csv').open() as f:
                summary=list(csv.DictReader(f))
            self.assertEqual(summary[0]['mean_latency_ms'],'200.0')
            self.assertEqual(summary[1]['measured_repetitions'],'3')
            self.assertEqual(summary[1]['fully_drained_repetitions'],'2')
            self.assertEqual(summary[1]['mean_latency_ms'],'')
            self.assertTrue((root/'figures/throughput.pdf').exists())

    def test_retains_nonmonotonic_and_timeout_points(self):
        try:
            import matplotlib
        except ImportError:
            self.skipTest('matplotlib not available in this interpreter')
        import plot
        with tempfile.TemporaryDirectory(dir=runner.REPO/'results') as directory:
            root=Path(directory)
            runner.write_json(root/'suite.json',dict(protocols=['agreement'],binary_sha256='a'*64,
                spec=dict(search=dict(latency_cap_ms=400))))
            runner.write_json(root/'suite-status.json',dict(complete=True))
            rows=[]
            for rate,mean,timeout in [(500,100,False),(1000,80,False),(2000,600,True)]:
                rows.append(dict(protocol='agreement',profile='B',binary_sha256='a'*64,offered_rps=rate,
                    achieved_rps=rate*.9,mean_ms=mean,valid=True,selected=True,
                    timed_out=timeout,all_requests_drained=not timeout,cohort_complete=not timeout))
            runner.write_csv(root/'attempts.csv',rows)
            plot.plot(root,root/'figures')
            self.assertTrue((root/'figures/throughput.png').exists())
            self.assertTrue((root/'figures/throughput.pdf').exists())
            runner.write_json(root/'suite-status.json',dict(complete=False))
            with self.assertRaises(ValueError):plot.plot(root,root/'refused')


if __name__=='__main__':unittest.main()
