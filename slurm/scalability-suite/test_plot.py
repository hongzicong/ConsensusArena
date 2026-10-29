"""Synthetic fixtures test rendering only; they are never benchmark evidence."""
import csv
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('scalability_plot', ROOT / 'plot.py')
plots = importlib.util.module_from_spec(spec)
spec.loader.exec_module(plots)


class PlotTests(unittest.TestCase):
    def test_render_and_reject_incomplete(self):
        with tempfile.TemporaryDirectory(dir=ROOT.parent.parent / 'results') as temp:
            root = Path(temp)
            for protocol in plots.PROTOCOLS:
                folder = root / protocol
                folder.mkdir()
                (folder / 'suite-status.json').write_text(json.dumps(dict(complete=True)))
                latency, resources = [], []
                for n in range(3, 32, 2):
                    latency.append(dict(protocol=protocol, replicas=n, profile='B', topology_id=1,
                        mean_ms=20+n, workload_model='synchronized-ramp-fixed-sustain-v2',
                        timed_out=False, cohort_complete=True))
                    for i in range(n):
                        resources.append(dict(protocol=protocol, replicas=n, profile='B', topology_id=1,
                            replica=f'r{i}', traffic_bytes_per_request=10+n, messages_per_request=2,
                            cpu_percent=50, mean_rss_mib=40, sampled_peak_rss_mib=42))
                for name, rows in [('baseline-latency.csv', latency), ('resources.csv', resources)]:
                    with (folder / name).open('w', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
                        writer.writeheader()
                        writer.writerows(rows)
            roots = [root / protocol for protocol in plots.PROTOCOLS]
            plots.plot(roots, root / 'figures')
            self.assertEqual(len(list((root / 'figures').glob('*'))), 5)
            (roots[0] / 'suite-status.json').write_text(json.dumps(dict(complete=False)))
            with self.assertRaisesRegex(ValueError, 'incomplete'):
                plots.load(roots)


if __name__ == '__main__':
    unittest.main()
