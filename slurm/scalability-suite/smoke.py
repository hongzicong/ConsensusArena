#!/usr/bin/env python3
"""Exercise executable sampling and placement at both scale endpoints, without networking."""
import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent
PROTOCOLS = ('agreement', 'paxos', 'swiftpaxos', 'bodega', 'kcensus', 'pqlfrontier')


def smoke(binary):
    # Keep configs in a space-free workspace path, matching cluster deployment.
    with tempfile.TemporaryDirectory(dir=ROOT.parent.parent / 'results') as temporary:
        for n in (3, 31):
            for protocol in PROTOCOLS:
                folder = Path(temporary) / f'{protocol}-{n}'
                folder.mkdir()
                source = ROOT / 'generated' / f'n{n}-c{n}-topology-00'
                shutil.copyfile(source / 'latency.conf', folder / 'latency.conf')
                conf = folder / 'cluster.conf'
                text = re.sub(r'^protocol:.*$', 'protocol: '+protocol,
                              (source / 'workload.conf').read_text(), flags=re.M)
                conf.write_text(text)
                try:
                    subprocess.run([sys.executable, str(ROOT.parent / 'prepare-heat-profile.py'),
                                    str(binary), str(conf)], check=True, timeout=7200,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    plan = json.loads(subprocess.check_output([str(binary), '-run', 'plan', '-config', str(conf)],
                        text=True, timeout=7200, stderr=subprocess.PIPE))
                except subprocess.CalledProcessError as error:
                    print(error.stderr, file=sys.stderr)
                    raise
                assert plan['protocol'].lower() == protocol, plan['protocol']
                print(f'OK {protocol} n={n}', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('binary', type=Path)
    args = parser.parse_args()
    smoke(args.binary.resolve())
