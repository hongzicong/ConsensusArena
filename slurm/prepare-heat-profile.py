#!/usr/bin/env python3
"""Freeze empirical observations before Bodega, PQL or Agreement planning."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def prepare(binary, config):
    config = Path(config).resolve()
    text = config.read_text()
    protocols = re.findall(r'^protocol:\s*(\S+)\s*$', text, flags=re.M | re.I)
    if len(protocols) != 1:
        raise ValueError('expected one protocol field')
    if protocols[0].lower() not in ('bodega', 'pql', 'agreement'):
        return
    profile = config.with_name('heat-profile.json')
    if profile.exists():
        raise FileExistsError('refusing to overwrite frozen heat profile: ' + str(profile))
    requests = int(os.environ.get('CONSENSUSARENA_HEAT_SAMPLES', '1000000'))
    seed = int(os.environ.get('CONSENSUSARENA_HEAT_SEED', '101'))
    result = json.loads(subprocess.check_output(
        [str(Path(binary).resolve()), '-run', 'heat-sample', '-config', str(config),
         '-heat-samples', str(requests), '-heat-seed', str(seed), '-heat-out', str(profile)],
        text=True, timeout=7200))
    # The generated configuration owns these inputs; remove inherited sample paths.
    text = re.sub(r'^heatProfile(?:SHA256)?:[^\n]*\n?', '', text, flags=re.M | re.I)
    text = text.rstrip() + '\nheatProfile: ' + str(profile) + '\nheatProfileSHA256: ' + result['sha256'] + '\n'
    config.write_text(text)
    config.with_name('heat-sample.json').write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    prepare(*sys.argv[1:])
