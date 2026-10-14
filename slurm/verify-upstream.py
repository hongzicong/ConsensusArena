#!/usr/bin/env python3
"""Verify all pinned upstream files except the declared platform-adapter edits."""
import hashlib
import json
from pathlib import Path

root = Path(__file__).resolve().parent.parent
manifest = json.loads((root / 'upstream-provenance.json').read_text())
allowed = set(manifest['modified_upstream_files'])
checked = 0
for name, expected in manifest['original_sha256_lf'].items():
    if name in allowed:
        continue
    path = root / name
    actual = hashlib.sha256(path.read_bytes().replace(b'\r\n', b'\n')).hexdigest()
    if actual != expected:
        raise SystemExit('Unexpected upstream modification: ' + name)
    checked += 1
for directory in ('swift', 'paxos', 'n2paxos', 'curp', 'fastpaxos', 'epaxos'):
    for path in (root / directory).rglob('*'):
        if path.is_file() and path.relative_to(root).as_posix() not in manifest['original_sha256_lf']:
            raise SystemExit('Unexpected protocol-package addition: ' + str(path))
print(f'{checked} upstream files unchanged after LF normalization; six protocol packages match {manifest["upstream_commit"]}.')
