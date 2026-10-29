#!/usr/bin/env python3
"""Reproduce all inputs and verify the existing runner plans exactly 15 B scenes."""
import importlib.util
from pathlib import Path
import sys
import tempfile

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parent))
from topology import generated_layouts
from generate import generate


def validate():
    with tempfile.TemporaryDirectory() as temp:
        fresh = Path(temp) / 'generated'
        generate(fresh)
        expected = {p.relative_to(fresh) for p in fresh.rglob('*') if p.is_file()}
        actual = {p.relative_to(ROOT / 'generated') for p in (ROOT / 'generated').rglob('*') if p.is_file()}
        assert expected == actual, 'generated file set differs'
        for path in expected:
            assert (fresh / path).read_bytes() == (ROOT / 'generated' / path).read_bytes(), path
    layouts = generated_layouts(ROOT / 'generated')
    assert len(layouts) == 15
    assert {x['replica_count'] for x in layouts} == set(range(3, 32, 2))
    spec = importlib.util.spec_from_file_location('benchmark_suite', ROOT.parent / 'benchmark-suite.py')
    suite = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(suite)
    plan = suite.scenes(dict(CONSENSUSARENA_SUITE='generated',
        CONSENSUSARENA_GENERATED_ROOT=str(ROOT / 'generated'), CONSENSUSARENA_YCSB_PROFILES='B'))
    assert len(plan) == 15 and all(x['profile'] == 'B' for x in plan)
    assert suite.allocation(plan) == (8, 63)
    print('Validated: aws-random only; 3..31 odd replicas; B; 15 scenes/protocol; 8 nodes/63 tasks at maximum.')


if __name__ == '__main__':
    validate()
