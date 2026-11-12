#!/usr/bin/env python3
"""Check source fidelity, reproducibility, and existing pipeline compatibility."""
import hashlib
import json
import os
from pathlib import Path
import runpy
import sys
import tempfile
import tomllib
from decimal import Decimal

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent
ARENA = ROOT.parent.parent
sys.path.insert(0, str(ROOT.parent))
import topology


def validate():
    manifest = json.loads((ROOT / "manifest.json").read_text())
    layouts = topology.generated_layouts(ROOT)
    assert len(layouts) == 6 and manifest['profiles'] == ['A', 'B']
    assert [r["topology_id"] for r in layouts] == [1, 2, 3, 4, 5, 6]
    assert [r['replica_count'] for r in layouts] == [7, 7, 7, 7, 3, 5]
    expected_na = {3: ['us-east-1', 'us-west-1', 'us-west-2'],
                   5: ['ca-central-1', 'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2']}
    for layout in layouts:
        folder = ROOT / layout["case"]
        meta = json.loads((folder / "case.json").read_text())
        source = ROOT / meta["source"]["bundled_path"]
        assert hashlib.sha256(source.read_bytes()).hexdigest() == meta["source"]["sha256"]
        snapshot = tomllib.loads(source.read_text(), parse_float=Decimal)
        n = layout['replica_count']
        indices = meta['source'].get('region_indices', list(range(7)))
        assert meta['regions'] == [snapshot['regions'][i] for i in indices]
        assert meta['workload']['profiles'] == ['A', 'B']
        if n != 7:
            assert source.name == 'aws-north-america-7.toml'
            assert [region.split()[0] for region in meta['regions']] == expected_na[n]
        entries = [line.split() for line in (folder / "latency.conf").read_text().splitlines()]
        matrix = {(a, b): Decimal(delay.removesuffix("ms")) for a, b, delay in entries}
        assert len(entries) == len(matrix) == (2*n)**2
        endpoints = [(f"0.0.{group}.{i+1}", i) for group in (0, 1) for i in range(n)]
        for a, i in endpoints:
            for b, j in endpoints:
                assert matrix[a, b] == 2 * Decimal(snapshot["raw_latencies"][indices[i]][indices[j]])
        settings = {}
        for line in (folder / "workload.conf").read_text().splitlines():
            line = line.split("//", 1)[0].strip()
            if ":" in line:
                key, value = line.split(":", 1)
                settings[key] = value.strip()
        assert settings["zipfSkew"] == "0.99"
        assert settings["keyCount"] == "100000" and settings["commandSize"] == "16"
        assert abs(float(settings["arrivalRate"]) * n - 20000) < 1e-9
        assert meta["workload"]["total_arrival_rate"] == 20000
        assert abs(meta["workload"]["arrival_rate_per_client"] * n - 20000) < 1e-9
        assert settings["keyMappingSeed"] == settings["workloadSeed"] == "1"
        assert settings["clones"] == "0" and settings["shuffleKeys"] == "true"
        assert settings["warmup"] == "5s" and settings["duration"] == "10s"
        assert layout["latency_jitter_ms"] == 0 and layout["bandwidth_kbps"] == 125000
        assert layout["replica_count"] == len(layout["clients"]) == n

    runner = runpy.run_path(str(ROOT.parent / "benchmark-suite.py"))
    env = {"CONSENSUSARENA_SUITE": "generated", "CONSENSUSARENA_GENERATED_ROOT": str(ROOT),
           "CONSENSUSARENA_REPLICAS": "3:5:7", "CONSENSUSARENA_YCSB_PROFILES": "A:B"}
    plan = runner["scenes"](env)
    cpu_env = dict(env, CONSENSUSARENA_RUNTIME_POLICY='replica-single-cpu-v2')
    assert len(plan) == 12 and runner["allocation"](plan, cpu_env) == (1, 15)
    assert len(runner["scenes"](dict(env, CONSENSUSARENA_YCSB_PROFILES="B"))) == 6
    assert len(runner['scenes'](dict(env, CONSENSUSARENA_REPLICAS='7'))) == 8
    assert runner['scenes']({k:v for k,v in env.items() if k != 'CONSENSUSARENA_YCSB_PROFILES'}) == plan
    for n, tasks, nodes in [(3, 7, 1), (5, 11, 1)]:
        subset = runner['scenes'](dict(env, CONSENSUSARENA_REPLICAS=str(n)))
        assert len(subset) == 2 and runner['allocation'](subset, cpu_env) == (nodes, tasks)

    # Runtime preparation and regeneration write only into an ignored workspace
    # local artifacts folder. TemporaryDirectory cleans its isolated directory.
    results = ARENA / ".local-artifacts" / "validation"
    results.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="kcensus-validation-", dir=results) as temporary:
        temp = Path(temporary).resolve()
        assert temp.parent == results.resolve()
        regenerate = runpy.run_path(str(ROOT / "generate.py"))["generate"]
        regenerate(temp / "regenerated")
        for relative in ["manifest.json"] + [f"{name}/{file}" for name in manifest["topologies"]
                                           for file in ("case.json", "workload.conf", "latency.conf")]:
            assert (ROOT / relative).read_bytes() == (temp / "regenerated" / relative).read_bytes()
        for layout in layouts:
            old = os.environ.get("CONSENSUSARENA_CASE")
            os.environ["CONSENSUSARENA_CASE"] = str(ROOT / layout["case"])
            try:
                prepared = topology.prepare(temp / layout["case"], layout['replica_count'], layout["topology_id"])
                assert prepared["case_sha256"] == layout["case_sha256"]
            finally:
                if old is None:
                    os.environ.pop("CONSENSUSARENA_CASE", None)
                else:
                    os.environ["CONSENSUSARENA_CASE"] = old
    print("Validated 4 unchanged source matrices, 6 deployments, 920 directed endpoint entries,")
    print("deterministic regeneration, runtime preparation and 12 A/B scenes (1 node / 15 tasks / 2 CPUs per task).")


if __name__ == "__main__":
    validate()
