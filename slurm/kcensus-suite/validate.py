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
    assert len(layouts) == 4
    assert [r["topology_id"] for r in layouts] == [1, 2, 3, 4]
    for layout in layouts:
        folder = ROOT / layout["case"]
        meta = json.loads((folder / "case.json").read_text())
        source = ROOT / meta["source"]["bundled_path"]
        assert hashlib.sha256(source.read_bytes()).hexdigest() == meta["source"]["sha256"]
        snapshot = tomllib.loads(source.read_text(), parse_float=Decimal)
        entries = [line.split() for line in (folder / "latency.conf").read_text().splitlines()]
        matrix = {(a, b): Decimal(delay.removesuffix("ms")) for a, b, delay in entries}
        assert len(entries) == len(matrix) == 196
        endpoints = [(f"0.0.{group}.{i+1}", i) for group in (0, 1) for i in range(7)]
        for a, i in endpoints:
            for b, j in endpoints:
                assert matrix[a, b] == 2 * Decimal(snapshot["raw_latencies"][i][j])
        settings = {}
        for line in (folder / "workload.conf").read_text().splitlines():
            line = line.split("//", 1)[0].strip()
            if ":" in line:
                key, value = line.split(":", 1)
                settings[key] = value.strip()
        assert settings["zipfSkew"] == "0.99"
        assert settings["keyCount"] == "100000" and settings["commandSize"] == "16"
        assert abs(float(settings["arrivalRate"]) * 7 - 20000) < 1e-9
        assert meta["workload"]["total_arrival_rate"] == 20000
        assert abs(meta["workload"]["arrival_rate_per_client"] * 7 - 20000) < 1e-9
        assert settings["keyMappingSeed"] == settings["workloadSeed"] == "1"
        assert settings["clones"] == "0" and settings["shuffleKeys"] == "true"
        assert settings["warmup"] == "5s" and settings["duration"] == "10s"
        assert layout["latency_jitter_ms"] == 0 and layout["bandwidth_kbps"] == 125000
        assert layout["replica_count"] == len(layout["clients"]) == 7

    runner = runpy.run_path(str(ROOT.parent / "benchmark-suite.py"))
    env = {"CONSENSUSARENA_SUITE": "generated", "CONSENSUSARENA_GENERATED_ROOT": str(ROOT),
           "CONSENSUSARENA_REPLICAS": "7", "CONSENSUSARENA_YCSB_PROFILES": "A:B:C"}
    plan = runner["scenes"](env)
    assert len(plan) == 12 and runner["allocation"](plan) == (2, 15)
    assert len(runner["scenes"](dict(env, CONSENSUSARENA_YCSB_PROFILES="B"))) == 4

    # Runtime preparation and regeneration write only into an ignored workspace
    # results folder. TemporaryDirectory owns and cleans its isolated directory.
    results = ARENA / "results"
    results.mkdir(exist_ok=True)
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
                prepared = topology.prepare(temp / layout["case"], 7, layout["topology_id"])
                assert prepared["case_sha256"] == layout["case_sha256"]
            finally:
                if old is None:
                    os.environ.pop("CONSENSUSARENA_CASE", None)
                else:
                    os.environ["CONSENSUSARENA_CASE"] = old
    print("Validated 4 source matrices, 784 directed endpoint entries, deterministic regeneration,")
    print("runtime preparation, and 12 A/B/C scenes (2 nodes / 15 tasks per allocation).")


if __name__ == "__main__":
    validate()
