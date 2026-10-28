#!/usr/bin/env python3
"""Rebuild the four frozen KCensus deployment inputs (Python 3.11+).

Run without arguments to populate this folder; existing cases are never replaced.
An optional output path supports independent reproducibility checks.
"""
from __future__ import annotations

import argparse
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
import tomllib

ROOT = Path(__file__).resolve().parent
DEPLOYMENTS = ("ring-7", "europe-7", "east-asia-7", "north-america-7")
SOURCE_REVISION = "b232c3320d7853ea332e9cb17a0acbf8386e125c"
TOTAL_ARRIVAL_RATE = 20000  # New paper's requested offered load, not KCensus's 1000.


def write(path: Path, value: str) -> None:
    path.write_text(value, encoding="utf-8", newline="\n")


def json_text(value: object) -> str:
    return json.dumps(value, indent=2) + "\n"


def generate(output: Path) -> None:
    cases = [f"n7-c7-topology-{i:02d}" for i in range(len(DEPLOYMENTS))]
    if (output / "manifest.json").exists() or any((output / case).exists() for case in cases):
        raise FileExistsError("refusing to replace existing suite inputs: " + str(output))
    template = (ROOT / "workload-template.conf").read_text(encoding="utf-8")
    # Freeze all workload choices; deployments differ only in network placement.
    overrides = {
        "writes": "50",  # Existing runner selects A/B/C: 50/5/0 percent.
        "commandSize": "16",  # Small-value approximation; see README.
        "clones": "0",
        "arrivalRate": format(TOTAL_ARRIVAL_RATE / 7, ".15g"),
        "warmup": "5s", "duration": "10s", "repetitions": "1",
        "keyCount": "100000", "zipfSkew": "0.99",
        "shuffleKeys": "true", "workloadSeed": "1",
        "preload": "true", "preloadSeed": "1",
    }
    for key, value in overrides.items():
        template, count = re.subn(r"^" + re.escape(key) + r":.*$",
                                  f"{key}: {value}", template, flags=re.M)
        if count != 1:
            raise ValueError("template must define exactly one " + key)

    prepared = []
    for index, deployment in enumerate(DEPLOYMENTS):
        filename = f"aws-{deployment}.toml"
        source = ROOT / "sources" / filename
        raw = source.read_bytes()
        config = tomllib.loads(raw.decode("utf-8"), parse_float=Decimal)
        regions = config["regions"]
        codes = [region.split()[0] for region in regions]
        matrix = config["raw_latencies"]
        if (len(regions) != 7 or len(set(codes)) != 7 or len(matrix) != 7
                or any(len(row) != 7 for row in matrix)):
            raise ValueError("expected seven distinct regions and a 7x7 matrix: " + filename)
        if any(not Decimal(v).is_finite() or v < 0 for row in matrix for v in row):
            raise ValueError("invalid delay in " + filename)
        if any(matrix[i][i] != 0 for i in range(7)):
            raise ValueError("expected zero regional diagonal: " + filename)

        replicas = [f"replica-{code}" for code in codes]
        clients = [f"client-{code}" for code in codes]
        membership = ["-- Replicas --"]
        membership += [f"{name} 0.0.0.{i+1}" for i, name in enumerate(replicas)]
        membership += ["", "-- Clients --"]
        membership += [f"{name} 0.0.1.{i+1}" for i, name in enumerate(clients)]
        membership += ["", "-- Master --", "m0 0.0.2.1", "", template.rstrip(),
                       "", "keyMappingSeed: 1", "", "-- Proxy --"]
        for replica, client in zip(replicas, clients):
            membership += [f"server_alias {replica}", f"{client} (local)", ""]
        membership += ["---", ""]

        endpoints = [(f"0.0.0.{i+1}", i) for i in range(7)]
        endpoints += [(f"0.0.1.{i+1}", i) for i in range(7)]
        latency = []
        for source_address, i in endpoints:
            for target_address, j in endpoints:
                # This directed RTT-equivalent input is halved by the existing
                # emulator. Do not sum opposite directions: preserve asymmetry.
                doubled = Decimal(matrix[i][j]) * 2
                latency.append(f"{source_address} {target_address} {doubled:f}ms\n")
        metadata = {
            "seed": 1,
            "deployment": deployment,
            "topology_model": "kcensus_aws_snapshot",
            "regions": regions,
            "replica_regions": dict(zip(replicas, codes)),
            "client_regions": dict(zip(clients, codes)),
            "client_placement": "one client colocated with each replica; zero added local delay",
            "source": {
                "repository": "https://github.com/LPD-EPFL/kcensus",
                "checkout_revision": SOURCE_REVISION,
                "path": "configs/" + filename,
                "bundled_path": "sources/" + filename,
                "sha256": hashlib.sha256(raw).hexdigest(),
                "unit": "estimated one-way milliseconds (ping RTT / 2)",
                "conversion": "input[i,j] = 2 * raw_latencies[i,j]; preserve direction and decimals",
            },
            "network": {
                "latency_jitter_ms": 0,
                "bandwidth_kbps": 125000,
                "bandwidth_source": "ConsensusArena default; not an AWS measurement",
                "bandwidth_scope": "KB/s per TCP connection per stream direction",
                "runtime_delay": "ceil(input[i,j] / 2) ms; physical transport overhead is additional",
            },
            "workload": {
                "total_arrival_rate": TOTAL_ARRIVAL_RATE,
                "offered_load_source": "new paper latency experiment target (KCensus baseline was 1000 total)",
                "arrival_rate_per_client": float(overrides["arrivalRate"]),
                "key_count": 100000, "zipf_skew": 0.99,
                "command_size_bytes": 16,
                "command_size_source": "fixed small-value approximation of KCensus variable strings",
                "shared_keyspace": True, "profiles": ["A", "B", "C"],
                "workload_seed": 1, "key_mapping_seed": 1,
            },
        }
        prepared.append((cases[index], "\n".join(membership), "".join(latency), metadata))

    output.mkdir(parents=True, exist_ok=True)
    for case, workload, latency, metadata in prepared:
        folder = output / case
        folder.mkdir()
        write(folder / "workload.conf", workload)
        write(folder / "latency.conf", latency)
        write(folder / "case.json", json_text(metadata))
    write(output / "manifest.json", json_text({
        "name": "kcensus-four-deployments-n7-zipf099-20krps",
        "topologies": cases,
        "deployments": {case: name for case, name in zip(cases, DEPLOYMENTS)},
    }))
    print(f"Generated {len(cases)} deployments (12 A/B/C scenes) in {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, nargs="?", default=ROOT)
    generate(parser.parse_args().output.resolve())
