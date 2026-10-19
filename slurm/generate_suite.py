#!/usr/bin/env python3
"""Generate reproducible raw evaluation inputs for the protocol planners.

Run without arguments to use slurm/experiment.toml and slurm/generated.
Existing non-empty output directories are never overwritten.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent
WARMUP_SECONDS = 5
DURATION_SECONDS = 10
REPETITIONS = 1

def stable_seed(seed: int, *parts: object) -> int:
    digest = hashlib.sha256((str(seed) + "|" + "|".join(map(str, parts))).encode()).digest()
    # ConsensusArena parses seeds into Go's signed int. Keep deterministic
    # derived seeds within the portable positive int64 range used on Jed.
    return int.from_bytes(digest[:8], "little") & ((1 << 63) - 1)


def endpoint_names(replicas: int, clients: int) -> tuple[list[str], list[str]]:
    return ([f"replica-{i:03d}" for i in range(replicas)],
            [f"client-{i:03d}" for i in range(clients)])


def sample_number(cfg: dict, fixed: str, ranged: str, rng: random.Random, cast):
    if ranged in cfg:
        low, high = map(cast, cfg[ranged])
        if high < low:
            raise ValueError(f"{ranged} must be ordered [minimum, maximum]")
        return rng.randint(low, high) if cast is int else rng.uniform(low, high)
    return cast(cfg[fixed])


def sample_option(cfg: dict, fixed: str, options: str, rng: random.Random, cast):
    values = cfg.get(options)
    return cast(rng.choice(values)) if values else cast(cfg[fixed])


def sample_case(exp: dict, network: dict, seed: int, topology: int) -> tuple[dict, dict, str]:
    rng = random.Random(stable_seed(seed, "parameters"))
    workload = dict(exp)
    workload["arrival_rate_per_client"] = sample_number(
        exp, "arrival_rate_per_client", "arrival_rate_per_client_range", rng, float)
    workload["command_size"] = sample_option(exp, "command_size", "command_size_options", rng, int)
    workload["key_count"] = sample_option(exp, "key_count", "key_count_options", rng, int)
    workload["zipf_skew"] = sample_number(exp, "zipf_skew", "zipf_skew_range", rng, float)

    sampled_network = dict(network)
    sampled_network["minimum_rtt_ms"] = sample_number(
        network, "minimum_rtt_ms", "minimum_rtt_ms_range", rng, int)
    sampled_network["maximum_rtt_ms"] = sample_number(
        network, "maximum_rtt_ms", "maximum_rtt_ms_range", rng, int)
    if sampled_network["maximum_rtt_ms"] <= sampled_network["minimum_rtt_ms"]:
        raise ValueError("sampled maximum RTT must exceed sampled minimum RTT")
    sampled_network["latency_jitter_ms"] = sample_option(
        network, "latency_jitter_ms", "latency_jitter_ms_options", rng, int)
    sampled_network["bandwidth_kbps"] = sample_option(
        network, "bandwidth_kbps", "bandwidth_kbps_options", rng, int)
    sampled_network["asymmetry"] = sample_number(
        network, "asymmetry", "asymmetry_range", rng, float)
    models = network.get("topology_models", ["uniform"])
    model = str(models[topology % len(models)])
    if model not in {"uniform", "clustered", "replica_adjacent"}:
        raise ValueError(f"unsupported topology model: {model}")
    return workload, sampled_network, model


def make_network(replica_names: list[str], client_names: list[str], cfg: dict,
                 seed: int, model: str):
    rng = random.Random(seed)
    names = replica_names + client_names
    if model == "uniform":
        points = {name: (rng.random(), rng.random()) for name in names}
    elif model == "clustered":
        centers = [(rng.random(), rng.random()) for _ in range(3)]
        points = {}
        for name in names:
            cx, cy = rng.choice(centers)
            points[name] = (min(1.0, max(0.0, rng.gauss(cx, .08))),
                            min(1.0, max(0.0, rng.gauss(cy, .08))))
    else:
        # One client is colocated with each replica. Supporting round-robin
        # assignment keeps this model well-defined for custom unequal counts,
        # while the paper suite uses equal replica and client counts.
        points = {name: (rng.random(), rng.random()) for name in replica_names}
        for index, name in enumerate(client_names):
            points[name] = points[replica_names[index % len(replica_names)]]
    low, high = int(cfg["minimum_rtt_ms"]), int(cfg["maximum_rtt_ms"])
    asymmetry = float(cfg.get("asymmetry", 0.0))
    matrix: dict[str, dict[str, int]] = {name: {} for name in names}
    for source in names:
        for target in names:
            if source == target:
                matrix[source][target] = 0
                continue
            sx, sy = points[source]
            tx, ty = points[target]
            distance = math.hypot(sx - tx, sy - ty) / math.sqrt(2)
            base = low + distance * (high - low)
            directional = random.Random(stable_seed(seed, source, target)).uniform(-asymmetry, asymmetry)
            matrix[source][target] = max(low, min(high, round(base * (1 + directional))))
    if model == "replica_adjacent":
        for index, client in enumerate(client_names):
            replica = replica_names[index % len(replica_names)]
            # The zero-delay pair has no latency toxic; other links retain
            # their independently sampled network controls.
            matrix[client][replica] = 0
            matrix[replica][client] = 0
    return points, matrix


def logical_addresses(replicas: list[str], clients: list[str]) -> dict[str, str]:
    result = {name: f"0.0.0.{i + 1}" for i, name in enumerate(replicas)}
    result.update({name: f"0.0.1.{i + 1}" for i, name in enumerate(clients)})
    return result


def write_latency(path: Path, names: list[str], matrix: dict[str, dict[str, int]],
                  addresses: dict[str, str]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for source in names:
            for target in names:
                stream.write(f"{addresses[source]} {addresses[target]} {matrix[source][target]}ms\n")


def write_config(path: Path, *, replicas: list[str], clients: list[str],
                 writes: int, exp: dict, seed: int,
                 matrix: dict[str, dict[str, int]]) -> None:
    lines = ["-- Replicas --"]
    lines += [f"{name} 0.0.0.{i + 1}" for i, name in enumerate(replicas)]
    lines += ["", "-- Clients --"]
    lines += [f"{name} 0.0.1.{i + 1}" for i, name in enumerate(clients)]
    lines += ["", "-- Master --", "m0 0.0.2.1", "", "masterPort: 7087", "port: 7070",
              "protocol: SwiftPaxos",
              "noop: false", "thrifty: false", "optread: false", "leaderless: false",
              "fast: true", "waitClosest: true",
              f"writes: {writes}", f"commandSize: {int(exp['command_size'])}", "clones: 0",
              f"arrivalRate: {float(exp['arrival_rate_per_client'])}",
              f"warmup: {WARMUP_SECONDS}s", f"duration: {DURATION_SECONDS}s",
              f"repetitions: {REPETITIONS}",
              f"keyCount: {int(exp['key_count'])}", f"zipfSkew: {float(exp['zipf_skew'])}",
              f"workloadSeed: {seed}", f"preload: {str(bool(exp.get('preload', True))).lower()}",
              f"preloadSeed: {int(exp.get('preload_seed', 1))}", "", "-- Proxy --"]
    # The proxy grouping supplies a generic closest-replica hint. Protocol
    # planners remain responsible for leader, quorum, and final client routes.
    for replica in replicas:
        assigned = [client for client in clients
                    if min(replicas, key=lambda peer: (matrix[client][peer], peer)) == replica]
        lines += [f"server_alias {replica}"]
        lines += [f"{client} (local)" for client in assigned]
        lines += [""]
    lines += ["---", ""]
    path.write_text("\n".join(lines), encoding="utf-8", newline="\n")


def generate(spec_path: Path, output: Path) -> None:
    spec = tomllib.loads(spec_path.read_text(encoding="utf-8"))
    exp, network = spec["experiment"], spec["network"]
    topology_count = int(exp.get("topologies_per_scale", 3))
    models = network.get("topology_models", ["uniform"])
    if topology_count < 1 or not models or any(model not in {"uniform", "clustered", "replica_adjacent"} for model in models):
        raise ValueError("positive topology count and supported topology models required")
    scales = [(int(scale['replicas']), int(scale['clients'])) for scale in spec['scale']]
    if not scales or len(set(scales)) != len(scales):
        raise ValueError("non-empty, distinct replica/client scales required")
    for n, c in scales:
        if n < 3 or n > 17 or n % 2 == 0 or c != n:
            raise ValueError("each scale needs odd replicas from 3 through 17 and one client per replica")
    if output.exists() and any(output.iterdir()):
        raise FileExistsError(f"refusing to replace non-empty output directory: {output}")
    output.mkdir(parents=True, exist_ok=True)
    topologies = []
    for scale_index, (n, c) in enumerate(scales):
        replicas, clients = endpoint_names(n, c)
        addresses = logical_addresses(replicas, clients)
        for topology in range(topology_count):
            case_seed = stable_seed(int(exp["seed"]), scale_index, topology)
            workload, case_network, model = sample_case(exp, network, case_seed, topology)
            points, matrix = make_network(replicas, clients, case_network, case_seed, model)
            root = output / f"n{n}-c{c}-topology-{topology:02d}"
            root.mkdir()
            write_latency(root / "latency.conf", replicas + clients, matrix, addresses)
            write_config(root / "workload.conf", replicas=replicas, clients=clients,
                         writes=50, exp=workload,
                         seed=stable_seed(case_seed, "workload"), matrix=matrix)
            # Membership and workload settings are stored only in workload.conf.
            metadata = {
                "seed": case_seed,
                "points": points, "topology_model": model,
                "network": {key: case_network[key] for key in (
                    "minimum_rtt_ms", "maximum_rtt_ms", "latency_jitter_ms",
                    "bandwidth_kbps", "asymmetry")},
            }
            (root / "case.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8", newline="\n")
            topologies.append(str(root.relative_to(output)).replace("\\", "/"))
    manifest = {"name": exp["name"], "topologies": topologies}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(f"generated {len(topologies)} cluster topologies in {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("spec", type=Path, nargs="?", default=ROOT / "experiment.toml",
                        help="TOML specification (default: slurm/experiment.toml)")
    parser.add_argument("output", type=Path, nargs="?", default=ROOT / "generated",
                        help="new/empty output directory (default: slurm/generated)")
    args = parser.parse_args()
    generate(args.spec.resolve(), args.output.resolve())
