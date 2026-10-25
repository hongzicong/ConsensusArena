"""External startup policy equivalent to the pinned master's plan.go functions.

No protocol implementation or registry is imported. The benchmark reference
revision is pinned in upstream_harness.py.
"""
from decimal import Decimal


def compensated_mean(values):
    total, correction = 0.0, 0.0
    weight = 1.0 / len(values)
    for value in values:
        x = weight * value
        next_total = total + x
        if abs(total) >= abs(x):
            correction += (total - next_total) + x
        else:
            correction += (x - next_total) + total
        total = next_total
    return total + correction


def plan(protocol, replicas, clients, matrix, objective='slow-first'):
    if objective not in ('slow-first', 'fast-first'):
        raise ValueError('unknown quorum objective: ' + objective)
    if protocol != 'paxos':
        raise ValueError('no master comparison policy for ' + protocol)
    clients = sorted(clients)  # master Config.Membership() order
    # Master parses the generated decimal RTT strings with time.ParseDuration:
    # truncate sub-nanosecond fractions before converting to one-way ms.
    matrix = {a: {b: int(Decimal(str(v))*1_000_000)/1_000_000 for b, v in row.items()}
              for a, row in matrix.items()}
    n, f = len(replicas), len(replicas) // 2
    d = [[0 if i == j else matrix[a][b] / 2 for j, b in enumerate(replicas)]
         for i, a in enumerate(replicas)]
    to = [[matrix[c][r] / 2 for r in replicas] for c in clients]
    back = [[matrix[r][c] / 2 for r in replicas] for c in clients]
    rtt = lambda c, r: to[c][r] + back[c][r]
    kth = lambda values, k: sorted(values)[k]
    routes = {a: replicas[min(range(n), key=lambda r: (rtt(c, r), replicas[r]))]
              for c, a in enumerate(clients)}
    result = dict(leader=None, fast_quorum=[], client_routes=routes,
                  fast_mean_ms=0.0, slow_mean_ms=0.0)
    best_key = None
    for leader in range(n):
        majority = kth([d[leader][r]+d[r][leader] for r in range(n) if r != leader], f-1)
        slow = [rtt(c, leader)+majority for c in range(len(clients))]
        sm = compensated_mean(slow)
        # Paxos has one dynamic-majority path, so both objectives coincide.
        score = (sm, sm, replicas[leader])
        if best_key is None or score < best_key:
            best_key = score
            result.update(leader=replicas[leader], fast_mean_ms=sm, slow_mean_ms=sm)
    return dict(result, model='leader-roundtrip-dynamic-majority', objective=objective)
