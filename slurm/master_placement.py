"""External startup policy equivalent to the pinned master's plan.go functions.

No protocol implementation or registry is imported. Differential checks against
the actual donor Go policies are in check-benchmark-parity.py.
"""
import itertools
import math
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
    if protocol not in ('swiftpaxos', 'paxos', 'curp', 'fastpaxos', 'epaxos'):
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
    if protocol == 'epaxos':
        return dict(result, model='nearest-replica-ingress')
    if protocol == 'fastpaxos':
        q = 3*n//4 + 1
        fast = [min(max(to[c][j], kth([to[c][v]+d[v][j] for v in range(n)], q-1))
                    + back[c][j] for j in range(n)) for c in range(len(clients))]
        return dict(result, model='any-three-quarter-fast-quorum', fast_quorum_size=q,
                    fast_mean_ms=compensated_mean(fast))
    models = {'swiftpaxos': 'swift-c2-leader-fixed-majority',
              'paxos': 'leader-roundtrip-dynamic-majority',
              'curp': 'witness-fast-and-commit-relay'}
    best_key = None
    for leader in range(n):
        if protocol == 'swiftpaxos':
            slow = [max(rtt(c, leader), kth([max(to[c][r], to[c][leader]+d[leader][r])
                    + back[c][r] for r in range(n) if r != leader], f-1))
                    for c in range(len(clients))]
            candidates = (q for q in itertools.combinations(range(n), f+1) if leader in q)
        else:
            majority = kth([d[leader][r]+d[r][leader] for r in range(n) if r != leader], f-1)
            if protocol == 'paxos':
                slow = [rtt(c, leader)+majority for c in range(len(clients))]
                fast = slow
            else:
                slow, fast = [], []
                for c in range(len(clients)):
                    cost = math.inf
                    for j in range(n):
                        quorum = min(kth([to[c][leader]+d[leader][r]+d[r][j]
                                          for r in range(n)], f),
                                     to[c][leader]+majority+d[leader][j])
                        cost = min(cost, max(to[c][j], to[c][leader]+d[leader][j], quorum)+back[c][j])
                    slow.append(cost)
                    fast.append(min(cost, max(rtt(c, leader), kth([rtt(c, r)
                                for r in range(n) if r != leader], f+(f+1)//2-1))))
            candidates = [()]
        sm = compensated_mean(slow)
        for quorum in candidates:
            if protocol == 'swiftpaxos':
                fast = [min(max(rtt(c, r) for r in quorum), slow[c]) for c in range(len(clients))]
            fm = compensated_mean(fast)
            aliases = sorted(replicas[r] for r in quorum)
            key = replicas[leader] + ''.join('\0'+a for a in aliases)
            score = (sm, fm, key) if objective == 'slow-first' else (fm, sm, key)
            if best_key is None or score < best_key:
                best_key = score
                result.update(leader=replicas[leader], fast_quorum=aliases,
                              fast_mean_ms=fm, slow_mean_ms=sm)
    return dict(result, model=models[protocol], objective=objective)
