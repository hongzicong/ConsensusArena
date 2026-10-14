# Upstream Slurm branch validation

Validated on 2026-10-14. This is integration evidence for
`codex/swiftpaxos-slurm`, not an XPaxos iteration or a comparative performance
or recovery result.

## Source and local checks

- Upstream pin: `35c69365f1c7737a08e237bfbaf828ee68897080`.
- Source guard: 43 upstream files unchanged after LF normalization; all six
  protocol packages match the pin. Six original files are declared platform
  adapters/documentation changes in `upstream-provenance.json`.
- Four current Go adapter tests pass: co-located/shuffled quorum identities,
  legacy bare-host compatibility, routing-map validation, and listener barrier.
- Linux adapter test binaries were also run on Jed for the initial three
  addressing tests. The final listener barrier is exercised by the live run.
- Nine Python tests pass for scene selection, co-location, sample accounting,
  incomplete/failed runs, scene identity contamination, smoke labeling,
  non-invasive listener observation, and required resource evidence.
- Linux amd64 cross-build and Bash syntax checks pass. Full native Windows
  compilation is unsupported by upstream's `syscall.SIGUSR1` use; the upstream
  hook package is retained rather than changed for Windows.

## Completed Slurm integration run

| Field | Observation |
| --- | --- |
| Job | `40381459`, `COMPLETED`, exit `0:0` |
| Tested adapter revision | `87383c5ae513d20e4c65ce982e2ca42e8d1a0455` |
| Binary SHA256 | `abdf47366da90d1d94bc63d8829f5a4a739ad89e4840edcb7d83cf92344547e4` |
| Toxiproxy | v2.12.0, SHA256 `556d891134a3c582dc1e1a3f7335fd55142e5965769855a00b944e13e48302fc` |
| Placement | 5 replicas, 10 clients, 1 master; topology 1; profile A |
| Allocation | Single-node `bigmem`, `serial` QOS, 16 tasks, 1 CPU/task |
| Workload | Native upstream pipeline, 32 measured requests/client plus the original bootstrap |
| Client completion | 10/10 successful clients; 320/320 measured requests |
| Resource observations | 16/16 complete CPU/RSS/context-switch records |
| Experiment validity | `valid=true`, suite complete, `diagnostic_only=true` |
| Allocation elapsed | 23 s, from Slurm accounting |

Portable evidence is under [validation/40381459/](validation/40381459/), including
the exact native summaries, scene status, and client/resource validation.
The full raw run is archived on Jed under
`~/consensusarena-results/swiftpaxos-upstream-40381459/`; the local raw archive
and extracted evidence are under the ignored `results/jed/` directory.

This run validates startup, proxied peer/client communication, source-style
execution, teardown, and collection. Its single-node diagnostic allocation,
upstream random workload, original completion semantics, and placement policy
make its latency observations unsuitable for a matched-workload architecture
claim or promotion gate. No full 27-scene or fault-recovery suite was run.

## Issues resolved during validation

1. Job `40381283` was invalid: Toxiproxy could accept a connection before the
   target data listener existed. The native peer code then retained a closed
   connection, leaving readiness at 4/5. A deployment startup barrier now checks
   Linux LISTEN tables without consuming a peer handshake, then releases dials.
2. Job `40381323` completed all 320 requests but initially had resource output
   only for the ten clients. Teardown now stops each application before GNU
   time, and validation requires every participant's resource record. The final
   job above passes this stronger check.
3. The original three-node standard-partition job `40381189` was cancelled
   while pending, because it used the earlier adapter and had a multi-hour
   scheduler estimate. Default benchmark resources remain unchanged; a tagged
   smoke mode permits cheaper integration checks on an eligible single node.

These fixes affect deployment/transport startup and measurement lifecycle;
upstream protocol packages, message codecs, voting, commit, fallback, and
recovery logic remain unchanged.
