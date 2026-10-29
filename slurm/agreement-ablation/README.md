# Agreement path ablation

Four independently collected arms: `full`, `no-cold`, `no-hot`, `neither`.
The full, no-hot and neither arms use the four KCensus topologies with A/B.
No-cold uses only Ring A and North America A, where cold was selected in the
original full plans. All use seven replicas/clients, 20k offered requests/s,
Zipf 0.99 and unchanged timing. Total: 26 scenes; read-only C is excluded.

Generate input roots once with `python slurm/agreement-ablation/generate.py`.
`launch.ps1 -Server <login>@jed.hpc.epfl.ch` freezes one binary and submits four
arrays, with one active scene per arm by default (eight nodes total).
Each array gets its own collector and immutable result directory. The full arm
is rerun to provide a control from exactly the same executable and runtime.

Each variant re-optimizes using only its allowed mechanisms. The plan records
its selected-plan fingerprint excluding physical Slurm addresses; fingerprints
may differ across arms, and reports expose those placement changes.
See `agreement/ABLATION.md` for the precise mechanism switches and safety scope.
Cold-off and both-off do not activate or route requests through the cold protocol.

Reports distinguish all sample-ranked hot/cold cohort keys from actual mechanism
activity. Unobserved keys are ordinary. Primary ablation comparisons are separated
from cases where the disabled path was inactive in the full control.
