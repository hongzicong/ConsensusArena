# Agreement hot/cold ablation

The experiment re-optimizes each variant under its allowed mechanisms. Hot-off
excludes the hot-certificate placement candidate, hot read races and hot owner
delegation, and models ordinary read fences for hot keys. Cold-off removes cold
placement candidates. The retained cold path is never forced on: the normal
selection model may reject it. All participants receive the same startup mode.

`agreementAblation` accepts `full`, `no-cold`, `no-hot`, or `neither`. Omitting it
preserves the normal implementation. A fresh `full` run is the matched control;
historical results remain separately identifiable.

- Cold-off prevents activation, routing, voting and client proof completion in
  the cold protocol. Former cold keys use Agreement's ordinary ordered engine.
- Hot-off disables hot-key read races, hot-key client certificates and hot-key
  completion-owner/old-value shortcuts. Hot keys use the ordinary ordered engine;
  non-hot ordinary-key certificates and cold handling remain available.
- Both-off combines these switches; it is still Agreement's ordinary engine,
  not a new Paxos/Bodega baseline. No quorum or lease check is weakened.

The sample's hot cohort comprises observed keys whose frequency is above the
mean observed-key frequency. The selected cold cohort comprises observed ranks
at or beyond ColdCutoff only when ColdEnabled is true. Unobserved keys are
ordinary. A hot classification is not itself evidence that a particular hot
optimization was exercised; reports distinguish cohort sizes, selected features
and recorded usage counters.

Safety grounding: `ConsensusWiki/wiki/proof-notes/read-freshness.md`, especially
the Bodega serving-authority/coverage cases and visibility-versus-stable-result
distinction, and the actual Agreement completion, client-certificate, cold and
read-release handlers. These switches select existing conservative completion
paths. They do not introduce new certificates, change quorum membership, or
permit early completion without its original evidence. Removing hot certificates
must also remove the owner's old-value shortcut; otherwise the coupling between
completion timing and read visibility would be misrepresented. Cold is disabled
before any cold generation exists, so no live cold state needs migration.

Tests must cover all four modes, preservation of each selected plan, hot/ordinary
certificate eligibility, suppression of hot races and owner shortcuts, and
disabled cold activation. Experiments retain seven replicas, four deployments,
A/B, Zipf 0.99, 20k offered requests/s, and existing measurement semantics.

Primary system-level comparisons include only cases with the corresponding mechanism
enabled in the full control and evidence of use. Other cases are reported as
inactive controls. Cold-only comparisons are expected for Ring A and North
America A. Read-only C has no hot write/certificate/race activity and no selected
cold placement; it is excluded from ablation runs rather than evidence of a benefit.
No-cold runs only Ring A/North America A; the full, no-hot and neither arms run
the eight A/B cases. Because roles can change, effects include placement changes
and interactions; they are not estimates of an isolated fixed-role path cost.
