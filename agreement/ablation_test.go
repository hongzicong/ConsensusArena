package agreement

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/state"
)

func TestAblationMetadataPreservesItsSelectedPlacement(t *testing.T) {
	base := config.DeploymentPlan{Leader: &config.PlannedReplica{Alias: "r0", Rank: 0, Endpoint: "node-a:7070"},
		Responders: []string{"r0", "r1"}, ClientRoutes: map[string]string{"c0": "r1"},
		WriteClientRoutes: map[string]string{"c0": "r0"}, ClientLearningAllKeys: true,
		ColdEnabled: true, ColdCutoff: 10, ColdQuorum: 3,
		ReadRaceRanges: map[string][]config.PlannedReadRaceRange{"c0": {{Start: 0, End: 2, Target: 0}}}}
	original, _ := json.Marshal(base)
	hash := ""
	for _, mode := range []string{"full", "no-cold", "no-hot", "neither"} {
		p, err := applyAblation(&config.Config{AgreementAblation: mode}, base)
		if err != nil {
			t.Fatal(err)
		}
		if hash == "" {
			hash = p.AgreementBasePlanSHA256
		}
		if p.AgreementBasePlanSHA256 != hash || !p.AgreementBaseColdEnabled {
			t.Fatal("ablation recomputed its own placement")
		}
		hotOff, coldOff, _ := ablationMode(mode)
		if p.AgreementHotDisabled != hotOff || p.ColdEnabled == coldOff {
			t.Fatal("wrong effective ablation switches")
		}
		p.AgreementAblation, p.AgreementBasePlanSHA256 = "", ""
		p.AgreementBaseColdEnabled, p.AgreementHotDisabled = false, false
		p.ColdEnabled = true
		data, _ := json.Marshal(p)
		if string(data) != string(original) {
			t.Fatal("ablation changed roles, routes, or selected hot/cold boundaries")
		}
	}
	remapped := base
	remapped.Leader = &config.PlannedReplica{Alias: "r0", Rank: 0, Endpoint: "node-z:7070"}
	p, _ := applyAblation(&config.Config{AgreementAblation: "full"}, remapped)
	if p.AgreementBasePlanSHA256 != hash {
		t.Fatal("physical Slurm address contaminated placement identity")
	}
	p, _ = applyAblation(&config.Config{}, base)
	if !reflect.DeepEqual(p, base) {
		t.Fatal("default mode changed normal Agreement")
	}
	if _, err := applyAblation(&config.Config{AgreementAblation: "typo"}, base); err == nil {
		t.Fatal("invalid ablation silently accepted")
	}
}

func ablationKeyPolicy() *keyPolicy {
	return &keyPolicy{count: 3, hotCutoff: 1, inverse: []int32{0, 1, -1}}
}

func TestHotOffDisablesShortcutsWithoutChangingOrdinaryKeys(t *testing.T) {
	e := &engine{current: roster{Ballot: 1, Leader: 0, Responders: 3}, opt: options{KeyPolicy: ablationKeyPolicy(),
		Completion: []completionSpan{{Start: 0, End: 2, Owner: 1}}}}
	v := entry{Ballot: 1, ReadFresh: true}
	r := request{Origin: 1, Proposal: defs.Propose{Command: state.Command{Op: state.PUT, K: 0}}}
	if !e.ownerMayBypass(v, r) {
		t.Fatal("control did not allow the existing hot owner shortcut")
	}
	e.opt.HotDisabled = true
	if e.ownerMayBypass(v, r) {
		t.Fatal("hot owner can still bypass pending writes")
	}
	if _, delegated := e.plannedCompletionOwner(0); delegated {
		t.Fatal("hot delegation survived hot-off")
	}
	r.Proposal.Command.K = 1
	if !e.ownerMayBypass(v, r) {
		t.Fatal("ordinary key behavior changed")
	}
	c := &Client{hotDisabled: true, learningEnabled: true, keyPolicy: ablationKeyPolicy(),
		completionRanges: []config.PlannedCompletionRange{{Start: 0, End: 2, Owner: config.PlannedReplica{Rank: 1}}}}
	c.startHotReadRace(defs.Propose{Command: state.Command{Op: state.GET, K: 0}}, 0)
	if _, delegated := c.plannedOwner(0); delegated {
		t.Fatal("client still routes hot writes to a delegated owner")
	}
	if owner, delegated := c.plannedOwner(1); !delegated || owner != 1 {
		t.Fatal("ordinary client delegation changed")
	}
}

func TestHotOffSuppressesOnlyHotClientCertificates(t *testing.T) {
	e := &engine{current: roster{Ballot: 1, Leader: 0, Responders: 3},
		opt: options{KeyPolicy: ablationKeyPolicy(), ClientLearning: true, AllKeyLearning: true, HotDisabled: true},
		log: map[uint64]entry{}}
	r := &Replica{engine: e}
	for _, key := range []state.Key{0, 1} {
		v := entry{Slot: uint64(key + 1), Ballot: 1, ReadFresh: true,
			Request: request{Proposal: defs.Propose{Command: state.Command{Op: state.PUT, K: key}}}}
		e.log[v.Slot] = v
		r.sendClientVotes(v)
	}
	if e.stats.HotCertificateVoteCommands != 0 || e.stats.ClientVoteCommands != 1 {
		t.Fatalf("hot-off leaked hot certificates or removed ordinary ones: hot=%d all=%d", e.stats.HotCertificateVoteCommands, e.stats.ClientVoteCommands)
	}
}

func TestColdOffNeverActivatesColdKeyHandling(t *testing.T) {
	base := config.DeploymentPlan{ColdEnabled: true, ColdCutoff: 1, ColdQuorum: 3}
	p, err := applyAblation(&config.Config{AgreementAblation: "no-cold"}, base)
	if err != nil {
		t.Fatal(err)
	}
	r := &Replica{engine: &engine{opt: options{KeyPolicy: ablationKeyPolicy()}}}
	cold := newColdReplica(r, p)
	if cold.isCold(1) || cold.requiredRoles() != 0 {
		t.Fatal("disabled cold path classified a key or imposed active cold roles")
	}
}
