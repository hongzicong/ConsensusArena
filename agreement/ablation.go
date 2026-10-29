package agreement

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/hongzicong/ConsensusArena/config"
)

func ablationMode(mode string) (hotOff, coldOff bool, err error) {
	switch mode {
	case "", "full":
	case "no-cold":
		coldOff = true
	case "no-hot":
		hotOff = true
	case "neither":
		hotOff, coldOff = true, true
	default:
		err = fmt.Errorf("agreementAblation must be full, no-cold, no-hot, or neither")
	}
	return
}

// Record the variant's selected placement. Physical addresses are excluded
// from the fingerprint because independently allocated Slurm scenes remap them.
func applyAblation(c *config.Config, p config.DeploymentPlan) (config.DeploymentPlan, error) {
	hotOff, coldOff, err := ablationMode(c.AgreementAblation)
	if err != nil || c.AgreementAblation == "" {
		return p, err
	}
	data, err := json.Marshal(p)
	if err != nil {
		return p, err
	}
	var logical any
	if err := json.Unmarshal(data, &logical); err != nil {
		return p, err
	}
	var stripEndpoints func(any)
	stripEndpoints = func(value any) {
		switch x := value.(type) {
		case map[string]any:
			delete(x, "endpoint")
			for _, child := range x {
				stripEndpoints(child)
			}
		case []any:
			for _, child := range x {
				stripEndpoints(child)
			}
		}
	}
	stripEndpoints(logical)
	data, err = json.Marshal(logical)
	if err != nil {
		return p, err
	}
	digest := sha256.Sum256(data)
	p.AgreementBasePlanSHA256 = hex.EncodeToString(digest[:])
	p.AgreementBaseColdEnabled = p.ColdEnabled
	p.AgreementAblation = c.AgreementAblation
	p.AgreementHotDisabled = hotOff
	if coldOff {
		p.ColdEnabled = false
	}
	return p, nil
}

func hotAblated(c *config.Config) bool {
	hotOff, _, _ := ablationMode(c.AgreementAblation)
	return hotOff
}

func coldAblated(c *config.Config) bool {
	_, coldOff, _ := ablationMode(c.AgreementAblation)
	return coldOff
}
