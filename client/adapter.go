package client

import "github.com/hongzicong/ConsensusArena/replica/defs"

// Adapter owns protocol routing and client completion. Start runs after the
// connections are ready; WaitReplies runs after optional fault instrumentation.
type Adapter interface {
	Start() error
	SendProposal(defs.Propose)
	WaitReplies(int)
	// RetryPolicy describes client retransmission/repair for experiment metadata.
	RetryPolicy() string
	Close()
}

// StandardClient supplies shared workload access and executed-result delivery.
// Protocols with custom evidence or routing override the relevant methods.
type StandardClient struct{ *BufferClient }

func (*StandardClient) Start() error        { return nil }
func (*StandardClient) Close()              {}
func (c *StandardClient) WaitReplies(_ int) { c.WaitAnyReplies() }
func (*StandardClient) RetryPolicy() string {
	return "none; unresolved requests are censored"
}

// SetProtocol must be called before Connect and before starting the workload.
func (c *Client) SetProtocol(adapter Adapter) { c.protocol = adapter }
