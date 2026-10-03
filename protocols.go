package main

import (
	"log"
	"strings"

	"github.com/hongzicong/ConsensusArena/bodega"
	"github.com/hongzicong/ConsensusArena/client"
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/curp"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/epaxos"
	"github.com/hongzicong/ConsensusArena/fastpaxos"
	"github.com/hongzicong/ConsensusArena/kcensus"
	"github.com/hongzicong/ConsensusArena/paxos"
	"github.com/hongzicong/ConsensusArena/placement"
	protocolapi "github.com/hongzicong/ConsensusArena/protocol"
	"github.com/hongzicong/ConsensusArena/swift"
)

// replicaStart contains deployment inputs; quorum and recovery rules stay in
// each protocol. Constructors retain responsibility for validating their options.
type replicaStart struct {
	config   *config.Config
	logger   *dlog.Logger
	id       int
	addrs    []string
	leader   bool
	failures int
}

// protocolSpec is the integration point for new protocols. Client configuration
// runs before the common client is constructed; installation runs before Connect.
// Deployment preparation runs after selecting the protocol and before startup.
type protocolSpec struct {
	planDeployment  func(*config.Config, *placement.Topology) (config.DeploymentPlan, error)
	configureClient func(*config.Config)
	installClient   func(*client.BufferClient, *config.Config, int)
	newReplica      func(replicaStart) protocolapi.Machine
}

func lookupProtocol(name string) (protocolSpec, bool) {
	p, ok := protocols[strings.ToLower(name)]
	return p, ok
}

var protocols = map[string]protocolSpec{
	"kcensus": {
		planDeployment: kcensus.PlanDeployment,
		installClient: func(b *client.BufferClient, c *config.Config, clone int) {
			// Preserve the original post-construction flag update and clone identity.
			b.Leaderless = true
			b.Fast = false
			kcensus.NewClient(b, c, clone)
		},
		newReplica: func(s replicaStart) protocolapi.Machine {
			log.Println("Starting KCensus replica...")
			return kcensus.New(s.config.Alias, s.id, s.addrs, s.config, s.logger)
		},
	},
	"bodega": {
		planDeployment: bodega.PlanDeployment,
		configureClient: func(c *config.Config) {
			c.Leaderless = true
			c.Fast = false
			c.WaitClosest = true
		},
		installClient: func(b *client.BufferClient, c *config.Config, _ int) {
			bodega.NewClient(b, len(c.ReplicaAddrs)).Unhold = c.BodegaUnhold
		},
		newReplica: func(s replicaStart) protocolapi.Machine {
			log.Println("Starting Bodega replica...")
			return bodega.New(s.config.Alias, s.id, s.addrs, s.leader, s.config, s.logger)
		},
	},
	"swiftpaxos": {
		planDeployment: swift.PlanDeployment,
		installClient: func(b *client.BufferClient, c *config.Config, _ int) {
			swift.NewClient(b, len(c.ReplicaAddrs))
		},
		newReplica: func(s replicaStart) protocolapi.Machine {
			log.Println("Starting SwiftPaxos replica...")
			swift.MaxDescRoutines = 100
			return swift.New(s.config.Alias, s.id, s.addrs, !s.config.Noop,
				s.config.Optread, true, 1, s.failures, s.config, s.logger, nil)
		},
	},
	"curp": {
		planDeployment: curp.PlanDeployment,
		installClient: func(b *client.BufferClient, c *config.Config, _ int) {
			curp.NewClient(b, len(c.ReplicaAddrs))
		},
		newReplica: func(s replicaStart) protocolapi.Machine {
			log.Println("Starting optimized CURP replica...")
			return curp.New(s.config.Alias, s.id, s.addrs, !s.config.Noop,
				s.failures, s.config, s.logger)
		},
	},
	"fastpaxos": {
		planDeployment: fastpaxos.PlanDeployment,
		configureClient: func(c *config.Config) {
			c.Fast = true
			c.WaitClosest = true
		},
		installClient: func(b *client.BufferClient, c *config.Config, _ int) {
			fastpaxos.NewClient(b, len(c.ReplicaAddrs))
		},
		newReplica: func(s replicaStart) protocolapi.Machine {
			log.Println("Starting Fast Paxos replica...")
			return fastpaxos.New(s.config.Alias, s.id, s.addrs, !s.config.Noop, s.failures, s.config, s.logger)
		},
	},
	"paxos": {
		planDeployment: paxos.PlanDeployment,
		configureClient: func(c *config.Config) {
			c.WaitClosest = false
			c.Fast = false
		},
		installClient: func(b *client.BufferClient, c *config.Config, _ int) {
			paxos.NewClient(b, len(c.ReplicaAddrs))
		},
		newReplica: func(s replicaStart) protocolapi.Machine {
			log.Println("Starting Paxos replica...")
			return paxos.New(s.config.Alias, s.id, s.addrs, s.failures, s.config, s.logger)
		},
	},
	"epaxos": {
		planDeployment: epaxos.PlanDeployment,
		configureClient: func(c *config.Config) {
			c.Leaderless = true
			c.Fast = false
		},
		installClient: func(b *client.BufferClient, c *config.Config, _ int) {
			epaxos.NewClient(b, len(c.ReplicaAddrs))
		},
		newReplica: func(s replicaStart) protocolapi.Machine {
			log.Println("Starting EPaxos replica...")
			return epaxos.New(s.config.Alias, s.id, s.addrs, !s.config.Noop, false, false, 5, false, s.failures, s.config, s.logger)
		},
	},
}
