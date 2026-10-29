package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"

	"github.com/hongzicong/ConsensusArena/client"
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/heatprofile"
	"github.com/hongzicong/ConsensusArena/master"
	"github.com/hongzicong/ConsensusArena/placement"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/resourceobs"
	"github.com/hongzicong/ConsensusArena/wireproto"
)

var (
	confs        = flag.String("config", "", "Deployment config `file` (required)")
	logFile      = flag.String("log", "", "Path to the log `file`")
	machineAlias = flag.String("alias", "", "An `alias` of this participant")
	machineType  = flag.String("run", "server", "Run a `participant`, which is either a server (or replica), a client, a master, or plan (inspect startup placement)")
	protocol     = flag.String("protocol", "", "Protocol to run. Overwrites `protocol` field of the config file")
	heatRequests = flag.Int("heat-samples", 1000000, "Independent offline offered requests to sample")
	heatSeed     = flag.Int64("heat-seed", 101, "Independent offline sampling seed")
	heatOutput   = flag.String("heat-out", "", "Output profile file for -run heat-sample")
)

func main() {
	flag.Parse()
	log.Printf("WIRE_CODEC name=%s", wireproto.Codec)

	if *confs == "" {
		flag.Usage()
		os.Exit(1)
	}

	c, err := config.Read(*confs, *machineAlias)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if *protocol != "" {
		c.Protocol = *protocol
	}
	p, ok := lookupProtocol(c.Protocol)
	if *machineType == "heat-sample" {
		if *heatOutput == "" {
			log.Fatal("-heat-out is required")
		}
		profile, err := client.SampleHeat(c, *heatRequests, *heatSeed, p.heatSampleOptions)
		if err != nil {
			log.Fatal(err)
		}
		digest, err := heatprofile.Write(*heatOutput, profile)
		if err != nil {
			log.Fatal(err)
		}
		if err := json.NewEncoder(os.Stdout).Encode(map[string]interface{}{
			"profile": *heatOutput, "sha256": digest, "requests": profile.Requests,
			"observed_keys": len(profile.Keys), "sample_seed": profile.SampleSeed,
			"schema": profile.Schema, "sampled_clients": len(profile.Clients),
		}); err != nil {
			log.Fatal(err)
		}
		return
	}
	if !ok {
		log.Fatalf("unknown protocol %q", c.Protocol)
	}
	topology, err := placement.Load(c, *confs)
	if err != nil {
		log.Fatalf("load topology: %v", err)
	}
	plan, err := p.planDeployment(c, topology)
	if err != nil {
		log.Fatalf("plan %s: %v", c.Protocol, err)
	}
	if err = placement.Apply(c, topology, plan); err != nil {
		log.Fatalf("apply plan: %v", err)
	}
	if *machineType == "plan" {
		// Diagnostic output only: startup always computes its plan from raw inputs.
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(struct {
			config.DeploymentPlan
			Inputs *placement.Topology `json:"inputs"`
		}{c.Plan, topology}); err != nil {
			log.Fatal(err)
		}
		return
	}
	if dialMap := os.Getenv("CONSENSUSARENA_DIAL_MAP"); dialMap != "" {
		if err := defs.ConfigureDialMap(dialMap); err != nil {
			log.Fatalf("configure Toxiproxy dial map: %v", err)
		}
	}

	switch *machineType {
	case "replica":
		fallthrough
	case "server":
		c.MachineType = config.ReplicaMachine
	case "client":
		c.MachineType = config.ClientMachine
	case "master":
		c.MachineType = config.MasterMachine
	default:
		fmt.Println("Unknown participant type")
		flag.Usage()
		os.Exit(1)
	}

	switch c.MachineType {
	case config.ReplicaMachine:
		defs.LocalAddr = c.ReplicaAddrs[c.Alias]
	case config.ClientMachine:
		defs.LocalAddr = c.ClientAddrs[c.Alias]
	}

	run(c)
}

func run(conf *config.Config) {
	switch conf.MachineType {
	case config.MasterMachine:
		runMaster(conf)
	case config.ClientMachine:
		runClient(conf, true)
	case config.ReplicaMachine:
		resourceobs.Start(os.Getenv("CONSENSUSARENA_RESOURCE_RUN"), conf.Alias)
		runReplica(conf, dlog.New(*logFile, true))
	}
}

func runMaster(conf *config.Config) {
	m := master.New(len(conf.ReplicaAddrs), conf.MasterPort, conf, dlog.New(*logFile, true))
	m.Run()
}

func runClient(conf *config.Config, verbose bool) {
	if p, ok := lookupProtocol(conf.Protocol); ok && p.configureClient != nil {
		p.configureClient(conf)
	}
	var wg sync.WaitGroup
	for i := 0; i < conf.Clones+1; i++ {
		wg.Add(1)
		go func(i int) {
			runSingleClient(conf, i, verbose)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func runSingleClient(conf *config.Config, i int, verbose bool) {
	var l *dlog.Logger
	if i == 0 {
		l = dlog.New(*logFile, verbose)
	} else {
		f := *logFile
		if f == "" {
			f = "client_"
		}
		l = dlog.New(f+strconv.Itoa(i), verbose)
		// TODO: remove if already exists
	}

	server := conf.Proxy.ProxyOf(conf.ClientAddrs[conf.Alias])
	server = conf.ReplicaAddrs[server]
	cl := client.NewClientLog(server, conf.MasterAddr, conf.MasterPort, conf.Fast, conf.Leaderless, verbose, l)
	workloadSeed := client.DeriveWorkloadSeed(int64(conf.WorkloadSeed), conf.Alias, i)
	b := client.NewBufferClient(cl, conf.CommandSize, conf.Writes, conf.KeyCount, conf.ZipfSkew, workloadSeed)
	b.SetKeyMapping(conf.ShuffleKeys, int64(conf.KeyMappingSeed))
	if conf.UniqueKeys {
		members, err := conf.Membership()
		if err != nil {
			log.Fatal(err)
		}
		aliases := members.Clients
		ordinal := sort.SearchStrings(aliases, conf.Alias)
		if ordinal == len(aliases) || aliases[ordinal] != conf.Alias {
			log.Fatal("unique-key client alias is not configured")
		}
		if err := b.UniqueKeysFor(ordinal*(conf.Clones+1)+i, len(aliases)*(conf.Clones+1)); err != nil {
			log.Fatal(err)
		}
	}
	b.PoissonArrivals(conf.ArrivalRate)
	b.MeasureFor(conf.Warmup, conf.Duration)
	if p, ok := lookupProtocol(conf.Protocol); ok {
		p.installClient(b, conf, i)
	}
	if err := b.Connect(); err != nil {
		log.Fatal(err)
	}
	if err := b.ConfigureFaultRun(conf.Alias, i); err != nil {
		log.Fatal(err)
	}
	if err := b.ConfigureLatencyRun(conf.Alias, i); err != nil {
		log.Fatal(err)
	}
	waitFrom := b.LeaderID
	if b.Fast || b.Leaderless || conf.WaitClosest {
		waitFrom = b.ClosestID
	}
	b.WaitReplies(waitFrom)
	b.Loop()
}
