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
	"github.com/hongzicong/ConsensusArena/master"
	"github.com/hongzicong/ConsensusArena/placement"
	"github.com/hongzicong/ConsensusArena/replica/defs"
)

var (
	confs        = flag.String("config", "", "Deployment config `file` (required)")
	logFile      = flag.String("log", "", "Path to the log `file`")
	machineAlias = flag.String("alias", "", "An `alias` of this participant")
	machineType  = flag.String("run", "server", "Run a `participant`, which is either a server (or replica), a client, a master, or plan (inspect startup placement)")
	protocol     = flag.String("protocol", "", "Protocol to run. Overwrites `protocol` field of the config file")
)

func main() {
	flag.Parse()

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

func run(c *config.Config) {
	switch c.MachineType {
	case config.MasterMachine:
		runMaster(c)
	case config.ClientMachine:
		runClient(c, true)
	case config.ReplicaMachine:
		runReplica(c, dlog.New(*logFile, true))
	}
}

func runMaster(c *config.Config) {
	m := master.New(len(c.ReplicaAddrs), c.MasterPort, c, dlog.New(*logFile, true))
	m.Run()
}

func runClient(c *config.Config, verbose bool) {
	if p, ok := lookupProtocol(c.Protocol); ok && p.configureClient != nil {
		p.configureClient(c)
	}
	var wg sync.WaitGroup
	for i := 0; i < c.Clones+1; i++ {
		wg.Add(1)
		go func(i int) {
			runSingleClient(c, i, verbose)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func runSingleClient(c *config.Config, i int, verbose bool) {
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

	server := c.Proxy.ProxyOf(c.ClientAddrs[c.Alias])
	server = c.ReplicaAddrs[server]
	cl := client.NewClientLog(server, c.MasterAddr, c.MasterPort, c.Fast, c.Leaderless, verbose, l)
	workloadSeed := client.DeriveWorkloadSeed(int64(c.WorkloadSeed), c.Alias, i)
	b := client.NewBufferClient(cl, c.CommandSize, c.Writes, c.KeyCount, c.ZipfSkew, workloadSeed)
	if c.UniqueKeys {
		members, err := c.Membership()
		if err != nil {
			log.Fatal(err)
		}
		aliases := members.Clients
		ordinal := sort.SearchStrings(aliases, c.Alias)
		if ordinal == len(aliases) || aliases[ordinal] != c.Alias {
			log.Fatal("unique-key client alias is not configured")
		}
		if err := b.UniqueKeysFor(ordinal*(c.Clones+1)+i, len(aliases)*(c.Clones+1)); err != nil {
			log.Fatal(err)
		}
	}
	b.PoissonArrivals(c.ArrivalRate)
	b.MeasureFor(c.Warmup, c.Duration)
	if p, ok := lookupProtocol(c.Protocol); ok {
		p.installClient(b, c, i)
	}
	if err := b.Connect(); err != nil {
		log.Fatal(err)
	}
	if err := b.ConfigureFaultRun(c.Alias, i); err != nil {
		log.Fatal(err)
	}
	waitFrom := b.LeaderId
	if b.Fast || b.Leaderless || c.WaitClosest {
		waitFrom = b.ClosestId
	}
	b.WaitReplies(waitFrom)
	b.Loop()
}
