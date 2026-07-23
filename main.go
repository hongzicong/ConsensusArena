package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/hongzicong/ConsensusArena/client"
	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/curp"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/master"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/swift"
)

var (
	confs        = flag.String("config", "", "Deployment config `file` (required)")
	latency      = flag.String("latency", "", "Latency config `file`")
	logFile      = flag.String("log", "", "Path to the log `file`")
	machineAlias = flag.String("alias", "", "An `alias` of this participant")
	machineType  = flag.String("run", "server", "Run a `participant`, which is either a server (or replica), a client or a master")
	protocol     = flag.String("protocol", "", "Protocol to run. Overwrites `protocol` field of the config file")
	quorum       = flag.String("quorum", "", "Quorum config `file`")
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
	defs.LatencyConf = *latency

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

	c.Quorum = *quorum
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

	switch strings.ToLower(c.Protocol) {
	case "swiftpaxos":
	case "curp":
	case "fastpaxos":
		c.Fast = true
		c.WaitClosest = true
	case "n2paxos":
		c.Fast = true
		c.WaitClosest = true
	case "epaxos":
		c.Leaderless = true
		c.Fast = false
	case "paxos":
		c.WaitClosest = false
		c.Fast = false
	}

	server := c.Proxy.ProxyOf(c.ClientAddrs[c.Alias])
	server = c.ReplicaAddrs[server]
	cl := client.NewClientLog(server, c.MasterAddr, c.MasterPort, c.Fast, c.Leaderless, verbose, l)
	workloadSeed := client.DeriveWorkloadSeed(int64(c.WorkloadSeed), c.Alias, i)
	b := client.NewBufferClient(cl, c.CommandSize, c.Writes, c.KeyCount, c.ZipfSkew, workloadSeed)
	b.PoissonArrivals(c.ArrivalRate)
	b.MeasureFor(c.Warmup, c.Duration)
	if err := b.Connect(); err != nil {
		log.Fatal(err)
	}
	if p := strings.ToLower(c.Protocol); p == "swiftpaxos" {
		cl := swift.NewClient(b, len(c.ReplicaAddrs))
		if cl == nil {
			return
		}
		cl.Loop()
	} else if p == "curp" {
		cl := curp.NewClient(b, len(c.ReplicaAddrs))
		if cl == nil {
			return
		}
		cl.Loop()
	} else {
		waitFrom := b.LeaderId
		if b.Fast || b.Leaderless || c.WaitClosest {
			waitFrom = b.ClosestId
		}
		b.WaitReplies(waitFrom)
		b.Loop()
	}
}
