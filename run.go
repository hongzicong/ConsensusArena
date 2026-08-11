package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
	"time"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/curp"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/epaxos"
	"github.com/hongzicong/ConsensusArena/fastpaxos"
	"github.com/hongzicong/ConsensusArena/n2paxos"
	"github.com/hongzicong/ConsensusArena/paxos"
	"github.com/hongzicong/ConsensusArena/replica/defs"
	"github.com/hongzicong/ConsensusArena/swift"
)

func runReplica(c *config.Config, logger *dlog.Logger) {
	maddr := fmt.Sprintf("%s:%d", c.MasterAddr, c.MasterPort)
	addr, port := replicaEndpoint(c.ReplicaAddrs[c.Alias], c.Port)
	log.Printf("Server starting on %s:%d", addr, port)
	replicaId, nodeList, isLeader := registerWithMaster(c.Alias, addr, maddr, port)
	f := (len(c.ReplicaAddrs) - 1) / 2
	log.Printf("Tolerating %d max. failures", f)

	switch strings.ToLower(c.Protocol) {
	case "swiftpaxos":
		log.Println("Starting SwiftPaxos replica...")
		swift.MaxDescRoutines = 100
		rep := swift.New(c.Alias, replicaId, nodeList, !c.Noop,
			c.Optread, true, false, 1, f, c, logger, nil)
		rpc.Register(rep)
	case "curp":
		log.Println("Starting optimized CURP replica...")
		curp.MaxDescRoutines = 100
		rep := curp.New(c.Alias, replicaId, nodeList, !c.Noop,
			1, f, true, c, logger)
		rpc.Register(rep)
	case "fastpaxos":
		log.Println("Starting Fast Paxos replica...")
		rep := fastpaxos.New(c.Alias, replicaId, nodeList, !c.Noop, f, c, logger)
		rpc.Register(rep)
	case "n2paxos":
		log.Println("Starting N²Paxos replica...")
		rep := n2paxos.New(c.Alias, replicaId, nodeList, !c.Noop, 1, f, c, logger)
		rpc.Register(rep)
	case "paxos":
		log.Println("Starting Paxos replica...")
		rep := paxos.New(c.Alias, replicaId, nodeList, isLeader, f, c, logger)
		rpc.Register(rep)
	case "epaxos":
		log.Println("Starting EPaxos replica...")
		rep := epaxos.New(c.Alias, replicaId, nodeList, !c.Noop, false, false, 5, false, f, c, logger)
		rpc.Register(rep)
	}

	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port+1000))
	if err != nil {
		log.Fatal("listen error:", err)
	}
	http.Serve(l, nil)
}

func replicaEndpoint(endpoint string, defaultPort int) (string, int) {
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil {
		return endpoint, defaultPort
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return endpoint, defaultPort
	}
	return host, port
}

func registerWithMaster(alias, addr, mAddr string, port int) (int, []string, bool) {
	var reply defs.RegisterReply
	args := &defs.RegisterArgs{
		Alias: alias,
		Addr:  addr,
		Port:  port,
	}
	log.Printf("connecting to: %v", mAddr)

	for {
		mcli, err := rpc.DialHTTP("tcp", mAddr)
		if err == nil {
			for {
				// TODO: This is an active wait...
				err = mcli.Call("Master.Register", args, &reply)
				if err == nil {
					if reply.Ready {
						break
					}
					time.Sleep(4)
				} else {
					log.Printf("%v", err)
				}
			}
			break
		} else {
			log.Printf("%v", err)
		}
		time.Sleep(4)
	}

	return reply.ReplicaId, reply.NodeList, reply.IsLeader
}
