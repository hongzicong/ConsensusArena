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
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/paxos"
	"github.com/hongzicong/ConsensusArena/replica/defs"
)

func runReplica(c *config.Config, logger *dlog.Logger) {
	port := c.Port

	log.Printf("Server starting on port %d", port)
	maddr := fmt.Sprintf("%s:%d", c.MasterAddr, c.MasterPort)
	addr := c.ReplicaAddrs[c.Alias]
	// Slurm co-locates replicas on a host with distinct listening ports.
	if host, configuredPort, err := net.SplitHostPort(addr); err == nil {
		parsed, err := strconv.Atoi(configuredPort)
		if err != nil || parsed != port {
			log.Fatalf("replica endpoint %q does not match configured port %d", addr, port)
		}
		addr = host
	}
	replicaId, nodeList, isLeader := registerWithMaster(addr, maddr, port)
	f := (len(c.ReplicaAddrs) - 1) / 2
	log.Printf("Tolerating %d max. failures", f)

	switch strings.ToLower(c.Protocol) {
	case "paxos":
		log.Println("Starting Paxos replica...")
		rep := paxos.New(c.Alias, replicaId, nodeList, isLeader, f, c, logger)
		rpc.Register(rep)
	default:
		log.Fatalf("unsupported protocol %q; only Paxos is available", c.Protocol)
	}

	rpc.HandleHTTP()
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port+1000))
	if err != nil {
		log.Fatal("listen error:", err)
	}
	http.Serve(l, nil)
}

func registerWithMaster(addr, mAddr string, port int) (int, []string, bool) {
	var reply defs.RegisterReply
	args := &defs.RegisterArgs{
		Addr: addr,
		Port: port,
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
