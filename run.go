package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"time"

	"github.com/hongzicong/ConsensusArena/config"
	"github.com/hongzicong/ConsensusArena/dlog"
	"github.com/hongzicong/ConsensusArena/replica/defs"
)

func runReplica(conf *config.Config, logger *dlog.Logger) {
	maddr := fmt.Sprintf("%s:%d", conf.MasterAddr, conf.MasterPort)
	addr, port := replicaEndpoint(conf.ReplicaAddrs[conf.Alias], conf.Port)
	log.Printf("Server starting on %s:%d", addr, port)
	replicaID, nodeList, isLeader := registerWithMaster(conf.Alias, addr, maddr, port)
	f := (len(conf.ReplicaAddrs) - 1) / 2
	log.Printf("Tolerating %d max. failures", f)

	if p, ok := lookupProtocol(conf.Protocol); ok {
		rep := p.newReplica(replicaStart{config: conf, logger: logger, id: replicaID, addrs: nodeList, leader: isLeader, failures: f})
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
