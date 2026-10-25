# ConsensusArena: Paxos baseline

This checkout retains the classic Paxos baseline and shared benchmark infrastructure.
The Go module path matches the ConsensusArena repository.

Installation
------------

From this checkout:

    go build -o results/paxos .

On Windows, use `go build -o results/paxos.exe .`.

Implemented protocols
---------------------

| Protocol | Comments |
| --- | --- |
| Paxos | The classic Paxos protocol; the only available baseline. |

Usage
-----
#### participants
There are three types of participants: *master*, *servers* and *clients*. 
The servers and clients implement the protocol logic. 
The master maintains the configuration of the system.

#### deployment configuration
To setup a run, the participants read deployment configuration file. 
See [aws.conf][config] for an example of configuration file for AWS EC2.

#### launching a participant

Master:
    
    results/paxos -run master -config conf.conf

Server:

    results/paxos -run server -config conf.conf -alias server_name

Client:

    results/paxos -run client -config conf.conf -alias client_name

#### command line options

    -alias alias
        An alias of this participant
    -config file
        Deployment config file (required)
    -latency file
        Latency config file
    -log file
        Path to the log file
    -protocol protocol
        Protocol to run. Overwrites protocol field of the config file
    -quorum file
        Quorum config file
    -run participant
        Run a participant

See [quorum.conf][quorum] and [latency.conf][latency] for an example of quorum and latency configuration files.

Flint
-----

To have an idea on how different replication protocols would compare, we wrote a tool named [Flint][flint]. 
Flint takes as input a set of AWS regions.
It computes the expected latencies and estimates how the protocols perform in such a deployment.

[config]: aws.conf
[quorum]: quorum.conf
[latency]: latency.conf
[flint]: https://github.com/vonaka/flint

