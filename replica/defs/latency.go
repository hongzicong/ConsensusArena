package defs

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

var LatencyConf = ""
var LocalAddr = ""

type LatencyTable struct {
	ti      map[int]time.Duration
	tn      map[string]time.Duration
	d       time.Duration
	uniform bool
}

type latencyLink struct {
	from string
	to   string
}

func NewLatencyTable(conf, myAddr string, addrs []string) (*LatencyTable, error) {
	if conf == "" {
		return nil, nil
	}
	f, err := os.Open(conf)
	if err != nil {
		return nil, fmt.Errorf("open latency configuration %q: %w", conf, err)
	}
	defer f.Close()

	links := make(map[latencyLink]time.Duration)
	endpoints := make(map[string]struct{})
	var uniform *time.Duration
	s := bufio.NewScanner(f)
	line := 0
	for s.Scan() {
		line++
		data := strings.Fields(s.Text())
		if len(data) == 0 || strings.HasPrefix(data[0], "#") ||
			strings.HasPrefix(data[0], "//") {
			continue
		}
		if data[0] == "uniform" {
			if len(data) != 2 || uniform != nil || len(links) != 0 {
				return nil, fmt.Errorf("invalid uniform latency at %s:%d", conf, line)
			}
			d, err := time.ParseDuration(data[1])
			if err != nil {
				return nil, fmt.Errorf("invalid latency at %s:%d: %w", conf, line, err)
			}
			if d < 0 {
				return nil, fmt.Errorf("negative latency at %s:%d", conf, line)
			}
			uniform = &d
			continue
		}
		if len(data) != 3 || uniform != nil {
			return nil, fmt.Errorf("invalid latency entry at %s:%d", conf, line)
		}
		d, err := time.ParseDuration(data[2])
		if err != nil {
			return nil, fmt.Errorf("invalid latency at %s:%d: %w", conf, line, err)
		}
		if d < 0 {
			return nil, fmt.Errorf("negative latency at %s:%d", conf, line)
		}
		link := latencyLink{from: data[0], to: data[1]}
		if _, exists := links[link]; exists {
			return nil, fmt.Errorf("duplicate latency entry %q -> %q at %s:%d",
				link.from, link.to, conf, line)
		}
		links[link] = d
		endpoints[link.from] = struct{}{}
		endpoints[link.to] = struct{}{}
	}
	if err := s.Err(); err != nil {
		return nil, fmt.Errorf("read latency configuration %q: %w", conf, err)
	}

	if uniform != nil {
		return &LatencyTable{
			d:       *uniform / 2,
			uniform: true,
		}, nil
	}
	if len(links) == 0 {
		return nil, fmt.Errorf("latency configuration %q is empty", conf)
	}
	if _, exists := endpoints[myAddr]; !exists {
		return nil, fmt.Errorf("local endpoint %q is missing from latency configuration %q",
			myAddr, conf)
	}
	for from := range endpoints {
		for to := range endpoints {
			if _, exists := links[latencyLink{from: from, to: to}]; !exists {
				return nil, fmt.Errorf("latency configuration %q is missing %q -> %q",
					conf, from, to)
			}
		}
	}

	dt := &LatencyTable{
		ti: make(map[int]time.Duration, len(addrs)),
		tn: make(map[string]time.Duration, len(endpoints)),
	}
	for endpoint := range endpoints {
		dt.tn[endpoint] = links[latencyLink{from: myAddr, to: endpoint}] / 2
	}
	for id, addr := range addrs {
		delay, exists := dt.tn[addr]
		if !exists {
			return nil, fmt.Errorf("peer endpoint %q is missing from latency configuration %q",
				addr, conf)
		}
		dt.ti[id] = delay
	}

	return dt, nil
}

func (dt *LatencyTable) WaitDuration(addr string) time.Duration {
	if dt == nil {
		return 0
	}
	if dt.uniform {
		return dt.d
	}
	d, exists := dt.tn[addr]
	if !exists {
		panic(fmt.Sprintf("latency endpoint %q was not validated", addr))
	}
	return d
}

func (dt *LatencyTable) WaitDurationID(id int) time.Duration {
	if dt == nil {
		return 0
	}
	if dt.uniform {
		return dt.d
	}
	d, exists := dt.ti[id]
	if !exists {
		panic(fmt.Sprintf("latency peer id %d was not validated", id))
	}
	return d
}

func IP() string {
	if LocalAddr != "" {
		return LocalAddr
	}

	addrs, err := net.InterfaceAddrs()
	if err == nil {
		for _, addr := range addrs {
			ip, _, err := net.ParseCIDR(addr.String())
			if err == nil && ip.To4() != nil && !ip.IsLoopback() {
				return ip.String()
			}
		}
	}
	return "127.0.0.1"
}

const maxClientIdentityLength = 1024

var clientIdentityMagic = [4]byte{'C', 'A', 'I', '1'}

func WriteClientIdentity(w io.Writer, endpoint string) error {
	if endpoint == "" || len(endpoint) > maxClientIdentityLength {
		return fmt.Errorf("invalid client endpoint identity length %d", len(endpoint))
	}
	var header [6]byte
	copy(header[:4], clientIdentityMagic[:])
	binary.BigEndian.PutUint16(header[4:], uint16(len(endpoint)))
	if _, err := w.Write(header[:]); err != nil {
		return fmt.Errorf("write client identity header: %w", err)
	}
	if _, err := io.WriteString(w, endpoint); err != nil {
		return fmt.Errorf("write client identity: %w", err)
	}
	return nil
}

func ReadClientIdentity(r io.Reader) (string, error) {
	var header [6]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return "", fmt.Errorf("read client identity header: %w", err)
	}
	if string(header[:4]) != string(clientIdentityMagic[:]) {
		return "", fmt.Errorf("invalid client identity header")
	}
	length := int(binary.BigEndian.Uint16(header[4:]))
	if length == 0 || length > maxClientIdentityLength {
		return "", fmt.Errorf("invalid client endpoint identity length %d", length)
	}
	identity := make([]byte, length)
	if _, err := io.ReadFull(r, identity); err != nil {
		return "", fmt.Errorf("read client identity: %w", err)
	}
	return string(identity), nil
}
