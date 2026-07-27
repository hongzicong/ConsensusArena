package defs

import (
	"bufio"
	"net"
	"os"
	"strings"
	"time"
)

var LatencyConf = ""
var LocalAddr = ""

type LatencyTable struct {
	ti map[int]time.Duration
	tn map[string]time.Duration
	d  time.Duration
}

func NewLatencyTable(conf, myAddr string, addrs []string) *LatencyTable {
	if conf == "" {
		return nil
	}
	f, err := os.Open(conf)
	if err != nil {
		return nil
	}
	defer f.Close()

	dt := &LatencyTable{
		d: time.Duration(0),
	}

	s := bufio.NewScanner(f)
	for s.Scan() {
		data := strings.Split(s.Text(), " ")
		if len(data) == 2 {
			if data[0] == "uniform" {
				if d, err := time.ParseDuration(data[1]); err == nil {
					return &LatencyTable{
						d: time.Duration(int64(d) / int64(2)),
					}
				} else {
					return nil
				}
			}
		}
		if len(data) == 3 {
			d, err := time.ParseDuration(data[2])
			if err != nil {
				continue
			}
			d = time.Duration(int64(d) / int64(2))
			addr1, addr2 := data[0], data[1]
			for i := 0; i < 2; i++ {
				if !sameEndpoint(myAddr, addr1) {
					addr1, addr2 = data[1], data[0]
					continue
				}
				if dt.tn == nil {
					dt.tn = make(map[string]time.Duration)
				}
				dt.tn[addr2] = d
				for rid, addr := range addrs {
					if sameEndpoint(addr2, addr) {
						if dt.ti == nil {
							dt.ti = make(map[int]time.Duration)
						}
						dt.ti[rid] = d
					}
				}
			}
		}
	}

	return dt
}

func (dt *LatencyTable) WaitDuration(addr string) time.Duration {
	if dt == nil {
		return time.Duration(0)
	}
	d, exists := dt.tn[addr]
	if exists {
		return d
	}
	for endpoint, delay := range dt.tn {
		if sameEndpoint(endpoint, addr) {
			return delay
		}
	}
	return dt.d
}

func (dt *LatencyTable) WaitDurationID(id int) time.Duration {
	if dt == nil {
		return time.Duration(0)
	}
	d, exists := dt.ti[id]
	if exists {
		return d
	}
	return dt.d
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

func sameEndpoint(left, right string) bool {
	if left == right {
		return true
	}
	leftHost, leftHasPort := endpointHost(left)
	rightHost, rightHasPort := endpointHost(right)
	return (!leftHasPort || !rightHasPort) && leftHost == rightHost
}

func endpointHost(endpoint string) (string, bool) {
	host, _, err := net.SplitHostPort(endpoint)
	if err == nil {
		return host, true
	}
	return endpoint, false
}
