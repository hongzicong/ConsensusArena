package defs

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
)

// Initialized once before participants start; advertised identities stay unchanged.
var benchmarkDialMap = map[string]string{}

func ConfigureDialMap(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	addresses := map[string]string{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		if len(fields) != 2 {
			return fmt.Errorf("invalid dial-map entry: %q", scanner.Text())
		}
		for _, endpoint := range fields {
			if _, _, err := net.SplitHostPort(endpoint); err != nil {
				return fmt.Errorf("invalid endpoint %q: %w", endpoint, err)
			}
		}
		if _, exists := addresses[fields[0]]; exists {
			return fmt.Errorf("duplicate endpoint %q", fields[0])
		}
		addresses[fields[0]] = fields[1]
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(addresses) == 0 {
		return fmt.Errorf("empty dial map")
	}
	benchmarkDialMap = addresses
	return nil
}

func DialAddress(endpoint string) string {
	if address, ok := benchmarkDialMap[endpoint]; ok {
		return address
	}
	return endpoint
}
