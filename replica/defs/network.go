package defs

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
)

var LocalAddr = ""

var dialRoutes = struct {
	sync.RWMutex
	addresses map[string]string
}{addresses: make(map[string]string)}

// ConfigureDialMap loads endpoint rewrites used only for outgoing TCP dials.
// Listeners and addresses advertised through the master remain unchanged, so
// a process can reach peers through local Toxiproxy ports without registering
// those ports as replica identities.
func ConfigureDialMap(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open dial map %q: %w", path, err)
	}
	defer f.Close()

	addresses, err := parseDialMap(f, path)
	if err != nil {
		return err
	}
	dialRoutes.Lock()
	dialRoutes.addresses = addresses
	dialRoutes.Unlock()
	return nil
}

func parseDialMap(r io.Reader, source string) (map[string]string, error) {
	addresses := make(map[string]string)
	scanner := bufio.NewScanner(r)
	line := 0
	for scanner.Scan() {
		line++
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 || strings.HasPrefix(fields[0], "#") ||
			strings.HasPrefix(fields[0], "//") {
			continue
		}
		if len(fields) != 2 {
			return nil, fmt.Errorf("invalid dial-map entry at %s:%d", source, line)
		}
		for _, endpoint := range fields {
			if _, _, err := net.SplitHostPort(endpoint); err != nil {
				return nil, fmt.Errorf("invalid dial-map endpoint %q at %s:%d: %w",
					endpoint, source, line, err)
			}
		}
		if _, exists := addresses[fields[0]]; exists {
			return nil, fmt.Errorf("duplicate dial-map endpoint %q at %s:%d",
				fields[0], source, line)
		}
		addresses[fields[0]] = fields[1]
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read dial map %q: %w", source, err)
	}
	if len(addresses) == 0 {
		return nil, fmt.Errorf("dial map %q is empty", source)
	}
	return addresses, nil
}

func DialAddress(endpoint string) string {
	dialRoutes.RLock()
	mapped, exists := dialRoutes.addresses[endpoint]
	dialRoutes.RUnlock()
	if exists {
		return mapped
	}
	return endpoint
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
