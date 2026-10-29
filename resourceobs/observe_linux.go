//go:build linux

package resourceobs

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type sample struct {
	UnixNS         int64   `json:"unix_ns"`
	CPUSeconds     float64 `json:"cpu_seconds"`
	RSSBytes       uint64  `json:"rss_bytes"`
	SentFrames     uint64  `json:"sent_frames"`
	SentBytes      uint64  `json:"sent_bytes"`
	ReceivedFrames uint64  `json:"received_frames"`
	ReceivedBytes  uint64  `json:"received_bytes"`
}

func snapshot() (sample, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return sample{}, err
	}
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return sample{}, err
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return sample{}, fmt.Errorf("invalid /proc/self/statm")
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return sample{}, err
	}
	cpu := float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
	return sample{time.Now().UnixNano(), cpu, pages * uint64(os.Getpagesize()),
		sentFrames.Load(), sentBytes.Load(), receivedFrames.Load(), receivedBytes.Load()}, nil
}

// Start is opt-in and only called by replica processes. A complete file is
// published before the harness's 19s deadline, even when replicas are SIGKILLed.
func Start(run, alias string) {
	if run == "" {
		return
	}
	enabled.Store(true)
	go func() {
		if err := observe(run, alias); err != nil {
			_ = os.WriteFile(filepath.Join(run, "results", alias+"-resources.error"), []byte(err.Error()), 0600)
		}
	}()
}

func observe(run, alias string) error {
	var epoch int64
	for epoch == 0 {
		data, err := os.ReadFile(filepath.Join(run, "status", "start-unix-ns"))
		if err == nil {
			epoch, err = strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
			if err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
		if epoch == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	rows := make([]sample, 0, 11)
	for second := 5; second <= 15; second++ {
		target := time.Unix(0, epoch).Add(time.Duration(second) * time.Second)
		time.Sleep(time.Until(target))
		row, err := snapshot()
		if err != nil {
			return err
		}
		rows = append(rows, row)
	}
	data, err := json.Marshal(struct {
		Schema  string   `json:"schema"`
		Alias   string   `json:"alias"`
		EpochNS int64    `json:"epoch_ns"`
		Samples []sample `json:"samples"`
	}{"replica-resources-v1", alias, epoch, rows})
	if err != nil {
		return err
	}
	path := filepath.Join(run, "results", alias+"-resources.json")
	if err := os.WriteFile(path+".tmp", data, 0600); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}
