package pql

import (
	"fmt"
	"time"
)

func validatePacket(p *Packet) error {
	if p.Kind > packetSuspect || p.From < 0 || p.From >= 63 {
		return fmt.Errorf("PQL: unknown packet kind %d", p.Kind)
	}
	for _, r := range p.Records {
		if r.Slot < 0 {
			return fmt.Errorf("PQL: negative log slot")
		}
		if r.Request.Change != nil {
			change := r.Request.Change
			if !p.Control || change.Leader < 0 || change.Leader >= 63 || len(change.Updates) > configurationBatch {
				return fmt.Errorf("PQL: invalid configuration entry")
			}
		}
	}
	switch p.Kind {
	case packetAccept, packetAck, packetCommit:
		if len(p.Records) == 0 || len(p.Records) > PageSize {
			return fmt.Errorf("PQL: packet kind %d has invalid record count %d", p.Kind, len(p.Records))
		}
	case packetForward:
		if len(p.Requests) == 0 || len(p.Requests) > PageSize {
			return fmt.Errorf("PQL: invalid forwarded request count %d", len(p.Requests))
		}
	case packetForwardReply:
		if len(p.Requests) != 1 {
			return fmt.Errorf("PQL: forwarded reply must identify exactly one request")
		}
	case packetGuard, packetLeasePromise:
		if p.Duration <= 0 || time.Duration(p.Duration) > 30*time.Second {
			return fmt.Errorf("PQL: invalid lease duration %d", p.Duration)
		}
	}
	return nil
}
