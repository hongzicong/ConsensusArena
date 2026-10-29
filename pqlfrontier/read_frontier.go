package pqlfrontier

import "time"

// A finite prefix observed inside one valid authority interval. It carries no
// speculative value; completion always reads the actually executed state.
type readFrontier struct {
	slot, ballot, epoch int64
	until               time.Time
}

func (c *core) invalidateReadFrontier(r Request) {
	if _, ok := c.readFrontiers[r.ID]; ok {
		delete(c.readFrontiers, r.ID)
		c.frontierInvalidations++
	}
}

// localReadReady runs in the same serialized loop as Accept/Commit/application.
// The lease supplies visibility; execution through the captured prefix supplies
// a stable value. New writes must not extend an older read's valid frontier.
func (c *core) localReadReady(r Request, now time.Time) bool {
	if !c.active || c.preparing || !c.lease.canRead(now, c.executed) {
		c.invalidateReadFrontier(r)
		return false
	}
	f, ok := c.readFrontiers[r.ID]
	if ok && (f.ballot != c.ballot || f.epoch != c.lease.promisedTo || !now.Before(f.until)) {
		c.invalidateReadFrontier(r)
		ok = false
	}
	if !ok {
		if c.updating[r.Command.K] == 0 {
			return true
		}
		f = readFrontier{slot: c.high, ballot: c.ballot, epoch: c.lease.promisedTo, until: c.lease.readUntil}
		c.readFrontiers[r.ID] = f
		c.frontierCaptures++
	}
	if c.executed < f.slot {
		return false
	}
	delete(c.readFrontiers, r.ID)
	c.frontierReleases++
	if c.updating[r.Command.K] > 0 {
		c.frontierConcurrentReleases++
	}
	return true
}
