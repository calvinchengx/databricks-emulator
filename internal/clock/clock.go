// Package clock is the emulator's controllable time source. Token expiry,
// job timestamps and seed times all flow through it so tests do not sleep.
package clock

import (
	"sync"
	"time"
)

// Clock is a concurrency-safe, offsettable, freezable wall clock.
type Clock struct {
	mu       sync.RWMutex
	offset   int64
	frozen   bool
	frozenAt int64
	realNow  func() int64
}

// New returns a clock tracking real time.
func New() *Clock {
	return &Clock{realNow: func() int64 { return time.Now().Unix() }}
}

// Now returns the current controlled time (epoch seconds).
func (c *Clock) Now() int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.frozen {
		return c.frozenAt
	}
	return c.realNow() + c.offset
}

// Advance moves controlled time by delta seconds.
func (c *Clock) Advance(seconds int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen {
		c.frozenAt += seconds
		return
	}
	c.offset += seconds
}

// Freeze pins time at the current controlled value.
func (c *Clock) Freeze() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.frozen {
		return
	}
	c.frozenAt = c.realNow() + c.offset
	c.frozen = true
}

// Unfreeze resumes real-time tracking from the frozen point.
func (c *Clock) Unfreeze() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.frozen {
		return
	}
	c.offset = c.frozenAt - c.realNow()
	c.frozen = false
}
