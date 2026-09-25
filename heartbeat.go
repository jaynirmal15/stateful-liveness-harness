package main

import (
	"fmt"
	"sync/atomic"
	"time"
)

// Heartbeat is a liveness token published by exactly one maintenance loop.
// The loop calls Tick at the top of every iteration. The probe handler only
// ever reads. Nothing in this type does work, takes a lock, or touches a
// dependency -- that is the entire design.
type Heartbeat struct {
	Name   string
	Period time.Duration
	// Tolerance is how many periods may be missed before the loop is
	// considered stopped rather than late.
	Tolerance int

	last atomic.Int64 // UnixNano
}

func NewHeartbeat(name string, period time.Duration, tolerance int) *Heartbeat {
	return &Heartbeat{Name: name, Period: period, Tolerance: tolerance}
}

// Tick is called by the loop itself, at the top of every iteration.
func (h *Heartbeat) Tick(now time.Time) { h.last.Store(now.UnixNano()) }

// Deadline is how stale this loop may get before liveness fails.
func (h *Heartbeat) Deadline() time.Duration {
	return time.Duration(h.Tolerance) * h.Period
}

func (h *Heartbeat) Age(now time.Time) time.Duration {
	return now.Sub(time.Unix(0, h.last.Load()))
}

func (h *Heartbeat) Stale(now time.Time) bool { return h.Age(now) > h.Deadline() }

// Registry holds every maintenance loop whose failure the process cannot
// survive. If you cannot enumerate these, you do not have a probe problem.
type Registry struct{ loops []*Heartbeat }

func (r *Registry) Register(h *Heartbeat) *Heartbeat {
	r.loops = append(r.loops, h)
	return h
}

// Live is the whole liveness decision: has every registered loop ticked
// recently enough. It reads timestamps and returns. It cannot be made to fail
// by an unreachable dependency, which is why it is safe to attach to a
// restart.
func (r *Registry) Live(now time.Time) (bool, string) {
	for _, h := range r.loops {
		if h.Stale(now) {
			return false, fmt.Sprintf("loop %q last ticked %v ago (deadline %v)",
				h.Name, h.Age(now).Round(time.Millisecond), h.Deadline())
		}
	}
	return true, "all loops ticking"
}

func (r *Registry) ResetAll(now time.Time) {
	for _, h := range r.loops {
		h.Tick(now)
	}
}
