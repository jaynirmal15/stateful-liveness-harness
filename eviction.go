package main

import "time"

// Guarded self-eviction is deliberately NOT a probe. It reads the one signal
// that genuinely says "my sessions are broken" -- the signal it would be
// reckless to wire to a restart -- and turns it into a drain, under four
// guards.
const (
	EvalPeriod     = 10 * time.Second
	EvalWindow     = 60 * time.Second
	PeerWindow     = 30 * time.Second // what peers are asked about
	MinOutcomes    = 10               // over the window: an idle pod is not a broken pod
	MinPeerOutcome = 3                // over the peer window
	FailureRatio   = 0.8              // sustained, not instantaneous
	FleetWideLimit = 0.34             // if a third of the fleet agrees, it is not local
	DrainGrace     = 30 * time.Second
)

type outcome struct{ ok, bad int }

type EvictionController struct {
	svc              *Service
	window           []outcome
	lastOK, lastFail int
	nextAt           time.Time
	fired            bool
}

func NewEvictionController(s *Service) *EvictionController {
	return &EvictionController{svc: s}
}

func (e *EvictionController) tail(n int) (ok, bad int) {
	if n > len(e.window) {
		n = len(e.window)
	}
	for _, o := range e.window[len(e.window)-n:] {
		ok, bad = ok+o.ok, bad+o.bad
	}
	return ok, bad
}

// Distressed is this pod's own verdict, and it is deliberately conservative:
// guard 1 is the full window (sustained, not one bad scrape) and guard 2 is a
// minimum number of outcomes over that window (an idle pod cannot be broken).
func (e *EvictionController) Distressed() bool {
	n := int(EvalWindow / EvalPeriod)
	if len(e.window) < n {
		return false
	}
	ok, bad := e.tail(n)
	return ok+bad >= MinOutcomes && float64(bad)/float64(ok+bad) >= FailureRatio
}

// PeerSignal is what this pod publishes for guard 3: a short-window
// observation, not a verdict. It has to be responsive and low-bar, because its
// job is to stop other pods evicting during a fleet-wide failure -- and it
// must not depend on this pod having filled its own 60s window, or the first
// pod to fill one evicts itself out of an outage everyone is in.
func (e *EvictionController) PeerSignal() bool {
	ok, bad := e.tail(int(PeerWindow / EvalPeriod))
	return ok+bad >= MinPeerOutcome && float64(bad)/float64(ok+bad) >= FailureRatio
}

func (e *EvictionController) observe() {
	ok := e.svc.OutcomeOK - e.lastOK
	bad := e.svc.OutcomeFail - e.lastFail
	e.lastOK, e.lastFail = e.svc.OutcomeOK, e.svc.OutcomeFail
	e.window = append(e.window, outcome{ok, bad})
}

// Step advances the evaluation and returns true when this pod wants to drain.
// Guards 3 and 4 -- the fleet-wide check and the disruption budget -- are
// applied by the caller, because neither is a decision one pod is entitled to
// make alone.
func (e *EvictionController) Step(now time.Time) bool {
	if !e.svc.Running || e.svc.Draining || e.fired || now.Before(e.nextAt) {
		return false
	}
	e.nextAt = now.Add(EvalPeriod)
	e.observe()
	return e.Distressed()
}

// StepUnguarded is the version with none of the four guards, kept only to
// measure what the guards are worth: "most of my sessions are failing, so I am
// leaving."
func (e *EvictionController) StepUnguarded(now time.Time) bool {
	if !e.svc.Running || e.svc.Draining || e.fired || now.Before(e.nextAt) {
		return false
	}
	e.nextAt = now.Add(EvalPeriod)
	e.observe()
	o := e.window[len(e.window)-1]
	return o.ok+o.bad > 0 && float64(o.bad)/float64(o.ok+o.bad) >= FailureRatio
}

func (e *EvictionController) Reset() {
	e.window, e.fired, e.nextAt = nil, false, time.Time{}
	e.lastOK, e.lastFail = e.svc.OutcomeOK, e.svc.OutcomeFail
}
