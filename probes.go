package main

import "time"

type ProbeResult struct {
	OK      bool
	Latency time.Duration
	Reason  string
}

type Probe func(s *Service, now time.Time) ProbeResult

// NaiveHealth is the handler almost everyone ships: the HTTP server is up, so
// return 200. It proves the listener is accepting and the handler returned.
func NaiveHealth(s *Service, now time.Time) ProbeResult {
	return ProbeResult{OK: true, Latency: time.Millisecond, Reason: "200"}
}

// DeepHealth is the obvious fix: fail when every session is broken. It reads
// the maintenance loop's accounting, which is the flaw -- and it is externally
// caused, which is the bigger one.
func DeepHealth(s *Service, now time.Time) ProbeResult {
	total := s.statEstablished + s.statFailed
	if total > 0 && float64(s.statEstablished)/float64(total) < DeepThreshold {
		return ProbeResult{OK: false, Latency: 2 * time.Millisecond,
			Reason: "majority of sessions unhealthy"}
	}
	return ProbeResult{OK: true, Latency: 2 * time.Millisecond, Reason: "200"}
}

var DeepThreshold = 0.5

// HeartbeatLive is the design: every maintenance loop has ticked recently.
func HeartbeatLive(s *Service, now time.Time) ProbeResult {
	ok, why := s.reg.Live(now)
	return ProbeResult{OK: ok, Latency: time.Millisecond, Reason: why}
}

// SharedLock wraps a probe so that it waits behind the session-manager lock,
// which is what happens when the handler touches the same mutex the work path
// holds. The check itself is correct; the probe still fails.
// The probe arrives at an arbitrary point in the sweep cycle, so its wait is
// (hold - elapsed since the sweep began). Phase comes from a low-discrepancy
// sequence: deterministic across runs, but not accidentally locked to the
// loop's period, which is what would happen if both were exact multiples.
func SharedLock(p Probe) Probe {
	return func(s *Service, now time.Time) ProbeResult {
		r := p(s, now)
		s.probeSeq++
		phase := golden(s.probeSeq)
		hold := time.Duration(len(s.sessions)) * s.lockPerSession
		if wait := hold - time.Duration(phase*float64(MaintenancePeriod)); wait > 0 {
			r.Latency += wait
			r.Reason = "waited on session-manager lock"
		}
		return r
	}
}

func golden(n int) float64 {
	x := float64(n) * 0.6180339887498949
	return x - float64(int(x))
}
