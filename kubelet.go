package main

import "time"

// ProbeSpec mirrors the kubelet's documented probe fields.
type ProbeSpec struct {
	InitialDelay     time.Duration
	Period           time.Duration
	Timeout          time.Duration
	FailureThreshold int
	SuccessThreshold int
}

var (
	LivenessSpec = ProbeSpec{
		InitialDelay:     10 * time.Second,
		Period:           5 * time.Second,
		Timeout:          1 * time.Second,
		FailureThreshold: 3,
		SuccessThreshold: 1,
	}
	ReadinessSpec = ProbeSpec{
		InitialDelay:     2 * time.Second,
		Period:           5 * time.Second,
		Timeout:          1 * time.Second,
		FailureThreshold: 3,
		SuccessThreshold: 1,
	}
)

// ProbeRunner models the kubelet loop for one probe on one container:
// initial delay, fixed period, timeout counted as a failure, consecutive
// failure and success thresholds.
type ProbeRunner struct {
	Spec  ProbeSpec
	Probe Probe

	nextRun      time.Time
	armedAt      time.Time
	armedGen     int
	fails, succs int
	Failing      bool
}

func NewProbeRunner(spec ProbeSpec, p Probe) *ProbeRunner {
	return &ProbeRunner{Spec: spec, Probe: p}
}

// Arm is called whenever a fresh container starts.
func (r *ProbeRunner) Arm(now time.Time) {
	r.armedAt = now
	r.nextRun = now.Add(r.Spec.InitialDelay)
	r.fails, r.succs = 0, 0
	r.Failing = false
}

// Step returns true on the probe run that crosses FailureThreshold.
func (r *ProbeRunner) Step(s *Service, now time.Time) bool {
	if !s.Running || now.Before(r.nextRun) {
		return false
	}
	r.nextRun = now.Add(r.Spec.Period)
	res := r.Probe(s, now)
	ok := res.OK && res.Latency <= r.Spec.Timeout // a timeout counts as a failure
	if ok {
		r.fails = 0
		r.succs++
		if r.succs >= r.Spec.SuccessThreshold {
			r.Failing = false
		}
		return false
	}
	r.succs = 0
	r.fails++
	if r.fails >= r.Spec.FailureThreshold {
		r.Failing = true
		r.fails = 0 // kubelet restarts the container; counters reset with it
		return true
	}
	return false
}
