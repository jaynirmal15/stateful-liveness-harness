package main

import "time"

var Trace bool

const (
	ClientPatience = 30 * time.Second // wait for in-place recovery before moving
	ReconnectDelay = 1 * time.Second
	PodBootSkew    = 300 * time.Millisecond
)

type Client struct {
	ID          int
	pod         *Service
	podGen      int
	sess        *Session
	failedSince time.Time
	retryAt     time.Time
	endsAt      time.Time
	nth         int
}

// lifetime gives every client a session of finite, staggered duration, so the
// fleet has steady churn rather than one static assignment. Churn is what
// turns an undetected bad pod into a black hole: it keeps being handed new
// work.
func (c *Client) lifetime() time.Duration {
	if c.nth == 0 {
		return time.Duration(10+c.ID%120) * time.Second
	}
	return time.Duration(90+(c.ID*7)%61) * time.Second
}

// release detaches the client. The server only reclaims the session if its
// maintenance loop is alive -- reclaiming IS the loop's job, so a dead loop
// leaks the session and the pod slowly fills with zombies.
func (c *Client) release(now time.Time) {
	if c.pod != nil && c.pod.maintainAlive {
		c.pod.remove(c.sess)
	}
	c.sess, c.pod, c.failedSince = nil, nil, time.Time{}
	c.retryAt = now.Add(ReconnectDelay)
}

// Strategy is the one variable under test. Readiness is held constant across
// all strategies -- admission control, never session health -- so the
// comparison isolates the liveness decision.
type Strategy struct {
	Name       string
	Liveness   Probe
	SharedLock bool // probe handler contends on the session-manager lock
	Eviction   bool // guarded self-eviction enabled
}

type Metrics struct {
	Restarts          int
	SessionsDestroyed int
	WorkingLost       int
	Drains            int
	PodSecondsDown    float64
	ClientSecondsLost float64
	DetectAt          time.Duration // first corrective action after the fault
	Detected          bool
	RecoveryLag       time.Duration // after the dependency returns
	Recovered         bool
	PeakPodsDown      int
	FinalWorking      int
	WorkingSeries     []int // clients with a working session, sampled once per second
}

type Fleet struct {
	svcs      []*Service
	live      []*ProbeRunner
	ready     []*ProbeRunner
	evict     []*EvictionController
	clients   []*Client
	depOK     bool
	start     time.Time
	strat     Strategy
	rr        int
	m         Metrics
	activeCli int
	faultAt   time.Duration
	unguarded bool // self-eviction with none of the four guards
}

func NewFleet(pods, capacity, clients int, st Strategy) *Fleet {
	start := time.Unix(0, 0).UTC()
	f := &Fleet{depOK: true, start: start, strat: st}
	for i := 0; i < pods; i++ {
		s := NewService(podName(i), capacity, &f.depOK, start.Add(time.Duration(i)*PodBootSkew))
		lp := st.Liveness
		if st.SharedLock {
			lp = SharedLock(lp)
		}
		lr := NewProbeRunner(LivenessSpec, lp)
		lr.Arm(s.StartedAt)
		rr := NewProbeRunner(ReadinessSpec, func(sv *Service, now time.Time) ProbeResult {
			return ProbeResult{OK: sv.Admit(now), Latency: time.Millisecond}
		})
		rr.Arm(s.StartedAt)
		lr.armedGen, rr.armedGen = s.Gen, s.Gen
		f.svcs = append(f.svcs, s)
		f.live = append(f.live, lr)
		f.ready = append(f.ready, rr)
		f.evict = append(f.evict, NewEvictionController(s))
	}
	for i := 0; i < clients; i++ {
		f.clients = append(f.clients, &Client{ID: i, retryAt: start})
	}
	f.activeCli = clients
	return f
}

func podName(i int) string {
	return string(rune('a'+i/10)) + string(rune('0'+i%10))
}

// working is ground truth, not accounting. A session on a pod whose
// maintenance loop has stopped is not working, however the pod reports it.
func (f *Fleet) working(c *Client) bool {
	if c.sess == nil || c.pod == nil || !c.pod.Running || c.pod.Gen != c.podGen {
		return false
	}
	return c.sess.State == SessionEstablished && c.pod.maintainAlive
}

// distressedFraction is guard 3's input: how much of the fleet is seeing the
// same thing right now. It reads each pod's raw observation, not its verdict.
func (f *Fleet) distressedFraction() float64 {
	n := 0
	for _, e := range f.evict {
		if e.svc.Running && e.PeerSignal() {
			n++
		}
	}
	return float64(n) / float64(len(f.evict))
}

func (f *Fleet) draining() int {
	n := 0
	for _, s := range f.svcs {
		if s.Draining {
			n++
		}
	}
	return n
}

func (f *Fleet) Step(now time.Time) {
	for _, s := range f.svcs {
		s.Step(now)
	}

	down := 0
	for i, s := range f.svcs {
		if !s.Running {
			down++
			continue
		}
		if s.Gen != f.live[i].armedGen {
			f.live[i].Arm(s.StartedAt)
			f.live[i].armedGen = s.Gen
			f.ready[i].Arm(s.StartedAt)
			f.ready[i].armedGen = s.Gen
			f.evict[i].Reset()
		}
		f.ready[i].Step(s, now)
		if f.live[i].Step(s, now) {
			d, w := s.Restart(now)
			f.m.SessionsDestroyed += d
			s.WorkingLost += w
			f.m.Restarts++
			f.note(now)
		}
	}
	if down > f.m.PeakPodsDown {
		f.m.PeakPodsDown = down
	}
	f.m.PodSecondsDown += float64(down) * Tick.Seconds()

	if f.strat.Eviction {
		for i, e := range f.evict {
			if f.unguarded {
				// No window, no denominator, no fleet-wide check, no budget:
				// "my sessions look bad, so I am leaving."
				if !e.StepUnguarded(now) {
					continue
				}
			} else {
				if !e.Step(now) {
					continue
				}
				// Guard 3: if a third of the fleet sees the same thing, the
				// cause is not here and leaving will not help.
				if f.distressedFraction() > FleetWideLimit {
					continue
				}
				// Guard 4: a disruption budget, so the fleet cannot all go.
				if f.draining() >= 1 {
					continue
				}
			}
			if Trace {
				println("DRAIN", f.strat.Name, f.svcs[i].Name, "t=", int(now.Sub(f.start).Seconds()),
					"frac%=", int(f.distressedFraction()*100),
					"est=", f.svcs[i].statEstablished, "fail=", f.svcs[i].statFailed)
			}
			e.fired = true
			f.svcs[i].StartDrain(now, DrainGrace)
			f.m.Drains++
			f.note(now)
		}
	}

	f.stepClients(now)
}

func (f *Fleet) note(now time.Time) {
	if !f.m.Detected && f.faultAt > 0 && now.Sub(f.start) >= f.faultAt {
		f.m.Detected = true
		f.m.DetectAt = now.Sub(f.start) - f.faultAt
	}
}

func (f *Fleet) stepClients(now time.Time) {
	for _, c := range f.clients[:f.activeCli] {
		// Session lost with the container, or released by a completed drain.
		if c.sess != nil && (c.pod == nil || c.pod.Gen != c.podGen || !c.pod.Running) {
			c.sess, c.pod, c.failedSince = nil, nil, time.Time{}
			c.retryAt = now.Add(ReconnectDelay)
		}
		// Natural end of a session: the call finishes, the consumer moves on.
		if c.sess != nil && !now.Before(c.endsAt) {
			c.release(now)
		}
		if c.sess != nil && c.sess.State == SessionFailed {
			if c.failedSince.IsZero() {
				c.failedSince = now
			} else if now.Sub(c.failedSince) >= ClientPatience {
				// Give up on in-place recovery and go somewhere else.
				c.release(now)
			}
		} else if c.sess != nil {
			c.failedSince = time.Time{}
		}
		if c.sess == nil && !now.Before(c.retryAt) {
			if pod := f.pick(now); pod != nil {
				c.pod, c.podGen = pod, pod.Gen
				c.sess = pod.Establish(c.ID, now)
				c.endsAt = now.Add(c.lifetime())
				c.nth++
			} else {
				c.retryAt = now.Add(ReconnectDelay)
			}
		}
	}
}

// pick is the load balancer: round-robin over pods that pass readiness.
func (f *Fleet) pick(now time.Time) *Service {
	for i := 0; i < len(f.svcs); i++ {
		f.rr = (f.rr + 1) % len(f.svcs)
		s := f.svcs[f.rr]
		if s.Running && !f.ready[f.rr].Failing && s.Admit(now) {
			return s
		}
	}
	return nil
}
