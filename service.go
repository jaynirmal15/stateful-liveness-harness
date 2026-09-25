package main

import "time"

// AffectedPct is the share of sessions a fleet-wide dependency failure takes.
// A real outage is rarely total, and the survivors are what a mass restart
// destroys.
var AffectedPct = 90

// Timings. Everything here is a multiple of the simulation tick.
const (
	Tick = 100 * time.Millisecond

	MaintenancePeriod = 1 * time.Second
	ReaperPeriod      = 5 * time.Second
	LeasePeriod       = 2 * time.Second

	ConnectTime    = 500 * time.Millisecond // connecting -> established
	ConnectTimeout = 5 * time.Second        // connecting -> failed when unreachable
	RecoverDelay   = 2 * time.Second        // failed -> connecting once reachable again

	StartupTime     = 5 * time.Second
	RestartBackoff0 = 10 * time.Second
	RestartBackoffM = 300 * time.Second
)

// Service is one pod: a process holding sessions, kept true by a handful of
// maintenance loops.
type Service struct {
	Name string

	reg      Registry
	maintain *Heartbeat
	reap     *Heartbeat
	lease    *Heartbeat

	sessions []*Session
	nextID   int
	Capacity int

	// Fault switches. maintainAlive models the failure this article is about:
	// a maintenance goroutine dies while the HTTP server keeps answering.
	maintainAlive bool
	reapAlive     bool
	leaseAlive    bool

	// mediaPathOK is this pod's own egress path. When false, sessions on this
	// pod fail while every other pod is fine -- a gray failure.
	mediaPathOK bool

	// depOK is the fleet-wide dependency: the relay, the bus, the coordinator.
	depOK *bool

	// Accounting, updated ONLY by the maintenance loop. A deep health check
	// reads these fields, so when the loop stops, the deep check freezes on
	// the last good answer.
	statEstablished int
	statFailed      int
	statAt          time.Time

	// Cumulative session outcomes: transitions into established, and into
	// failed. Monotonic counters, exactly what /metrics would carry.
	OutcomeOK   int
	OutcomeFail int

	// lockPerSession models the session-manager lock being held across a full
	// sweep. A probe handler that takes the same lock waits behind it.
	lockPerSession time.Duration
	lockHeldUntil  time.Time
	probeSeq       int

	Draining      bool
	drainDeadline time.Time

	Running     bool
	Restarts    int
	Drained     int
	WorkingLost int       // sessions that were established and got taken away
	Gen         int       // bumped whenever the held sessions are discarded
	restartAt   time.Time // when the container comes back
	StartedAt   time.Time // when the current container started

	// next scheduled run for each loop
	nextMaintain, nextReap, nextLease time.Time
}

func NewService(name string, capacity int, depOK *bool, start time.Time) *Service {
	s := &Service{
		Name:        name,
		Capacity:    capacity,
		depOK:       depOK,
		mediaPathOK: true,
	}
	s.maintain = s.reg.Register(NewHeartbeat("session-maintenance", MaintenancePeriod, 5))
	s.reap = s.reg.Register(NewHeartbeat("session-reaper", ReaperPeriod, 3))
	s.lease = s.reg.Register(NewHeartbeat("lease-renewal", LeasePeriod, 3))
	s.boot(start)
	return s
}

// boot brings a fresh container up. Sessions do not survive it.
func (s *Service) boot(now time.Time) {
	s.sessions = nil
	s.Gen++
	s.maintainAlive, s.reapAlive, s.leaseAlive = true, true, true
	s.Draining = false
	s.Running = true
	s.StartedAt = now
	s.statEstablished, s.statFailed, s.statAt = 0, 0, now
	s.reg.ResetAll(now)
	s.nextMaintain, s.nextReap, s.nextLease = now, now, now
	s.lockHeldUntil = now
}

// Restart is what a failed liveness probe buys you. Note the cost: every
// session on the pod, including the ones that were working.
func (s *Service) Restart(now time.Time) (destroyed, working int) {
	destroyed = len(s.sessions)
	for _, sess := range s.sessions {
		if sess.State == SessionEstablished && s.maintainAlive {
			working++ // sessions that were fine, killed to fix the ones that weren't
		}
	}
	s.sessions = nil
	s.Running = false
	s.Restarts++
	backoff := RestartBackoff0 << uint(min(s.Restarts-1, 5))
	if backoff > RestartBackoffM {
		backoff = RestartBackoffM
	}
	s.restartAt = now.Add(StartupTime + backoff)
	return destroyed, working
}

func (s *Service) Step(now time.Time) {
	if !s.Running {
		if !now.Before(s.restartAt) {
			s.boot(now)
		}
		return
	}
	if s.Draining && !now.Before(s.drainDeadline) {
		// Drain complete. The sessions had the whole grace period to recover
		// in place; the ones that did not are released so their clients can go
		// somewhere useful, and the pod is replaced. Nothing was killed
		// mid-flight, and only this pod was ever affected.
		s.Drained++
		// Count fairly: sessions still working when the grace period expires
		// are released too. The drain gave them a chance to recover in place;
		// a restart gives them nothing.
		for _, sess := range s.sessions {
			if sess.State == SessionEstablished && s.maintainAlive {
				s.WorkingLost++
			}
		}
		s.sessions = nil
		s.Gen++
		s.Running = false
		s.restartAt = now.Add(StartupTime)
		// A drained pod is deleted, and the replacement is scheduled fresh --
		// possibly onto a different node. That is the difference between
		// self-eviction and exit(1): a liveness restart puts the same
		// container back on the same node, with the same broken path.
		s.mediaPathOK = true
		return
	}
	if !now.Before(s.nextMaintain) {
		s.nextMaintain = now.Add(MaintenancePeriod)
		s.stepMaintenance(now)
	}
	if !now.Before(s.nextReap) {
		s.nextReap = now.Add(ReaperPeriod)
		if s.reapAlive {
			s.reap.Tick(now)
		}
	}
	if !now.Before(s.nextLease) {
		s.nextLease = now.Add(LeasePeriod)
		if s.leaseAlive {
			s.lease.Tick(now)
		}
	}
}

func (s *Service) stepMaintenance(now time.Time) {
	if !s.maintainAlive {
		return // loop is dead: sessions stop advancing, stats freeze, no tick
	}
	est, fail := 0, 0
	for _, sess := range s.sessions {
		reachable := (*s.depOK || !sess.Affected) && s.mediaPathOK
		before := sess.State
		switch sess.State {
		case SessionConnecting:
			if reachable {
				if now.Sub(sess.ChangedAt) >= ConnectTime {
					sess.set(SessionEstablished, now)
				}
			} else if now.Sub(sess.ChangedAt) >= ConnectTimeout {
				sess.set(SessionFailed, now)
			}
		case SessionEstablished:
			if !reachable {
				sess.set(SessionFailed, now)
			}
		case SessionFailed:
			if reachable && now.Sub(sess.ChangedAt) >= RecoverDelay {
				sess.set(SessionConnecting, now)
			}
		}
		// Outcome counters -- the ones you already export. An instantaneous
		// ratio of session states is not a usable signal under churn: a pod
		// mid-reconnect holds mostly connecting sessions and reads as fine.
		if before != sess.State {
			switch sess.State {
			case SessionEstablished:
				s.OutcomeOK++
			case SessionFailed:
				s.OutcomeFail++
			}
		}
		switch sess.State {
		case SessionEstablished:
			est++
		case SessionFailed:
			fail++
		}
	}
	s.statEstablished, s.statFailed, s.statAt = est, fail, now
	// The sweep holds the session-manager lock for its duration.
	s.lockHeldUntil = now.Add(time.Duration(len(s.sessions)) * s.lockPerSession)
	s.maintain.Tick(now)
}

// Admit is the readiness question: should this pod be given a NEW session.
// Capacity, drain state, and the dependency needed to *establish* -- never the
// health of sessions already held.
func (s *Service) Admit(now time.Time) bool {
	return s.Running && !s.Draining && len(s.sessions) < s.Capacity
}

func (s *Service) Establish(clientID int, now time.Time) *Session {
	s.nextID++
	sess := &Session{ID: s.nextID, ClientID: clientID, State: SessionConnecting,
		ChangedAt: now, Affected: (clientID*7919)%100 < AffectedPct}
	s.sessions = append(s.sessions, sess)
	return sess
}

func (s *Service) remove(target *Session) {
	for i, sess := range s.sessions {
		if sess == target {
			s.sessions = append(s.sessions[:i], s.sessions[i+1:]...)
			return
		}
	}
}

func (s *Service) StartDrain(now time.Time, grace time.Duration) {
	s.Draining = true
	s.drainDeadline = now.Add(grace)
}

// liveEstablished is ground truth, used only by the measurement harness --
// never by a probe.
func (s *Service) liveEstablished() int {
	n := 0
	for _, sess := range s.sessions {
		if sess.State == SessionEstablished {
			n++
		}
	}
	return n
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
