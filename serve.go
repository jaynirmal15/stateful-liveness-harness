package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Serve runs the same design as a real process, so the simulation's
// conclusions can be checked on an actual cluster. The probe listener is a
// separate server on a separate port with its own goroutines: that separation
// is the point, not an implementation detail.
type Server struct {
	reg      Registry
	maintain *Heartbeat
	reap     *Heartbeat
	lease    *Heartbeat

	// mu is the session-manager lock. The maintenance sweep holds it. So does
	// /livez-shared, which is why /livez-shared is the wrong design.
	mu       sync.Mutex
	sessions []*Session
	nextID   int
	capacity int

	established atomic.Int64
	failed      atomic.Int64
	outcomeOK   atomic.Int64
	outcomeBad  atomic.Int64
	held        atomic.Int64

	// Fault switches, flipped over HTTP so the failures can be injected
	// against a running pod.
	loopAlive atomic.Bool
	pathOK    atomic.Bool
	draining  atomic.Bool

	sweepCost time.Duration
}

func Serve() {
	s := &Server{capacity: envInt("CAPACITY", 60),
		sweepCost: time.Duration(envInt("SWEEP_COST_US", 0)) * time.Microsecond}
	s.maintain = s.reg.Register(NewHeartbeat("session-maintenance", MaintenancePeriod, 5))
	s.reap = s.reg.Register(NewHeartbeat("session-reaper", ReaperPeriod, 3))
	s.lease = s.reg.Register(NewHeartbeat("lease-renewal", LeasePeriod, 3))
	s.reg.ResetAll(time.Now())
	s.loopAlive.Store(true)
	s.pathOK.Store(true)

	go s.maintenanceLoop()
	go s.tickLoop(s.reap, ReaperPeriod)
	go s.tickLoop(s.lease, LeasePeriod)

	// The probe listener: its own server, its own goroutines, no shared locks.
	probes := http.NewServeMux()
	probes.HandleFunc("/healthz", s.naive)
	probes.HandleFunc("/livez", s.livez)
	probes.HandleFunc("/livez-shared", s.livezShared)
	probes.HandleFunc("/deepz", s.deepz)
	probes.HandleFunc("/readyz", s.readyz)
	probes.HandleFunc("/metrics", s.metrics)

	app := http.NewServeMux()
	app.HandleFunc("/session", s.session)
	app.HandleFunc("/debug/kill-loop", func(w http.ResponseWriter, r *http.Request) {
		s.loopAlive.Store(false)
		fmt.Fprintln(w, "maintenance loop stopped; the HTTP server is still here")
	})
	app.HandleFunc("/debug/break-path", func(w http.ResponseWriter, r *http.Request) {
		s.pathOK.Store(false)
		fmt.Fprintln(w, "egress path broken; sessions will fail, loops keep ticking")
	})
	app.HandleFunc("/debug/drain", func(w http.ResponseWriter, r *http.Request) {
		s.draining.Store(true)
		fmt.Fprintln(w, "draining: readiness false, existing sessions untouched")
	})

	go func() { log.Fatal(http.ListenAndServe(":8081", probes)) }()
	log.Printf("probes on :8081, app on :8080, capacity %d, sweep cost %v/session",
		s.capacity, s.sweepCost)
	log.Fatal(http.ListenAndServe(":8080", app))
}

func (s *Server) maintenanceLoop() {
	t := time.NewTicker(MaintenancePeriod)
	defer t.Stop()
	for range t.C {
		if !s.loopAlive.Load() {
			return // the goroutine is gone; nothing below it runs again
		}
		now := time.Now()
		s.mu.Lock()
		est, fail := 0, 0
		for _, sess := range s.sessions {
			before := sess.State
			if !s.pathOK.Load() {
				sess.set(SessionFailed, now)
			} else if sess.State != SessionEstablished {
				sess.set(SessionEstablished, now)
			}
			if before != sess.State {
				if sess.State == SessionEstablished {
					s.outcomeOK.Add(1)
				} else {
					s.outcomeBad.Add(1)
				}
			}
			if sess.State == SessionEstablished {
				est++
			} else {
				fail++
			}
			if s.sweepCost > 0 {
				time.Sleep(s.sweepCost) // hold the lock, as a real sweep would
			}
		}
		s.established.Store(int64(est))
		s.failed.Store(int64(fail))
		s.held.Store(int64(len(s.sessions)))
		s.mu.Unlock()
		s.maintain.Tick(now) // recorded by the loop, read by the probe
	}
}

func (s *Server) tickLoop(h *Heartbeat, period time.Duration) {
	t := time.NewTicker(period)
	defer t.Stop()
	for range t.C {
		h.Tick(time.Now())
	}
}

func (s *Server) naive(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "ok") // proves the listener accepted and the handler returned
}

// livez reads heartbeat timestamps and nothing else. No lock, no dependency,
// no work.
func (s *Server) livez(w http.ResponseWriter, r *http.Request) {
	if ok, why := s.reg.Live(time.Now()); !ok {
		http.Error(w, why, http.StatusServiceUnavailable)
		return
	}
	fmt.Fprintln(w, "ok")
}

// livezShared is the same check behind the session-manager lock: correct
// logic, wrong plumbing.
func (s *Server) livezShared(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.livez(w, r)
}

func (s *Server) deepz(w http.ResponseWriter, r *http.Request) {
	est, fail := s.established.Load(), s.failed.Load()
	if est+fail > 0 && float64(est)/float64(est+fail) < DeepThreshold {
		http.Error(w, "majority of sessions unhealthy", http.StatusServiceUnavailable)
		return
	}
	fmt.Fprintln(w, "ok")
}

// readyz is admission control: capacity and drain state. Not session health.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		http.Error(w, "draining", http.StatusServiceUnavailable)
		return
	}
	if s.held.Load() >= int64(s.capacity) {
		http.Error(w, "at capacity", http.StatusServiceUnavailable)
		return
	}
	fmt.Fprintln(w, "ok")
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	fmt.Fprintf(w, "sessions_held %d\n", s.held.Load())
	fmt.Fprintf(w, "sessions_established %d\n", s.established.Load())
	fmt.Fprintf(w, "sessions_failed %d\n", s.failed.Load())
	fmt.Fprintf(w, "session_outcomes_total{result=\"ok\"} %d\n", s.outcomeOK.Load())
	fmt.Fprintf(w, "session_outcomes_total{result=\"failed\"} %d\n", s.outcomeBad.Load())
	for _, h := range s.reg.loops {
		fmt.Fprintf(w, "maintenance_loop_age_seconds{loop=%q} %.3f\n", h.Name, h.Age(now).Seconds())
		fmt.Fprintf(w, "maintenance_loop_deadline_seconds{loop=%q} %.3f\n", h.Name, h.Deadline().Seconds())
	}
}

func (s *Server) session(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sessions) >= s.capacity {
		http.Error(w, "at capacity", http.StatusServiceUnavailable)
		return
	}
	s.nextID++
	s.sessions = append(s.sessions, &Session{ID: s.nextID, State: SessionConnecting,
		ChangedAt: time.Now()})
	fmt.Fprintf(w, "session %d\n", s.nextID)
}

func envInt(k string, def int) int {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n := 0
	for _, c := range v {
		if c < '0' || c > '9' {
			return def
		}
		n = n*10 + int(c-'0')
	}
	return n
}
