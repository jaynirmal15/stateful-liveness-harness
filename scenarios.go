package main

import "time"

type Scenario struct {
	Name         string
	Desc         string
	Pods         int
	Capacity     int
	Clients      int
	StartClients int
	Duration     time.Duration
	FaultAt      time.Duration
	RecoverAt    time.Duration
	LockPer      time.Duration
	// ControlInject keeps the injection in the control run. Scenario 4's
	// injection is a load increase, not a fault: the control has to carry it.
	ControlInject bool
	Inject        func(f *Fleet, elapsed time.Duration)
	Strategies    []Strategy
}

var (
	StratNaive     = Strategy{Name: "naive /healthz (200 OK)", Liveness: NaiveHealth}
	StratDeep      = Strategy{Name: "deep check (sessions)", Liveness: DeepHealth}
	StratHeartbeat = Strategy{Name: "heartbeat /livez", Liveness: HeartbeatLive}
	StratHBEvict   = Strategy{Name: "heartbeat + guarded eviction", Liveness: HeartbeatLive, Eviction: true}
	StratDeepEvict = Strategy{Name: "heartbeat + UNguarded eviction", Liveness: HeartbeatLive, Eviction: true}
	StratHBShared  = Strategy{Name: "heartbeat, probe shares the lock", Liveness: HeartbeatLive, SharedLock: true}
	StratHBIso     = Strategy{Name: "heartbeat, isolated probe path", Liveness: HeartbeatLive}
)

func Scenarios() []Scenario {
	return []Scenario{
		{
			Name: "1. Maintenance loop dies, HTTP server lives",
			Desc: "pod a3's session-maintenance goroutine stops at t=120s. The listener keeps accepting.",
			Pods: 10, Capacity: 60, Clients: 400,
			Duration: 600 * time.Second, FaultAt: 120 * time.Second,
			Inject: func(f *Fleet, e time.Duration) {
				if e == 120*time.Second {
					f.svcs[3].maintainAlive = false
				}
			},
			Strategies: []Strategy{StratNaive, StratDeep, StratHeartbeat},
		},
		{
			Name: "2. Fleet-wide dependency failure",
			Desc: "the shared relay is unreachable from t=120s to t=300s. Every pod's sessions fail at once.",
			Pods: 10, Capacity: 60, Clients: 400,
			Duration: 600 * time.Second, FaultAt: 120 * time.Second, RecoverAt: 300 * time.Second,
			Inject: func(f *Fleet, e time.Duration) {
				switch e {
				case 120 * time.Second:
					f.depOK = false
				case 300 * time.Second:
					f.depOK = true
				}
			},
			Strategies: []Strategy{StratNaive, StratDeep, StratHeartbeat, StratDeepEvict, StratHBEvict},
		},
		{
			Name: "3. One pod's egress path breaks (gray failure)",
			Desc: "pod a3 can still establish sessions and still passes readiness, but every session it holds fails from t=120s. Its loops are fine, so restarting is not the answer.",
			Pods: 10, Capacity: 60, Clients: 400,
			Duration: 600 * time.Second, FaultAt: 120 * time.Second,
			Inject: func(f *Fleet, e time.Duration) {
				if e == 120*time.Second {
					f.svcs[3].mediaPathOK = false
				}
			},
			Strategies: []Strategy{StratNaive, StratDeep, StratHeartbeat, StratHBEvict},
		},
		{
			Name: "4. Probe path contends with the work path under load",
			Desc: "the session-manager lock is held across each maintenance sweep. Client count doubles at t=120s. Nothing is broken.",
			Pods: 10, Capacity: 60, Clients: 400, StartClients: 200,
			Duration: 600 * time.Second, FaultAt: 120 * time.Second,
			LockPer: 40 * time.Millisecond, ControlInject: true,
			Inject: func(f *Fleet, e time.Duration) {
				if e == 120*time.Second {
					f.activeCli = len(f.clients)
				}
			},
			Strategies: []Strategy{StratHBShared, StratHBIso},
		},
	}
}

func Run(sc Scenario, st Strategy) Metrics { return run(sc, st, true) }

// Control runs the same scenario with no fault injected, to separate the harm
// caused by the fault and the response from the churn that is always present.
func Control(sc Scenario) Metrics { return run(sc, StratHeartbeat, sc.ControlInject) }

func run(sc Scenario, st Strategy, inject bool) Metrics {
	f := NewFleet(sc.Pods, sc.Capacity, sc.Clients, st)
	f.faultAt = sc.FaultAt
	if sc.StartClients > 0 {
		f.activeCli = sc.StartClients
	}
	if st.Name == StratDeepEvict.Name {
		f.unguarded = true
	}
	for _, s := range f.svcs {
		s.lockPerSession = sc.LockPer
	}

	steps := int(sc.Duration / Tick)
	for i := 0; i <= steps; i++ {
		e := time.Duration(i) * Tick
		now := f.start.Add(e)
		if sc.Inject != nil && inject {
			sc.Inject(f, e)
		}
		f.Step(now)

		w := 0
		for _, c := range f.clients[:f.activeCli] {
			if f.working(c) {
				w++
			}
		}
		f.m.ClientSecondsLost += float64(f.activeCli-w) * Tick.Seconds()
		f.m.FinalWorking = w
		if i%int(time.Second/Tick) == 0 {
			f.m.WorkingSeries = append(f.m.WorkingSeries, w)
		}
		f.m.WorkingLost = 0
		for _, s := range f.svcs {
			f.m.WorkingLost += s.WorkingLost
		}
		if sc.RecoverAt > 0 && e >= sc.RecoverAt && !f.m.Recovered &&
			float64(w) >= 0.95*float64(f.activeCli) {
			f.m.Recovered = true
			f.m.RecoveryLag = e - sc.RecoverAt
		}
	}
	return f.m
}
