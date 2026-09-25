# Liveness for stateful services — the harness

Supporting evidence for *Health Checks Lie: Liveness for Stateful Services*.

```
go run .              # the four scenarios
go run . -sweep       # every claim against every parameter variant
go run . -tradeoffs   # the two probe-tuning tradeoffs
go run . -serve       # run it as a real process (probes on :8081)
k8s/reproduce.sh      # the central finding on a real kind cluster
```

No dependencies beyond the standard library. Every run is deterministic —
there is no RNG anywhere, and repeated runs are byte-identical.

## What is real and what is modelled

The distinction matters, because the thing under test has to be real code and
the environment around it has to be reproducible.

**Real, and shared between the simulation and the running server:** the
heartbeat registry (`heartbeat.go`), the session state machine (`session.go`),
the probe handlers (`probes.go`), and the self-eviction controller
(`eviction.go`). `-serve` runs these as an actual HTTP service with real
goroutines, real tickers and a real mutex.

**Modelled:** the kubelet's probe loop (`kubelet.go` — initial delay, period,
timeout counted as a failure, consecutive failure and success thresholds,
restart backoff from 10s doubling to a 5m cap) and the clock. The fleet, the
clients and the load balancer (`fleet.go`) are a model too.

Modelling the kubelet is what buys determinism, so run `k8s/reproduce.sh` to
confirm the central finding against a real one.

## The four scenarios

All four use 10 pods, 400 clients with staggered session lifetimes, a 600s run,
and a fault at t=120s. Readiness is held constant across every strategy —
capacity, drain state, and the dependency needed to *establish* a session,
never the health of sessions already held — so the comparison isolates the
liveness decision. Harm is reported as **excess client-seconds without a
working session**, over a control run with the same churn and no fault.

1. **The maintenance loop dies, the HTTP server lives.** The failure the
   article is about.
2. **A fleet-wide dependency failure**, partial (90% of sessions) and temporary
   (t=120s to t=300s). The case where reacting is worse than not reacting.
3. **One pod's egress path breaks.** A gray failure: the pod's loops are fine,
   it still passes readiness, and every session it touches dies.
4. **The probe handler contends with the work path**, on a fleet where nothing
   is broken.

## Results

| scenario | strategy | restarts | working sessions killed | excess client-sec |
|---|---|---:|---:|---:|
| 1 dead loop | naive `/healthz` | 0 | 0 | 4,665 |
| | deep session check | 0 | 0 | 4,665 |
| | heartbeat `/livez` | 1 | 0 | **674** |
| 2 dependency | naive `/healthz` | 0 | 0 | 63,611 |
| | deep session check | 40 | 218 | **91,207** |
| | heartbeat `/livez` | 0 | 0 | 63,611 |
| | heartbeat + unguarded eviction | 0 | 210 | 64,816 |
| | heartbeat + guarded eviction | 0 | 0 | 63,611 |
| 3 gray failure | naive `/healthz` | 0 | 0 | 7,107 |
| | deep session check | 6 | 0 | 789 |
| | heartbeat `/livez` | 0 | 0 | 7,107 |
| | heartbeat + guarded eviction | 0 | 0 | **1,408** |
| 4 contention | probe shares the lock | 50 | 2,927 | 118,976 |
| | isolated probe path | 0 | 0 | **0** |

Four things worth pulling out.

**Neither the naive check nor the deep check ever notices a dead maintenance
loop.** The naive check is measuring the HTTP server. The deep check reads
session accounting — which is written by the maintenance loop, so when the loop
stops, the deep check freezes on its last good answer and returns 200 forever.
The deep check depends on the thing it is supposed to be checking.

**In a fleet-wide failure the deep check is worse than doing nothing**: 43%
more harm than the naive check, 218 working sessions killed, and full recovery
delayed by 63 seconds *after the dependency came back*, because the fleet was
in CrashLoopBackOff when it did.

**The deep check wins scenario 3** — and it wins it for an unflattering reason.
It never fixes anything: a liveness restart puts the same container back on the
same node with the same broken path. It looks good only because the restart
backoff escalates — a 165-second park by the fifth restart, with a 300-second
one queued when the run ended. That is eviction by accident, paid for in 369
seconds of pod downtime across six restarts that repaired nothing. Guarded self-eviction gets the same
outcome deliberately, in one drain, with zero restarts and zero working
sessions killed, and the rescheduled pod actually lands somewhere healthy.

**Scenario 4 has no fault in it at all.** 50 restarts and 2,927 killed working
sessions came from a probe handler taking the same mutex as the work path, at a
sweep holding that lock 40ms per session.

## Sensitivity

`-sweep` checks six claims against 33 parameter combinations — three fleet
sizes (5/10/30 pods) by eleven variants (probe period 3/5/10s, failure
threshold 2/3/6, probe timeout 1/2s, deep-check threshold 0.2/0.5/0.8,
dependency severity 50/75/90/100%). All 198 hold.

The claims are deliberately stated as inequalities that must hold everywhere
("the deep check never does better than doing nothing") rather than as effects
at one parameter setting. Where an effect is conditional, `-tradeoffs`
quantifies it instead of asserting it:

- The deep check's harm falls as the probe is loosened — 18 restarts and 78
  killed sessions at period 10s / threshold 6, against 40 and 218 at the
  baseline — but only because it has become too sluggish to act on much of
  anything. The minute-long recovery delays appear only in the tight
  configurations. There is no setting at which it beats doing nothing.
- Raising `timeoutSeconds` or `failureThreshold` does stop the contention
  restarts in scenario 4 — and doubles the time to detect a genuinely dead
  loop, from 16s to 31s. Isolating the probe path buys the same safety for
  free.

## The guards on self-eviction

Self-eviction earns its place in scenario 3 and has to be prevented from firing
in scenario 2. Four guards do that:

1. **A window, not a scrape.** The failure rate must hold across the full 60s
   window.
2. **A denominator.** At least 10 session outcomes in that window. An idle pod
   is not a broken pod.
3. **A fleet-wide check.** If more than a third of the fleet reports the same
   thing, the cause is not local and leaving will not help.
4. **A disruption budget.** One pod at a time.

Two implementation details turn out to matter more than they look.

*Guard 3 must publish the raw observation, not the pod's verdict.* If pods
share verdicts, the first one to fill its 60s window sees everyone else still
undecided and evicts itself out of an outage the whole fleet is in. Pod boot
skew alone is enough to trigger this.

*The signal must be an outcome rate over a window, not a ratio of current
session states.* A pod mid-reconnect holds mostly `connecting` sessions and
reads as perfectly healthy on an instantaneous ratio. Monotonic outcome
counters — the ones already on `/metrics` — are the right input.

## Files

| | |
|---|---|
| `heartbeat.go` | the design: a registry of maintenance loops and their deadlines |
| `session.go` | session state machine |
| `service.go` | one pod: sessions, loops, accounting, restart and drain |
| `probes.go` | the four liveness strategies |
| `kubelet.go` | model of the kubelet probe loop |
| `eviction.go` | guarded self-eviction |
| `fleet.go` | pods, clients, load balancer, metrics |
| `scenarios.go` | the four scenarios and the runner |
| `sweep.go` | claims and the sensitivity sweep |
| `tradeoffs.go` | the two probe-tuning tradeoffs |
| `serve.go` | the same design as a real HTTP service |
| `k8s/` | Dockerfile, manifests and a kind reproduction script |
