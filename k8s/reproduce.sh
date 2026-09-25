#!/usr/bin/env bash
# Reproduce the harness's central finding on a real cluster: a process whose
# session-maintenance loop has stopped keeps passing both the naive health
# check and the deep session check, and fails only the heartbeat check.
#
# Requires: kind, kubectl, docker. Takes about three minutes.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> building image"
docker build -t liveness-demo:latest .

echo "==> creating cluster"
kind get clusters 2>/dev/null | grep -qx liveness || kind create cluster --name liveness
kind load docker-image liveness-demo:latest --name liveness

echo "==> deploying"
kubectl apply -f k8s/demo.yaml
kubectl -n liveness-demo rollout status deploy/naive deploy/deep deploy/heartbeat --timeout=180s

pod()  { kubectl -n liveness-demo get pod -l app="$1" -o jsonpath='{.items[0].metadata.name}'; }
hit()  { kubectl -n liveness-demo exec "$(pod "$1")" -- wget -q -O- "http://localhost:$2$3" 2>/dev/null || true; }
code() { kubectl -n liveness-demo exec "$(pod "$1")" -- \
           wget -q -S -O /dev/null "http://localhost:8081$2" 2>&1 | awk '/HTTP\//{print $2; exit}'; }

echo "==> giving each pod 20 sessions"
for app in naive deep heartbeat; do
  for _ in $(seq 1 20); do hit "$app" 8080 /session >/dev/null; done
done
sleep 3

echo
echo "==> BEFORE killing the loop"
printf '  %-10s %8s %8s %8s\n' deployment healthz deepz livez
for app in naive deep heartbeat; do
  printf '  %-10s %8s %8s %8s\n' "$app" "$(code "$app" /healthz)" "$(code "$app" /deepz)" "$(code "$app" /livez)"
done

echo
echo "==> stopping the session-maintenance goroutine in one pod of each deployment"
echo "    (the listener keeps accepting; only the loop stops)"
for app in naive deep heartbeat; do hit "$app" 8080 /debug/kill-loop; done

echo "==> waiting 45s for the kubelet to react"
sleep 45

echo
echo "==> AFTER"
kubectl -n liveness-demo get pods -o custom-columns=\
'POD:.metadata.name,READY:.status.containerStatuses[0].ready,RESTARTS:.status.containerStatuses[0].restartCount'

echo
echo "Expected: the naive and deep pods report 0 restarts and READY=true while"
echo "holding 20 dead sessions. Only the heartbeat pod restarted."
echo
echo "Scenario 4 (probe contention) — redeploy with SWEEP_COST_US=20000 and point"
echo "the liveness probe at /livez-shared instead of /livez."
echo
echo "Clean up: kind delete cluster --name liveness"
