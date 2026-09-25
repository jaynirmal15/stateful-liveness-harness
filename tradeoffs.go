package main

import (
	"fmt"
	"time"
)

// Tradeoffs measures the two effects the sweep surfaced as conditional rather
// than universal. Both are about probe tuning, and both cut against the
// instinct they seem to support.
func Tradeoffs() {
	base := baselineParams()

	fmt.Println("A. The deep check's harm scales with how responsively you tune it")
	fmt.Println("   (scenario 2, fleet-wide dependency failure, 10 pods / 400 clients)")
	fmt.Printf("\n   %-26s %9s %9s %14s %13s\n",
		"probe config", "restarts", "working", "pod-sec down", "recovery lag")
	fmt.Println("   " + line(76))
	for _, cfg := range probeConfigs() {
		base.apply()
		LivenessSpec.Period, LivenessSpec.FailureThreshold = cfg.period, cfg.threshold
		s := Scenarios()[1]
		m := Run(s, StratDeep)
		fmt.Printf("   %-26s %9d %9d %14.0f %13s\n", cfg.name, m.Restarts, m.WorkingLost,
			m.PodSecondsDown, dur(m.RecoveryLag, m.Recovered))
	}
	fmt.Println("\n   * baseline. Restarts, sessions killed and pod downtime all fall as the")
	fmt.Println("   probe is loosened, and the minute-long recovery delays appear only in the")
	fmt.Println("   tight configurations. The tuning you would choose to catch problems")
	fmt.Println("   quickly is the tuning that turns a dependency blip into a fleet-wide")
	fmt.Println("   CrashLoopBackOff — and loosening it only hides the deep check's harm by")
	fmt.Println("   making it slower to act on anything at all.")

	fmt.Println("\n\nB. Loosening the probe to survive lock contention costs real detection time")
	fmt.Println("   (scenario 4 restarts vs scenario 1 detection, same probe config)")
	fmt.Printf("\n   %-26s %12s %12s %16s\n",
		"probe config", "false", "working", "time to detect")
	fmt.Printf("   %-26s %12s %12s %16s\n",
		"", "restarts", "killed", "a dead loop")
	fmt.Println("   " + line(70))
	for _, cfg := range probeConfigs() {
		base.apply()
		LivenessSpec.Period, LivenessSpec.FailureThreshold = cfg.period, cfg.threshold
		scs := Scenarios()
		shared := Run(scs[3], StratHBShared)
		detect := Run(scs[0], StratHeartbeat)
		fmt.Printf("   %-26s %12d %12d %16s\n", cfg.name, shared.Restarts, shared.WorkingLost,
			dur(detect.DetectAt, detect.Detected))
	}
	fmt.Println("\n   Raising timeout or failureThreshold does stop the contention restarts.")
	fmt.Println("   It also slows down the one failure the probe exists to catch. Isolating")
	fmt.Println("   the probe path buys the same safety for free.")
	base.apply()
}

type probeCfg struct {
	name      string
	period    time.Duration
	threshold int
}

func probeConfigs() []probeCfg {
	return []probeCfg{
		{"period 3s, threshold 2", 3 * time.Second, 2},
		{"period 3s, threshold 3", 3 * time.Second, 3},
		{"period 5s, threshold 3  *", 5 * time.Second, 3},
		{"period 5s, threshold 6", 5 * time.Second, 6},
		{"period 10s, threshold 3", 10 * time.Second, 3},
		{"period 10s, threshold 6", 10 * time.Second, 6},
	}
}

func line(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = '-'
	}
	return string(b)
}
