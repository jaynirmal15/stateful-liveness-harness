package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

func dur(d time.Duration, ok bool) string {
	if !ok {
		return "never"
	}
	return d.Round(100 * time.Millisecond).String()
}

func main() {
	Trace = os.Getenv("TRACE") != ""
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "-sweep":
			Sweep()
			return
		case "-tradeoffs":
			Tradeoffs()
			return
		case "-csv":
			CSV()
			return
		case "-serve":
			Serve()
			return
		}
	}
	fmt.Println("Liveness for stateful services — deterministic probe harness")
	fmt.Println(strings.Repeat("=", 78))
	fmt.Println("10 pods · 400 clients · 600s · kubelet probe semantics modelled")
	fmt.Printf("liveness probe: period=%v timeout=%v failureThreshold=%d\n",
		LivenessSpec.Period, LivenessSpec.Timeout, LivenessSpec.FailureThreshold)
	fmt.Printf("maintenance loop: period=%v staleness deadline=%v\n",
		MaintenancePeriod, 5*MaintenancePeriod)
	fmt.Println("readiness is held constant across strategies: capacity, drain state,")
	fmt.Println("and the dependency needed to establish — never the health of held sessions.")

	for _, sc := range Scenarios() {
		ctl := Control(sc)
		fmt.Printf("\n\n%s\n%s\n%s\n", sc.Name, strings.Repeat("=", len(sc.Name)), wrap(sc.Desc, 88))
		fmt.Printf("\ncontrol run (same churn, no fault): %.0f client-seconds lost\n\n", ctl.ClientSecondsLost)
		fmt.Printf("  %-33s %6s %7s %7s %8s %11s %10s\n",
			"liveness strategy", "restr", "drains", "working", "pod-sec", "EXCESS", "corrective")
		fmt.Printf("  %-33s %6s %7s %7s %8s %11s %10s\n",
			"", "", "", "lost", "down", "client-sec", "action at")
		fmt.Println("  " + strings.Repeat("-", 90))
		for _, st := range sc.Strategies {
			m := Run(sc, st)
			fmt.Printf("  %-33s %6d %7d %7d %8.0f %11.0f %10s\n",
				st.Name, m.Restarts, m.Drains, m.WorkingLost, m.PodSecondsDown,
				m.ClientSecondsLost-ctl.ClientSecondsLost, dur(m.DetectAt, m.Detected))
			if sc.RecoverAt > 0 {
				fmt.Printf("  %-33s   → time to full recovery after the dependency returned: %s\n",
					"", dur(m.RecoveryLag, m.Recovered))
			}
		}
	}
	fmt.Println()
}

func wrap(s string, w int) string {
	var out, line []string
	n := 0
	for _, word := range strings.Fields(s) {
		if n+len(word)+1 > w && n > 0 {
			out = append(out, strings.Join(line, " "))
			line, n = nil, 0
		}
		line = append(line, word)
		n += len(word) + 1
	}
	return strings.Join(append(out, strings.Join(line, " ")), "\n")
}
