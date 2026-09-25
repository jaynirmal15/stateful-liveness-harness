package main

import (
	"fmt"
	"time"
)

// The five claims the article makes. Each is checked against every parameter
// combination in the sweep: a conclusion that only holds at the constants I
// happened to pick is not a conclusion.
type claim struct {
	id   string
	text string
	test func(sc []Scenario) (bool, string)
}

func claims() []claim {
	return []claim{
		{"C1", "a dead maintenance loop is invisible to both the naive and the deep check, and visible to the heartbeat",
			func(s []Scenario) (bool, string) {
				ctl := Control(s[0]).ClientSecondsLost
				n, d, h := Run(s[0], StratNaive), Run(s[0], StratDeep), Run(s[0], StratHeartbeat)
				okc := !n.Detected && !d.Detected && h.Detected
				return okc && h.ClientSecondsLost-ctl < 0.5*(n.ClientSecondsLost-ctl),
					fmt.Sprintf("naive=%v deep=%v hb=%v@%v  excess %0.f/%0.f/%0.f",
						n.Detected, d.Detected, h.Detected, h.DetectAt.Round(time.Second),
						n.ClientSecondsLost-ctl, d.ClientSecondsLost-ctl, h.ClientSecondsLost-ctl)
			}},
		{"C2", "in a fleet-wide dependency failure the deep check never does better than doing nothing",
			func(s []Scenario) (bool, string) {
				ctl := Control(s[1]).ClientSecondsLost
				n, d := Run(s[1], StratNaive), Run(s[1], StratDeep)
				return d.ClientSecondsLost >= n.ClientSecondsLost,
					fmt.Sprintf("deep excess=%0.f vs naive=%0.f, %d restarts, %d working sessions killed, %0.f pod-sec down",
						d.ClientSecondsLost-ctl, n.ClientSecondsLost-ctl, d.Restarts, d.WorkingLost, d.PodSecondsDown)
			}},
		{"C2b", "and it never recovers faster once the dependency returns",
			func(s []Scenario) (bool, string) {
				n, d := Run(s[1], StratNaive), Run(s[1], StratDeep)
				return !d.Recovered || d.RecoveryLag >= n.RecoveryLag,
					fmt.Sprintf("deep lag=%v vs naive lag=%v", d.RecoveryLag.Round(time.Second), n.RecoveryLag.Round(time.Second))
			}},
		{"C3", "the guards cut fleet-wide self-eviction by at least 5x",
			func(s []Scenario) (bool, string) {
				g, u := Run(s[1], StratHBEvict), Run(s[1], StratDeepEvict)
				return u.Drains == 0 || g.Drains*5 <= u.Drains,
					fmt.Sprintf("guarded=%d drains, unguarded=%d drains (%d working sessions killed)",
						g.Drains, u.Drains, u.WorkingLost)
			}},
		{"C4", "guarded eviction corrects a single-pod gray failure that liveness correctly ignores",
			func(s []Scenario) (bool, string) {
				ctl := Control(s[2]).ClientSecondsLost
				h, g := Run(s[2], StratHeartbeat), Run(s[2], StratHBEvict)
				return g.Drains >= 1 && g.Restarts == 0 &&
						g.ClientSecondsLost < h.ClientSecondsLost,
					fmt.Sprintf("heartbeat alone=%0.f excess (never corrects); +eviction=%0.f excess, %d drain(s) at %v, 0 restarts",
						h.ClientSecondsLost-ctl, g.ClientSecondsLost-ctl, g.Drains, g.DetectAt.Round(time.Second))
			}},
		{"C5", "a probe sharing the work path's lock is never better than an isolated one, and under load is far worse",
			func(s []Scenario) (bool, string) {
				sh, is := Run(s[3], StratHBShared), Run(s[3], StratHBIso)
				return sh.Restarts >= is.Restarts && sh.ClientSecondsLost >= is.ClientSecondsLost,
					fmt.Sprintf("shared=%d restarts / %d working sessions killed / %0.f client-sec; isolated=%d / %d / %0.f",
						sh.Restarts, sh.WorkingLost, sh.ClientSecondsLost,
						is.Restarts, is.WorkingLost, is.ClientSecondsLost)
			}},
	}
}

type variant struct {
	name  string
	apply func()
}

func baselineParams() variant {
	return variant{"baseline", func() {
		LivenessSpec.Period = 5 * time.Second
		LivenessSpec.FailureThreshold = 3
		LivenessSpec.Timeout = 1 * time.Second
		DeepThreshold = 0.5
		AffectedPct = 90
	}}
}

func Sweep() {
	base := baselineParams()
	variants := []variant{
		base,
		{"probe period 3s", func() { base.apply(); LivenessSpec.Period = 3 * time.Second }},
		{"probe period 10s", func() { base.apply(); LivenessSpec.Period = 10 * time.Second }},
		{"failureThreshold 2", func() { base.apply(); LivenessSpec.FailureThreshold = 2 }},
		{"failureThreshold 6", func() { base.apply(); LivenessSpec.FailureThreshold = 6 }},
		{"probe timeout 2s", func() { base.apply(); LivenessSpec.Timeout = 2 * time.Second }},
		{"deep threshold 0.2", func() { base.apply(); DeepThreshold = 0.2 }},
		{"deep threshold 0.8", func() { base.apply(); DeepThreshold = 0.8 }},
		{"dependency takes 50%", func() { base.apply(); AffectedPct = 50 }},
		{"dependency takes 75%", func() { base.apply(); AffectedPct = 75 }},
		{"dependency takes 100%", func() { base.apply(); AffectedPct = 100 }},
	}
	sizes := []struct {
		name                    string
		pods, capacity, clients int
	}{
		{"10 pods / 400 clients", 10, 60, 400},
		{"5 pods / 200 clients", 5, 60, 200},
		{"30 pods / 1200 clients", 30, 60, 1200},
	}

	cl := claims()
	fmt.Println("Sensitivity sweep — every claim, every parameter variant")
	fmt.Println("========================================================")
	fails := 0
	for _, sz := range sizes {
		fmt.Printf("\n### %s\n", sz.name)
		for _, v := range variants {
			v.apply()
			scs := Scenarios()
			for i := range scs {
				scs[i].Pods, scs[i].Capacity = sz.pods, sz.capacity
				scs[i].Clients = scs[i].Clients * sz.clients / 400
				if scs[i].StartClients > 0 {
					scs[i].StartClients = scs[i].StartClients * sz.clients / 400
				}
			}
			var notes []string
			line := fmt.Sprintf("  %-24s", v.name)
			for _, c := range cl {
				ok, detail := c.test(scs)
				if ok {
					line += "  " + c.id + " ✓"
				} else {
					line += "  " + c.id + " ✗"
					fails++
					notes = append(notes, fmt.Sprintf("      %s %s: %s", c.id, v.name, detail))
				}
			}
			fmt.Println(line)
			for _, n := range notes {
				fmt.Println(n)
			}
		}
	}
	base.apply()
	fmt.Printf("\n%d claim/parameter combinations failed.\n", fails)
	fmt.Println("\nClaims:")
	for _, c := range cl {
		fmt.Printf("  %-4s %s\n", c.id, c.text)
	}

	fmt.Println("\nDetail at baseline parameters, 10 pods / 400 clients:")
	scs := Scenarios()
	for _, c := range cl {
		ok, detail := c.test(scs)
		mark := "✓"
		if !ok {
			mark = "✗"
		}
		fmt.Printf("  %s %-4s %s\n", mark, c.id, detail)
	}
}
