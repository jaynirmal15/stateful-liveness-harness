package main

import (
	"fmt"
	"os"
	"strings"
)

// CSV emits the per-second count of clients holding a working session, for the
// scenarios and strategies the article charts. Ground truth, not accounting.
func CSV() {
	scs := Scenarios()
	type col struct {
		name string
		m    Metrics
	}
	out := [][]col{
		{
			{"scenario1_naive", Run(scs[0], StratNaive)},
			{"scenario1_deep", Run(scs[0], StratDeep)},
			{"scenario1_heartbeat", Run(scs[0], StratHeartbeat)},
		},
		{
			{"scenario2_naive", Run(scs[1], StratNaive)},
			{"scenario2_deep", Run(scs[1], StratDeep)},
			{"scenario2_heartbeat", Run(scs[1], StratHeartbeat)},
		},
	}

	var head []string
	head = append(head, "t")
	n := 0
	for _, grp := range out {
		for _, c := range grp {
			head = append(head, c.name)
			if len(c.m.WorkingSeries) > n {
				n = len(c.m.WorkingSeries)
			}
		}
	}
	w := os.Stdout
	fmt.Fprintln(w, strings.Join(head, ","))
	for i := 0; i < n; i++ {
		row := []string{fmt.Sprint(i)}
		for _, grp := range out {
			for _, c := range grp {
				if i < len(c.m.WorkingSeries) {
					row = append(row, fmt.Sprint(c.m.WorkingSeries[i]))
				} else {
					row = append(row, "")
				}
			}
		}
		fmt.Fprintln(w, strings.Join(row, ","))
	}
}
