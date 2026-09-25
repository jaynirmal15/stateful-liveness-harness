package main

import "time"

type SessionState int

const (
	SessionConnecting SessionState = iota
	SessionEstablished
	SessionFailed
)

func (s SessionState) String() string {
	switch s {
	case SessionConnecting:
		return "connecting"
	case SessionEstablished:
		return "established"
	default:
		return "failed"
	}
}

// Session is a unit of held state: a peer connection, a partition assignment,
// a replication stream. Its state is advanced by the maintenance loop and by
// nothing else -- which is exactly why a health check that reads session state
// is really reading the maintenance loop's last opinion of session state.
type Session struct {
	ID        int
	ClientID  int
	State     SessionState
	ChangedAt time.Time
	// Affected marks sessions that depend on the part of the shared
	// dependency that fails. A real dependency outage is partial, and the
	// survivors are what a mass restart destroys.
	Affected bool
}

func (s *Session) set(st SessionState, now time.Time) {
	if s.State != st {
		s.State = st
		s.ChangedAt = now
	}
}
