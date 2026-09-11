package domain

import "testing"

func TestSessionStateTransitions(t *testing.T) {
	for _, next := range []SessionState{SessionStateActive, SessionStateCompleted, SessionStateExpired, SessionStateRevoked} {
		if !SessionStateActive.CanTransitionTo(next) {
			t.Errorf("Active should transition to %q", next)
		}
	}
	for _, terminal := range []SessionState{SessionStateCompleted, SessionStateExpired, SessionStateRevoked} {
		if terminal.CanTransitionTo(SessionStateActive) {
			t.Errorf("terminal state %q must not transition to Active", terminal)
		}
	}
}
