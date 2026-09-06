package harness

import (
	"strings"
	"testing"
)

func TestRuntimeSessionStateTransitions(t *testing.T) {
	valid := [][2]RuntimeSessionState{
		{RuntimeSessionStatePending, RuntimeSessionStateBooting},
		{RuntimeSessionStateBooting, RuntimeSessionStateReady},
		{RuntimeSessionStateReady, RuntimeSessionStateTurnRunning},
		{RuntimeSessionStateTurnRunning, RuntimeSessionStateIdle},
		{RuntimeSessionStateIdle, RuntimeSessionStateReleasing},
		{RuntimeSessionStateReleasing, RuntimeSessionStateRetained},
		{RuntimeSessionStateRetained, RuntimeSessionStateDeleting},
		{RuntimeSessionStateDeleting, RuntimeSessionStateDeleted},
		{RuntimeSessionStateIdle, RuntimeSessionStateSuspended},
		{RuntimeSessionStateSuspended, RuntimeSessionStateBooting},
	}
	for _, transition := range valid {
		if err := ValidateRuntimeSessionTransition(transition[0], transition[1]); err != nil {
			t.Fatalf("ValidateRuntimeSessionTransition(%s,%s) error = %v", transition[0], transition[1], err)
		}
	}
	invalid := [][2]RuntimeSessionState{
		{RuntimeSessionStateDeleted, RuntimeSessionStateReady},
		{RuntimeSessionStatePending, RuntimeSessionStateTurnRunning},
		{RuntimeSessionStateReady, RuntimeSessionStateSuspended},
	}
	for _, transition := range invalid {
		if err := ValidateRuntimeSessionTransition(transition[0], transition[1]); err == nil {
			t.Fatalf("ValidateRuntimeSessionTransition(%s,%s) error = nil, want error", transition[0], transition[1])
		}
	}
}

func TestRuntimeSessionValidationRequiresOwnership(t *testing.T) {
	session := RuntimeSession{ID: "runtime-a", State: RuntimeSessionStatePending, CleanupPolicy: RuntimeCleanupPolicyDelete}
	if err := session.Validate(); err == nil || !strings.Contains(err.Error(), "namespace is required") {
		t.Fatalf("Validate() = %v, want namespace required", err)
	}
	session.Owner = RuntimeSessionOwner{Namespace: "default", SessionName: "session-a", Provider: ProviderKindKubernetesService}
	if err := session.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}
