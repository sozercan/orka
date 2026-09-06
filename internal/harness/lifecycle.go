package harness

import (
	"fmt"
	"strings"
	"time"
)

type RuntimeSessionState string

const (
	RuntimeSessionStatePending     RuntimeSessionState = "Pending"
	RuntimeSessionStateBooting     RuntimeSessionState = "Booting"
	RuntimeSessionStateReady       RuntimeSessionState = "Ready"
	RuntimeSessionStateTurnRunning RuntimeSessionState = "TurnRunning"
	RuntimeSessionStateIdle        RuntimeSessionState = "Idle"
	RuntimeSessionStateReleasing   RuntimeSessionState = "Releasing"
	RuntimeSessionStateRetained    RuntimeSessionState = "Retained"
	RuntimeSessionStateSuspended   RuntimeSessionState = "Suspended"
	RuntimeSessionStateDeleting    RuntimeSessionState = "Deleting"
	RuntimeSessionStateDeleted     RuntimeSessionState = "Deleted"
	RuntimeSessionStateFailed      RuntimeSessionState = "Failed"
	RuntimeSessionStateUnhealthy   RuntimeSessionState = "Unhealthy"
)

type RuntimeCleanupPolicy string

const (
	RuntimeCleanupPolicyDelete  RuntimeCleanupPolicy = "delete"
	RuntimeCleanupPolicyRetain  RuntimeCleanupPolicy = "retain"
	RuntimeCleanupPolicySuspend RuntimeCleanupPolicy = "suspend"
)

type RuntimeSessionOwner struct {
	Namespace   string       `json:"namespace"`
	SessionName string       `json:"sessionName"`
	ActiveTask  string       `json:"activeTask,omitempty"`
	AgentName   string       `json:"agentName,omitempty"`
	Provider    ProviderKind `json:"provider"`
}

type RuntimeSession struct {
	ID            RuntimeSessionID     `json:"id"`
	Owner         RuntimeSessionOwner  `json:"owner"`
	State         RuntimeSessionState  `json:"state"`
	CleanupPolicy RuntimeCleanupPolicy `json:"cleanupPolicy"`
	IdleTimeout   time.Duration        `json:"idleTimeout,omitempty"`
	MaxLifetime   time.Duration        `json:"maxLifetime,omitempty"`
	CreatedAt     time.Time            `json:"createdAt,omitempty"`
	UpdatedAt     time.Time            `json:"updatedAt,omitempty"`
}

var allowedRuntimeSessionTransitions = map[RuntimeSessionState]map[RuntimeSessionState]struct{}{
	RuntimeSessionStatePending: {
		RuntimeSessionStateBooting: {}, RuntimeSessionStateDeleting: {}, RuntimeSessionStateFailed: {},
	},
	RuntimeSessionStateBooting: {
		RuntimeSessionStateReady: {}, RuntimeSessionStateUnhealthy: {}, RuntimeSessionStateDeleting: {}, RuntimeSessionStateFailed: {},
	},
	RuntimeSessionStateReady: {
		RuntimeSessionStateTurnRunning: {}, RuntimeSessionStateIdle: {}, RuntimeSessionStateReleasing: {}, RuntimeSessionStateDeleting: {}, RuntimeSessionStateUnhealthy: {},
	},
	RuntimeSessionStateTurnRunning: {
		RuntimeSessionStateIdle: {}, RuntimeSessionStateReady: {}, RuntimeSessionStateReleasing: {}, RuntimeSessionStateDeleting: {}, RuntimeSessionStateFailed: {}, RuntimeSessionStateUnhealthy: {},
	},
	RuntimeSessionStateIdle: {
		RuntimeSessionStateTurnRunning: {}, RuntimeSessionStateReleasing: {}, RuntimeSessionStateRetained: {}, RuntimeSessionStateSuspended: {}, RuntimeSessionStateDeleting: {}, RuntimeSessionStateUnhealthy: {},
	},
	RuntimeSessionStateReleasing: {
		RuntimeSessionStateRetained: {}, RuntimeSessionStateSuspended: {}, RuntimeSessionStateDeleting: {}, RuntimeSessionStateDeleted: {}, RuntimeSessionStateFailed: {},
	},
	RuntimeSessionStateRetained: {
		RuntimeSessionStateBooting: {}, RuntimeSessionStateDeleting: {}, RuntimeSessionStateFailed: {},
	},
	RuntimeSessionStateSuspended: {
		RuntimeSessionStateBooting: {}, RuntimeSessionStateDeleting: {}, RuntimeSessionStateFailed: {},
	},
	RuntimeSessionStateDeleting: {
		RuntimeSessionStateDeleted: {}, RuntimeSessionStateFailed: {},
	},
	RuntimeSessionStateUnhealthy: {
		RuntimeSessionStateBooting: {}, RuntimeSessionStateDeleting: {}, RuntimeSessionStateFailed: {},
	},
	RuntimeSessionStateFailed: {
		RuntimeSessionStateDeleting: {},
	},
	RuntimeSessionStateDeleted: {},
}

func IsKnownRuntimeSessionState(state RuntimeSessionState) bool {
	_, ok := allowedRuntimeSessionTransitions[state]
	return ok
}

func RuntimeSessionTransitionAllowed(from, to RuntimeSessionState) bool {
	if from == to && from != "" {
		return true
	}
	allowed, ok := allowedRuntimeSessionTransitions[from]
	if !ok {
		return false
	}
	_, ok = allowed[to]
	return ok
}

func ValidateRuntimeSessionTransition(from, to RuntimeSessionState) error {
	if !IsKnownRuntimeSessionState(from) {
		return fmt.Errorf("unsupported runtime session state %q", from)
	}
	if !IsKnownRuntimeSessionState(to) {
		return fmt.Errorf("unsupported runtime session state %q", to)
	}
	if !RuntimeSessionTransitionAllowed(from, to) {
		return fmt.Errorf("runtime session transition %s -> %s is not allowed", from, to)
	}
	return nil
}

func (s RuntimeSession) Validate() error {
	if strings.TrimSpace(string(s.ID)) == "" {
		return fmt.Errorf("runtime session id is required")
	}
	if strings.TrimSpace(s.Owner.Namespace) == "" {
		return fmt.Errorf("namespace is required")
	}
	if strings.TrimSpace(s.Owner.SessionName) == "" {
		return fmt.Errorf("session name is required")
	}
	if strings.TrimSpace(string(s.Owner.Provider)) == "" {
		return fmt.Errorf("runtime provider is required")
	}
	if !IsKnownRuntimeSessionState(s.State) {
		return fmt.Errorf("unsupported runtime session state %q", s.State)
	}
	if !IsKnownRuntimeCleanupPolicy(s.CleanupPolicy) {
		return fmt.Errorf("unsupported runtime cleanup policy %q", s.CleanupPolicy)
	}
	if s.IdleTimeout < 0 {
		return fmt.Errorf("idle timeout must be non-negative")
	}
	if s.MaxLifetime < 0 {
		return fmt.Errorf("max lifetime must be non-negative")
	}
	return nil
}

func IsKnownRuntimeCleanupPolicy(policy RuntimeCleanupPolicy) bool {
	switch policy {
	case RuntimeCleanupPolicyDelete, RuntimeCleanupPolicyRetain, RuntimeCleanupPolicySuspend:
		return true
	default:
		return false
	}
}
