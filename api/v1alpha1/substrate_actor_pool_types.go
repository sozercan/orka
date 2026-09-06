/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// SubstrateActorPoolPhase describes controller progress for an actor pool.
// +kubebuilder:validation:Enum=Pending;Ready;Failed
type SubstrateActorPoolPhase string

const (
	SubstrateActorPoolPhasePending SubstrateActorPoolPhase = "Pending"
	SubstrateActorPoolPhaseReady   SubstrateActorPoolPhase = "Ready"
	SubstrateActorPoolPhaseFailed  SubstrateActorPoolPhase = "Failed"

	// MaxSubstrateActorPoolTargetActors bounds per-pool actor reconciliation work.
	MaxSubstrateActorPoolTargetActors int32 = 1000
)

// SubstrateActorPoolSpec defines an operator-owned oversubscription pool.
type SubstrateActorPoolSpec struct {
	// TemplateRef is the ActorTemplate used for pool members.
	// +kubebuilder:validation:Required
	TemplateRef WorkspaceTemplateReference `json:"templateRef"`

	// WorkerPoolRef is the Substrate WorkerPool this Orka pool targets.
	// +optional
	WorkerPoolRef *WorkspaceTemplateReference `json:"workerPoolRef,omitempty"`

	// TargetActors is the desired number of stateful actors tracked for this
	// pool. It may exceed the physical worker budget to express oversubscription.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	// +optional
	TargetActors int32 `json:"targetActors,omitempty"`

	// PrecreateActors asks the controller to create deterministic warm actors up
	// to TargetActors. Substrate may suspend them when the WorkerPool is full.
	// +optional
	PrecreateActors bool `json:"precreateActors,omitempty"`
}

// SubstrateActorPoolStatus reports safe pool telemetry.
type SubstrateActorPoolStatus struct {
	// Phase is the current controller-observed phase.
	// +optional
	Phase SubstrateActorPoolPhase `json:"phase,omitempty"`

	// ObservedGeneration is the latest generation reconciled by the controller.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// WorkerCount is the number of workers reported by Substrate for this pool.
	// +optional
	WorkerCount int32 `json:"workerCount,omitempty"`

	// ActorCount is the number of actors reported by Substrate for this pool.
	// +optional
	ActorCount int32 `json:"actorCount,omitempty"`

	// RunningActorCount is the number of pool actors currently running.
	// +optional
	RunningActorCount int32 `json:"runningActorCount,omitempty"`

	// SuspendedActorCount is the number of pool actors currently suspended.
	// +optional
	SuspendedActorCount int32 `json:"suspendedActorCount,omitempty"`

	// ActorsPerWorker is ActorCount divided by WorkerCount, formatted as a decimal string.
	// +optional
	ActorsPerWorker string `json:"actorsPerWorker,omitempty"`

	// Message contains sanitized reconciliation context.
	// +optional
	Message string `json:"message,omitempty"`

	// Conditions represent the current state of the pool.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Actors",type=integer,JSONPath=`.status.actorCount`
// +kubebuilder:printcolumn:name="Workers",type=integer,JSONPath=`.status.workerCount`
// +kubebuilder:printcolumn:name="Density",type=string,JSONPath=`.status.actorsPerWorker`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// SubstrateActorPool is the Schema for Substrate actor pools.
type SubstrateActorPool struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   SubstrateActorPoolSpec   `json:"spec,omitempty"`
	Status SubstrateActorPoolStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// SubstrateActorPoolList contains a list of SubstrateActorPool.
type SubstrateActorPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SubstrateActorPool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&SubstrateActorPool{}, &SubstrateActorPoolList{})
}
