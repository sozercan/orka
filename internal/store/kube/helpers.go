package kube

import (
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/store"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
)

const (
	annotationLogicalName       = "core.orka.ai/logical-name"
	annotationControllerEpoch   = "core.orka.ai/controller-epoch"
	annotationDomainVersion     = "core.orka.ai/domain-version"
	annotationRequestDigest     = "core.orka.ai/request-digest"
	annotationAcquiredAt        = "core.orka.ai/acquired-at"
	annotationSessionName       = "core.orka.ai/session-name"
	annotationSessionUID        = "core.orka.ai/session-uid"
	annotationLeaseGeneration   = "core.orka.ai/lease-generation"
	annotationLeaseMode         = "core.orka.ai/lease-mode"
	annotationTaskUID           = "core.orka.ai/task-uid"
	annotationAttempt           = "core.orka.ai/attempt"
	annotationPromptID          = "core.orka.ai/prompt-id"
	annotationSessionLineage    = "core.orka.ai/session-lineage-digest"
	annotationLeaseExpiresAt    = "core.orka.ai/lease-expires-at"
	annotationOperationID       = "core.orka.ai/operation-id"
	annotationOperationDigest   = "core.orka.ai/operation-digest"
	annotationMutationToken     = "core.orka.ai/epoch-mutation-token"
	annotationMutationExpiresAt = "core.orka.ai/epoch-mutation-expires-at"
	leaseModeEmpty              = "empty"
	leaseModeMutation           = "mutation"
	leaseModeReconciliation     = "reconciliation"
	controllerEpochNamePrefix   = "controller-epoch-"
	controllerEpochLeasePrefix  = "controller-epoch-lock-"
	promptAttemptNamePrefix     = "prompt-attempt-"
	runtimeSessionNamePrefix    = "runtime-session-"
	runtimeSessionLeasePrefix   = "runtime-session-lock-"
	branchClaimNamePrefix       = "branch-claim-"
	publicationNamePrefix       = "publication-"
	externalEffectNamePrefix    = "external-effect-"
)

var dnsDigestEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

type epochSnapshot struct {
	Name                 string
	Epoch                int64
	HolderID             string
	LeaseResourceVersion string
	MutationToken        string
	MutationLease        *coordinationv1.Lease
	LocalMutationSlot    bool
}

func validateKubernetesNamespace(namespace string) error {
	if namespace == "" {
		return store.ValidationErrorf("control namespace is required")
	}
	if problems := utilvalidation.IsDNS1123Label(namespace); len(problems) > 0 {
		return store.ValidationErrorf("control namespace %q is invalid: %s", namespace, strings.Join(problems, "; "))
	}
	return nil
}

func metaTime(value time.Time) *metav1.Time {
	if value.IsZero() {
		return nil
	}
	result := metav1.NewTime(value.UTC())
	return &result
}

func timeValue(value *metav1.Time) time.Time {
	if value == nil {
		return time.Time{}
	}
	return value.UTC()
}

func optionalTimeValue(value *metav1.Time) *time.Time {
	if value == nil {
		return nil
	}
	result := value.UTC()
	return &result
}

func dnsDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return strings.ToLower(dnsDigestEncoding.EncodeToString(sum[:]))
}

func objectName(prefix, logicalID string) string {
	return prefix + dnsDigest(logicalID)
}

func controlLabels(logicalID string) map[string]string {
	return map[string]string{corev1alpha1.ControlRecordIDHashLabel: dnsDigest(logicalID)}
}

func labelIfValid(labels map[string]string, key, value string) {
	if value == "" || len(utilvalidation.IsValidLabelValue(value)) != 0 {
		return
	}
	labels[key] = value
}

func mapKubernetesError(action string, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case apierrors.IsNotFound(err):
		return fmt.Errorf("%w: %s", store.ErrNotFound, action)
	case apierrors.IsConflict(err), apierrors.IsAlreadyExists(err):
		return fmt.Errorf("%w: %s: %v", store.ErrConflict, action, err)
	default:
		return fmt.Errorf("%s: %w", action, err)
	}
}

func setMutationStatus(status *corev1alpha1.ControlRecordMutationStatus, fence store.ControllerEpochFence, snapshot epochSnapshot, version int64, operationID, operationDigest string, createdAt, updatedAt time.Time) {
	status.ControllerEpochName = fence.Name
	status.ControllerEpoch = fence.Epoch
	status.ControllerEpochLeaseResourceVersion = snapshot.LeaseResourceVersion
	status.LastOperationID = operationID
	status.LastOperationDigest = operationDigest
	status.Version = version
	status.CreatedAt = metaTime(createdAt)
	status.UpdatedAt = metaTime(updatedAt)
}

func parsePositiveInt64(field, value string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 1 {
		return 0, fmt.Errorf("invalid %s annotation %q", field, value)
	}
	return parsed, nil
}

func parseNonNegativeInt64(field, value string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("invalid %s annotation %q", field, value)
	}
	return parsed, nil
}

func formatTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func parseTime(field, value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid %s annotation %q: %w", field, value, err)
	}
	return parsed.UTC(), nil
}

func sameOptionalTime(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}
