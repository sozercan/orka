/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/labels"
	"github.com/orka-agents/orka/internal/store"
	"github.com/orka-agents/orka/internal/workerenv"
)

const (
	repositoryValidationReviewBindingEventType = "validation_review_bound"
	repositoryValidationReviewBindingSummary   = "Repository validation review bound"
	repositoryValidationBindingEventType       = "validation_command_bound"
	repositoryValidationBindingSummary         = "Repository validation command bound"
	repositoryValidationBindingActor           = "controller"
	repositoryValidationDefaultCredentialKey   = "token"
	// RepositoryValidationCommandSecretKey is the only data key accepted in a
	// controller-owned repository validation command Secret.
	RepositoryValidationCommandSecretKey = "command"
)

var errRepositoryValidationBindingConflict = errors.New("repository validation command binding conflict")

var errRepositoryValidationBindingMissing = errors.New("repository validation command binding is missing")

type repositoryValidationReviewBinding struct {
	MonitorUID           string `json:"monitorUID"`
	ReviewTaskName       string `json:"reviewTaskName"`
	Image                string `json:"image"`
	HeadSHA              string `json:"headSHA"`
	WorkspaceDigest      string `json:"workspaceDigest"`
	ReviewTaskSpecDigest string `json:"reviewTaskSpecDigest"`
}

type repositoryValidationWorkspaceIdentity struct {
	Intent            corev1alpha1.WorkspaceIntent               `json:"intent"`
	GitRepo           string                                     `json:"gitRepo"`
	SourceRepository  *corev1alpha1.RepositoryIdentity           `json:"sourceRepository,omitempty"`
	Ref               string                                     `json:"ref"`
	SubPath           string                                     `json:"subPath,omitempty"`
	ReadCredentialRef *corev1alpha1.WorkspaceCredentialReference `json:"readCredentialRef,omitempty"`
}

// IsRepositoryValidationCommandBindingInvalid reports whether validation
// failed because the durable binding is missing or conflicts with the Task.
// Other errors may be transient storage failures.
func IsRepositoryValidationCommandBindingInvalid(err error) bool {
	return errors.Is(err, errRepositoryValidationBindingMissing) ||
		errors.Is(err, errRepositoryValidationBindingConflict)
}

// RepositoryValidationBindingStore is the least-privilege durable dependency
// used to bind a review workspace and its validation command before execution.
type RepositoryValidationBindingStore interface {
	CreateMonitorEvent(context.Context, *store.MonitorEvent) error
	ListMonitorEvents(context.Context, store.MonitorEventFilter) ([]store.MonitorEvent, string, error)
}

// RepositoryValidationReviewBindingEvent returns the append-only event that
// freezes the review workspace before the review Task can call run_validation.
func RepositoryValidationReviewBindingEvent(reviewTask *corev1alpha1.Task, monitor *corev1alpha1.RepositoryMonitor) (*store.MonitorEvent, error) {
	if reviewTask == nil || monitor == nil || reviewTask.Spec.Workspace == nil {
		return nil, fmt.Errorf("review task, repository monitor, and review workspace are required")
	}
	image := strings.TrimSpace(reviewTask.Annotations[labels.AnnotationRepositoryValidationImage])
	headSHA := strings.TrimSpace(reviewTask.Annotations[labels.AnnotationMonitorHeadSHA])
	runID := strings.TrimSpace(reviewTask.Annotations[labels.AnnotationMonitorRunID])
	itemKind := strings.TrimSpace(reviewTask.Annotations[labels.AnnotationMonitorItemKind])
	itemNumber, err := strconv.ParseInt(strings.TrimSpace(reviewTask.Annotations[labels.AnnotationMonitorItemNumber]), 10, 64)
	workspaceDigest, digestErr := repositoryValidationWorkspaceDigest(reviewTask.Spec.Workspace)
	reviewTaskSpecDigest, specDigestErr := repositoryValidationReviewTaskSpecDigest(reviewTask.Spec)
	if reviewTask.Namespace == "" || reviewTask.Name == "" || monitor.Namespace != reviewTask.Namespace ||
		monitor.Name == "" || monitor.UID == "" || image == "" || headSHA == "" || runID == "" ||
		itemKind == "" || itemNumber <= 0 || err != nil || digestErr != nil || specDigestErr != nil {
		return nil, fmt.Errorf("repository validation review binding identity is incomplete")
	}
	binding := repositoryValidationReviewBinding{
		MonitorUID:           string(monitor.UID),
		ReviewTaskName:       reviewTask.Name,
		Image:                image,
		HeadSHA:              headSHA,
		WorkspaceDigest:      workspaceDigest,
		ReviewTaskSpecDigest: reviewTaskSpecDigest,
	}
	metadata, err := json.Marshal(binding)
	if err != nil {
		return nil, fmt.Errorf("encode repository validation review binding: %w", err)
	}
	return &store.MonitorEvent{
		ID:               repositoryValidationReviewBindingEventID(reviewTask.Namespace, reviewTask.Name, string(metadata)),
		MonitorNamespace: monitor.Namespace,
		MonitorName:      monitor.Name,
		RunID:            runID,
		ItemKind:         itemKind,
		ItemNumber:       itemNumber,
		ItemSHA:          headSHA,
		EventType:        repositoryValidationReviewBindingEventType,
		Actor:            repositoryValidationBindingActor,
		Summary:          repositoryValidationReviewBindingSummary,
		MetadataJSON:     string(metadata),
	}, nil
}

// EnsureRepositoryValidationReviewBinding persists the immutable review
// workspace binding before the controller creates or adopts the review Task.
func EnsureRepositoryValidationReviewBinding(ctx context.Context, bindingStore RepositoryValidationBindingStore, expected *store.MonitorEvent) error {
	if bindingStore == nil || expected == nil {
		return fmt.Errorf("repository validation review binding store is unavailable")
	}
	createErr := bindingStore.CreateMonitorEvent(ctx, expected)
	if createErr == nil {
		return nil
	}
	verifyErr := validateRepositoryValidationReviewBindingEvent(ctx, bindingStore, expected)
	if verifyErr == nil || errors.Is(verifyErr, errRepositoryValidationBindingConflict) {
		return verifyErr
	}
	return fmt.Errorf("persist repository validation review binding: %v; verification failed: %w", createErr, verifyErr)
}

// ValidateRepositoryValidationReviewBinding verifies that the live review Task
// still has the controller-frozen workspace, monitor, image, and head identity.
func ValidateRepositoryValidationReviewBinding(ctx context.Context, bindingStore RepositoryValidationBindingStore, reviewTask *corev1alpha1.Task, monitor *corev1alpha1.RepositoryMonitor) error {
	expected, err := RepositoryValidationReviewBindingEvent(reviewTask, monitor)
	if err != nil {
		return errRepositoryValidationBindingConflict
	}
	return validateRepositoryValidationReviewBindingEvent(ctx, bindingStore, expected)
}

func validateRepositoryValidationReviewBindingEvent(ctx context.Context, bindingStore RepositoryValidationBindingStore, expected *store.MonitorEvent) error {
	if bindingStore == nil || expected == nil {
		return errRepositoryValidationBindingMissing
	}
	filter := store.MonitorEventFilter{
		Namespace: expected.MonitorNamespace,
		ID:        expected.ID,
		EventType: repositoryValidationReviewBindingEventType,
		Limit:     1,
	}
	for {
		events, cursor, err := bindingStore.ListMonitorEvents(ctx, filter)
		if err != nil {
			return fmt.Errorf("load repository validation review binding: %w", err)
		}
		for i := range events {
			if events[i].ID != expected.ID {
				continue
			}
			if repositoryValidationReviewBindingMatches(&events[i], expected) {
				return nil
			}
			return errRepositoryValidationBindingConflict
		}
		if cursor == "" {
			return errRepositoryValidationBindingMissing
		}
		filter.Cursor = cursor
	}
}

func repositoryValidationReviewBindingMatches(existing, expected *store.MonitorEvent) bool {
	if existing == nil || expected == nil || existing.ID != expected.ID ||
		existing.MonitorNamespace != expected.MonitorNamespace || existing.MonitorName != expected.MonitorName ||
		existing.RunID != expected.RunID || existing.ItemKind != expected.ItemKind || existing.ItemNumber != expected.ItemNumber ||
		existing.ItemSHA != expected.ItemSHA || existing.EventType != expected.EventType || existing.Actor != expected.Actor ||
		existing.Summary != expected.Summary {
		return false
	}
	var existingBinding, expectedBinding repositoryValidationReviewBinding
	if json.Unmarshal([]byte(existing.MetadataJSON), &existingBinding) != nil || json.Unmarshal([]byte(expected.MetadataJSON), &expectedBinding) != nil {
		return false
	}
	return existingBinding == expectedBinding
}

func repositoryValidationWorkspaceDigest(workspace *corev1alpha1.WorkspaceConfig) (string, error) {
	if workspace == nil {
		return "", fmt.Errorf("review workspace is required")
	}
	var sourceRepository *corev1alpha1.RepositoryIdentity
	if workspace.SourceRepository != nil {
		sourceCopy := *workspace.SourceRepository
		sourceRepository = &sourceCopy
	}
	var readCredentialRef *corev1alpha1.WorkspaceCredentialReference
	if workspace.ReadCredentialRef != nil {
		credentialCopy := *workspace.ReadCredentialRef
		if credentialCopy.Key == "" {
			credentialCopy.Key = repositoryValidationDefaultCredentialKey
		}
		readCredentialRef = &credentialCopy
	}
	identity := repositoryValidationWorkspaceIdentity{
		Intent:            workspace.Intent,
		GitRepo:           workspace.GitRepo,
		SourceRepository:  sourceRepository,
		Ref:               workspace.Ref,
		SubPath:           workspace.SubPath,
		ReadCredentialRef: readCredentialRef,
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return "", fmt.Errorf("encode repository validation workspace identity: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func repositoryValidationReviewTaskSpecDigest(spec corev1alpha1.TaskSpec) (string, error) {
	normalized := spec.DeepCopy()
	// CRD defaulting fills these scheduling-only fields after creation. They do
	// not affect a one-shot review Task, so normalize them before hashing while
	// retaining every capability-bearing field in the digest.
	normalized.ConcurrencyPolicy = ""
	normalized.StartingDeadlineSeconds = nil
	normalized.SuccessfulRunsHistoryLimit = nil
	normalized.FailedRunsHistoryLimit = nil
	if normalized.Workspace != nil && normalized.Workspace.ReadCredentialRef != nil && normalized.Workspace.ReadCredentialRef.Key == "" {
		normalized.Workspace.ReadCredentialRef.Key = repositoryValidationDefaultCredentialKey
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("encode repository validation review task spec: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

func repositoryValidationReviewBindingEventID(namespace, reviewTaskName, bindingMetadata string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(namespace) + "\x00" + strings.TrimSpace(reviewTaskName) + "\x00" + bindingMetadata))
	return "mevt-validation-review-" + hex.EncodeToString(digest[:16])
}

type repositoryValidationCommandBinding struct {
	MonitorUID         string `json:"monitorUID"`
	ReviewTaskName     string `json:"reviewTaskName"`
	ReviewTaskUID      string `json:"reviewTaskUID"`
	ValidationTaskName string `json:"validationTaskName"`
	Image              string `json:"image"`
	HeadSHA            string `json:"headSHA"`
	CommandDigest      string `json:"commandDigest"`
}

// RepositoryValidationCommandBinding is the controller-owned execution
// provenance persisted before a repository validation Task is created.
type RepositoryValidationCommandBinding struct {
	MonitorNamespace   string
	MonitorName        string
	RunID              string
	ItemKind           string
	ItemNumber         int64
	MonitorUID         string
	ReviewTaskName     string
	ReviewTaskUID      string
	ValidationTaskName string
	Image              string
	HeadSHA            string
	CommandDigest      string
}

// FindRepositoryValidationCommandBinding finds the durable binding for one
// deterministic validation Task name. A conflicting duplicate fails closed.
func FindRepositoryValidationCommandBinding(ctx context.Context, bindingStore RepositoryValidationBindingStore, namespace, validationTaskName string) (*RepositoryValidationCommandBinding, error) {
	if bindingStore == nil {
		return nil, fmt.Errorf("repository validation command binding store is unavailable")
	}
	namespace = strings.TrimSpace(namespace)
	validationTaskName = strings.TrimSpace(validationTaskName)
	if namespace == "" || validationTaskName == "" {
		return nil, fmt.Errorf("repository validation task identity is incomplete")
	}

	filter := store.MonitorEventFilter{
		Namespace: namespace,
		ID:        repositoryValidationCommandBindingEventID(namespace, validationTaskName),
		EventType: repositoryValidationBindingEventType,
		Limit:     1,
	}
	var found *RepositoryValidationCommandBinding
	for {
		events, cursor, err := bindingStore.ListMonitorEvents(ctx, filter)
		if err != nil {
			return nil, fmt.Errorf("load repository validation command binding: %w", err)
		}
		for i := range events {
			event := &events[i]
			if event.ID != filter.ID {
				continue
			}
			var binding repositoryValidationCommandBinding
			if json.Unmarshal([]byte(event.MetadataJSON), &binding) != nil || binding.ValidationTaskName != validationTaskName {
				continue
			}
			candidate := &RepositoryValidationCommandBinding{
				MonitorNamespace:   event.MonitorNamespace,
				MonitorName:        event.MonitorName,
				RunID:              event.RunID,
				ItemKind:           event.ItemKind,
				ItemNumber:         event.ItemNumber,
				MonitorUID:         binding.MonitorUID,
				ReviewTaskName:     binding.ReviewTaskName,
				ReviewTaskUID:      binding.ReviewTaskUID,
				ValidationTaskName: binding.ValidationTaskName,
				Image:              binding.Image,
				HeadSHA:            binding.HeadSHA,
				CommandDigest:      binding.CommandDigest,
			}
			if event.Actor != repositoryValidationBindingActor || event.Summary != repositoryValidationBindingSummary ||
				candidate.MonitorNamespace != namespace || candidate.MonitorName == "" || candidate.MonitorUID == "" ||
				candidate.ReviewTaskName == "" || candidate.ReviewTaskUID == "" || candidate.Image == "" ||
				candidate.HeadSHA == "" || candidate.CommandDigest == "" || event.ItemSHA != candidate.HeadSHA {
				return nil, errRepositoryValidationBindingConflict
			}
			if found != nil && *found != *candidate {
				return nil, errRepositoryValidationBindingConflict
			}
			found = candidate
		}
		if cursor == "" {
			return found, nil
		}
		filter.Cursor = cursor
	}
}

// MatchesReview reports whether a durable binding belongs to the exact review
// Task and RepositoryMonitor that requested it.
func (b *RepositoryValidationCommandBinding) MatchesReview(parent *corev1alpha1.Task, monitor *corev1alpha1.RepositoryMonitor, image, headSHA string) bool {
	if b == nil || parent == nil || monitor == nil {
		return false
	}
	itemNumber, err := strconv.ParseInt(strings.TrimSpace(parent.Annotations[labels.AnnotationMonitorItemNumber]), 10, 64)
	return err == nil && itemNumber > 0 &&
		b.MonitorNamespace == parent.Namespace && b.MonitorNamespace == monitor.Namespace &&
		b.MonitorName == monitor.Name && b.MonitorUID == string(monitor.UID) &&
		b.ReviewTaskName == parent.Name && b.ReviewTaskUID == string(parent.UID) &&
		b.ValidationTaskName == RepositoryValidationTaskName(parent) &&
		b.RunID == strings.TrimSpace(parent.Annotations[labels.AnnotationMonitorRunID]) &&
		b.ItemKind == strings.TrimSpace(parent.Annotations[labels.AnnotationMonitorItemKind]) &&
		b.ItemNumber == itemNumber && b.Image == strings.TrimSpace(image) &&
		b.HeadSHA == strings.TrimSpace(headSHA)
}

// MatchesCommand reports whether command is the value accepted before the
// validation Task was created.
func (b *RepositoryValidationCommandBinding) MatchesCommand(command string) bool {
	return b != nil && b.CommandDigest == RepositoryValidationCommandDigest(command)
}

// RepositoryValidationCommandSecretName returns the deterministic Secret name
// used for one repository validation Task. Secret and Task names may match
// because Kubernetes names are scoped by resource kind.
func RepositoryValidationCommandSecretName(validationTaskName string) string {
	return strings.TrimSpace(validationTaskName)
}

// ValidateRepositoryValidationCommandSecret verifies that the immutable Secret
// belongs to the exact review and contains the command bound before Task
// creation. It never returns or embeds the command value in an error.
func ValidateRepositoryValidationCommandSecret(parent, validationTask *corev1alpha1.Task, secret *corev1.Secret, binding *RepositoryValidationCommandBinding) error {
	if parent == nil || validationTask == nil || secret == nil || binding == nil {
		return errRepositoryValidationBindingConflict
	}
	if !repositoryValidationCommandBindingMatchesParent(parent, binding) ||
		validationTask.Namespace != binding.MonitorNamespace || validationTask.Name != binding.ValidationTaskName ||
		validationTask.Annotations[labels.AnnotationRepositoryMonitorName] != binding.MonitorName ||
		validationTask.Annotations[labels.AnnotationMonitorHeadSHA] != binding.HeadSHA ||
		validationTask.Annotations[labels.AnnotationRepositoryValidationImage] != binding.Image ||
		validationTask.Annotations[labels.AnnotationRepositoryValidationCommandDigest] != binding.CommandDigest ||
		!repositoryValidationCommandSecretObjectMatches(parent, secret, binding) ||
		!repositoryValidationCommandSecretMetadataMatches(parent, secret, binding) ||
		!repositoryValidationCommandSecretDataMatches(secret, binding) {
		return errRepositoryValidationBindingConflict
	}
	return nil
}

// ValidateRepositoryValidationOrphanCommandSecret verifies a command Secret
// after its deterministic child Task has disappeared. Callers must first
// verify the binding against the owning review Task and RepositoryMonitor.
func ValidateRepositoryValidationOrphanCommandSecret(parent *corev1alpha1.Task, secret *corev1.Secret, binding *RepositoryValidationCommandBinding) error {
	if parent == nil || secret == nil || binding == nil ||
		!repositoryValidationCommandBindingMatchesParent(parent, binding) ||
		!repositoryValidationCommandSecretObjectMatches(parent, secret, binding) ||
		!repositoryValidationCommandSecretMetadataMatches(parent, secret, binding) ||
		!repositoryValidationCommandSecretDataMatches(secret, binding) {
		return errRepositoryValidationBindingConflict
	}
	return nil
}

func repositoryValidationCommandBindingMatchesParent(parent *corev1alpha1.Task, binding *RepositoryValidationCommandBinding) bool {
	return parent != nil && binding != nil && binding.MonitorNamespace == parent.Namespace &&
		binding.ReviewTaskName == parent.Name && binding.ReviewTaskUID == string(parent.UID) &&
		binding.ValidationTaskName == RepositoryValidationTaskName(parent)
}

func repositoryValidationCommandSecretObjectMatches(parent *corev1alpha1.Task, secret *corev1.Secret, binding *RepositoryValidationCommandBinding) bool {
	owner := metav1.GetControllerOf(secret)
	return secret.Namespace == binding.MonitorNamespace &&
		secret.Name == RepositoryValidationCommandSecretName(binding.ValidationTaskName) &&
		secret.Type == corev1.SecretTypeOpaque && secret.Immutable != nil && *secret.Immutable &&
		owner != nil && owner.APIVersion == corev1alpha1.GroupVersion.String() && owner.Kind == taskKindString &&
		owner.Name == parent.Name && owner.UID == parent.UID
}

func repositoryValidationCommandSecretMetadataMatches(parent *corev1alpha1.Task, secret *corev1.Secret, binding *RepositoryValidationCommandBinding) bool {
	return secret.Labels[labels.LabelManaged] == trueStr &&
		secret.Labels[labels.LabelCreatedBy] == repositoryValidationCreatedBy &&
		secret.Labels[labels.LabelPurpose] == repositoryValidationPurpose &&
		secret.Labels[labels.LabelParentTask] == labels.SelectorValue(parent.Name) &&
		secret.Annotations[labels.AnnotationParentTaskName] == parent.Name &&
		secret.Annotations[labels.AnnotationParentTaskUID] == string(parent.UID) &&
		secret.Annotations[labels.AnnotationRepositoryMonitorName] == binding.MonitorName &&
		secret.Annotations[labels.AnnotationMonitorHeadSHA] == binding.HeadSHA &&
		secret.Annotations[labels.AnnotationRepositoryValidationImage] == binding.Image
}

func repositoryValidationCommandSecretDataMatches(secret *corev1.Secret, binding *RepositoryValidationCommandBinding) bool {
	command := string(secret.Data[RepositoryValidationCommandSecretKey])
	return len(secret.Data) == 1 && command != "" && command == strings.TrimSpace(command) &&
		len(command) <= workerenv.RepositoryValidationMaxCommandBytes && utf8.ValidString(command) && strings.IndexByte(command, 0) < 0 &&
		binding.MatchesCommand(command)
}

// RepositoryValidationCommandBindingEvent returns the append-only event that
// binds a review Task to the originally requested validation command.
func RepositoryValidationCommandBindingEvent(parent *corev1alpha1.Task, monitor *corev1alpha1.RepositoryMonitor, validationTask *corev1alpha1.Task, image, headSHA, command string) (*store.MonitorEvent, error) {
	if parent == nil || monitor == nil || validationTask == nil {
		return nil, fmt.Errorf("review task, repository monitor, and validation task are required")
	}
	image = strings.TrimSpace(image)
	headSHA = strings.TrimSpace(headSHA)
	command = strings.TrimSpace(command)
	commandDigest := RepositoryValidationCommandDigest(command)
	runID := strings.TrimSpace(parent.Annotations[labels.AnnotationMonitorRunID])
	itemKind := strings.TrimSpace(parent.Annotations[labels.AnnotationMonitorItemKind])
	itemNumberText := strings.TrimSpace(parent.Annotations[labels.AnnotationMonitorItemNumber])
	itemNumber, err := strconv.ParseInt(itemNumberText, 10, 64)
	if parent.Namespace == "" || parent.Name == "" || parent.UID == "" ||
		monitor.Namespace != parent.Namespace || monitor.Name == "" || monitor.UID == "" ||
		validationTask.Namespace != parent.Namespace || validationTask.Name != RepositoryValidationTaskName(parent) ||
		validationTask.Annotations[labels.AnnotationRepositoryValidationCommandDigest] != commandDigest ||
		image == "" || headSHA == "" || command == "" || runID == "" || itemKind == "" || itemNumber <= 0 || err != nil {
		return nil, fmt.Errorf("repository validation command binding identity is incomplete")
	}
	binding := repositoryValidationCommandBinding{
		MonitorUID:         string(monitor.UID),
		ReviewTaskName:     parent.Name,
		ReviewTaskUID:      string(parent.UID),
		ValidationTaskName: validationTask.Name,
		Image:              image,
		HeadSHA:            headSHA,
		CommandDigest:      commandDigest,
	}
	metadata, err := json.Marshal(binding)
	if err != nil {
		return nil, fmt.Errorf("encode repository validation command binding: %w", err)
	}
	return &store.MonitorEvent{
		ID:               repositoryValidationCommandBindingEventID(parent.Namespace, validationTask.Name),
		MonitorNamespace: monitor.Namespace,
		MonitorName:      monitor.Name,
		RunID:            runID,
		ItemKind:         itemKind,
		ItemNumber:       itemNumber,
		ItemSHA:          headSHA,
		EventType:        repositoryValidationBindingEventType,
		Actor:            repositoryValidationBindingActor,
		Summary:          repositoryValidationBindingSummary,
		MetadataJSON:     string(metadata),
	}, nil
}

// RepositoryValidationCommandBindingFilter returns the narrow lookup used for
// one command-binding event.
func RepositoryValidationCommandBindingFilter(event *store.MonitorEvent) store.MonitorEventFilter {
	if event == nil {
		return store.MonitorEventFilter{Limit: 1}
	}
	return store.MonitorEventFilter{
		Namespace:   event.MonitorNamespace,
		ID:          event.ID,
		MonitorName: event.MonitorName,
		RunID:       event.RunID,
		ItemKind:    event.ItemKind,
		ItemNumber:  event.ItemNumber,
		EventType:   event.EventType,
		Limit:       1,
	}
}

func repositoryValidationCommandBindingEventID(namespace, validationTaskName string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(namespace) + "\x00" + strings.TrimSpace(validationTaskName)))
	return "mevt-validation-" + hex.EncodeToString(digest[:16])
}

// RepositoryValidationCommandBindingMatches reports whether a stored event
// contains the exact controller-generated binding.
func RepositoryValidationCommandBindingMatches(existing, expected *store.MonitorEvent) bool {
	if existing == nil || expected == nil || existing.ID != expected.ID ||
		existing.MonitorNamespace != expected.MonitorNamespace || existing.MonitorName != expected.MonitorName ||
		existing.RunID != expected.RunID || existing.ItemKind != expected.ItemKind || existing.ItemNumber != expected.ItemNumber ||
		existing.ItemSHA != expected.ItemSHA || existing.EventType != expected.EventType || existing.Actor != expected.Actor ||
		existing.Summary != expected.Summary {
		return false
	}
	var existingBinding, expectedBinding repositoryValidationCommandBinding
	if json.Unmarshal([]byte(existing.MetadataJSON), &existingBinding) != nil || json.Unmarshal([]byte(expected.MetadataJSON), &expectedBinding) != nil {
		return false
	}
	return existingBinding == expectedBinding
}

// RepositoryValidationCommandDigest returns the durable digest used to bind a
// reviewer-selected command to its validation Task.
func RepositoryValidationCommandDigest(command string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(command)))
	return "sha256:" + hex.EncodeToString(digest[:])
}

// ValidateRepositoryValidationCommandBinding verifies that the durable event
// still binds the validation Task to the command accepted by run_validation.
func ValidateRepositoryValidationCommandBinding(ctx context.Context, bindingStore RepositoryValidationBindingStore, expected *store.MonitorEvent) error {
	if bindingStore == nil || expected == nil {
		return errRepositoryValidationBindingMissing
	}
	filter := RepositoryValidationCommandBindingFilter(expected)
	for {
		events, cursor, err := bindingStore.ListMonitorEvents(ctx, filter)
		if err != nil {
			return fmt.Errorf("load repository validation command binding: %w", err)
		}
		for i := range events {
			if events[i].ID != expected.ID {
				continue
			}
			if RepositoryValidationCommandBindingMatches(&events[i], expected) {
				return nil
			}
			return errRepositoryValidationBindingConflict
		}
		if cursor == "" {
			return errRepositoryValidationBindingMissing
		}
		filter.Cursor = cursor
	}
}

func ensureRepositoryValidationCommandBinding(ctx context.Context, bindingStore RepositoryValidationBindingStore, expected *store.MonitorEvent) error {
	if bindingStore == nil || expected == nil {
		return fmt.Errorf("repository validation command binding store is unavailable")
	}
	createErr := bindingStore.CreateMonitorEvent(ctx, expected)
	if createErr == nil {
		return nil
	}
	verifyErr := ValidateRepositoryValidationCommandBinding(ctx, bindingStore, expected)
	if verifyErr == nil || errors.Is(verifyErr, errRepositoryValidationBindingConflict) {
		return verifyErr
	}
	return fmt.Errorf("persist repository validation command binding: %v; verification failed: %w", createErr, verifyErr)
}
