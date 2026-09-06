// Package kube implements Kubernetes-authoritative ACP control records.
//
// Controller epochs and Session mutation ownership are serialized by
// coordination.k8s.io Leases. The CR status resourceVersion is the CAS token
// for all other logical state transitions. SessionTurn persistence remains a
// separate SQLite concern and may be supplied through WithSessionTurnPersistence.
package kube

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/orka-agents/orka/internal/store"
	"golang.org/x/sync/semaphore"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ErrSessionTurnStoreNotConfigured is returned by the SessionTurn methods when
// the Kubernetes control store was constructed without SQLite turn persistence.
var ErrSessionTurnStoreNotConfigured = errors.New("session turn persistence is not configured")

// ErrOutboxStoreNotConfigured is returned when the composite SQLite outbox
// persistence adapter was not configured.
var ErrOutboxStoreNotConfigured = errors.New("outbox persistence is not configured")

// ErrBranchClaimAccessDisabled is returned when a static harness v1
// installation reaches a publication-only BranchClaim path. Harness v1 must
// never observe or mutate the cluster-scoped claims owned by harness v2.
var ErrBranchClaimAccessDisabled = errors.New("cluster-scoped branch claim access is disabled")

// SQLitePersistence is the complete SQLite-owned half of the hard-cutover
// control store. The concrete sqlite.Store satisfies this interface without
// treating any SQLite control row as authoritative.
type SQLitePersistence interface {
	store.SessionTurnPersistenceStore
	store.OutboxPersistenceStore
	store.SessionCleanupPersistenceStore
}

// Option configures Store.
type Option func(*Store) error

// WithSessionTurnPersistence configures the narrow SQLite persistence adapter
// used for SessionTurn/transcript/deferred-outbox transactions.
func WithSessionTurnPersistence(turns store.SessionTurnPersistenceStore) Option {
	return func(s *Store) error {
		if turns == nil {
			return store.ValidationErrorf("session turn persistence must not be nil")
		}
		s.sessionTurns = turns
		return nil
	}
}

// WithHarnessV1Attempts supplies the route-specific receipt store used when a
// protocol-neutral SessionTurn references a harness v1 attempt rather than a
// v2 PromptAttempt. Kubernetes SessionControl and Lease state remain the
// mutation authority; this store is consulted only for immutable attempt
// identity and terminal-receipt validation.
func WithHarnessV1Attempts(attempts store.HarnessV1AttemptStore) Option {
	return func(s *Store) error {
		if attempts == nil {
			return store.ValidationErrorf("harness v1 attempt store must not be nil")
		}
		s.harnessV1Attempts = attempts
		return nil
	}
}

// WithAPIReader configures the uncached reader used for authoritative control
// records. Controller-runtime cached clients remain the writer, but must not be
// trusted for immediate read-after-write recovery decisions.
func WithAPIReader(reader client.Reader) Option {
	return func(s *Store) error {
		if reader == nil {
			return store.ValidationErrorf("Kubernetes API reader must not be nil")
		}
		s.reader = reader
		return nil
	}
}

// WithWatchNamespace confines namespaced control-record lookups to the static
// controller installation's immutable watch namespace. Cluster-scoped control
// records, such as BranchClaims, remain unaffected.
func WithWatchNamespace(namespace string) Option {
	return func(s *Store) error {
		namespace = strings.TrimSpace(namespace)
		if err := validateKubernetesNamespace(namespace); err != nil {
			return err
		}
		s.watchNamespace = namespace
		return nil
	}
}

// WithoutClusterScopedBranchClaims configures the publication-free harness v1
// control-store path. Session continuity remains Kubernetes-authoritative, but
// Session cleanup cannot list or mutate the cluster-scoped BranchClaim kind.
func WithoutClusterScopedBranchClaims() Option {
	return func(s *Store) error {
		s.branchClaimsEnabled = false
		return nil
	}
}

// WithOutboxPersistence configures the SQLite outbox adapter used behind the
// Kubernetes controller-epoch fence.
func WithOutboxPersistence(outbox store.OutboxPersistenceStore) Option {
	return func(s *Store) error {
		if outbox == nil {
			return store.ValidationErrorf("outbox persistence must not be nil")
		}
		s.outbox = outbox
		return nil
	}
}

// WithSessionCleanupPersistence configures the SQLite half of the fenced
// cross-store Session deletion protocol.
func WithSessionCleanupPersistence(cleanup store.SessionCleanupPersistenceStore) Option {
	return func(s *Store) error {
		if cleanup == nil {
			return store.ValidationErrorf("session cleanup persistence must not be nil")
		}
		s.sessionCleanup = cleanup
		return nil
	}
}

// Store maps ACP control-store interfaces to Kubernetes CR status and Leases.
type Store struct {
	client              client.Client
	reader              client.Reader
	controlNamespace    string
	watchNamespace      string
	sessionTurns        store.SessionTurnPersistenceStore
	harnessV1Attempts   store.HarnessV1AttemptStore
	outbox              store.OutboxPersistenceStore
	sessionCleanup      store.SessionCleanupPersistenceStore
	branchClaimsEnabled bool
	epochMutations      *semaphore.Weighted
}

// NewComposite constructs the hard-cutover DurableControlStore: Kubernetes is
// authoritative for controller/session/prompt/publication/branch state while
// persistence stores only SQLite-owned SessionTurn, transcript, and outbox data.
func NewComposite(kubeClient client.Client, controlNamespace string, persistence SQLitePersistence, options ...Option) (*Store, error) {
	if persistence == nil {
		return nil, store.ValidationErrorf("SQLite persistence store is required")
	}
	combined := make([]Option, 0, len(options)+3)
	combined = append(combined, WithSessionTurnPersistence(persistence), WithOutboxPersistence(persistence), WithSessionCleanupPersistence(persistence))
	if attempts, ok := persistence.(store.HarnessV1AttemptStore); ok {
		combined = append(combined, WithHarnessV1Attempts(attempts))
	}
	combined = append(combined, options...)
	return New(kubeClient, controlNamespace, combined...)
}

// New constructs a Kubernetes ACP control store. The caller must add the core
// v1alpha1 and coordination/v1 APIs to the client's scheme. controlNamespace
// contains the singleton ControllerEpoch record and its authoritative Lease.
func New(kubeClient client.Client, controlNamespace string, options ...Option) (*Store, error) {
	if kubeClient == nil {
		return nil, store.ValidationErrorf("Kubernetes client is required")
	}
	controlNamespace = strings.TrimSpace(controlNamespace)
	if err := validateKubernetesNamespace(controlNamespace); err != nil {
		return nil, err
	}
	result := &Store{
		client: kubeClient, controlNamespace: controlNamespace, branchClaimsEnabled: true,
		epochMutations: semaphore.NewWeighted(1),
	}
	for _, option := range options {
		if option == nil {
			return nil, store.ValidationErrorf("Kubernetes control-store option must not be nil")
		}
		if err := option(result); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (s *Store) requireBranchClaimAccess() error {
	if s == nil || !s.branchClaimsEnabled {
		return ErrBranchClaimAccessDisabled
	}
	return nil
}

// GetSessionTurn delegates SQLite-only SessionTurn reads.
func (s *Store) GetSessionTurn(ctx context.Context, id string) (*store.SessionTurn, error) {
	if s.sessionTurns == nil {
		return nil, ErrSessionTurnStoreNotConfigured
	}
	return s.sessionTurns.GetSessionTurn(ctx, id)
}

func (s *Store) readClient() client.Reader {
	if s != nil && s.reader != nil {
		return s.reader
	}
	if s == nil {
		return nil
	}
	return s.client
}

func (s *Store) namespacedListOptions(options ...client.ListOption) []client.ListOption {
	if s == nil || s.watchNamespace == "" {
		return options
	}
	return append(options, client.InNamespace(s.watchNamespace))
}

func (s *Store) requireClient() error {
	if s == nil || s.client == nil {
		return fmt.Errorf("kubernetes control store is not initialized")
	}
	return nil
}

var (
	_ store.ControllerEpochStore         = (*Store)(nil)
	_ store.PromptAttemptStore           = (*Store)(nil)
	_ store.SessionControlStore          = (*Store)(nil)
	_ store.SessionCleanupStore          = (*Store)(nil)
	_ store.SessionCleanupRecoveryStore  = (*Store)(nil)
	_ store.BranchClaimStore             = (*Store)(nil)
	_ store.PublicationStore             = (*Store)(nil)
	_ store.ExternalEffectStore          = (*Store)(nil)
	_ store.ExternalEffectIdentityReader = (*Store)(nil)
	_ store.OutboxProjectionStore        = (*Store)(nil)
	_ store.DurableControlStore          = (*Store)(nil)
)
