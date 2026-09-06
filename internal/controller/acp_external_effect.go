package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	publisherservice "github.com/orka-agents/orka/internal/publisher/service"
	"github.com/orka-agents/orka/internal/store"
)

// externalEffectLeaseSettlementMargin is the slack an effect lease keeps beyond
// its bounded call duration so a response returned at the deadline can still be
// committed under a valid lease.
const externalEffectLeaseSettlementMargin = time.Minute

// maxACPExternalEffectCallDuration bounds one in-flight brokered custom-Tool
// call so its outcome can always be committed while the ledger lease is still
// valid. Without this bound, a call that outlives the lease plus the
// reconciliation grace period is marked OutcomeUnknown by
// reconcileExpiredExternalEffects, and a late upstream success can never be
// settled. Publisher-backed effects instead honor the configured publish
// timeout (envACPPublisherEffectTimeout) and size their lease from it, so the
// invariant lease > call duration holds for every effect kind.
const maxACPExternalEffectCallDuration = 4 * time.Minute

// envACPPublisherEffectTimeout mirrors the Workspace/Publisher's
// ORKA_PUBLISHER_PUBLISH_TIMEOUT on the controller. When the publisher is
// configured with a publish timeout above the default external-effect call
// bound, set the same value on the controller so publisher-backed effects and
// their ledger leases are sized to the real operation deadline instead of
// truncating it.
const envACPPublisherEffectTimeout = "ORKA_PUBLISHER_PUBLISH_TIMEOUT"

// externalEffectCallTimeout returns the bounded call duration for one external
// effect. Brokered custom-Tool calls keep the fixed clamp their descriptors
// were admitted under (customACPMCPToolDescriptor rejects longer HTTP
// timeouts); publisher-backed effects honor the controller-visible publish
// timeout so legitimately configured long publisher operations are not
// truncated. The claimed lease is always sized as call timeout plus settlement
// margin, so no effect can run past its own lease.
func externalEffectCallTimeout(identity store.ExternalEffectIdentity) time.Duration {
	if !publisherBackedExternalEffectKind(identity.Kind) {
		return maxACPExternalEffectCallDuration
	}
	raw := strings.TrimSpace(os.Getenv(envACPPublisherEffectTimeout))
	if raw == "" {
		return maxACPExternalEffectCallDuration
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed <= 0 {
		return maxACPExternalEffectCallDuration
	}
	return parsed
}

func publisherBackedExternalEffectKind(kind string) bool {
	return strings.HasPrefix(kind, "publisher.") || strings.HasPrefix(kind, "workspace.")
}

var externalEffectLeaseSequence atomic.Uint64

// runACPExternalEffect persists a canonical pre-execution identity before one
// idempotent publisher/broker operation. A committed response is replayed from
// the ledger; an in-flight identity is reclaimed only under the current epoch
// and exact request digest.
func runACPExternalEffect[T any](
	ctx context.Context,
	d *ACPDispatcher,
	fence store.ControllerEpochFence,
	identity store.ExternalEffectIdentity,
	request any,
	call func(context.Context) (T, error),
) (T, error) {
	return runExternalEffect(ctx, d.Store, fence, identity, request, call)
}

func runExternalEffect[T any](
	ctx context.Context,
	effects store.ExternalEffectStore,
	fence store.ControllerEpochFence,
	identity store.ExternalEffectIdentity,
	request any,
	call func(context.Context) (T, error),
) (T, error) {
	response, _, err := runExternalEffectWithReplay(ctx, effects, fence, identity, request, call)
	return response, err
}

func runExternalEffectWithReplay[T any](
	ctx context.Context,
	effects store.ExternalEffectStore,
	fence store.ControllerEpochFence,
	identity store.ExternalEffectIdentity,
	request any,
	call func(context.Context) (T, error),
) (T, bool, error) {
	return runExternalEffectWithReplayCallTimeout(
		ctx, effects, fence, identity, request, externalEffectCallTimeout(identity), call,
	)
}

// runExternalEffectWithReplayCallTimeout runs one external effect with an
// explicit bounded call duration. Callers that know the operation's real
// configured deadline (for example a brokered Tool's spec.http.timeout) use it
// so the effect call and the ledger lease — always sized as call timeout plus
// settlement margin — cover the full legitimate operation instead of the
// per-kind default clamp.
func runExternalEffectWithReplayCallTimeout[T any](
	ctx context.Context,
	effects store.ExternalEffectStore,
	fence store.ControllerEpochFence,
	identity store.ExternalEffectIdentity,
	request any,
	callTimeout time.Duration,
	call func(context.Context) (T, error),
) (T, bool, error) {
	var zero T
	if effects == nil {
		return zero, false, fmt.Errorf("external-effect store is required")
	}
	if callTimeout <= 0 {
		return zero, false, fmt.Errorf("external-effect call timeout must be positive")
	}
	requestDigest, err := acpDomainDigest("external-effect-request", map[string]any{
		"identity": identity, "request": request,
	})
	if err != nil {
		return zero, false, err
	}
	now := time.Now().UTC()
	effect, err := effects.ReserveExternalEffect(ctx, store.ReserveExternalEffectRequest{
		Identity: identity, RequestDigest: requestDigest, Fence: fence, CreatedAt: now,
	})
	if err != nil {
		return zero, false, err
	}
	if effect.State == store.ExternalEffectSucceeded {
		var response T
		if len(effect.Response) == 0 || json.Unmarshal(effect.Response, &response) != nil {
			return zero, false, fmt.Errorf("external effect %s has an invalid committed response", effect.ID)
		}
		return response, true, nil
	}
	if effect.State == store.ExternalEffectFailed || effect.State == store.ExternalEffectOutcomeUnknown {
		return zero, false, fmt.Errorf("external effect %s is terminal in state %s", effect.ID, effect.State)
	}
	// Size the lease from this effect's bounded call duration so the outcome of
	// a call that runs to its deadline can still be committed under a valid
	// lease, regardless of the configured per-kind timeout.
	leaseExpiry := now.Add(callTimeout + externalEffectLeaseSettlementMargin)
	leaseOwner := externalEffectLeaseOwner(fence, identity, now)
	claimed, err := effects.TransitionExternalEffect(ctx, store.ExternalEffectTransition{
		ID: effect.ID, Fence: fence, ExpectedVersion: effect.Version, ExpectedState: effect.State,
		NewState: store.ExternalEffectInFlight, RequestDigest: requestDigest,
		ExpectedLeaseOwner: effect.LeaseOwner, LeaseOwner: leaseOwner, LeaseExpiresAt: &leaseExpiry, UpdatedAt: now,
	})
	if err != nil {
		return zero, false, err
	}
	// Bound the call to the duration the claimed lease accounts for. A call
	// that ran past the lease plus the reconciliation grace period would be
	// classified OutcomeUnknown while still in flight, making a late success
	// permanently unsettleable.
	callCtx, cancelCall := context.WithTimeout(ctx, callTimeout)
	response, callErr := call(callCtx)
	// Capture the call context's error before cancelCall() so a deadline that
	// elapsed during the call is still observable (cancelCall would otherwise
	// overwrite it with context.Canceled).
	callCtxErr := callCtx.Err()
	cancelCall()
	if callErr != nil {
		// Leave the exact effect in-flight. The same identity/digest may be
		// reclaimed and classified by a later reconciliation; a different request
		// cannot reuse it.
		return zero, false, callErr
	}
	if callCtxErr != nil {
		// The effect implementation ignored cancellation and returned a result
		// after its bounded call deadline had already elapsed. The configured
		// tool/publisher timeout expired, so the response must not be committed
		// as Succeeded; leave the effect in-flight for explicit reconciliation.
		return zero, false, callCtxErr
	}
	if err := ctx.Err(); err != nil {
		// The side effect may have crossed its external boundary, but prompt
		// authority was revoked before a response could be committed. Leave the
		// ledger in-flight for explicit reconciliation rather than claiming success.
		return zero, false, err
	}
	encoded, err := json.Marshal(response)
	if err != nil {
		return zero, false, err
	}
	sum := sha256.Sum256(encoded)
	responseDigest := "sha256:" + hex.EncodeToString(sum[:])
	completed, err := effects.TransitionExternalEffect(ctx, store.ExternalEffectTransition{
		ID: claimed.ID, Fence: fence, ExpectedVersion: claimed.Version, ExpectedState: store.ExternalEffectInFlight,
		NewState: store.ExternalEffectSucceeded, RequestDigest: requestDigest,
		ResponseDigest: responseDigest, Response: encoded, ExpectedLeaseOwner: leaseOwner,
		UpdatedAt: time.Now().UTC(),
	})
	if err != nil {
		return zero, false, err
	}
	if completed.State != store.ExternalEffectSucceeded {
		return zero, false, fmt.Errorf("external effect %s did not commit success", completed.ID)
	}
	return response, false, nil
}

func externalEffectLeaseOwner(fence store.ControllerEpochFence, identity store.ExternalEffectIdentity, now time.Time) string {
	sequence := externalEffectLeaseSequence.Add(1)
	sum := sha256.Sum256(fmt.Appendf(nil, "%s\x00%d\x00%s\x00%s\x00%d", fence.HolderID, fence.Epoch, identity.OperationID, now.UTC().Format(time.RFC3339Nano), sequence))
	return "effect-" + hex.EncodeToString(sum[:16])
}

const defaultACPExternalEffectRetryDelay = 5 * time.Second

// runACPExternalEffectWithRetry retries the same immutable external-effect
// operation while retaining its controller-side lease until it commits, a
// non-retryable response is proven, or the caller's bounded reconciliation
// context expires. Publisher operations are idempotent by operation ID, and
// prompt input is never replayed.
func runACPExternalEffectWithRetry[T any](
	ctx context.Context,
	d *ACPDispatcher,
	fence store.ControllerEpochFence,
	identity store.ExternalEffectIdentity,
	request any,
	call func(context.Context) (T, error),
) (T, error) {
	return runACPExternalEffectWithRetryPolicy(
		ctx, d, fence, identity, request,
		defaultACPExternalEffectRetryDelay, externalEffectCallTimeout(identity), call,
	)
}

func runACPExternalEffectWithRetryPolicy[T any](
	ctx context.Context,
	d *ACPDispatcher,
	fence store.ControllerEpochFence,
	identity store.ExternalEffectIdentity,
	request any,
	retryDelay time.Duration,
	retryBudget time.Duration,
	call func(context.Context) (T, error),
) (T, error) {
	var zero T
	if retryDelay <= 0 {
		return zero, fmt.Errorf("external-effect retry delay must be positive")
	}
	if retryBudget <= 0 || retryBudget > externalEffectCallTimeout(identity) {
		return zero, fmt.Errorf("external-effect retry budget must be positive and leave the required lease settlement margin")
	}
	return runACPExternalEffect(ctx, d, fence, identity, request, func(callCtx context.Context) (T, error) {
		retryCtx, cancel := context.WithTimeout(callCtx, retryBudget)
		defer cancel()
		for {
			value, err := call(retryCtx)
			if retryErr := retryCtx.Err(); retryErr != nil {
				return zero, fmt.Errorf("bounded external-effect reconciliation expired: %w", retryErr)
			}
			if err == nil {
				return value, nil
			}
			if !retryableACPExternalEffectError(err) {
				return zero, err
			}
			timer := time.NewTimer(retryDelay)
			select {
			case <-retryCtx.Done():
				timer.Stop()
				return zero, fmt.Errorf("bounded external-effect reconciliation expired: %w", retryCtx.Err())
			case <-timer.C:
			}
		}
	})
}

func retryableACPExternalEffectError(err error) bool {
	if err == nil {
		return false
	}
	if clientErr, ok := errors.AsType[*publisherservice.ClientError](err); ok {
		return clientErr.Response.Retryable || clientErr.StatusCode == http.StatusTooManyRequests || clientErr.StatusCode >= 500
	}
	if errors.Is(err, store.ErrConflict) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	// Transport failures intentionally do not expose a typed upstream response.
	// Their side-effect outcome is ambiguous and must be reconciled with the same
	// durable identity rather than classified as a deterministic request error.
	return true
}

func settleACPExternalEffect(
	ctx context.Context,
	d *ACPDispatcher,
	fence store.ControllerEpochFence,
	identity store.ExternalEffectIdentity,
	state store.ExternalEffectState,
	response any,
) error {
	return settleExternalEffectStore(ctx, d.Store, fence, identity, state, response)
}

func settleExternalEffectStore(
	ctx context.Context,
	effects store.ExternalEffectStore,
	fence store.ControllerEpochFence,
	identity store.ExternalEffectIdentity,
	state store.ExternalEffectState,
	response any,
) error {
	id, err := identity.CanonicalID()
	if err != nil {
		return err
	}
	effect, err := effects.GetExternalEffect(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		return err
	}
	if effect.State == state {
		return nil
	}
	if effect.State == store.ExternalEffectSucceeded || effect.State == store.ExternalEffectFailed || effect.State == store.ExternalEffectOutcomeUnknown {
		return fmt.Errorf("external effect %s is already terminal in state %s", effect.ID, effect.State)
	}
	var encoded json.RawMessage
	responseDigest := ""
	if response != nil {
		value, marshalErr := json.Marshal(response)
		if marshalErr != nil {
			return marshalErr
		}
		encoded = value
		sum := sha256.Sum256(value)
		responseDigest = "sha256:" + hex.EncodeToString(sum[:])
	}
	_, err = effects.TransitionExternalEffect(ctx, store.ExternalEffectTransition{
		ID: effect.ID, Fence: fence, ExpectedVersion: effect.Version, ExpectedState: effect.State,
		NewState: state, RequestDigest: effect.RequestDigest, ResponseDigest: responseDigest, Response: encoded,
		ExpectedLeaseOwner: effect.LeaseOwner, UpdatedAt: time.Now().UTC(),
	})
	return err
}

func settleACPExternalEffectError(
	ctx context.Context,
	d *ACPDispatcher,
	fence store.ControllerEpochFence,
	identity store.ExternalEffectIdentity,
	callErr error,
) error {
	state := store.ExternalEffectOutcomeUnknown
	var clientErr *publisherservice.ClientError
	if errors.As(callErr, &clientErr) && !clientErr.Response.Retryable && clientErr.StatusCode != http.StatusTooManyRequests && clientErr.StatusCode < 500 {
		state = store.ExternalEffectFailed
	}
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return settleACPExternalEffect(settleCtx, d, fence, identity, state, nil)
}

const acpExternalEffectReconcileGrace = time.Minute

func (d *ACPDispatcher) reconcileExpiredExternalEffects(ctx context.Context) error {
	if d.Client == nil || d.Store == nil || d.Epochs == nil {
		return nil
	}
	fence, err := d.Epochs.CurrentFence(ctx)
	if err != nil {
		return err
	}
	var effects corev1alpha1.ExternalEffectList
	if err := d.Client.List(ctx, &effects); err != nil {
		return err
	}
	now := time.Now().UTC()
	for i := range effects.Items {
		effect := &effects.Items[i]
		if store.ExternalEffectState(effect.Status.State) != store.ExternalEffectInFlight || effect.Status.LeaseExpiresAt == nil ||
			now.Before(effect.Status.LeaseExpiresAt.Add(acpExternalEffectReconcileGrace)) {
			continue
		}
		_, err := d.Store.TransitionExternalEffect(ctx, store.ExternalEffectTransition{
			ID: effect.Spec.ID, Fence: fence, ExpectedVersion: effect.Status.Version,
			ExpectedState: store.ExternalEffectInFlight, NewState: store.ExternalEffectOutcomeUnknown,
			RequestDigest: effect.Spec.RequestDigest, ExpectedLeaseOwner: effect.Status.LeaseOwner, UpdatedAt: now,
		})
		if err != nil && !errors.Is(err, store.ErrConflict) {
			return fmt.Errorf("reconcile expired external effect %s/%s: %w", effect.Namespace, effect.Name, err)
		}
	}
	return nil
}
