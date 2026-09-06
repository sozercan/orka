/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"context"
	"fmt"

	"strings"

	fiber "github.com/gofiber/fiber/v3"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
	"github.com/orka-agents/orka/internal/llm"
)

// ProviderResolver resolves LLM providers from Kubernetes CRDs and secrets.
// It consolidates provider lookup, API key resolution, and provider construction
// logic shared by the chat, OpenAI-compat, and Anthropic-compat handlers.
type ProviderResolver struct {
	client client.Client
	config ChatConfig
}

// ResolveOpts configures how a provider is resolved.
type ResolveOpts struct {
	ProviderName string // explicit provider name (chat handler)
	ModelStr     string // "provider/model" format or plain model (compat handlers)
	Model        string // explicit model override (chat handler)
	AgentRef     string // agent reference for agent-based resolution (chat handler)
	Namespace    string
	RequireModel bool // return error if model is empty after resolution
	// RequireExplicitProvider disables every implicit Provider selection,
	// including configured, default-named, and sole-ready Providers. Handlers
	// set it for context-token requests so every request without an explicit
	// Provider or Agent-bound Provider has the same outcome.
	RequireExplicitProvider bool
	// AuthorizeProviderReference runs on an explicitly named Provider before
	// its credential is read or any implicit selection happens. It receives
	// the loaded Provider's metadata (name, namespace, type) so type-based
	// grants apply; when the Provider does not exist it receives the name
	// only, and its rejection is returned in place of the not-found error so
	// missing and unauthorized names stay indistinguishable to scoped
	// callers.
	AuthorizeProviderReference func(ProviderResolutionInfo) error
	// AuthorizeProviderUse runs after the effective model is known but before
	// the Provider credential is read.
	AuthorizeProviderUse func(ProviderResolutionInfo, string) error
}

// ProviderResolutionInfo contains the Provider CRD metadata selected for a request.
type ProviderResolutionInfo struct {
	Name      string
	Namespace string
	Type      string
}

// NewProviderResolver creates a new ProviderResolver.
func NewProviderResolver(c client.Client, config ChatConfig) *ProviderResolver {
	return &ProviderResolver{client: c, config: config}
}

// Resolve finds the appropriate LLM provider and model for a request.
// It handles provider/model string parsing, Kubernetes secret resolution,
// and provider construction.
//
// When AgentRef or ProviderName is set, provider lookups are fatal (errors are returned).
// Otherwise, intermediate lookups are non-fatal and fall through to defaults.
func (r *ProviderResolver) Resolve(ctx context.Context, opts ResolveOpts) (llm.Provider, string, error) {
	provider, model, _, err := r.ResolveWithInfo(ctx, opts)
	return provider, model, err
}

// ResolveWithInfo finds the appropriate LLM provider and model for a request,
// and also returns the selected Provider CRD metadata.
func (r *ProviderResolver) ResolveWithInfo(ctx context.Context, opts ResolveOpts) (llm.Provider, string, ProviderResolutionInfo, error) {
	if opts.AgentRef != "" || opts.ProviderName != "" {
		return r.resolveFromExplicit(ctx, opts)
	}
	return r.resolveFromModelStr(ctx, opts)
}

// resolveFromExplicit handles the chat handler path with explicit provider names
// and agent refs. Provider lookups by name are fatal.
func (r *ProviderResolver) resolveFromExplicit(ctx context.Context, opts ResolveOpts) (llm.Provider, string, ProviderResolutionInfo, error) {
	var model string
	var providerCRD *corev1alpha1.Provider
	var providerName string

	if opts.AgentRef != "" {
		agent := &corev1alpha1.Agent{}
		if err := r.client.Get(ctx, types.NamespacedName{Name: opts.AgentRef, Namespace: opts.Namespace}, agent); err != nil {
			return nil, "", ProviderResolutionInfo{}, fmt.Errorf("agent %q not found: %w", opts.AgentRef, err)
		}
		if agent.Spec.Model != nil && agent.Spec.Model.Name != "" {
			model = agent.Spec.Model.Name
		}
		if agent.Spec.ProviderRef != nil {
			providerName = agent.Spec.ProviderRef.Name
		}
	}

	if opts.ProviderName != "" {
		if providerName != "" && providerName != opts.ProviderName {
			// The bound Provider's name is withheld: this error can reach a
			// context token before provider-use authorization runs, and it
			// must not become an enumeration path around the
			// authorization-aware Providers API.
			return nil, "", ProviderResolutionInfo{}, fmt.Errorf("agent %q is bound to a different provider; omit the provider or choose an agent without a providerRef", opts.AgentRef)
		}
		providerName = opts.ProviderName
	}

	if providerName != "" {
		p, err := r.LookupProvider(ctx, providerName, opts.Namespace)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Masking: a name the caller may not reference gets the same
				// answer whether or not the Provider exists. Only the name is
				// known here, so a type-based grant cannot apply; a caller
				// holding one sees a missing Provider and a wrong-type
				// Provider identically.
				if authErr := authorizeProviderReference(opts, ProviderResolutionInfo{Name: providerName, Namespace: opts.Namespace}); authErr != nil {
					return nil, "", ProviderResolutionInfo{}, authErr
				}
			}
			return nil, "", ProviderResolutionInfo{}, err
		}
		// The loaded Provider carries its type, so type-based grants
		// (allowedProviders naming a provider type) authorize here exactly as
		// they do for the Providers list.
		if err := authorizeProviderReference(opts, providerResolutionInfo(p)); err != nil {
			return nil, "", ProviderResolutionInfo{}, err
		}
		providerCRD = p
	}
	if providerCRD == nil && opts.RequireExplicitProvider {
		return nil, "", ProviderResolutionInfo{}, explicitProviderRequiredError()
	}

	if providerCRD == nil && r.config.Provider != "" {
		p, err := r.LookupProvider(ctx, r.config.Provider, opts.Namespace)
		if err != nil {
			return nil, "", ProviderResolutionInfo{}, err
		}
		providerCRD = p
	}

	if providerCRD == nil {
		p, err := r.LookupProvider(ctx, "default", opts.Namespace)
		if err != nil {
			// Runtime Agents carry no providerRef; the coordinator still needs
			// a chat Provider, so apply the same sole-ready fallback as the
			// model-string path instead of demanding one named "default".
			fallback, fallbackErr := r.soleReadyProvider(ctx, opts.Namespace, "", opts.RequireExplicitProvider)
			if fallbackErr != nil {
				return nil, "", ProviderResolutionInfo{}, fallbackErr
			}
			p = fallback
		}
		providerCRD = p
	}

	// Model resolution priority: opts.Model > agent model > provider default > config model
	if opts.Model != "" {
		model = opts.Model
	}
	if model == "" {
		model = providerCRD.Spec.DefaultModel
	}
	if model == "" {
		model = r.config.Model
	}
	providerInfo := providerResolutionInfo(providerCRD)
	if err := authorizeProviderUse(opts, providerInfo, model); err != nil {
		return nil, "", ProviderResolutionInfo{}, err
	}

	apiKey, err := r.ResolveAPIKey(ctx, providerCRD)
	if err != nil {
		return nil, "", ProviderResolutionInfo{}, err
	}

	provider, resolvedModel, err := r.buildProvider(providerCRD, apiKey, model)
	if err != nil {
		return nil, "", ProviderResolutionInfo{}, err
	}

	return provider, resolvedModel, providerInfo, nil
}

// resolveFromModelStr handles the compat handler path with "provider/model"
// format strings. An explicit provider prefix is authoritative and fails
// closed; fallback applies only when the model string names no provider.
func (r *ProviderResolver) resolveFromModelStr(ctx context.Context, opts ResolveOpts) (llm.Provider, string, ProviderResolutionInfo, error) {
	var providerName, model string

	if idx := strings.Index(opts.ModelStr, "/"); idx > 0 {
		providerName = opts.ModelStr[:idx]
		model = opts.ModelStr[idx+1:]
	} else {
		model = opts.ModelStr
	}

	var providerCRD *corev1alpha1.Provider

	if providerName != "" {
		p, err := r.LookupProvider(ctx, providerName, opts.Namespace)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Masking: a name the caller may not reference gets the same
				// answer whether or not the Provider exists. Only the name is
				// known here, so a type-based grant cannot apply; a caller
				// holding one sees a missing Provider and a wrong-type
				// Provider identically.
				if authErr := authorizeProviderReference(opts, ProviderResolutionInfo{Name: providerName, Namespace: opts.Namespace}); authErr != nil {
					return nil, "", ProviderResolutionInfo{}, authErr
				}
			}
			return nil, "", ProviderResolutionInfo{}, err
		}
		// The loaded Provider carries its type, so type-based grants
		// (allowedProviders naming a provider type) authorize here exactly as
		// they do for the Providers list.
		if err := authorizeProviderReference(opts, providerResolutionInfo(p)); err != nil {
			return nil, "", ProviderResolutionInfo{}, err
		}
		providerCRD = p
	}
	if providerCRD == nil && opts.RequireExplicitProvider {
		return nil, "", ProviderResolutionInfo{}, explicitProviderRequiredError()
	}

	if providerCRD == nil && r.config.Provider != "" {
		p := &corev1alpha1.Provider{}
		if err := r.client.Get(ctx, types.NamespacedName{Name: r.config.Provider, Namespace: opts.Namespace}, p); err == nil {
			providerCRD = p
		}
	}

	if providerCRD == nil {
		p := &corev1alpha1.Provider{}
		if err := r.client.Get(ctx, types.NamespacedName{Name: "default", Namespace: opts.Namespace}, p); err == nil {
			providerCRD = p
		}
	}
	if providerCRD == nil {
		fallback, err := r.soleReadyProvider(ctx, opts.Namespace, providerName, opts.RequireExplicitProvider)
		if err != nil {
			return nil, "", ProviderResolutionInfo{}, err
		}
		providerCRD = fallback
	}

	if model == "" {
		model = providerCRD.Spec.DefaultModel
	}
	if model == "" {
		model = r.config.Model
	}
	if opts.RequireModel && model == "" {
		return nil, "", ProviderResolutionInfo{}, fmt.Errorf("no model specified and no default model configured")
	}
	providerInfo := providerResolutionInfo(providerCRD)
	if err := authorizeProviderUse(opts, providerInfo, model); err != nil {
		return nil, "", ProviderResolutionInfo{}, err
	}

	apiKey, err := r.ResolveAPIKey(ctx, providerCRD)
	if err != nil {
		return nil, "", ProviderResolutionInfo{}, err
	}

	provider, resolvedModel, err := r.buildProvider(providerCRD, apiKey, model)
	if err != nil {
		return nil, "", ProviderResolutionInfo{}, err
	}

	return provider, resolvedModel, providerInfo, nil
}

func authorizeProviderReference(opts ResolveOpts, provider ProviderResolutionInfo) error {
	if opts.AuthorizeProviderReference == nil {
		return nil
	}
	return opts.AuthorizeProviderReference(provider)
}

func authorizeProviderUse(opts ResolveOpts, provider ProviderResolutionInfo, model string) error {
	if opts.AuthorizeProviderUse == nil {
		return nil
	}
	return opts.AuthorizeProviderUse(provider, model)
}

func explicitProviderRequiredError() error {
	return fmt.Errorf("no provider selected and no default Provider is configured; pass an explicit provider")
}

// providerResolutionInfo extracts stable metadata from a Provider CRD.
func providerResolutionInfo(providerCRD *corev1alpha1.Provider) ProviderResolutionInfo {
	if providerCRD == nil {
		return ProviderResolutionInfo{}
	}
	return ProviderResolutionInfo{
		Name:      providerCRD.Name,
		Namespace: providerCRD.Namespace,
		Type:      string(providerCRD.Spec.Type),
	}
}

// buildProvider constructs an LLM provider from a Provider CRD, API key, and model.
func (r *ProviderResolver) buildProvider(providerCRD *corev1alpha1.Provider, apiKey, model string) (llm.Provider, string, error) {
	providerConfig := llm.ProviderConfig{
		APIKey:       apiKey,
		BaseURL:      providerCRD.Spec.BaseURL,
		ProviderType: string(providerCRD.Spec.Type),
	}
	if providerCRD.Spec.Azure != nil {
		providerConfig.AzureAPIVersion = providerCRD.Spec.Azure.APIVersion
	}

	provider, err := llm.NewProvider(string(providerCRD.Spec.Type), providerConfig)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create LLM provider: %w", err)
	}

	return provider, model, nil
}

// LookupProvider fetches a Provider CRD by name and namespace.
func (r *ProviderResolver) LookupProvider(ctx context.Context, name, namespace string) (*corev1alpha1.Provider, error) {
	p := &corev1alpha1.Provider{}
	if err := r.client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, p); err != nil {
		return nil, fmt.Errorf("provider %q not found in namespace %q: %w", name, namespace, err)
	}
	return p, nil
}

// ResolveAPIKey extracts the API key from a Provider CRD's secret reference.
func (r *ProviderResolver) ResolveAPIKey(ctx context.Context, providerCRD *corev1alpha1.Provider) (string, error) {
	secretName := providerCRD.Spec.SecretRef.Name
	secretKey := providerCRD.Spec.SecretRef.Key
	if secretKey == "" {
		secretKey = "api-key"
	}
	secret := &corev1.Secret{}
	if err := r.client.Get(ctx, types.NamespacedName{Name: secretName, Namespace: providerCRD.Namespace}, secret); err != nil {
		return "", fmt.Errorf("failed to get provider secret %q: %w", secretName, err)
	}
	apiKeyBytes, ok := secret.Data[secretKey]
	if !ok {
		return "", fmt.Errorf("secret %q has no key %q", secretName, secretKey)
	}
	return string(apiKeyBytes), nil
}

// soleReadyProvider selects the namespace's only ready Provider when no
// provider was named and no default is configured: an unambiguous choice is
// friendlier than an error, and the caller's provider-use authorization still
// applies to the selected Provider. With zero or several candidates the error
// names what exists so the caller can choose.
func (r *ProviderResolver) soleReadyProvider(ctx context.Context, namespace, requestedName string, requireExplicitProvider bool) (*corev1alpha1.Provider, error) {
	list := &corev1alpha1.ProviderList{}
	if err := r.client.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("no provider %q found and no default Provider is configured (listing Providers failed: %w)", requestedName, err)
	}
	var ready []*corev1alpha1.Provider
	for i := range list.Items {
		item := &list.Items[i]
		if item.Status.Ready {
			ready = append(ready, item)
		}
	}
	if len(ready) == 1 && !requireExplicitProvider {
		return ready[0], nil
	}
	if requireExplicitProvider {
		// One uniform outcome for scoped callers, independent of how many
		// Providers exist or are ready.
		return nil, explicitProviderRequiredError()
	}
	prefix := "no provider selected"
	if requestedName != "" {
		prefix = fmt.Sprintf("no provider %q found", requestedName)
	}
	if len(list.Items) == 0 {
		return nil, fmt.Errorf("%s and namespace %q has no Providers; create a Provider (or one named \"default\") or configure the chat default provider", prefix, namespace)
	}
	// The error deliberately names and counts no Providers: resolution runs
	// with controller credentials before the caller's provider-use
	// authorization, and a scoped context token must not learn anything
	// about Providers outside its scopes from this message. The
	// authorization-aware list endpoints are the enumeration surface.
	return nil, fmt.Errorf("%s and no default Provider is configured; list the Providers you can use with the providers API and pass one, create a Provider named \"default\", or configure the chat default provider", prefix)
}

// requestUsesContextToken reports whether the request authenticated with a
// transaction context token, whose Provider visibility is scope-limited.
func requestUsesContextToken(c fiber.Ctx) bool {
	ui := GetUserInfo(c)
	return ui != nil && ui.AuthType == AuthTypeContextToken && ui.ContextToken != nil
}

func requestRequiresExplicitProvider(c fiber.Ctx, authorization ContextTokenAuthorizationConfig) bool {
	return authorization.enforcing() && requestUsesContextToken(c)
}
