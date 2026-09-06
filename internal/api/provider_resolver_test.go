/*
Copyright (c) 2026.

MIT License - see LICENSE file for details.
*/

package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

type providerReadTrackingClient struct {
	client.Client
	providerReads int
	secretReads   int
}

func (c *providerReadTrackingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	switch obj.(type) {
	case *corev1alpha1.Provider:
		c.providerReads++
	case *corev1.Secret:
		c.secretReads++
	}
	return c.Client.Get(ctx, key, obj, opts...)
}

// helpers

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1alpha1.AddToScheme(s)
	_ = corev1.AddToScheme(s)
	return s
}

func makeProvider(name, ns string, ptype corev1alpha1.ProviderType, secretName, defaultModel string) *corev1alpha1.Provider { //nolint:unparam
	return &corev1alpha1.Provider{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1alpha1.ProviderSpec{
			Type:         ptype,
			DefaultModel: defaultModel,
			SecretRef:    corev1alpha1.ProviderSecretRef{Name: secretName},
		},
	}
}

func makeSecret(name, ns, key, value string) *corev1.Secret { //nolint:unparam
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data:       map[string][]byte{key: []byte(value)},
	}
}

func makeAgent(name, ns string, providerRef *corev1alpha1.ProviderReference, model *corev1alpha1.ModelConfig) *corev1alpha1.Agent {
	return &corev1alpha1.Agent{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: corev1alpha1.AgentSpec{
			ProviderRef: providerRef,
			Model:       model,
		},
	}
}

// Tests

const (
	testRuntimeAgentModel = "gpt-5.6-sol"
	testRuntimeAgentName  = "runtime-agent"
)

func TestProviderResolver_LookupProvider(t *testing.T) {
	provider := makeProvider("my-provider", "default", corev1alpha1.ProviderTypeOpenAI, "my-secret", "gpt-4")
	k := fake.NewClientBuilder().WithScheme(newScheme()).WithRuntimeObjects(provider).Build()
	r := NewProviderResolver(k, DefaultChatConfig())

	t.Run("found", func(t *testing.T) {
		p, err := r.LookupProvider(context.Background(), "my-provider", "default")
		require.NoError(t, err)
		assert.Equal(t, "my-provider", p.Name)
	})

	t.Run("not found", func(t *testing.T) {
		_, err := r.LookupProvider(context.Background(), "nonexistent", "default")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

func TestProviderResolver_ResolveAPIKey(t *testing.T) {
	provider := makeProvider("p", "default", corev1alpha1.ProviderTypeOpenAI, "my-secret", "")

	t.Run("default key name", func(t *testing.T) {
		secret := makeSecret("my-secret", "default", "api-key", "sk-test-123")
		k := fake.NewClientBuilder().WithScheme(newScheme()).WithRuntimeObjects(provider, secret).Build()
		r := NewProviderResolver(k, DefaultChatConfig())

		key, err := r.ResolveAPIKey(context.Background(), provider)
		require.NoError(t, err)
		assert.Equal(t, "sk-test-123", key)
	})

	t.Run("custom key name", func(t *testing.T) {
		p := &corev1alpha1.Provider{
			ObjectMeta: metav1.ObjectMeta{Name: "p2", Namespace: "default"},
			Spec: corev1alpha1.ProviderSpec{
				Type:      corev1alpha1.ProviderTypeOpenAI,
				SecretRef: corev1alpha1.ProviderSecretRef{Name: "custom-secret", Key: "token"},
			},
		}
		secret := makeSecret("custom-secret", "default", "token", "sk-custom")
		k := fake.NewClientBuilder().WithScheme(newScheme()).WithRuntimeObjects(p, secret).Build()
		r := NewProviderResolver(k, DefaultChatConfig())

		key, err := r.ResolveAPIKey(context.Background(), p)
		require.NoError(t, err)
		assert.Equal(t, "sk-custom", key)
	})

	t.Run("secret not found", func(t *testing.T) {
		k := fake.NewClientBuilder().WithScheme(newScheme()).Build()
		r := NewProviderResolver(k, DefaultChatConfig())

		_, err := r.ResolveAPIKey(context.Background(), provider)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get provider secret")
	})

	t.Run("secret missing key", func(t *testing.T) {
		secret := makeSecret("my-secret", "default", "wrong-key", "value")
		k := fake.NewClientBuilder().WithScheme(newScheme()).WithRuntimeObjects(provider, secret).Build()
		r := NewProviderResolver(k, DefaultChatConfig())

		_, err := r.ResolveAPIKey(context.Background(), provider)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `has no key "api-key"`)
	})
}

func TestProviderResolverMasksMissingAndDisallowedExplicitNames(t *testing.T) {
	// A denied reference gets the same error whether or not the Provider
	// exists, and no credential is read either way.
	denied := errors.New("provider reference denied")
	provider := makeProvider("hidden", "default", corev1alpha1.ProviderTypeOpenAI, "hidden-secret", "gpt-4o")
	secret := makeSecret("hidden-secret", "default", "api-key", "test-key")

	for _, tc := range []struct {
		name    string
		objects []runtime.Object
	}{
		{name: "existing provider", objects: []runtime.Object{provider, secret}},
		{name: "missing provider"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := fake.NewClientBuilder().WithScheme(newScheme()).WithRuntimeObjects(tc.objects...).Build()
			tracked := &providerReadTrackingClient{Client: base}
			resolver := NewProviderResolver(tracked, DefaultChatConfig())

			_, _, err := resolver.Resolve(context.Background(), ResolveOpts{
				ModelStr:  "hidden/gpt-4o",
				Namespace: "default",
				AuthorizeProviderReference: func(ProviderResolutionInfo) error {
					return denied
				},
			})
			require.ErrorIs(t, err, denied)
			require.Zero(t, tracked.secretReads)
		})
	}
}

func TestProviderResolverAuthorizesReferenceWithProviderType(t *testing.T) {
	// A grant by provider type ("openai") must admit a Provider named
	// differently ("prod") once its type is known, exactly as the Providers
	// list does; a Provider of another type and a missing name are both
	// denied with the same error.
	denied := errors.New("provider reference denied")
	byType := func(info ProviderResolutionInfo) error {
		if info.Type == string(corev1alpha1.ProviderTypeOpenAI) {
			return nil
		}
		return denied
	}
	prod := makeProvider("prod", "default", corev1alpha1.ProviderTypeOpenAI, "prod-secret", "gpt-4o")
	prodSecret := makeSecret("prod-secret", "default", "api-key", "test-key")
	other := makeProvider("other", "default", corev1alpha1.ProviderTypeAnthropic, "other-secret", "claude-sonnet-4-20250514")
	otherSecret := makeSecret("other-secret", "default", "api-key", "test-key")
	base := fake.NewClientBuilder().WithScheme(newScheme()).WithRuntimeObjects(prod, prodSecret, other, otherSecret).Build()
	resolver := NewProviderResolver(base, DefaultChatConfig())

	_, _, info, err := resolver.ResolveWithInfo(context.Background(), ResolveOpts{
		ProviderName:               "prod",
		Namespace:                  "default",
		AuthorizeProviderReference: byType,
	})
	require.NoError(t, err)
	require.Equal(t, "prod", info.Name)
	require.Equal(t, string(corev1alpha1.ProviderTypeOpenAI), info.Type)

	for _, name := range []string{"other", "missing"} {
		_, _, err := resolver.Resolve(context.Background(), ResolveOpts{
			ProviderName:               name,
			Namespace:                  "default",
			AuthorizeProviderReference: byType,
		})
		require.ErrorIs(t, err, denied, name)
	}
}

func TestProviderResolverAuthorizesUseBeforeReadingCredential(t *testing.T) {
	denied := errors.New("provider use denied")
	provider := makeProvider("hidden", "default", corev1alpha1.ProviderTypeOpenAI, "hidden-secret", "gpt-4o")
	secret := makeSecret("hidden-secret", "default", "api-key", "test-key")
	base := fake.NewClientBuilder().WithScheme(newScheme()).WithRuntimeObjects(provider, secret).Build()
	tracked := &providerReadTrackingClient{Client: base}
	resolver := NewProviderResolver(tracked, DefaultChatConfig())

	_, _, err := resolver.Resolve(context.Background(), ResolveOpts{
		ModelStr:  "hidden/gpt-4o",
		Namespace: "default",
		AuthorizeProviderUse: func(ProviderResolutionInfo, string) error {
			return denied
		},
	})
	require.ErrorIs(t, err, denied)
	require.Equal(t, 1, tracked.providerReads)
	require.Zero(t, tracked.secretReads)
}

func TestProviderResolver_Resolve(t *testing.T) {
	const (
		ns                 = "default"
		openaiProviderName = "openai"
	)

	// Shared objects
	openaiProvider := makeProvider(openaiProviderName, ns, corev1alpha1.ProviderTypeOpenAI, "openai-secret", "gpt-4")
	openaiSecret := makeSecret("openai-secret", ns, "api-key", "sk-openai")
	anthropicProvider := makeProvider("anthropic", ns, corev1alpha1.ProviderTypeAnthropic, "anthropic-secret", "claude-sonnet-4-20250514")
	anthropicSecret := makeSecret("anthropic-secret", ns, "api-key", "sk-anthropic")
	defaultProvider := makeProvider("default", ns, corev1alpha1.ProviderTypeOpenAI, "default-secret", "gpt-3.5-turbo")
	defaultSecret := makeSecret("default-secret", ns, "api-key", "sk-default")

	tests := []struct {
		name      string
		objects   []runtime.Object
		config    ChatConfig
		opts      ResolveOpts
		wantModel string
		wantErr   string
		wantPType string // provider Name()
	}{
		{
			name:    "explicit provider name",
			objects: []runtime.Object{openaiProvider, openaiSecret},
			config:  DefaultChatConfig(),
			opts: ResolveOpts{
				ProviderName:            openaiProviderName,
				Model:                   "gpt-4o",
				Namespace:               ns,
				RequireExplicitProvider: true,
			},
			wantModel: "gpt-4o",
			wantPType: openaiProviderName,
		},
		{
			name:    "explicit provider uses default model from CRD",
			objects: []runtime.Object{openaiProvider, openaiSecret},
			config:  DefaultChatConfig(),
			opts: ResolveOpts{
				ProviderName: openaiProviderName,
				Namespace:    ns,
			},
			wantModel: "gpt-4",
			wantPType: openaiProviderName,
		},
		{
			name:    "model str provider/model format",
			objects: []runtime.Object{anthropicProvider, anthropicSecret},
			config:  DefaultChatConfig(),
			opts: ResolveOpts{
				ModelStr:                "anthropic/claude-sonnet-4-20250514",
				Namespace:               ns,
				RequireExplicitProvider: true,
			},
			wantModel: "claude-sonnet-4-20250514",
			wantPType: "anthropic",
		},
		{
			name:    "model str explicit missing provider fails closed",
			objects: []runtime.Object{readyProvider(anthropicProvider), anthropicSecret},
			config:  DefaultChatConfig(),
			opts: ResolveOpts{
				ModelStr:  "missing/claude-sonnet-4-20250514",
				Namespace: ns,
			},
			wantErr: `provider "missing" not found`,
		},
		{
			name:    "model str plain model uses config default provider",
			objects: []runtime.Object{openaiProvider, openaiSecret},
			config: func() ChatConfig {
				c := DefaultChatConfig()
				c.Provider = openaiProviderName
				return c
			}(),
			opts: ResolveOpts{
				ModelStr:  "gpt-4o",
				Namespace: ns,
			},
			wantModel: "gpt-4o",
			wantPType: openaiProviderName,
		},
		{
			name:    "model str falls through to default provider CRD",
			objects: []runtime.Object{defaultProvider, defaultSecret},
			config:  DefaultChatConfig(),
			opts: ResolveOpts{
				ModelStr:  "some-model",
				Namespace: ns,
			},
			wantModel: "some-model",
			wantPType: openaiProviderName,
		},
		{
			name: "agent ref with provider and model",
			objects: []runtime.Object{
				openaiProvider, openaiSecret,
				makeAgent("my-agent", ns,
					&corev1alpha1.ProviderReference{Name: openaiProviderName},
					&corev1alpha1.ModelConfig{Name: "gpt-4o"},
				),
			},
			config: DefaultChatConfig(),
			opts: ResolveOpts{
				AgentRef:                "my-agent",
				Namespace:               ns,
				RequireExplicitProvider: true,
			},
			wantModel: "gpt-4o",
			wantPType: openaiProviderName,
		},
		{
			name: "agent ref without provider falls to config provider",
			objects: []runtime.Object{
				openaiProvider, openaiSecret,
				makeAgent("agent-no-prov", ns, nil, &corev1alpha1.ModelConfig{Name: "gpt-4o"}),
			},
			config: func() ChatConfig {
				c := DefaultChatConfig()
				c.Provider = openaiProviderName
				return c
			}(),
			opts: ResolveOpts{
				AgentRef:  "agent-no-prov",
				Namespace: ns,
			},
			wantModel: "gpt-4o",
			wantPType: openaiProviderName,
		},
		{
			name:    "agent not found",
			objects: []runtime.Object{},
			config:  DefaultChatConfig(),
			opts: ResolveOpts{
				AgentRef:  "nonexistent",
				Namespace: ns,
			},
			wantErr: "agent \"nonexistent\" not found",
		},
		{
			name:    "provider not found (explicit)",
			objects: []runtime.Object{},
			config:  DefaultChatConfig(),
			opts: ResolveOpts{
				ProviderName: "missing",
				Namespace:    ns,
			},
			wantErr: "not found",
		},
		{
			name:    "no provider configured and no default CRD",
			objects: []runtime.Object{},
			config:  DefaultChatConfig(),
			opts: ResolveOpts{
				Namespace: ns,
			},
			wantErr: "no provider selected and namespace \"default\" has no Providers",
		},
		{
			name: "runtime agent without providerRef falls back to the sole ready Provider",
			objects: []runtime.Object{
				readyProvider(openaiProvider), openaiSecret,
				makeAgent(testRuntimeAgentName, ns, nil, &corev1alpha1.ModelConfig{Name: testRuntimeAgentModel}),
			},
			config: DefaultChatConfig(),
			opts: ResolveOpts{
				AgentRef:  testRuntimeAgentName,
				Namespace: ns,
			},
			wantModel: testRuntimeAgentModel,
			wantPType: openaiProviderName,
		},
		{
			name: "runtime agent without providerRef and several Providers stays non-enumerating",
			objects: []runtime.Object{
				readyProvider(anthropicProvider), anthropicSecret, readyProvider(openaiProvider), openaiSecret,
				makeAgent(testRuntimeAgentName, ns, nil, &corev1alpha1.ModelConfig{Name: testRuntimeAgentModel}),
			},
			config: DefaultChatConfig(),
			opts: ResolveOpts{
				AgentRef:  testRuntimeAgentName,
				Namespace: ns,
			},
			wantErr: "no provider selected and no default Provider is configured; list the Providers you can use",
		},
		{
			name: "runtime agent accepts an explicit provider",
			objects: []runtime.Object{
				readyProvider(anthropicProvider), anthropicSecret, readyProvider(openaiProvider), openaiSecret,
				makeAgent(testRuntimeAgentName, ns, nil, &corev1alpha1.ModelConfig{Name: testRuntimeAgentModel}),
			},
			config: DefaultChatConfig(),
			opts: ResolveOpts{
				AgentRef:     testRuntimeAgentName,
				ProviderName: openaiProviderName,
				Namespace:    ns,
			},
			wantModel: testRuntimeAgentModel,
			wantPType: openaiProviderName,
		},
		{
			name: "agent bound to a provider rejects a different explicit provider",
			objects: []runtime.Object{
				readyProvider(anthropicProvider), anthropicSecret, readyProvider(openaiProvider), openaiSecret,
				makeAgent("bound-agent", ns, &corev1alpha1.ProviderReference{Name: openaiProviderName}, &corev1alpha1.ModelConfig{Name: "gpt-4o"}),
			},
			config: DefaultChatConfig(),
			opts: ResolveOpts{
				AgentRef:     "bound-agent",
				ProviderName: "anthropic",
				Namespace:    ns,
			},
			wantErr: "agent \"bound-agent\" is bound to a different provider",
		},
		{
			name:    "no provider configured falls back to the sole ready Provider",
			objects: []runtime.Object{readyProvider(anthropicProvider), anthropicSecret},
			config:  DefaultChatConfig(),
			opts: ResolveOpts{
				Namespace: ns,
			},
			wantModel: "claude-sonnet-4-20250514",
			wantPType: "anthropic",
		},
		{
			name:    "no provider configured with several Providers stays non-enumerating",
			objects: []runtime.Object{readyProvider(anthropicProvider), anthropicSecret, readyProvider(openaiProvider), openaiSecret},
			config:  DefaultChatConfig(),
			opts: ResolveOpts{
				Namespace: ns,
			},
			wantErr: "list the Providers you can use with the providers API",
		},
		{
			name:    "secret not found during resolve",
			objects: []runtime.Object{openaiProvider}, // no secret
			config:  DefaultChatConfig(),
			opts: ResolveOpts{
				ProviderName: openaiProviderName,
				Namespace:    ns,
			},
			wantErr: "failed to get provider secret",
		},
		{
			name: "azure provider includes API version",
			objects: func() []runtime.Object {
				p := &corev1alpha1.Provider{
					ObjectMeta: metav1.ObjectMeta{Name: "azure", Namespace: ns},
					Spec: corev1alpha1.ProviderSpec{
						Type:      corev1alpha1.ProviderTypeAzureOpenAI,
						BaseURL:   "https://my.openai.azure.com",
						SecretRef: corev1alpha1.ProviderSecretRef{Name: "azure-secret"},
						Azure: &corev1alpha1.AzureConfig{
							DeploymentName: "gpt4-deploy",
							APIVersion:     "2024-02-15-preview",
						},
					},
				}
				s := makeSecret("azure-secret", ns, "api-key", "sk-azure")
				return []runtime.Object{p, s}
			}(),
			config: DefaultChatConfig(),
			opts: ResolveOpts{
				ProviderName: "azure",
				Model:        "gpt-4",
				Namespace:    ns,
			},
			wantModel: "gpt-4",
			wantPType: openaiProviderName, // azure-openai uses the openai provider internally
		},
		{
			name: "require model with empty model from provider CRD",
			objects: func() []runtime.Object {
				p := makeProvider("default", ns, corev1alpha1.ProviderTypeOpenAI, "empty-model-secret", "fallback-model")
				s := makeSecret("empty-model-secret", ns, "api-key", "sk-x")
				return []runtime.Object{p, s}
			}(),
			config: func() ChatConfig {
				c := DefaultChatConfig()
				c.Model = ""
				return c
			}(),
			opts: ResolveOpts{
				Namespace:    ns,
				RequireModel: true,
			},
			wantModel: "fallback-model",
			wantPType: openaiProviderName,
		},
		{
			name: "require model with no model anywhere errors",
			objects: func() []runtime.Object {
				p := makeProvider("default", ns, corev1alpha1.ProviderTypeOpenAI, "no-model-secret", "")
				s := makeSecret("no-model-secret", ns, "api-key", "sk-x")
				return []runtime.Object{p, s}
			}(),
			config: func() ChatConfig {
				c := DefaultChatConfig()
				c.Model = ""
				return c
			}(),
			opts: ResolveOpts{
				Namespace:    ns,
				RequireModel: true,
			},
			wantErr: "no model specified",
		},
		{
			name:    "fallback chain: config.Provider -> default",
			objects: []runtime.Object{defaultProvider, defaultSecret},
			config: func() ChatConfig {
				c := DefaultChatConfig()
				// config.Provider points to nonexistent, should fall to "default"
				c.Provider = "nonexistent-config-prov"
				return c
			}(),
			opts: ResolveOpts{
				ModelStr:  "some-model",
				Namespace: ns,
			},
			wantModel: "some-model",
			wantPType: openaiProviderName,
		},
		{
			name: "opts.Model overrides agent model",
			objects: []runtime.Object{
				openaiProvider, openaiSecret,
				makeAgent("override-agent", ns,
					&corev1alpha1.ProviderReference{Name: openaiProviderName},
					&corev1alpha1.ModelConfig{Name: "agent-model"},
				),
			},
			config: DefaultChatConfig(),
			opts: ResolveOpts{
				AgentRef:  "override-agent",
				Model:     "explicit-model",
				Namespace: ns,
			},
			wantModel: "explicit-model",
			wantPType: openaiProviderName,
		},
		{
			name: "config model used as last resort",
			objects: []runtime.Object{
				makeProvider(openaiProviderName, ns, corev1alpha1.ProviderTypeOpenAI, "openai-secret", ""),
				openaiSecret,
			},
			config: func() ChatConfig {
				c := DefaultChatConfig()
				c.Model = "config-fallback-model"
				return c
			}(),
			opts: ResolveOpts{
				ProviderName: openaiProviderName,
				Namespace:    ns,
			},
			wantModel: "config-fallback-model",
			wantPType: openaiProviderName,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			k := fake.NewClientBuilder().WithScheme(newScheme()).WithRuntimeObjects(tt.objects...).Build()
			resolver := NewProviderResolver(k, tt.config)

			provider, model, err := resolver.Resolve(context.Background(), tt.opts)

			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantModel, model)
			if tt.wantPType != "" {
				assert.Equal(t, tt.wantPType, provider.Name())
			}
		})
	}
}

// readyProvider returns a copy of the Provider with status.ready set, as the
// controller reports for a usable Provider.
func readyProvider(provider *corev1alpha1.Provider) *corev1alpha1.Provider {
	ready := provider.DeepCopy()
	ready.Status.Ready = true
	return ready
}

func TestProviderResolverRequireExplicitProviderHasUniformFallbackError(t *testing.T) {
	t.Parallel()
	const want = "no provider selected and no default Provider is configured; pass an explicit provider"

	provider := makeProvider("hidden", "default", corev1alpha1.ProviderTypeOpenAI, "hidden-secret", "gpt-4o")
	for name, tc := range map[string]struct {
		config  ChatConfig
		objects []runtime.Object
	}{
		"no Providers": {
			config: DefaultChatConfig(),
		},
		"one ready Provider": {
			config:  DefaultChatConfig(),
			objects: []runtime.Object{readyProvider(provider)},
		},
		"two ready Providers": {
			config:  DefaultChatConfig(),
			objects: []runtime.Object{readyProvider(provider), readyProvider(makeProvider("other", "default", corev1alpha1.ProviderTypeAnthropic, "other-secret", "claude"))},
		},
		"default-named Provider": {
			config:  DefaultChatConfig(),
			objects: []runtime.Object{makeProvider("default", "default", corev1alpha1.ProviderTypeOpenAI, "default-secret", "gpt-4o")},
		},
		"configured Provider": {
			config: func() ChatConfig {
				config := DefaultChatConfig()
				config.Provider = "hidden"
				return config
			}(),
			objects: []runtime.Object{provider},
		},
	} {
		t.Run(name, func(t *testing.T) {
			resolver := NewProviderResolver(fake.NewClientBuilder().WithScheme(newScheme()).WithRuntimeObjects(tc.objects...).Build(), tc.config)
			_, _, err := resolver.Resolve(context.Background(), ResolveOpts{Namespace: "default", RequireExplicitProvider: true})
			require.EqualError(t, err, want)
		})
	}
}

func TestRequestRequiresExplicitProviderHonorsAuthorizationMode(t *testing.T) {
	tests := []struct {
		name         string
		mode         string
		contextToken bool
		want         bool
	}{
		{name: "off allows implicit provider", mode: ContextTokenAuthorizationModeOff, contextToken: true},
		{name: "audit allows implicit provider", mode: ContextTokenAuthorizationModeAudit, contextToken: true},
		{name: "enforce requires explicit provider", mode: ContextTokenAuthorizationModeEnforce, contextToken: true, want: true},
		{name: "enforce does not affect other authentication", mode: ContextTokenAuthorizationModeEnforce},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authorization, err := NewContextTokenAuthorizationConfig(ContextTokenAuthorizationConfigOptions{Mode: tt.mode})
			require.NoError(t, err)

			var got bool
			app := fiber.New()
			app.Get("/", func(c fiber.Ctx) error {
				if tt.contextToken {
					c.Locals(UserInfoContextKey, &UserInfo{
						AuthType:     AuthTypeContextToken,
						ContextToken: &ContextToken{},
					})
				}
				got = requestRequiresExplicitProvider(c, authorization)
				return c.SendStatus(http.StatusNoContent)
			})

			resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/", nil))
			require.NoError(t, err)
			require.Equal(t, http.StatusNoContent, resp.StatusCode)
			require.Equal(t, tt.want, got)
		})
	}
}
