package api

import (
	"strings"

	"github.com/gofiber/fiber/v3"

	gatewayv1alpha1 "github.com/orka-agents/orka/api/gateway/v1alpha1"
	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

type apiNamespaceSource uint8

const (
	apiQueryNamespace apiNamespaceSource = iota
	apiCreateNamespace
	apiMetadataNamespace
	apiBodyNamespace
	apiQueryThenBodyNamespace
	apiBodyThenQueryNamespace
	apiClusterScoped
)

// apiResourcePermission names the Kubernetes resource or documented virtual
// resource being authorized. A slash separates a resource and subresource.
// Create on a Kubernetes collection has no name, just like the API server's
// authorization request; named actions on virtual subresources use the parent.
type apiResourcePermission struct {
	group     string
	resource  string
	verb      string
	nameParam string
}

type apiRoutePolicy struct {
	namespace    apiNamespaceSource
	permissions  []apiResourcePermission
	identityOnly bool
	trimStoreID  bool
}

func coreAPIPolicy(verb, resource, nameParam string, additional ...apiResourcePermission) apiRoutePolicy {
	return apiRoutePolicy{permissions: append([]apiResourcePermission{{corev1alpha1.GroupVersion.Group, resource, verb, nameParam}}, additional...)}
}

func gatewayAPIPolicy(verb, resource, nameParam string) apiRoutePolicy {
	return apiRoutePolicy{permissions: []apiResourcePermission{{gatewayv1alpha1.GroupVersion.Group, resource, verb, nameParam}}}
}

func (p apiRoutePolicy) inNamespace(source apiNamespaceSource) apiRoutePolicy {
	p.namespace = source
	return p
}

func (p apiRoutePolicy) withStoreID() apiRoutePolicy {
	p.trimStoreID = true
	return p
}

// externalAPIPolicies is the complete external user API inventory. Keep the
// independent HTTP cases and the operator reference in sync when adding routes.
// Existing handler checks still enforce loaded-object, gateway and workspace
// permissions. Compound operations preflight their write permissions here,
// before a handler can persist a record, enqueue work or contact a provider.
//
//nolint:goconst // Literal permission tuples keep the route inventory auditable.
var externalAPIPolicies = map[string]apiRoutePolicy{
	"POST /api/v1/tasks":              coreAPIPolicy("create", "tasks", "").inNamespace(apiCreateNamespace),
	"GET /api/v1/tasks":               coreAPIPolicy("list", "tasks", ""),
	"GET /api/v1/tasks/:id":           coreAPIPolicy("get", "tasks", "id"),
	"DELETE /api/v1/tasks/:id":        coreAPIPolicy("delete", "tasks", "id"),
	"GET /api/v1/tasks/:id/logs":      coreAPIPolicy("get", "tasks", "id"),
	"GET /api/v1/tasks/:id/events":    coreAPIPolicy("get", "tasks", "id"),
	"GET /api/v1/tasks/:id/stream":    coreAPIPolicy("get", "tasks", "id"),
	"GET /api/v1/tasks/:id/trace":     coreAPIPolicy("get", "tasks", "id"),
	"GET /api/v1/tasks/:id/approvals": coreAPIPolicy("get", "tasks", "id"),
	"POST /api/v1/tasks/:id/approvals/:approvalID/decision": coreAPIPolicy("update", "tasks/approvals", "id",
		apiResourcePermission{corev1alpha1.GroupVersion.Group, "tasks", "patch", "id"}),
	"POST /api/v1/tasks/:id/fork": coreAPIPolicy("get", "tasks", "id",
		apiResourcePermission{corev1alpha1.GroupVersion.Group, "tasks", "create", ""}),
	"GET /api/v1/tasks/:id/result": coreAPIPolicy("get", "tasks", "id"),
	"GET /api/v1/tasks/:id/plan":   coreAPIPolicy("get", "tasks", "id"),
	"GET /api/v1/tasks/:id/children": coreAPIPolicy("get", "tasks", "id",
		apiResourcePermission{corev1alpha1.GroupVersion.Group, "tasks", "list", ""}),
	"GET /api/v1/tasks/:id/artifacts":           coreAPIPolicy("get", "tasks", "id"),
	"GET /api/v1/tasks/:id/artifacts/:filename": coreAPIPolicy("get", "tasks", "id"),

	"GET /api/v1/sessions":            coreAPIPolicy("list", "sessions", ""),
	"GET /api/v1/sessions/:id":        coreAPIPolicy("get", "sessions", "id"),
	"GET /api/v1/sessions/:id/events": coreAPIPolicy("get", "sessions", "id"),
	"GET /api/v1/sessions/:id/stream": coreAPIPolicy("get", "sessions", "id"),
	"DELETE /api/v1/sessions/:id":     coreAPIPolicy("delete", "sessions", "id"),

	"GET /api/v1/gatewayclasses":                gatewayAPIPolicy("list", "gatewayclasses", "").inNamespace(apiClusterScoped),
	"GET /api/v1/gatewayclasses/:name":          gatewayAPIPolicy("get", "gatewayclasses", "name").inNamespace(apiClusterScoped),
	"GET /api/v1/gateways":                      gatewayAPIPolicy("list", "gateways", ""),
	"GET /api/v1/gateways/:name":                gatewayAPIPolicy("get", "gateways", "name"),
	"GET /api/v1/gatewaybindings":               gatewayAPIPolicy("list", "gatewaybindings", ""),
	"GET /api/v1/gatewaybindings/:name":         gatewayAPIPolicy("get", "gatewaybindings", "name"),
	"GET /api/v1/gateway-events":                gatewayAPIPolicy("list", "gatewayevents", ""),
	"GET /api/v1/gateway-events/:id":            gatewayAPIPolicy("get", "gatewayevents", "id"),
	"GET /api/v1/gateway-deliveries":            gatewayAPIPolicy("list", "gatewaydeliveries", ""),
	"GET /api/v1/gateway-deliveries/:id":        gatewayAPIPolicy("get", "gatewaydeliveries", "id"),
	"POST /api/v1/gateway-deliveries/:id/retry": gatewayAPIPolicy("update", "gatewaydeliveries", "id"),

	"GET /api/v1/memories":                     coreAPIPolicy("list", "memories", ""),
	"POST /api/v1/memories":                    coreAPIPolicy("create", "memories", "").inNamespace(apiBodyNamespace),
	"GET /api/v1/memories/:id":                 coreAPIPolicy("get", "memories", "id"),
	"PUT /api/v1/memories/:id":                 coreAPIPolicy("update", "memories", "id").inNamespace(apiQueryThenBodyNamespace),
	"DELETE /api/v1/memories/:id":              coreAPIPolicy("delete", "memories", "id"),
	"POST /api/v1/memories/:id/disable":        coreAPIPolicy("update", "memories", "id"),
	"POST /api/v1/memories/:id/enable":         coreAPIPolicy("update", "memories", "id"),
	"GET /api/v1/memory-proposals":             coreAPIPolicy("list", "memoryproposals", ""),
	"POST /api/v1/memory-proposals":            coreAPIPolicy("create", "memoryproposals", "").inNamespace(apiBodyNamespace),
	"GET /api/v1/memory-proposals/:id":         coreAPIPolicy("get", "memoryproposals", "id"),
	"POST /api/v1/memory-proposals/:id/review": coreAPIPolicy("review", "memoryproposals", "id").inNamespace(apiBodyThenQueryNamespace).withStoreID(),
	"POST /api/v1/memory-proposals/:id/apply": coreAPIPolicy("apply", "memoryproposals", "id",
		apiResourcePermission{corev1alpha1.GroupVersion.Group, "memories", "create", ""}).inNamespace(apiBodyThenQueryNamespace).withStoreID(),
	"POST /api/v1/memory-proposals/:id/archive": coreAPIPolicy("update", "memoryproposals", "id").withStoreID(),

	"GET /api/v1/providers":               coreAPIPolicy("list", "providers", ""),
	"POST /api/v1/providers":              coreAPIPolicy("create", "providers", "").inNamespace(apiCreateNamespace),
	"GET /api/v1/providers/:name":         coreAPIPolicy("get", "providers", "name"),
	"PUT /api/v1/providers/:name":         coreAPIPolicy("update", "providers", "name"),
	"DELETE /api/v1/providers/:name":      coreAPIPolicy("delete", "providers", "name"),
	"GET /api/v1/tools":                   coreAPIPolicy("list", "tools", ""),
	"POST /api/v1/tools":                  coreAPIPolicy("create", "tools", "").inNamespace(apiCreateNamespace),
	"GET /api/v1/tools/:name":             coreAPIPolicy("get", "tools", "name"),
	"PUT /api/v1/tools/:name":             coreAPIPolicy("update", "tools", "name"),
	"DELETE /api/v1/tools/:name":          coreAPIPolicy("delete", "tools", "name"),
	"GET /api/v1/runtime-pools":           coreAPIPolicy("list", "runtimepools", ""),
	"GET /api/v1/runtime-pools/:name":     coreAPIPolicy("get", "runtimepools", "name"),
	"GET /api/v1/agent-runtimes":          coreAPIPolicy("list", "agentruntimes", ""),
	"POST /api/v1/agent-runtimes":         coreAPIPolicy("create", "agentruntimes", "").inNamespace(apiMetadataNamespace),
	"GET /api/v1/agent-runtimes/:name":    coreAPIPolicy("get", "agentruntimes", "name"),
	"PUT /api/v1/agent-runtimes/:name":    coreAPIPolicy("update", "agentruntimes", "name"),
	"DELETE /api/v1/agent-runtimes/:name": coreAPIPolicy("delete", "agentruntimes", "name"),
	"POST /api/v1/agents":                 coreAPIPolicy("create", "agents", "").inNamespace(apiCreateNamespace),
	"GET /api/v1/agents":                  coreAPIPolicy("list", "agents", ""),
	"GET /api/v1/agents/:name":            coreAPIPolicy("get", "agents", "name"),
	"PUT /api/v1/agents/:name":            coreAPIPolicy("patch", "agents", "name"),
	"DELETE /api/v1/agents/:name":         coreAPIPolicy("delete", "agents", "name"),
	"POST /api/v1/skills":                 coreAPIPolicy("create", "skills", "").inNamespace(apiCreateNamespace),
	"GET /api/v1/skills":                  coreAPIPolicy("list", "skills", ""),
	"GET /api/v1/skills/:name":            coreAPIPolicy("get", "skills", "name"),
	"GET /api/v1/skills/:name/content":    coreAPIPolicy("get", "skills", "name"),
	"PUT /api/v1/skills/:name":            coreAPIPolicy("update", "skills", "name"),
	"DELETE /api/v1/skills/:name":         coreAPIPolicy("delete", "skills", "name"),

	"POST /api/v1/security/repositories":                   coreAPIPolicy("create", "repositoryscans", "").inNamespace(apiCreateNamespace),
	"GET /api/v1/security/repositories":                    coreAPIPolicy("list", "repositoryscans", ""),
	"GET /api/v1/security/repositories/:name":              coreAPIPolicy("get", "repositoryscans", "name"),
	"PUT /api/v1/security/repositories/:name":              coreAPIPolicy("update", "repositoryscans", "name"),
	"DELETE /api/v1/security/repositories/:name":           coreAPIPolicy("delete", "repositoryscans", "name"),
	"GET /api/v1/security/repositories/:name/threat-model": coreAPIPolicy("get", "repositoryscans/threatmodel", "name"),
	"PUT /api/v1/security/repositories/:name/threat-model": coreAPIPolicy("update", "repositoryscans/threatmodel", "name"),
	"GET /api/v1/security/repositories/:name/scans":        coreAPIPolicy("list", "repositoryscans/scans", "name"),
	"POST /api/v1/security/repositories/:name/scans": coreAPIPolicy("create", "repositoryscans/scans", "name",
		apiResourcePermission{corev1alpha1.GroupVersion.Group, "tasks", "list", ""},
		apiResourcePermission{corev1alpha1.GroupVersion.Group, "tasks", "create", ""},
		apiResourcePermission{corev1alpha1.GroupVersion.Group, "repositoryscans/status", "patch", "name"}),
	"GET /api/v1/security/repositories/:name/slices":           coreAPIPolicy("list", "repositoryscans/slices", "name"),
	"GET /api/v1/security/repositories/:name/slices/:sliceID":  coreAPIPolicy("get", "repositoryscans/slices", "name"),
	"GET /api/v1/security/repositories/:name/dropped-findings": coreAPIPolicy("list", "repositoryscans/droppedfindings", "name"),
	"GET /api/v1/security/repositories/:name/findings":         coreAPIPolicy("list", "repositoryscans/findings", "name"),
	"GET /api/v1/security/findings/:id":                        coreAPIPolicy("get", "securityfindings", "id"),
	"POST /api/v1/security/findings/:id/dismiss":               coreAPIPolicy("update", "securityfindings", "id"),
	"POST /api/v1/security/findings/:id/reopen":                coreAPIPolicy("update", "securityfindings", "id"),
	// Named action grants include their fixed finding-state transitions.
	// General edits, including dismissal/reopening, require securityfindings update.
	"POST /api/v1/security/findings/:id/validate": coreAPIPolicy("create", "securityfindings/validation", "id",
		apiResourcePermission{corev1alpha1.GroupVersion.Group, "tasks", "create", ""}),
	"POST /api/v1/security/findings/:id/patch": coreAPIPolicy("create", "securityfindings/patches", "id",
		apiResourcePermission{corev1alpha1.GroupVersion.Group, "tasks", "create", ""}),
	"GET /api/v1/security/findings/:id/patches":       coreAPIPolicy("list", "securityfindings/patches", "id"),
	"POST /api/v1/security/findings/:id/pull-request": coreAPIPolicy("get", "securityfindings/pullrequest", "id"),

	"POST /api/v1/monitors/repositories":         coreAPIPolicy("create", "repositorymonitors", "").inNamespace(apiCreateNamespace),
	"GET /api/v1/monitors/repositories":          coreAPIPolicy("list", "repositorymonitors", ""),
	"GET /api/v1/monitors/repositories/:name":    coreAPIPolicy("get", "repositorymonitors", "name"),
	"PUT /api/v1/monitors/repositories/:name":    coreAPIPolicy("update", "repositorymonitors", "name"),
	"DELETE /api/v1/monitors/repositories/:name": coreAPIPolicy("delete", "repositorymonitors", "name"),
	"POST /api/v1/monitors/repositories/:name/runs": coreAPIPolicy("create", "repositorymonitors/runs", "name",
		apiResourcePermission{corev1alpha1.GroupVersion.Group, "repositorymonitors", "patch", "name"}),
	"GET /api/v1/monitors/repositories/:name/runs":  coreAPIPolicy("list", "repositorymonitors/runs", "name"),
	"GET /api/v1/monitors/repositories/:name/items": coreAPIPolicy("list", "repositorymonitors/items", "name"),
	"POST /api/v1/monitors/repositories/:name/commands": coreAPIPolicy("create", "repositorymonitors/commands", "name",
		apiResourcePermission{corev1alpha1.GroupVersion.Group, "repositorymonitors/runs", "create", "name"},
		apiResourcePermission{corev1alpha1.GroupVersion.Group, "repositorymonitors", "patch", "name"}),
	"GET /api/v1/monitors/commands":                              coreAPIPolicy("list", "monitorcommands", ""),
	"GET /api/v1/monitors/commands/:id":                          coreAPIPolicy("get", "monitorcommands", "id"),
	"GET /api/v1/monitors/actions":                               coreAPIPolicy("list", "monitoractions", ""),
	"GET /api/v1/monitors/actions/:id":                           coreAPIPolicy("get", "monitoractions", "id"),
	"GET /api/v1/monitors/work-actions":                          coreAPIPolicy("list", "monitorworkactions", ""),
	"GET /api/v1/monitors/work-actions/:id":                      coreAPIPolicy("get", "monitorworkactions", "id"),
	"GET /api/v1/monitors/implementation-jobs":                   coreAPIPolicy("list", "monitorimplementationjobs", ""),
	"GET /api/v1/monitors/implementation-jobs/:id":               coreAPIPolicy("get", "monitorimplementationjobs", "id"),
	"GET /api/v1/monitors/implementation-jobs/:id/patch-preview": coreAPIPolicy("get", "monitorimplementationjobs", "id"),
	"GET /api/v1/monitors/mutations":                             coreAPIPolicy("list", "monitormutations", ""),
	"GET /api/v1/monitors/mutations/:id":                         coreAPIPolicy("get", "monitormutations", "id"),
	"GET /api/v1/monitors/events":                                coreAPIPolicy("list", "monitorevents", ""),

	"GET /api/v1/substrate-actor-pools":          coreAPIPolicy("list", "substrateactorpools", ""),
	"POST /api/v1/substrate-actor-pools":         coreAPIPolicy("create", "substrateactorpools", "").inNamespace(apiCreateNamespace),
	"GET /api/v1/substrate-actor-pools/:name":    coreAPIPolicy("get", "substrateactorpools", "name"),
	"PUT /api/v1/substrate-actor-pools/:name":    coreAPIPolicy("update", "substrateactorpools", "name"),
	"DELETE /api/v1/substrate-actor-pools/:name": coreAPIPolicy("delete", "substrateactorpools", "name"),
	"GET /api/v1/auth/validate":                  {identityOnly: true},
	"GET /api/v1/auth/whoami":                    {identityOnly: true},
	"GET /api/v1/secrets":                        {permissions: []apiResourcePermission{{"", "secrets", "list", ""}}},
	"POST /api/v1/chat":                          coreAPIPolicy("create", "chats", "").inNamespace(apiBodyNamespace),
	"GET /api/v1/chat/config":                    coreAPIPolicy("get", "chats/config", ""),
	"DELETE /api/v1/chat/:sessionId":             coreAPIPolicy("delete", "sessions", "sessionId"),
	"POST /openai/v1/chat/completions":           coreAPIPolicy("create", "chats", ""),
	"GET /openai/v1/models":                      coreAPIPolicy("list", "providers", ""),
	"POST /anthropic/v1/messages":                coreAPIPolicy("create", "chats", ""),
	"GET /anthropic/v1/models":                   coreAPIPolicy("list", "providers", ""),
}

// externalAPIRouter installs authorization on the endpoint, where Fiber has
// resolved its path parameters. Group middleware cannot read those parameters.
// A new endpoint without a policy fails closed, and the inventory test requires
// its independent authorization case before it can be shipped.
type externalAPIRouter struct {
	router fiber.Router
	prefix string
	h      *Handlers
}

func (s *Server) externalAPIGroup(prefix string, auth fiber.Handler) externalAPIRouter {
	return externalAPIRouter{router: s.app.Group(prefix, auth), prefix: prefix, h: s.handlers}
}

func (r externalAPIRouter) Get(path string, handler fiber.Handler) {
	r.router.Get(path, r.h.authorizeExternalRoute(fiber.MethodGet+" "+r.prefix+path), handler)
}

func (r externalAPIRouter) Post(path string, handler fiber.Handler) {
	r.router.Post(path, r.h.authorizeExternalRoute(fiber.MethodPost+" "+r.prefix+path), handler)
}

func (r externalAPIRouter) Put(path string, handler fiber.Handler) {
	r.router.Put(path, r.h.authorizeExternalRoute(fiber.MethodPut+" "+r.prefix+path), handler)
}

func (r externalAPIRouter) Delete(path string, handler fiber.Handler) {
	r.router.Delete(path, r.h.authorizeExternalRoute(fiber.MethodDelete+" "+r.prefix+path), handler)
}

func (h *Handlers) authorizeExternalRoute(route string) fiber.Handler {
	return func(c fiber.Ctx) error {
		if err := h.checkExternalRoute(c, route); err != nil {
			if strings.HasPrefix(route, "POST /openai/") || strings.HasPrefix(route, "GET /openai/") {
				return openAIContextTokenAuthorizationError(c, err)
			}
			if strings.HasPrefix(route, "POST /anthropic/") || strings.HasPrefix(route, "GET /anthropic/") {
				return anthropicContextTokenAuthorizationError(c, err)
			}
			return err
		}
		return c.Next()
	}
}

func (h *Handlers) checkExternalRoute(c fiber.Ctx, route string) error {
	policy, ok := externalAPIPolicies[route]
	if !ok || (!policy.identityOnly && len(policy.permissions) == 0) {
		return fiber.NewError(fiber.StatusForbidden, "authorization policy is unavailable")
	}
	ui := GetUserInfo(c)
	if ui == nil {
		return fiber.NewError(fiber.StatusForbidden, "not authorized")
	}
	// Direct OIDC validation and transaction tokens have their own contracts.
	// Do not reinterpret them as Kubernetes TokenReview identities.
	if policy.identityOnly || ui.AuthType != AuthTypeTokenReview {
		return nil
	}
	namespace, err := h.externalRequestNamespace(c, policy)
	if err != nil {
		return err
	}
	for _, permission := range policy.permissions {
		name := ""
		if permission.nameParam != "" {
			name = c.Params(permission.nameParam)
		}
		if policy.trimStoreID {
			name = strings.TrimSpace(name)
		}
		if err := authorizeKubernetesResourceAction(c.Context(), h.clientset, ui,
			namespace, permission.verb, permission.group, permission.resource, name); err != nil {
			return err
		}
	}
	return nil
}

func (h *Handlers) externalRequestNamespace(c fiber.Ctx, policy apiRoutePolicy) (string, error) {
	if policy.namespace == apiClusterScoped {
		return "", nil
	}
	explicit := c.Query("namespace", "")
	if policy.namespace != apiQueryNamespace {
		bodyNamespace, metadataNamespace, err := bindExternalNamespace(c, policy.namespace)
		if err != nil {
			return "", err
		}
		switch policy.namespace {
		case apiCreateNamespace:
			explicit = bodyNamespace
			if explicit == "" {
				explicit = metadataNamespace
			}
		case apiMetadataNamespace:
			explicit = metadataNamespace
		case apiBodyNamespace:
			explicit = bodyNamespace
		case apiQueryThenBodyNamespace:
			if explicit == "" {
				explicit = bodyNamespace
			}
		case apiBodyThenQueryNamespace:
			if bodyNamespace != "" {
				explicit = bodyNamespace
			}
		}
	}
	// Proposal actions trim namespace in the store. Reject noncanonical input
	// before namespace isolation/SAR so authorization cannot cover a different
	// namespace from the eventual write.
	if policy.trimStoreID && explicit != strings.TrimSpace(explicit) {
		return "", fiber.NewError(fiber.StatusForbidden, "namespace not allowed")
	}
	return h.resolveNamespace(c, explicit)
}

func bindExternalNamespace(c fiber.Ctx, source apiNamespaceSource) (string, string, error) {
	if strings.TrimSpace(string(c.Body())) == "" {
		return "", "", nil
	}
	// Match the handler's typed JSON binding, including duplicate keys and
	// case-insensitive field names. Ignore fields the handler itself ignores.
	var flat struct {
		Namespace string `json:"namespace"`
	}
	var manifest struct {
		Metadata struct {
			Namespace string `json:"namespace"`
		} `json:"metadata"`
	}
	switch source {
	case apiCreateNamespace:
		var hybrid struct {
			Namespace string `json:"namespace"`
			Metadata  struct {
				Namespace string `json:"namespace"`
			} `json:"metadata"`
		}
		if err := c.Bind().JSON(&hybrid); err != nil {
			return "", "", fiber.NewError(fiber.StatusBadRequest, "invalid request body")
		}
		return hybrid.Namespace, hybrid.Metadata.Namespace, nil
	case apiMetadataNamespace:
		if err := c.Bind().JSON(&manifest); err != nil {
			return "", "", fiber.NewError(fiber.StatusBadRequest, "invalid request body")
		}
		return "", manifest.Metadata.Namespace, nil
	default:
		if err := c.Bind().JSON(&flat); err != nil {
			return "", "", fiber.NewError(fiber.StatusBadRequest, "invalid request body")
		}
		return flat.Namespace, "", nil
	}
}
