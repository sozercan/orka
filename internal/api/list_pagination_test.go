package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

const (
	listPaginationTestNamespace = "default"
	listPaginationTasksRoute    = "/tasks"
	listPaginationToolsRoute    = "/tools"
	listPaginationAgentsRoute   = "/agents"
)

// paginatingReader emulates the API server's chunked list semantics on top of
// the fake client, which ignores Limit/Continue: items are served in name
// order, Continue is an opaque offset, and RemainingItemCount is populated
// (the handlers must strip it before responding).
type paginatingReader struct {
	client.Reader
	lists int
}

func (r *paginatingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	r.lists++
	options := &client.ListOptions{}
	for _, opt := range opts {
		opt.ApplyToList(options)
	}
	if err := r.Reader.List(ctx, list, client.InNamespace(options.Namespace)); err != nil {
		return err
	}
	items, err := meta.ExtractList(list)
	if err != nil {
		return err
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].(client.Object).GetName() < items[j].(client.Object).GetName()
	})
	offset := 0
	if options.Continue != "" {
		if offset, err = strconv.Atoi(options.Continue); err != nil {
			return fmt.Errorf("invalid continue token %q", options.Continue)
		}
	}
	if offset > len(items) {
		offset = len(items)
	}
	end := len(items)
	if options.Limit > 0 && offset+int(options.Limit) < end {
		end = offset + int(options.Limit)
	}
	if err := meta.SetList(list, items[offset:end]); err != nil {
		return err
	}
	list.SetContinue("")
	list.SetRemainingItemCount(nil)
	if end < len(items) {
		list.SetContinue(strconv.Itoa(end))
		remaining := int64(len(items) - end)
		list.SetRemainingItemCount(&remaining)
	}
	return nil
}

// cacheEmulatingClient behaves like controller-runtime's cache reader for a
// limited List: it truncates at Limit and stamps the unusable sentinel.
type cacheEmulatingClient struct {
	client.Client
	limitedLists int
}

func (c *cacheEmulatingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	options := &client.ListOptions{}
	for _, opt := range opts {
		opt.ApplyToList(options)
	}
	if err := c.Client.List(ctx, list, client.InNamespace(options.Namespace)); err != nil {
		return err
	}
	if options.Limit <= 0 {
		return nil
	}
	c.limitedLists++
	items, err := meta.ExtractList(list)
	if err != nil {
		return err
	}
	if int64(len(items)) > options.Limit {
		items = items[:options.Limit]
	}
	if err := meta.SetList(list, items); err != nil {
		return err
	}
	list.SetContinue(cacheContinueUnsupported)
	return nil
}

type paginatedListCase struct {
	route   string
	handler func(*Handlers) fiber.Handler
	object  func(name string) client.Object
}

func paginatedListCases() []paginatedListCase {
	objectMeta := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: listPaginationTestNamespace}
	}
	return []paginatedListCase{
		{route: listPaginationTasksRoute, handler: func(h *Handlers) fiber.Handler { return h.ListTasks },
			object: func(name string) client.Object { return &corev1alpha1.Task{ObjectMeta: objectMeta(name)} }},
		{route: listPaginationToolsRoute, handler: func(h *Handlers) fiber.Handler { return h.ListTools },
			object: func(name string) client.Object { return &corev1alpha1.Tool{ObjectMeta: objectMeta(name)} }},
		{route: listPaginationAgentsRoute, handler: func(h *Handlers) fiber.Handler { return h.ListAgents },
			object: func(name string) client.Object { return &corev1alpha1.Agent{ObjectMeta: objectMeta(name)} }},
		{route: "/skills", handler: func(h *Handlers) fiber.Handler { return h.ListSkills },
			object: func(name string) client.Object { return &corev1alpha1.Skill{ObjectMeta: objectMeta(name)} }},
		{route: "/monitors", handler: func(h *Handlers) fiber.Handler { return h.ListRepositoryMonitors },
			object: func(name string) client.Object { return &corev1alpha1.RepositoryMonitor{ObjectMeta: objectMeta(name)} }},
		{route: "/scans", handler: func(h *Handlers) fiber.Handler { return h.ListRepositoryScans },
			object: func(name string) client.Object { return &corev1alpha1.RepositoryScan{ObjectMeta: objectMeta(name)} }},
		{route: "/providers", handler: func(h *Handlers) fiber.Handler { return h.ListProviders },
			object: func(name string) client.Object { return &corev1alpha1.Provider{ObjectMeta: objectMeta(name)} }},
		{route: "/substrate-actor-pools", handler: func(h *Handlers) fiber.Handler { return h.ListSubstrateActorPools },
			object: func(name string) client.Object { return &corev1alpha1.SubstrateActorPool{ObjectMeta: objectMeta(name)} }},
		{route: "/runtime-pools", handler: func(h *Handlers) fiber.Handler { return h.ListRuntimePools },
			object: func(name string) client.Object { return &corev1alpha1.RuntimePool{ObjectMeta: objectMeta(name)} }},
		{route: "/agent-runtimes", handler: func(h *Handlers) fiber.Handler { return h.ListAgentRuntimes },
			object: func(name string) client.Object { return &corev1alpha1.AgentRuntime{ObjectMeta: objectMeta(name)} }},
	}
}

func paginatedListScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1alpha1.AddToScheme(scheme))
	return scheme
}

// paginatedListNames decodes one list page and returns the listed item names
// plus the continue token. Handlers either return Kubernetes objects
// (metadata.name) or flattened maps (name); built-in tools are skipped.
func paginatedListNames(t *testing.T, app *fiber.App, target string) ([]string, string) {
	t.Helper()
	response, err := app.Test(httptest.NewRequest(http.MethodGet, target, nil))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode, "GET %s", target)
	var body struct {
		Items    []map[string]any `json:"items"`
		Metadata struct {
			Continue           string `json:"continue"`
			RemainingItemCount *int64 `json:"remainingItemCount"`
		} `json:"metadata"`
	}
	require.NoError(t, json.NewDecoder(response.Body).Decode(&body))
	// The API server's collection-wide count would reveal how many objects a
	// scoped caller cannot see on later pages, so it is never forwarded.
	require.Nil(t, body.Metadata.RemainingItemCount, "remainingItemCount must not be forwarded from paginated lists")
	names := make([]string, 0, len(body.Items))
	for _, item := range body.Items {
		if builtin, _ := item["builtin"].(bool); builtin {
			// Built-ins are collected like any other item so the caller can
			// assert they appear exactly once across all pages.
			name, _ := item["name"].(string)
			names = append(names, "builtin:"+name)
			continue
		}
		if name, ok := item["name"].(string); ok {
			names = append(names, name)
			continue
		}
		objectMeta, _ := item["metadata"].(map[string]any)
		name, _ := objectMeta["name"].(string)
		require.NotEmpty(t, name, "item without a name: %v", item)
		names = append(names, name)
	}
	require.NotEqual(t, cacheContinueUnsupported, body.Metadata.Continue)
	return names, body.Metadata.Continue
}

func TestPaginatedListsFollowContinueThroughAPIReader(t *testing.T) {
	const total = 5
	for _, tc := range paginatedListCases() {
		t.Run(strings.TrimPrefix(tc.route, "/"), func(t *testing.T) {
			scheme := paginatedListScheme(t)
			objects := make([]client.Object, 0, total)
			want := make([]string, 0, total)
			for i := range total {
				name := fmt.Sprintf("item-%d", i)
				objects = append(objects, tc.object(name))
				want = append(want, name)
			}
			reader := &paginatingReader{Reader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()}
			cached := listFailingClient{Client: fake.NewClientBuilder().WithScheme(scheme).Build()}
			handlers := NewHandlers(HandlersConfig{Client: cached, APIReader: reader})
			app := fiber.New()
			app.Get(tc.route, tc.handler(handlers))

			var got, gotBuiltins []string
			continueToken := ""
			pages := 0
			for {
				target := tc.route + "?namespace=" + listPaginationTestNamespace + "&limit=2"
				if continueToken != "" {
					target += "&continue=" + continueToken
				}
				names, next := paginatedListNames(t, app, target)
				crdNames, builtins := splitBuiltinListNames(names)
				require.LessOrEqual(t, len(crdNames), 2)
				if pages == 0 {
					gotBuiltins = builtins
				} else {
					require.Empty(t, builtins, "built-in tools must only be emitted on the first page")
				}
				got = append(got, crdNames...)
				pages++
				if next == "" {
					break
				}
				require.Less(t, pages, total+1, "continue tokens did not terminate")
				continueToken = next
			}
			require.Equal(t, 3, pages)
			require.Equal(t, want, got, "every item must be listed exactly once")
			require.Equal(t, pages, reader.lists)
			if tc.route == listPaginationToolsRoute {
				require.NotEmpty(t, gotBuiltins, "the first tools page must carry the built-in tools")
				require.Len(t, gotBuiltins, len(builtinToolsList), "built-ins must be unique across pages")
			}
		})
	}
}

func TestPaginatedListsWithoutAPIReaderReturnCompleteUnlimitedPage(t *testing.T) {
	const total = 4
	for _, tc := range paginatedListCases() {
		t.Run(strings.TrimPrefix(tc.route, "/"), func(t *testing.T) {
			scheme := paginatedListScheme(t)
			objects := make([]client.Object, 0, total)
			want := make([]string, 0, total)
			for i := range total {
				name := fmt.Sprintf("item-%d", i)
				objects = append(objects, tc.object(name))
				want = append(want, name)
			}
			cached := &cacheEmulatingClient{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()}
			handlers := NewHandlers(HandlersConfig{Client: cached})
			app := fiber.New()
			app.Get(tc.route, tc.handler(handlers))

			names, next := paginatedListNames(t, app, tc.route+"?namespace="+listPaginationTestNamespace+"&limit=2")
			names, builtins := splitBuiltinListNames(names)
			sort.Strings(names)
			require.Equal(t, want, names, "cache fallback must not truncate")
			if tc.route == listPaginationToolsRoute {
				require.Len(t, builtins, len(builtinToolsList), "the single page must carry every built-in tool once")
			}
			require.Empty(t, next)
			require.Zero(t, cached.limitedLists, "cache must never be asked for a limited list")

			response, err := app.Test(httptest.NewRequest(http.MethodGet, tc.route+"?namespace="+listPaginationTestNamespace+"&limit=2&continue=2", nil))
			require.NoError(t, err)
			require.Equal(t, http.StatusBadRequest, response.StatusCode, "a continue token cannot have been issued without an API reader")
		})
	}
}

func TestListPageReportsReaderFailures(t *testing.T) {
	scheme := paginatedListScheme(t)
	handlers := NewHandlers(HandlersConfig{
		Client:    fake.NewClientBuilder().WithScheme(scheme).Build(),
		APIReader: failingGatewayIdentityReader{err: errors.New("API unavailable")},
	})
	err := handlers.listPage(context.Background(), &corev1alpha1.TaskList{}, &client.ListOptions{Limit: 1}, "tasks")
	var fiberErr *fiber.Error
	require.ErrorAs(t, err, &fiberErr)
	require.Equal(t, http.StatusInternalServerError, fiberErr.Code)
	require.Contains(t, fiberErr.Message, "failed to list tasks: API unavailable")
}

// splitBuiltinListNames separates the "builtin:" entries paginatedListNames
// records for built-in tools from the CRD-backed item names.
func splitBuiltinListNames(names []string) (crd, builtins []string) {
	for _, name := range names {
		if rest, ok := strings.CutPrefix(name, "builtin:"); ok {
			builtins = append(builtins, rest)
			continue
		}
		crd = append(crd, name)
	}
	return crd, builtins
}

func TestCollectAuthorizedPagesFillsAcrossFilteredPages(t *testing.T) {
	// Pages where the filter hides every item on page 2 and one item on page
	// 3: a scoped caller must receive one filled page and one cursor, not an
	// empty page (and cursor) per hidden object, and never more than limit.
	pages := map[string]struct {
		items []string
		next  string
	}{
		"":  {items: []string{"a", "b"}, next: "2"},
		"2": {items: []string{}, next: "3"},
		"3": {items: []string{"c"}, next: "4"},
		"4": {items: []string{"d", "e"}, next: ""},
	}
	var fetched []string
	var limits []int64
	fetch := func(continueToken string, pageLimit int64) ([]string, string, error) {
		fetched = append(fetched, continueToken)
		limits = append(limits, pageLimit)
		page := pages[continueToken]
		items := page.items
		if pageLimit > 0 && int64(len(items)) > pageLimit {
			items = items[:pageLimit]
		}
		return items, page.next, nil
	}
	items, next, err := collectAuthorizedPages(2, "", fetch)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, items)
	require.Equal(t, "2", next)

	fetched, limits = nil, nil
	items, next, err = collectAuthorizedPages(2, "2", fetch)
	require.NoError(t, err)
	require.Equal(t, []string{"c", "d"}, items, "the walk must continue past the emptied page until the page is filled")
	require.Equal(t, "", next)
	require.Equal(t, []string{"2", "3", "4"}, fetched)
	require.Equal(t, []int64{2, 2, 1}, limits, "each fetch is asked for only the remaining capacity")

	// An unlimited request is served as one complete page.
	fetched = nil
	items, next, err = collectAuthorizedPages(0, "", fetch)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, items)
	require.Equal(t, "2", next)
	require.Equal(t, []string{""}, fetched)

	// The walk continues past long runs of hidden objects whether the page
	// is empty or partially filled, so authorized results are neither hidden
	// behind an empty page nor split prematurely at a hidden boundary.
	const hiddenCursor = "more"
	const firstAuthorized = "first-authorized"
	calls := 0
	items, next, err = collectAuthorizedPages(2, "", func(string, int64) ([]string, string, error) {
		calls++
		switch calls {
		case 1:
			return []string{firstAuthorized}, hiddenCursor, nil
		case 40:
			return []string{"late"}, "after-late", nil
		}
		return nil, hiddenCursor, nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{firstAuthorized, "late"}, items)
	require.Equal(t, "after-late", next)
	require.Equal(t, 40, calls)

	// A full authorized page cannot return a cursor bounded by a hidden raw
	// object. Each raw fetch is capped at the remaining authorized capacity,
	// so after filtering a hidden boundary leaves the result short and forces
	// another fetch. The cursor returned when the result fills is therefore
	// bounded by the authorized object that filled it.
	type rawItem struct {
		name    string
		allowed bool
	}
	raw := []rawItem{
		{name: "allowed-a", allowed: true},
		{name: "hidden-a"},
		{name: "hidden-b"},
		{name: "allowed-b", allowed: true},
		{name: "hidden-tail"},
	}
	offsets := map[string]int{"": 0}
	items, next, err = collectAuthorizedPages(2, "", func(cursor string, pageLimit int64) ([]string, string, error) {
		start := offsets[cursor]
		end := min(start+int(pageLimit), len(raw))
		page := make([]string, 0, end-start)
		for _, item := range raw[start:end] {
			if item.allowed {
				page = append(page, item.name)
			}
		}
		if end == len(raw) {
			return page, "", nil
		}
		next := "after-" + raw[end-1].name
		offsets[next] = end
		return page, next, nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"allowed-a", "allowed-b"}, items)
	require.Equal(t, "after-allowed-b", next)

	// The page budget is the residual bound. It must not expose a raw storage
	// cursor whose boundary was set by objects hidden from the caller or claim
	// that the collection ended while more raw pages remain.
	calls = 0
	items, next, err = collectAuthorizedPages(1, "", func(string, int64) ([]string, string, error) {
		calls++
		return nil, hiddenCursor, nil
	})
	var fiberErr *fiber.Error
	require.ErrorAs(t, err, &fiberErr)
	require.Equal(t, fiber.StatusServiceUnavailable, fiberErr.Code)
	require.Contains(t, fiberErr.Message, "authorized list scan exceeded")
	require.Empty(t, items)
	require.Empty(t, next)
	require.Equal(t, maxAuthorizedListPages, calls)

	_, _, err = collectAuthorizedPages(1, "", func(string, int64) ([]string, string, error) { return nil, "", errors.New("boom") })
	require.EqualError(t, err, "boom")
}
