import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { RouterProvider, createRouter } from '@tanstack/react-router'
import { routeTree } from './routeTree.gen'
import { useAuthStore } from './stores/auth'
import { useChatStore } from './stores/chat'
import { useUIStore } from './stores/ui'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30 * 1000,
      retry: 1,
    },
  },
})

// Cached state belongs to the token that fetched or created it. On any
// identity change (logout, or signing in as someone else) the query cache is
// cleared and the chat store is reset — transcript, session, provider/model
// selections (including their localStorage persistence), and the turn epoch,
// which also aborts any in-flight chat turn — so a new session can never
// read or reuse the previous token's data.
let lastToken = useAuthStore.getState().token
useAuthStore.subscribe((state) => {
  if (state.token !== lastToken) {
    lastToken = state.token
    queryClient.clear()
    useChatStore.getState().resetForIdentityChange()
  }
})

// Namespace changes can happen from the global header while Chat is not
// mounted. Keep the chat store synchronized here so switching namespaces
// always resets the old transcript and aborts its in-flight turn.
let lastNamespace = useUIStore.getState().namespace
useChatStore.getState().selectNamespace(lastNamespace)
useUIStore.subscribe((state) => {
  if (state.namespace !== lastNamespace) {
    lastNamespace = state.namespace
    useChatStore.getState().selectNamespace(state.namespace)
  }
})

const router = createRouter({ routeTree })

declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router
  }
}

export function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  )
}
