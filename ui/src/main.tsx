import { StrictMode, Suspense, lazy, useEffect } from 'react'
import { QueryClientProvider } from '@tanstack/react-query'
import { createRoot } from 'react-dom/client'
import '@fontsource-variable/recursive/full.css'
import './console.css'
import { queryClient } from './lib/query'
import { navigate, usePath } from './lib/router'
import { AdminApp } from './pages/AdminApp'

// its own chunk: the docs' nav and page index aren't needed by the console
const DocsApp = lazy(() => import('./pages/docs/DocsApp').then((m) => ({ default: m.DocsApp })))

const isDocs = (path: string) => path === '/docs' || path.startsWith('/docs/')

function App() {
  const path = usePath()
  const admin = path === '/admin' || path.startsWith('/admin/')
  const ok = admin || isDocs(path)
  useEffect(() => {
    if (!ok) navigate('/admin/', { replace: true })
  }, [ok])
  if (isDocs(path))
    return (
      <Suspense fallback={null}>
        <DocsApp path={path} />
      </Suspense>
    )
  return admin ? <AdminApp path={path.replace(/\/$/, '') || '/admin'} /> : null
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  </StrictMode>,
)
