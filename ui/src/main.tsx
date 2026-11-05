import { StrictMode, Suspense, lazy, useEffect } from 'react'
import { QueryClientProvider } from '@tanstack/react-query'
import { createRoot } from 'react-dom/client'
import '@fontsource-variable/recursive/full.css'
import './console.css'
import { queryClient } from './lib/query'
import { navigate, usePath } from './lib/router'
import { AdminApp } from './pages/AdminApp'

// their own chunks: the docs' nav and page index and the landing page aren't needed by the console
const DocsApp = lazy(() => import('./pages/docs/DocsApp').then((m) => ({ default: m.DocsApp })))

const Home = lazy(() => import('./pages/home/Home').then((m) => ({ default: m.Home })))

const isDocs = (path: string) => path === '/docs' || path.startsWith('/docs/')

function App() {
  const path = usePath()
  const admin = path === '/admin' || path.startsWith('/admin/')
  const home = path === '/'
  const ok = admin || home || isDocs(path)
  useEffect(() => {
    if (!ok) navigate('/admin/', { replace: true })
  }, [ok])
  if (isDocs(path))
    return (
      <Suspense fallback={null}>
        <DocsApp path={path} />
      </Suspense>
    )
  if (home)
    return (
      <Suspense fallback={null}>
        <Home />
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
