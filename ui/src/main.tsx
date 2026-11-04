import { StrictMode, useEffect } from 'react'
import { QueryClientProvider } from '@tanstack/react-query'
import { createRoot } from 'react-dom/client'
import '@fontsource-variable/recursive/full.css'
import './console.css'
import { queryClient } from './lib/query'
import { navigate, usePath } from './lib/router'
import { AdminApp } from './pages/AdminApp'

function App() {
  const path = usePath()
  const ok = path === '/admin' || path.startsWith('/admin/')
  useEffect(() => {
    if (!ok) navigate('/admin/', { replace: true })
  }, [ok])
  return ok ? <AdminApp path={path.replace(/\/$/, '') || '/admin'} /> : null
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <App />
    </QueryClientProvider>
  </StrictMode>,
)
