import { useEffect, useState } from 'react'
import { Icon, Mark } from '../../components/icons'
import { setTheme } from '../../components/Shell'
import { Link } from '../../lib/router'

// The docs' bar and footer. The docs are public, the console isn't, so the
// console link shows only where /admin answers (the API listener has none).

let reachable: Promise<boolean> | null = null

export function useConsoleReachable() {
  const [ok, setOk] = useState(false)
  useEffect(() => {
    let live = true
    reachable ??= fetch('/admin/').then(
      (r) => r.ok,
      () => false,
    )
    reachable.then((v) => live && setOk(v))
    return () => {
      live = false
    }
  }, [])
  return ok
}

export function PubTop({ dark, consoleHere }: { dark: boolean; consoleHere: boolean }) {
  return (
    <header className="cx-pub-top">
      <Link to="/docs" className="cx-wordmark" aria-label="marco docs">
        <Mark />
        marco
      </Link>
      <Link to="/docs" className="l on" aria-current="page">
        Docs
      </Link>
      {consoleHere && (
        <a href="/admin/" className="l">
          Console
        </a>
      )}
      <span className="cx-spacer" />
      <button type="button" className="cx-iconbtn" onClick={() => setTheme(dark ? 'light' : 'dark')} title="Toggle theme" aria-label={`Switch to ${dark ? 'light' : 'dark'} theme`}>
        <Icon name="theme" />
      </button>
    </header>
  )
}

export function PubFoot({ consoleHere }: { consoleHere: boolean }) {
  return (
    <footer className="cx-pubfoot">
      <span>marco</span>
      <Link to="/docs">Docs</Link>
      <a href="/healthz">Health</a>
      <a href="/readyz">Ready</a>
      {consoleHere && <a href="/admin/">Operator console</a>}
    </footer>
  )
}
