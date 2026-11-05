import { useEffect, useState } from 'react'
import { Icon, Mark } from '../../components/icons'
import { setTheme } from '../../components/Shell'
import { Link } from '../../lib/router'

// The public pages' bar and footer. The docs are on both listeners, the console only on the
// admin one and the landing page (with the explorer) only on the API one, so their links show
// once /docs/_console says which listener this is (undefined until then).

export const GITHUB = 'https://github.com/jazware/marco'

let reachable: Promise<boolean> | null = null

export function useConsoleReachable(): boolean | undefined {
  const [ok, setOk] = useState<boolean | undefined>(undefined)
  useEffect(() => {
    let live = true
    reachable ??= fetch('/docs/_console').then(
      (r) => (r.ok ? r.json().then((j: { console?: boolean }) => j.console === true, () => false) : false),
      () => false,
    )
    reachable.then((v) => live && setOk(v))
    return () => {
      live = false
    }
  }, [])
  return ok
}

export function PubTop({ dark, consoleHere, here = 'docs' }: { dark: boolean; consoleHere: boolean | undefined; here?: 'docs' | 'home' }) {
  const home = consoleHere === false
  return (
    <header className="cx-pub-top">
      <Link to={home ? '/' : '/docs'} className="cx-wordmark" aria-label="marco">
        <Mark />
        marco
      </Link>
      <Link to="/docs" className={`l${here === 'docs' ? ' on' : ''}`} aria-current={here === 'docs' ? 'page' : undefined}>
        Docs
      </Link>
      {home && (
        <a href={here === 'home' ? '#explorer' : '/#explorer'} className="l">
          Explorer
        </a>
      )}
      {consoleHere && (
        <a href="/admin/" className="l">
          Console
        </a>
      )}
      <a href={GITHUB} className="l cx-pub-gh">
        GitHub
      </a>
      <span className="cx-spacer" />
      <button type="button" className="cx-iconbtn" onClick={() => setTheme(dark ? 'light' : 'dark')} title="Toggle theme" aria-label={`Switch to ${dark ? 'light' : 'dark'} theme`}>
        <Icon name="theme" />
      </button>
    </header>
  )
}

export function PubFoot({ consoleHere }: { consoleHere: boolean | undefined }) {
  return (
    <footer className="cx-pubfoot">
      <span>marco</span>
      <Link to="/docs">Docs</Link>
      {consoleHere === false && <a href="/#explorer">Explorer</a>}
      <a href={GITHUB}>GitHub</a>
      <a href="/healthz">Health</a>
      <a href="/readyz">Ready</a>
      {consoleHere && <a href="/admin/">Operator console</a>}
    </footer>
  )
}
