import { useEffect, useState, type ReactNode } from 'react'
import { DetailPage, detailKind } from '../components/Drawer'
import { ErrorState, Glyph, Loading } from '../components/kit'
import { Mark } from '../components/icons'
import { Shell, useTheme } from '../components/Shell'
import { SECTION, sectionOf } from '../components/sections'
import { admin, errText, getToken, initApi, isUnauthenticated, setToken } from '../lib/api'
import { queryClient } from '../lib/query'
import { keys } from '../lib/queries'
import { match, useSearch } from '../lib/router'
import { Explorer } from './Explorer'
import { Ingest } from './Ingest'
import { Overview } from './Overview'
import { Relations } from './Relations'
import { Requests } from './Requests'
import { Settings } from './Settings'
import { Storage } from './Storage'
import './details'

type Gate = { state: 'loading' } | { state: 'locked'; why?: string } | { state: 'open' } | { state: 'error'; error: unknown }

/** Asks GetSession first: a proxy login or a stored token opens the console at once. */
function useGate(): [Gate, (t: string) => void] {
  const [g, setG] = useState<Gate>({ state: 'loading' })
  const check = async () => {
    try {
      await initApi()
      const s = await admin().getSession({})
      queryClient.setQueryData(keys.session, s)
      setG(s.auth ? { state: 'open' } : { state: 'locked' })
    } catch (e) {
      if (isUnauthenticated(e)) {
        setG({ state: 'locked', why: getToken() ? 'Token refused' : errText(e) })
        if (getToken()) setToken('')
      } else setG({ state: 'error', error: e })
    }
  }
  useEffect(() => {
    check()
  }, [])
  return [
    g,
    (t: string) => {
      setToken(t)
      setG({ state: 'loading' })
      check()
    },
  ]
}

function Lock({ why, onToken }: { why?: string; onToken: (t: string) => void }) {
  const [t, setT] = useState('')
  const [, dark] = useTheme()
  return (
    <div className={`cx${dark ? ' dark' : ''}`} style={{ display: 'grid', placeItems: 'center', gridTemplateColumns: '1fr' }}>
      <form className="cx-dlg" style={{ width: 420 }} onSubmit={(e) => (e.preventDefault(), t.trim() && onToken(t.trim()))}>
        <header>
          <Mark />
          <h2>marco</h2>
        </header>
        <div className="db">
          <label className="cx-lbl" htmlFor="tok">
            Admin token
          </label>
          <input id="tok" type="password" className="cx-inp mono" autoFocus autoComplete="off" value={t} onChange={(e) => setT(e.target.value)} />
          {why && (
            <div className="dlg-err" role="alert">
              <Glyph k="err" />
              <span>{why}</span>
            </div>
          )}
        </div>
        <footer>
          <button type="submit" className="cx-btn primary" disabled={!t.trim()}>
            Unlock
          </button>
        </footer>
      </form>
    </div>
  )
}

function route(path: string): { crumbs: ReactNode[]; page: ReactNode } {
  const d = match('/admin/:section/:type/*', path)
  if (d && detailKind(d.type)) {
    const k = detailKind(d.type)!
    return { crumbs: [SECTION[k.section].group, SECTION[k.section].label, d['*']], page: <DetailPage type={d.type} id={d['*']} /> }
  }
  const s = sectionOf(path)
  const page = { overview: <Overview />, explorer: <Explorer />, relations: <Relations />, storage: <Storage />, ingest: <Ingest />, requests: <Requests />, settings: <Settings /> }[s.id]
  return { crumbs: s.id === 'overview' ? ['Overview'] : [s.group, s.label], page }
}

export function AdminApp({ path }: { path: string }) {
  const [gate, unlock] = useGate()
  const q = useSearch()
  if (gate.state === 'loading') return <Loading />
  if (gate.state === 'error') return <ErrorState error={gate.error} retry={() => location.reload()} />
  if (gate.state === 'locked') return <Lock why={gate.why} onToken={unlock} />
  const r = route(path)
  const actor = q.get('actor')
  const crumbs = sectionOf(path).id === 'explorer' && actor ? [...r.crumbs, actor] : r.crumbs
  return <Shell crumbs={crumbs}>{r.page}</Shell>
}

