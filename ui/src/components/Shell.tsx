import { useEffect, useState, type ReactNode } from 'react'
import { navigate, usePath, Link } from '../lib/router'
import { getLive, togglePaused, useLiveState, setSources } from '../lib/live'
import { useOverview } from '../lib/queries'
import { fmtCount } from '../lib/fmt'
import { Icon, Mark } from './icons'
import { DialogHost, InfoDialog, isDialogOpen, openDialog } from './dialogs'
import { Drawer, detailPath } from './Drawer'
import { Kbd } from './kit'
import { closePanel, panelOf, openPanel } from './nav'
import { openPalette } from './Palette'
import { SECTIONS, TABBAR, sectionOf, SECTION } from './sections'
import { Toasts } from './toast'

// The console's frame: the top bar with the warp-and-weft rule, the rail, the main column, the
// slide-over, dialogs and toasts, and the keyboard.

type Theme = 'system' | 'light' | 'dark'
const THEME_KEY = 'marco.console.theme'
const readTheme = (): Theme => {
  try {
    const t = localStorage.getItem(THEME_KEY)
    return t === 'light' || t === 'dark' ? t : 'system'
  } catch {
    return 'system'
  }
}
let theme: Theme = readTheme()
const themeSubs = new Set<() => void>()
export function setTheme(t: Theme) {
  theme = t
  try {
    localStorage.setItem(THEME_KEY, t)
  } catch {
    /* this tab only */
  }
  themeSubs.forEach((l) => l())
}
export function useTheme(): [Theme, boolean] {
  const [, force] = useState(0)
  const [sysDark, setSysDark] = useState(() => matchMedia('(prefers-color-scheme: dark)').matches)
  useEffect(() => {
    const l = () => force((x) => x + 1)
    themeSubs.add(l)
    const mq = matchMedia('(prefers-color-scheme: dark)')
    const m = () => setSysDark(mq.matches)
    mq.addEventListener('change', m)
    return () => {
      themeSubs.delete(l)
      mq.removeEventListener('change', m)
    }
  }, [])
  return [theme, theme === 'dark' || (theme === 'system' && sysDark)]
}

function KeyHelp({ close }: { close: () => void }) {
  const rows: [ReactNode, string][] = [
    [<Kbd k="⌘K" />, 'palette'],
    [<Kbd k={['g', 'o']} />, 'go to section (o e r s i q ,)'],
    [<Kbd k={['j', 'k']} />, 'move in rows'],
    [<Kbd k="↵" />, 'open row'],
    [<Kbd k="o" />, 'full page'],
    [<Kbd k="esc" />, 'close'],
    [<Kbd k="/" />, 'search'],
    [<Kbd k="space" />, 'pause live updates'],
    [<Kbd k="t" />, 'theme'],
  ]
  return (
    <InfoDialog title="Shortcuts" onClose={close}>
      <div className="cx-keys">
        {rows.map(([k, l], i) => (
          <div key={i} style={{ display: 'contents' }}>
            <span>{k}</span>
            <span className="t2">{l}</span>
          </div>
        ))}
      </div>
    </InfoDialog>
  )
}
export const openHelp = () => openDialog((close) => <KeyHelp close={close} />)

function useKeys() {
  useEffect(() => {
    let g = false
    let gt: ReturnType<typeof setTimeout> | undefined
    const rows = () => [...document.querySelectorAll<HTMLTableRowElement>('main tr[data-open]')]
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault()
        if (!isDialogOpen()) openPalette()
        return
      }
      const t = e.target as HTMLElement
      if (t.closest('input,textarea,select,[contenteditable]') || e.metaKey || e.ctrlKey || e.altKey || isDialogOpen()) return
      const panel = panelOf(new URLSearchParams(location.search))
      if (g) {
        g = false
        const s = SECTIONS.find((x) => x.key === e.key)
        if (s) {
          e.preventDefault()
          navigate(s.path)
        }
        return
      }
      switch (e.key) {
        case 'g':
          g = true
          clearTimeout(gt)
          gt = setTimeout(() => (g = false), 1200)
          break
        case 'Escape':
          if (panel) closePanel()
          else if (/\/admin\/[^/]+\/[^/]+\/.+/.test(location.pathname)) navigate(sectionOf(location.pathname).path)
          break
        case 'o':
          if (panel) navigate(detailPath(panel.type, panel.id) ?? location.pathname)
          break
        case '/': {
          e.preventDefault()
          const s = document.querySelector<HTMLInputElement>('main [data-search]')
          if (s) s.focus()
          else openPalette()
          break
        }
        case ' ':
          e.preventDefault()
          togglePaused()
          break
        case 't':
          setTheme(theme === 'dark' ? 'light' : theme === 'light' ? 'system' : 'dark')
          break
        case '?':
          openHelp()
          break
        case 'j':
        case 'k': {
          const rs = rows()
          if (!rs.length) break
          const cur = rs.findIndex((r) => r.classList.contains('sel'))
          const nx = Math.max(0, Math.min(rs.length - 1, cur < 0 ? 0 : cur + (e.key === 'j' ? 1 : -1)))
          const [type, ...id] = (rs[nx].dataset.open ?? '').split(':')
          if (type && type !== 'row') openPanel(type, id.join(':'), { replace: true })
          rs[nx].scrollIntoView({ block: 'nearest' })
          break
        }
        case 'Enter': {
          const r = rows().find((x) => x.classList.contains('sel')) ?? rows()[0]
          r?.click()
          break
        }
      }
    }
    addEventListener('keydown', onKey)
    return () => removeEventListener('keydown', onKey)
  }, [])
}

export function Shell({ crumbs, children }: { crumbs: ReactNode[]; children: ReactNode }) {
  const path = usePath()
  const sec = sectionOf(path)
  const live = useLiveState()
  const [, dark] = useTheme()
  const ov = useOverview()
  useKeys()
  useEffect(() => {
    document.documentElement.classList.toggle('dark', dark)
  }, [dark])
  const o = ov.data
  const dirty = o ? o.dirtyShards + o.overShards : 0
  const streamTitle = live.stale ? `not answering: ${live.staleError ?? ''}` : live.paused ? 'paused (space)' : 'reads every 2 s · writes every 1 s'
  return (
    <div className={`cx${dark ? ' dark' : ''}${live.stale ? ' stale' : ''}`}>
      <header className="cx-top">
        <Link className="cx-wordmark" to="/admin">
          <Mark />
          marco<span className="where">{location.host}</span>
        </Link>
        <nav className="cx-crumbs" aria-label="Breadcrumb">
          {crumbs.map((c, i) => (
            <span key={i} style={{ display: 'contents' }}>
              {i > 0 && <span className="sep">/</span>}
              {i === crumbs.length - 1 ? <b>{c}</b> : <span>{c}</span>}
            </span>
          ))}
        </nav>
        <div className="cx-spacer" />
        <button type="button" className="cx-stream" title={streamTitle} onClick={togglePaused} style={{ cursor: 'pointer' }}>
          <span className="dot" style={live.stale ? { background: 'var(--err)', boxShadow: 'none' } : live.paused ? { background: 'var(--info)', boxShadow: 'none' } : undefined} />
          {live.stale ? 'offline' : live.paused ? 'paused' : 'live'}
          {o && <span className="muted mono">seq {fmtCount(o.writeSeq)}</span>}
        </button>
        <button type="button" className="cx-kbtn" onClick={openPalette} aria-label="Command palette">
          <Icon name="search" />
          <span className="lbl">Jump to DID, at:// URI, shard…</span>
          <kbd>⌘K</kbd>
        </button>
        <button type="button" className="cx-iconbtn" aria-label="Toggle theme" title="Theme (t)" onClick={() => setTheme(dark ? 'light' : 'dark')}>
          <Icon name="theme" />
        </button>
      </header>
      <aside className="cx-side" aria-label="Sections">
        {(['Graph', 'Engine', 'System'] as const).map((grp) => (
          <div key={grp} style={{ display: 'contents' }}>
            <h6>{grp}</h6>
            {SECTIONS.filter((s) => s.group === grp).map((s) => {
              const on = s.id === sec.id
              const badge =
                s.id === 'storage' && dirty > 0 ? (
                  <span className="cx-badge info" title={o && o.overShards ? `dirty shards, ${o.overShards} over threshold` : 'dirty shards'}>
                    ◆ {dirty}
                  </span>
                ) : null
              return (
                <Link key={s.id} className={`cx-nav${on ? ' on' : ''}`} to={s.path} aria-current={on ? 'page' : undefined}>
                  <span className="ni">
                    <Icon name={s.id} />
                  </span>
                  {s.label}
                  {badge ?? <span className="k">g {s.key}</span>}
                </Link>
              )
            })}
          </div>
        ))}
        <div className="cx-sidefoot">
          {o && (
            <>
              <div className="mono">
                marco {o.version} · {o.commit}
              </div>
              <div className="mono">snapshot {o.snapshot?.build.slice(0, 8)}</div>
              <div>ops on {o.adminListen}</div>
            </>
          )}
          <div>
            <a href="#" onClick={(e) => (e.preventDefault(), openHelp())}>
              shortcuts
            </a>
          </div>
          <div>
            <a href="#" onClick={(e) => (e.preventDefault(), setSources(!getLive().showSources))}>
              {live.showSources ? 'Hide' : 'Show'} data sources
            </a>
          </div>
        </div>
      </aside>
      <main className="cx-main">
        <div className="cx-view">{children}</div>
      </main>
      <nav className="cx-tabbar" aria-label="Sections">
        {TABBAR.map((id) => (
          <Link key={id} to={SECTION[id].path} className={sec.id === id ? 'on' : ''}>
            <Icon name={id} size={18} />
            {SECTION[id].label}
          </Link>
        ))}
        <a href="#" onClick={(e) => (e.preventDefault(), openPalette())}>
          <Icon name="more" size={18} />
          More
        </a>
      </nav>
      <Drawer />
      <DialogHost />
      <Toasts />
    </div>
  )
}

