import { useEffect, useRef, type ReactNode } from 'react'
import { Link, navigate } from '../lib/router'
import { Icon } from './icons'
import { Empty, Kbd, Loading, PageHead, Updated } from './kit'
import { closePanel, fullPath, openPanel, usePanel } from './nav'
import { noteRecent } from './recent'
import { SECTION, type SectionId } from './sections'

// One renderer per kind of row (edge, actor, shard), shown in the slide-over or as a full page.
// A kind's `use` hook loads what it needs and returns the pieces.

export type DetailMode = 'drawer' | 'page'
export type DetailView = {
  title: ReactNode
  /** The title as text, for ⌘K's Recent. */
  text?: string
  chip?: ReactNode
  body: ReactNode
  foot?: ReactNode
  fresh?: { at?: number; error?: unknown }
  loading?: boolean
  missing?: ReactNode
}
export type DetailKind = { kind: string; section: SectionId; use: (id: string, mode: DetailMode) => DetailView }

const registry = new Map<string, DetailKind>()
export const registerDetail = (type: string, k: DetailKind) => void registry.set(type, k)
export const detailKind = (type: string) => registry.get(type)
export const detailPath = (type: string, id: string) => {
  const k = registry.get(type)
  return k ? fullPath(SECTION[k.section].path, type, id) : undefined
}

function useNoteRecent(type: string, id: string, k: DetailKind, v: DetailView) {
  const ready = !v.loading && !v.missing
  const title = v.text ?? (typeof v.title === 'string' ? v.title : id)
  useEffect(() => {
    if (ready) noteRecent({ type, id, title, kind: k.kind })
  }, [type, id, ready, title, k.kind])
}

function Inner({ type, id, k }: { type: string; id: string; k: DetailKind }) {
  const v = k.use(id, 'drawer')
  useNoteRecent(type, id, k, v)
  const body = useRef<HTMLDivElement>(null)
  useEffect(() => {
    body.current?.scrollTo(0, 0)
  }, [id])
  return (
    <>
      <header>
        <div style={{ minWidth: 0 }}>
          <div className="kind">{k.kind}</div>
          <h2 id="cx-drawer-t">{v.title}</h2>
        </div>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 8, alignItems: 'center' }}>
          {v.chip}
          <button type="button" className="cx-btn sm" title="Open as a full page (o)" onClick={() => navigate(detailPath(type, id)!)}>
            Full page <Icon name="ext" size={12} />
          </button>
          <button type="button" className="cx-iconbtn" aria-label="Close (esc)" onClick={closePanel}>
            <Icon name="close" />
          </button>
        </div>
      </header>
      <div className="dbody" ref={body}>
        {v.missing ? <Empty title="Not available">{v.missing}</Empty> : v.loading ? <Loading /> : v.body}
      </div>
      <div className="dfoot">
        {v.fresh && !v.missing && <Updated l={v.fresh} />}
        {v.foot}
        <span style={{ marginLeft: 'auto', display: 'flex', gap: 6, alignItems: 'center' }}>
          <Kbd k="o" /> full page <Kbd k="esc" /> close
        </span>
      </div>
    </>
  )
}

/** The slide-over, driven by ?open=type:id. */
export function Drawer() {
  const p = usePanel()
  const k = p && registry.get(p.type)
  if (!p || !k) return null
  return (
    <>
      <div className="cx-dscrim" onClick={closePanel} />
      <aside className="cx-drawer" role="dialog" aria-modal="false" aria-labelledby="cx-drawer-t">
        <Inner key={`${p.type}:${p.id}`} type={p.type} id={p.id} k={k} />
      </aside>
    </>
  )
}

/** A detail as a page of its own (/admin/<section>/<type>/<id>). */
export function DetailPage({ type, id }: { type: string; id: string }) {
  const k = registry.get(type)!
  const v = k.use(id, 'page')
  useNoteRecent(type, id, k, v)
  const sec = SECTION[k.section]
  return (
    <div className="cx-fullpage">
      <PageHead
        title={
          <>
            {v.title} {v.chip}
          </>
        }
        sub={
          <>
            <span className="cx-eyebrow">{k.kind}</span>
            {v.fresh && !v.missing && <Updated l={v.fresh} />}
          </>
        }
        actions={
          <>
            <Link className="cx-btn" to={sec.path}>
              ← {sec.label}
            </Link>
            <button
              type="button"
              className="cx-btn"
              onClick={() => {
                navigate(sec.path)
                openPanel(type, id)
              }}
            >
              Open as panel
            </button>
          </>
        }
      />
      <div className="cx-stack">{v.missing ? <Empty title="Not available">{v.missing}</Empty> : v.loading ? <Loading /> : v.body}</div>
    </div>
  )
}
