import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
import { navigate } from '../lib/router'
import { Glyph, type GlyphKind } from './kit'
import { Icon } from './icons'
import { openPanel } from './nav'
import { recentList } from './recent'
import { SECTIONS } from './sections'
import { closeDialog, openDialog } from './dialogs'

// ⌘K. On an empty query: what needs attention (one item per live banner), the last details
// opened in this tab, and the syntax. Typed: sections, a DID, an at:// URI, `pair a b`,
// `shard-041`, and whatever verbs pages register (`trace 1%`, `cpu 30s`).

export type PalItem = { id: string; glyph: ReactNode; label: ReactNode; kind?: ReactNode; run: () => void }
type Provider = (q: string) => PalItem[]

const providers = new Map<string, Provider>()
export const registerPalette = (id: string, p: Provider) => void providers.set(id, p)

let attention: PalItem[] = []
export const setAttention = (items: PalItem[]) => {
  attention = items
}

const g = (k: GlyphKind) => <Glyph k={k} />
const sym = (s: string, cls = 'muted') => <span className={`cx-g ${cls}`}>{s}</span>

const DID = /^did:[a-z]+:[a-zA-Z0-9._:%-]+$/

function builtin(q: string): PalItem[] {
  const out: PalItem[] = []
  const t = q.trim()
  if (DID.test(t)) out.push({ id: `actor:${t}`, glyph: sym('@'), label: t, kind: 'actor', run: () => navigate(`/admin/explorer?actor=${encodeURIComponent(t)}`) })
  if (t.startsWith('at://')) out.push({ id: `edge:${t}`, glyph: sym('→', 's-out'), label: t, kind: 'edge', run: () => openPanel('edge', t) })
  const pair = t.match(/^pair\s+(\S+)\s+(\S+)$/)
  if (pair) out.push({ id: 'pair', glyph: sym('›'), label: `pair ${pair[1]} ${pair[2]}`, kind: 'Relationships', run: () => navigate(`/admin/explorer?actor=${encodeURIComponent(pair[1])}&b=${encodeURIComponent(pair[2])}`) })
  const sh = t.match(/^shard[- ]?(\d{1,4})$/i)
  if (sh) out.push({ id: `shard:${sh[1]}`, glyph: sym('#'), label: `shard-${sh[1].padStart(3, '0')}`, kind: 'shard', run: () => openPanel('shard', String(Number(sh[1]))) })
  const lq = t.toLowerCase()
  for (const s of SECTIONS)
    if (lq && s.label.toLowerCase().includes(lq)) out.push({ id: `sec:${s.id}`, glyph: sym('›'), label: s.label, kind: 'section', run: () => navigate(s.path) })
  return out
}

function Pal({ close }: { close: () => void }) {
  const [q, setQ] = useState('')
  const [sel, setSel] = useState(0)
  const input = useRef<HTMLInputElement>(null)
  useEffect(() => input.current?.focus(), [])
  const groups = useMemo(() => {
    if (!q.trim()) {
      const rec = recentList().map<PalItem>((r) => ({
        id: `r:${r.type}:${r.id}`,
        glyph: r.type === 'edge' ? sym('→', 's-out') : r.type === 'shard' ? sym('#') : sym('@'),
        label: r.title,
        kind: r.kind.toLowerCase(),
        run: () => openPanel(r.type, r.id),
      }))
      return [
        ['Needs attention', attention],
        ['Recent', rec],
        [
          'Syntax',
          [
            {
              id: 'syntax',
              glyph: sym('›'),
              label: (
                <>
                  <span className="mono">pair a b</span> · <span className="mono">at://…</span> · <span className="mono">trace 1%</span> · <span className="mono">cpu 30s</span>
                </>
              ),
              run: () => input.current?.focus(),
            },
          ],
        ],
      ] as [string, PalItem[]][]
    }
    const found = [...builtin(q), ...[...providers.values()].flatMap((p) => p(q))]
    return [['Results', found]] as [string, PalItem[]][]
  }, [q])
  const flat = groups.flatMap(([, xs]) => xs)
  const pick = (i: number) => {
    const it = flat[i]
    if (!it) return
    close()
    it.run()
  }
  let i = -1
  return (
    <div className="cx-pal" role="dialog" aria-modal="true" aria-label="Command palette">
      <div className="pi">
        <Icon name="search" size={16} />
        <input
          ref={input}
          className="cx-inp"
          style={{ border: 0, background: 'transparent', flex: 1, fontSize: 15, padding: 0 }}
          value={q}
          placeholder="DID, at:// URI, shard, command…"
          aria-label="Search"
          spellCheck={false}
          onChange={(e) => {
            setQ(e.target.value)
            setSel(0)
          }}
          onKeyDown={(e) => {
            if (e.key === 'ArrowDown') {
              e.preventDefault()
              setSel((s) => Math.min(flat.length - 1, s + 1))
            } else if (e.key === 'ArrowUp') {
              e.preventDefault()
              setSel((s) => Math.max(0, s - 1))
            } else if (e.key === 'Enter') {
              e.preventDefault()
              pick(sel)
            }
          }}
        />
        <kbd>esc</kbd>
      </div>
      <div className="list">
        {groups.map(([name, xs]) =>
          xs.length ? (
            <div key={name}>
              <div className="pg cx-eyebrow">{name}</div>
              {xs.map((it) => {
                i++
                const me = i
                return (
                  <button key={it.id} type="button" className={`pr${me === sel ? ' on' : ''}`} onMouseEnter={() => setSel(me)} onClick={() => pick(me)}>
                    {it.glyph}
                    <span>{it.label}</span>
                    {it.kind && <span className="muted sm">{it.kind}</span>}
                    {me === sel && (
                      <span className="k">
                        <kbd>↵</kbd>
                      </span>
                    )}
                  </button>
                )
              })}
            </div>
          ) : null,
        )}
        {q.trim() && !flat.length && <div className="pg muted">No match</div>}
      </div>
    </div>
  )
}

export const openPalette = () => openDialog((close) => <Pal close={close} />, 'cx-palscrim')
export const closePalette = closeDialog
export { g as palGlyph }
