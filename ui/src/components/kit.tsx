import { useSyncExternalStore, type CSSProperties, type ReactNode } from 'react'
import { errText } from '../lib/api'
import { clock, ago } from '../lib/fmt'
import { useLiveState } from '../lib/live'
import { GLYPH, type Tone } from '../lib/tone'
import { Link } from '../lib/router'
import { toast } from './toast'

// The console's small parts, copied from vlpds's console kit by way of vlRelay's and re-skinned
// as loom. Presentational: data comes in as props. Status always pairs a colour with a glyph.

export type GlyphKind = keyof typeof GLYPH
export { GLYPH, type Tone }

export function Glyph({ k, title }: { k: GlyphKind; title?: string }) {
  return (
    <span className={`cx-g s-${k}`} title={title} aria-hidden={title ? undefined : true}>
      {GLYPH[k]}
    </span>
  )
}

export type ChipKind = Tone | 'acc' | 'plain' | 'out' | 'in' | 'sig'
export function Chip({ k, children, title, g }: { k: ChipKind; children: ReactNode; title?: string; g?: string | null }) {
  const glyph = g === null ? undefined : (g ?? (k === 'plain' ? undefined : GLYPH[k as GlyphKind]))
  return (
    <span className={`cx-chip ${k}`} title={title}>
      {glyph && <span aria-hidden="true">{glyph}</span>}
      {children}
    </span>
  )
}

export function Kbd({ k }: { k: string | string[] }) {
  const keys = Array.isArray(k) ? k : [k]
  return (
    <>
      {keys.map((x, i) => (
        <span key={i}>
          {i > 0 && ' '}
          <kbd>{x}</kbd>
        </span>
      ))}
    </>
  )
}

/** A value that updates live: hatched while the console is stale. */
export const LiveVal = ({ children }: { children: ReactNode }) => <span className="cx-live">{children}</span>

export const Swatch = ({ color }: { color: string }) => <span className="sw" style={{ background: `var(--${color})` }} />

// ---------------------------------------------------------------- sparklines, meters

/** A sparkline, 100 wide, stretched to its box. `l2` dashed on the same scale, `th` dotted. */
export function Spark({ data, l2, color = 'ink3', th, h = 22, fill = true, title }: { data: number[]; l2?: number[]; color?: string; th?: number; h?: number; fill?: boolean; title?: string }) {
  const a = data.filter((v) => isFinite(v))
  if (a.length < 2) return <svg className="cx-spark empty" viewBox={`0 0 100 ${h}`} style={{ height: h }} aria-hidden="true" />
  const all = [...a, ...(l2 ?? []).filter((v) => isFinite(v))]
  let lo = Math.min(...all)
  let hi = Math.max(...all, th ?? -Infinity)
  if (lo > 0 && lo < hi * 0.6) lo = 0
  const span = hi - lo || 1
  const pts = (vs: number[]) => vs.map((v, i) => `${((i * 100) / (vs.length - 1)).toFixed(1)},${(h - 2 - ((v - lo) / span) * (h - 4)).toFixed(1)}`).join(' ')
  const p = pts(data.map((v) => (isFinite(v) ? v : lo)))
  return (
    <svg className="cx-spark" viewBox={`0 0 100 ${h}`} preserveAspectRatio="none" style={{ height: h }} role={title ? 'img' : undefined} aria-label={title} aria-hidden={title ? undefined : true}>
      {fill && <polygon points={`0,${h} ${p} 100,${h}`} style={{ fill: `var(--${color})`, opacity: 0.12 }} />}
      {th !== undefined && <line className="th" x1="0" x2="100" y1={h - 2 - ((th - lo) / span) * (h - 4)} y2={h - 2 - ((th - lo) / span) * (h - 4)} />}
      {l2 && l2.length > 1 && <polyline className="l2" points={pts(l2)} style={{ stroke: `var(--${color})` }} />}
      <polyline className="l" points={p} style={{ stroke: `var(--${color})` }} />
    </svg>
  )
}

export function Meter({ v, max, k, th, wide, title }: { v: number; max: number; k?: 'out' | 'in' | 'warn' | 'acc'; th?: number; wide?: boolean; title?: string }) {
  const pct = max > 0 ? Math.min(100, Math.max(0, (v / max) * 100)) : 0
  return (
    <span className={`cx-meter${k ? ` ${k}` : ''}${wide ? ' wide' : ''}`} title={title} role="meter" aria-valuenow={v} aria-valuemin={0} aria-valuemax={max}>
      <i style={{ width: `${pct.toFixed(1)}%` }} />
      {th !== undefined && <span className="th" style={{ left: `${(th * 100).toFixed(1)}%` }} />}
    </span>
  )
}

// ---------------------------------------------------------------- tiles, health line, banners

export type TileSpec = { label: ReactNode; right?: ReactNode; value: ReactNode; unit?: ReactNode; sec?: ReactNode; spark?: ReactNode; to?: string; title?: string }
export function Tiles({ tiles, cols }: { tiles: TileSpec[]; cols?: number }) {
  return (
    <div className="cx-tilesbox">
      <div className="cx-tiles" style={cols ? { gridTemplateColumns: `repeat(${cols},minmax(0,1fr))` } : undefined}>
        {tiles.map((x, i) => {
          const inner = (
            <>
              <div className="tl">
                <span>{x.label}</span>
                {x.right && <span className="muted">{x.right}</span>}
              </div>
              <div className="tv">
                <LiveVal>{x.value}</LiveVal>
                {x.unit && <small>{x.unit}</small>}
                {x.sec && <span className="sec">{x.sec}</span>}
              </div>
              {x.spark}
            </>
          )
          return x.to ? (
            <Link key={i} to={x.to} className="cx-tile" title={x.title}>
              {inner}
            </Link>
          ) : (
            <div key={i} className="cx-tile" title={x.title}>
              {inner}
            </div>
          )
        })}
      </div>
    </div>
  )
}

export type HealthCell = { label: string; tone: Tone; value: ReactNode; unit?: ReactNode; sub: ReactNode; to: string; title?: string }
export function HealthLine({ cells }: { cells: HealthCell[] }) {
  return (
    <div className="cx-health" role="list" aria-label="Health">
      {cells.map((c) => (
        <Link key={c.label} to={c.to} className={`cx-hc${c.tone === 'warn' ? ' warn' : ''}`} role="listitem" title={c.title}>
          <span className="hl">
            <Glyph k={c.tone} />
            {c.label}
          </span>
          <span className="hv">
            <LiveVal>{c.value}</LiveVal>
            {c.unit && <small> {c.unit}</small>}
          </span>
          <span className="hs">{c.sub}</span>
        </Link>
      ))}
    </div>
  )
}

export type BannerSpec = { id: string; tone: Tone; title: ReactNode; desc?: ReactNode; right?: ReactNode }
export function Banners({ items }: { items: BannerSpec[] }) {
  if (!items.length) return null
  return (
    <div className="cx-banners">
      {items.map((b) => (
        <div key={b.id} className={`cx-banner ${b.tone}`}>
          <Glyph k={b.tone} />
          <span className="bt">{b.title}</span>
          {b.desc && <span className="bd">{b.desc}</span>}
          {b.right && <span className="bx">{b.right}</span>}
        </div>
      ))}
    </div>
  )
}

// ---------------------------------------------------------------- freshness

let now = Date.now()
const ticks = new Set<() => void>()
let ticker: ReturnType<typeof setInterval> | undefined
function subscribeNow(l: () => void) {
  ticks.add(l)
  ticker ??= setInterval(() => {
    now = Date.now()
    ticks.forEach((t) => t())
  }, 1000)
  return () => {
    ticks.delete(l)
    if (!ticks.size) ticker = (clearInterval(ticker), undefined)
  }
}
export const useNow = () => useSyncExternalStore(subscribeNow, () => now)

/** "updated 4s ago", the one way a page or drawer says when its data was read. */
export function Updated({ l }: { l: { at?: number; error?: unknown } }) {
  const { paused } = useLiveState()
  useNow()
  if (!l.at) return null
  const title = `Fetched at ${clock(l.at)}`
  if (paused)
    return (
      <span className="cx-upd paused" title={title}>
        paused · as of {clock(l.at)}
      </span>
    )
  if (l.error)
    return (
      <span className="cx-upd warn" title={`${title} · refresh failed: ${errText(l.error)}`}>
        <span className="cx-g">{GLYPH.warn}</span>not updating · {ago(l.at)}
      </span>
    )
  return (
    <span className="cx-upd" title={title}>
      updated {ago(l.at)}
    </span>
  )
}

/** The stalest of several reads. */
export const oldest = (...ls: { at?: number; error?: unknown }[]) => {
  const have = ls.filter((l) => l.at)
  if (!have.length) return {}
  const o = have.reduce((a, b) => ((a.at ?? 0) <= (b.at ?? 0) ? a : b))
  return { at: o.at, error: ls.find((l) => l.error)?.error }
}

/** Where a panel's data comes from; shown only with Settings' "Data sources" on. */
export function Src({ children }: { children: ReactNode }) {
  const { showSources } = useLiveState()
  if (!showSources) return null
  return (
    <span className="cx-chip plain mono" title="Source">
      {children}
    </span>
  )
}

// ---------------------------------------------------------------- panels, page head

export function Panel({ title, to, src, right, foot, children, className, style, id }: { title?: ReactNode; to?: string; src?: ReactNode; right?: ReactNode; foot?: ReactNode; children?: ReactNode; className?: string; style?: CSSProperties; id?: string }) {
  return (
    <section className={`cx-pn${className ? ` ${className}` : ''}`} style={style} id={id}>
      {(title || right) && (
        <div className="cx-pn-h">
          {title && <h3>{to ? <Link to={to}>{title} ›</Link> : title}</h3>}
          {src && <Src>{src}</Src>}
          {right && <div className="r">{right}</div>}
        </div>
      )}
      {children}
      {foot && <div className="cx-pn-f">{foot}</div>}
    </section>
  )
}
export const PanelBody = ({ children, style }: { children: ReactNode; style?: CSSProperties }) => (
  <div className="cx-pn-b" style={style}>
    {children}
  </div>
)

/** A section inside a drawer: a heading, a muted digest, a body. */
export function Sec({ title, digest, right, flush, children }: { title: ReactNode; digest?: ReactNode; right?: ReactNode; flush?: boolean; children: ReactNode }) {
  return (
    <div className="cx-sec">
      <div className="sh">
        {title}
        {digest && <span className="dg">{digest}</span>}
        {right && <div className="r">{right}</div>}
      </div>
      <div className={`sb${flush ? ' flush' : ''}`}>{children}</div>
    </div>
  )
}

export function PageHead({ title, sub, actions }: { title: ReactNode; sub?: ReactNode; actions?: ReactNode }) {
  return (
    <div className="cx-ph">
      <div style={{ minWidth: 0 }}>
        <h1>{title}</h1>
        {sub && <div className="sub">{sub}</div>}
      </div>
      {actions && <div className="acts">{actions}</div>}
    </div>
  )
}

/** Muted middle dot between facts in a sub line. */
export const Dot = () => <span className="muted">·</span>

export function KV({ rows }: { rows: ([ReactNode, ReactNode] | [ReactNode, ReactNode, ReactNode])[] }) {
  return (
    <dl className="cx-kv">
      {rows.map((r, i) => (
        <div key={i}>
          <dt>{r[0]}</dt>
          <dd>{r[1]}</dd>
          <dd className="x">{r[2]}</dd>
        </div>
      ))}
    </dl>
  )
}

export function Strip({ items }: { items: [ReactNode, string, string?][] }) {
  return (
    <div className="cx-strip">
      {items.map(([v, l, t]) => (
        <div key={l} title={t}>
          <b>
            <LiveVal>{v}</LiveVal>
          </b>
          <span>{l}</span>
        </div>
      ))}
    </div>
  )
}

/** Two-column rows with a right-hand chip (Explorer's pair panel). */
export function Pairs({ rows }: { rows: [ReactNode, ReactNode, ReactNode][] }) {
  return (
    <div className="cx-pair">
      {rows.map(([k, v, x], i) => (
        <div key={i}>
          <span className="pk">{k}</span>
          <span className="pv">{v}</span>
          <span className="px">{x}</span>
        </div>
      ))}
    </div>
  )
}

export function Legend({ items, right }: { items: [ReactNode, ReactNode][]; right?: ReactNode }) {
  return (
    <div className="cx-legend">
      {items.map(([g, l], i) => (
        <span key={i}>
          {g}
          {l}
        </span>
      ))}
      {right && <span style={{ marginLeft: 'auto' }}>{right}</span>}
    </div>
  )
}

/** Click to copy: the value itself is the button. */
export function Copy({ text, children, className }: { text: string; children?: ReactNode; className?: string }) {
  return (
    <button
      type="button"
      className={`cx-copy${className ? ` ${className}` : ''}`}
      title={`${text}\ncopy`}
      onClick={(e) => {
        e.stopPropagation()
        navigator.clipboard.writeText(text).then(
          () => toast(`Copied ${text.length > 48 ? `${text.slice(0, 48)}…` : text}`),
          () => toast('Copy failed'),
        )
      }}
    >
      {children ?? text}
    </button>
  )
}

export function CopyBtn({ text, label = 'Copy' }: { text: string; label?: string }) {
  return (
    <button
      type="button"
      className="cx-btn sm quiet"
      onClick={() =>
        navigator.clipboard.writeText(text).then(
          () => toast('Copied'),
          () => toast('Copy failed'),
        )
      }
    >
      {label}
    </button>
  )
}

export const Spinner = () => <span className="cx-spin" role="status" aria-label="Loading" />

export function Empty({ title, children }: { title?: ReactNode; children?: ReactNode }) {
  return (
    <div className="cx-empty">
      {title && <b>{title}</b>}
      {children}
    </div>
  )
}

export const Loading = ({ label = 'Loading…' }: { label?: string }) => (
  <div className="cx-empty">
    <Spinner /> {label}
  </div>
)

export function ErrorState({ error, retry }: { error: unknown; retry?: () => void }) {
  return (
    <div className="cx-errbox" role="alert">
      <Glyph k="err" />
      <span className="msg">{errText(error)}</span>
      {retry && (
        <button type="button" className="cx-btn sm" onClick={retry}>
          Retry
        </button>
      )}
    </div>
  )
}

export function Loaded<T>({ load, children }: { load: { data?: T; error?: unknown; loading: boolean; reload?: () => void }; children: (d: T) => ReactNode }) {
  if (load.data !== undefined) return <>{children(load.data)}</>
  if (load.error) return <ErrorState error={load.error} retry={load.reload} />
  if (load.loading) return <Loading />
  return null
}

export const Toggle = ({ on, onChange, label }: { on: boolean; onChange: (v: boolean) => void; label: string }) => (
  <button type="button" className={`cx-toggle${on ? ' on' : ''}`} aria-pressed={on} aria-label={label} onClick={() => onChange(!on)} />
)

export function Seg<T extends string>({ value, options, onChange, label }: { value: T; options: { v: T; label: ReactNode; n?: ReactNode }[]; onChange: (v: T) => void; label: string }) {
  return (
    <div className="cx-seg" role="group" aria-label={label}>
      {options.map((o) => (
        <button key={o.v} type="button" className={o.v === value ? 'on' : undefined} aria-pressed={o.v === value} onClick={() => onChange(o.v)}>
          {o.label}
          {o.n !== undefined && <span className="n">{o.n}</span>}
        </button>
      ))}
    </div>
  )
}

/** → or ← in the relation's colour. */
export const Arrow = ({ dir }: { dir: 'out' | 'in' }) => <span className={dir === 'out' ? 's-out' : 's-in'}>{dir === 'out' ? '→' : '←'}</span>

/** A DID, mono and muted, click to copy. */
export const Did = ({ did, full }: { did: string; full?: boolean }) => (
  <Copy text={did} className="cx-did">
    {full ? did : did}
  </Copy>
)
