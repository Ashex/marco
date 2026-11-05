import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
import { Mark } from '../../components/icons'
import { useTheme } from '../../components/Shell'
import { toast, Toasts } from '../../components/toast'
import { Link } from '../../lib/router'
import '../../docs.css'
import '../../home.css'
import { GITHUB, PubFoot, PubTop, useConsoleReachable } from '../docs/PubTop'
import { CALLS, NAMES, RELS, body, curl, handlesIn, type Call, type Field, type Values } from './calls'

// `/` on the API listener: what marco is, and an explorer for marco.v1.Graph's reads against
// this origin. Writes and Admin aren't offered: the API listener 404s them anyway.

export function Home() {
  const [, dark] = useTheme()
  const consoleHere = useConsoleReachable()
  useEffect(() => {
    document.documentElement.classList.toggle('dark', dark)
    document.title = 'marco: a graph database for atproto'
  }, [dark])
  useEffect(() => {
    if (location.hash) document.getElementById(location.hash.slice(1))?.scrollIntoView()
  }, [])
  return (
    <div className={`cx cx-pubroot cx-docsroot cx-home${dark ? ' dark' : ''}`}>
      <PubTop dark={dark} consoleHere={consoleHere} here="home" />
      <main className="home">
        <section className="home-hero">
          <div className="home-id">
            <h1>
              <span className="home-mark">
                <Mark />
              </span>
              marco
            </h1>
            <p className="home-line">A graph database for atproto actor-to-actor edges: follows, blocks, list items and any record whose subject is a DID.</p>
            <div className="home-cta">
              <Link to="/docs" className="cx-btn primary">
                Docs
              </Link>
              <a href="#explorer" className="cx-btn">
                Explorer
              </a>
              <a href={GITHUB} className="cx-btn">
                GitHub
              </a>
            </div>
          </div>
          <div className="facts home-facts">
            <Fact v="2.97B" l="edges" n="follows + blocks" tone="blue" />
            <Fact v="38.5" u="GB" l="snapshot" n="13 B/edge" tone="amber" />
            <Fact v="1.05M" u="Has/s" l="server throughput" n="16 threads" tone="cyan" />
            <Fact v="8" l="read RPCs" n="Connect · gRPC · gRPC‑Web" />
          </div>
        </section>
        <Explorer />
      </main>
      <div className="docs-foot">
        <PubFoot consoleHere={consoleHere} />
      </div>
      <Toasts />
    </div>
  )
}

function Fact({ v, u, l, n, tone }: { v: string; u?: string; l: string; n?: string; tone?: string }) {
  return (
    <div className={`fact${tone ? ` fact-${tone}` : ''}`}>
      <div className="fact-v">
        {v}
        {u && <span className="fact-u">{u}</span>}
      </div>
      <div className="fact-l">{l}</div>
      {n && <div className="fact-n">{n}</div>}
    </div>
  )
}

// ---------------------------------------------------------------- explorer

type Result = {
  method: string
  status: number
  ms: number
  bytes: number
  json?: unknown
  text: string
}

const resolved = new Map<string, string>()

class Problem extends Error {}

async function resolveHandle(h: string): Promise<string> {
  const hit = resolved.get(h)
  if (hit) return hit
  const r = await fetch(`/resolve?handle=${encodeURIComponent(h)}`).catch(() => null)
  if (!r) throw new Problem(`${h}: lookup failed, try again`)
  if (r.status === 404 && !(r.headers.get('content-type') ?? '').includes('json')) throw new Problem(`${h}: handles are off here, use a DID`)
  const j = (await r.json().catch(() => ({}))) as { did?: string; error?: string }
  if (r.ok && j.did) {
    resolved.set(h, j.did)
    return j.did
  }
  const why: Record<string, string> = { not_found: 'no such handle', invalid_handle: 'not a handle', rate_limited: 'rate limited, try again in a second', upstream: 'resolver unreachable' }
  throw new Problem(`${h}: ${why[j.error ?? ''] ?? `lookup failed (${r.status})`}`)
}

function useReady() {
  const [s, setS] = useState<'ready' | 'catching up' | 'down' | null>(null)
  useEffect(() => {
    fetch('/readyz').then(
      (r) => setS(r.ok ? 'ready' : 'catching up'),
      () => setS('down'),
    )
  }, [])
  return s
}

function Explorer() {
  const [name, setName] = useState('Counts')
  const [vals, setVals] = useState<Record<string, Values>>(() => Object.fromEntries(CALLS.map((c) => [c.name, { ...c.defaults }])))
  const [res, setRes] = useState<Result | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [dids, setDids] = useState<Record<string, string>>({})
  const ready = useReady()
  const call = CALLS.find((c) => c.name === name)!
  const v = vals[name]
  const set = (k: string, x: string) => setVals((all) => ({ ...all, [name]: { ...all[name], [k]: x } }))

  // the request as it stands, handles shown as resolved once they have been
  const preview = useMemo(() => {
    try {
      return body(call.build(v, (s) => (s.startsWith('did:') || s.startsWith('at://') ? s : (dids[s.replace(/^@/, '').toLowerCase()] ?? s))))
    } catch {
      return '{}'
    }
  }, [call, v, dids])

  const run = useCallback(
    async (over?: Values) => {
      const values = over ?? v
      setBusy(true)
      setErr(null)
      try {
        const hs = handlesIn(call, values)
        const got: Record<string, string> = {}
        for (const h of hs) got[h] = await resolveHandle(h)
        if (hs.length) setDids((d) => ({ ...d, ...got }))
        const did = (s: string) => {
          if (s.startsWith('did:') || s.startsWith('at://')) return s
          return got[s.replace(/^@/, '').toLowerCase()] ?? s
        }
        const json = body(call.build(values, did))
        const t0 = performance.now()
        const r = await fetch(`/marco.v1.Graph/${call.name}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: json })
        const text = await r.text()
        const ms = performance.now() - t0
        let parsed: unknown
        try {
          parsed = JSON.parse(text)
        } catch {
          parsed = undefined
        }
        setRes({ method: call.name, status: r.status, ms, bytes: new TextEncoder().encode(text).length, json: parsed, text })
      } catch (e) {
        setErr(e instanceof Problem ? e.message : `request failed: ${(e as Error).message}`)
      } finally {
        setBusy(false)
      }
    },
    [call, v],
  )

  const runRef = useRef(run)
  runRef.current = run
  const onKey = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) {
      e.preventDefault()
      runRef.current()
    }
  }

  const cursor = res?.method === name && res.json && typeof res.json === 'object' ? (res.json as { cursor?: string }).cursor : undefined
  const canPage = !!cursor && call.fields.some((f) => f.k === 'cursor')
  const next = () => {
    const nv = { ...v, cursor: cursor! }
    setVals((all) => ({ ...all, [name]: nv }))
    run(nv)
  }

  const curlText = curl(location.origin, call.name, preview)
  const copy = () =>
    navigator.clipboard.writeText(curlText).then(
      () => toast('Copied'),
      () => toast('Copy failed, select the text'),
    )

  return (
    <section className="ex" id="explorer" aria-label="Explorer" onKeyDown={onKey}>
      <header className="ex-head">
        <h2>Explorer</h2>
        <span className="ex-svc">marco.v1.Graph</span>
        <span className="cx-spacer" />
        {ready && (
          <a href="/readyz" className={`cx-chip ${ready === 'ready' ? 'ok' : ready === 'down' ? 'err' : 'warn'}`} title="/readyz">
            {ready}
          </a>
        )}
      </header>
      <nav className="ex-calls" aria-label="Calls">
        {CALLS.map((c) => (
          <button
            key={c.name}
            type="button"
            className="ex-call"
            aria-pressed={c.name === name}
            title={c.tip.replace(/`/g, '')}
            onClick={() => {
              setName(c.name)
              setErr(null)
            }}
          >
            <span className="cx-jack" aria-hidden="true" />
            <span className="ex-call-n">{c.name}</span>
            <span className="ex-call-h">{c.hint}</span>
          </button>
        ))}
      </nav>
      <div className="ex-body">
        <form
          className="ex-form"
          onSubmit={(e) => {
            e.preventDefault()
            run()
          }}
        >
          <div className="ex-tip" title={call.tip.replace(/`/g, '')}>
            <code>POST /marco.v1.Graph/{call.name}</code>
          </div>
          {call.presets && (
            <div className="ex-presets">
              {call.presets.map((p) => (
                <button key={p.label} type="button" className="cx-btn sm quiet" onClick={() => setVals((all) => ({ ...all, [name]: { ...all[name], ...p.values, cursor: '' } }))}>
                  {p.label}
                </button>
              ))}
            </div>
          )}
          <Fields call={call} v={v} set={set} dids={dids} />
          <div className="ex-run">
            <button type="submit" className="cx-btn primary" disabled={busy}>
              {busy ? 'Running…' : 'Run'}
            </button>
            <kbd title="Run">{navigator.platform.startsWith('Mac') ? '⌘' : 'Ctrl'} ↵</kbd>
            <span className="cx-spacer" />
            <button type="button" className="cx-btn sm quiet" onClick={() => setVals((all) => ({ ...all, [name]: { ...call.defaults } }))}>
              Reset
            </button>
          </div>
        </form>
        <div className="ex-out">
          <div className="ex-pane">
            <div className="ex-pane-h">
              <span>curl</span>
              <span className="cx-spacer" />
              <button type="button" className="cx-btn sm" onClick={copy}>
                Copy
              </button>
            </div>
            <pre className="code ex-curl">
              <code>{curlText}</code>
            </pre>
          </div>
          <div className="ex-pane">
            <div className="ex-pane-h">
              <span>Response</span>
              {res && res.method === name && <Stats r={res} />}
              <span className="cx-spacer" />
              {canPage && (
                <button type="button" className="cx-btn sm" onClick={next} disabled={busy}>
                  Next page
                </button>
              )}
            </div>
            {err ? (
              <div className="ex-err" role="alert">
                {err}
              </div>
            ) : res && res.method === name ? (
              <pre className="code ex-json" aria-live="polite">
                <code>{res.json === undefined ? res.text : <Json value={res.json} />}</code>
              </pre>
            ) : (
              <div className="ex-empty">
                <span className="cx-jack" aria-hidden="true" />
                <span>
                  Run <b>{name}</b>
                </span>
              </div>
            )}
          </div>
        </div>
      </div>
    </section>
  )
}

function Stats({ r }: { r: Result }) {
  const err = r.status >= 400 || (r.json && typeof r.json === 'object' && 'code' in (r.json as object))
  const items = summary(r.json)
  return (
    <span className="ex-stats">
      <span className={`cx-chip ${err ? 'err' : 'ok'}`}>{err ? ((r.json as { code?: string })?.code ?? r.status) : r.status}</span>
      <span className="n" title="Round trip from this browser">
        {r.ms < 10 ? r.ms.toFixed(1) : Math.round(r.ms)} ms
      </span>
      <span className="n">{r.bytes < 1000 ? `${r.bytes} B` : `${(r.bytes / 1000).toFixed(1)} kB`}</span>
      {items && <span className="n">{items}</span>}
    </span>
  )
}

/** "25 actors", "3 edges": the size of the response's main list. */
function summary(j: unknown): string | null {
  if (!j || typeof j !== 'object') return null
  const o = j as Record<string, unknown>
  const parts: string[] = []
  for (const k of ['edges', 'actors', 'relationships', 'counts', 'blocked']) {
    const a = o[k]
    if (Array.isArray(a)) {
      parts.push(`${a.length} ${k === 'blocked' ? 'pairs' : k}`)
      break
    }
  }
  if (typeof o.total === 'string') parts.push(`${Number(o.total).toLocaleString('en-US')}${o.totalCapped ? '+' : ''} total`)
  return parts.length ? parts.join(' · ') : null
}

function Fields({ call, v, set, dids }: { call: Call; v: Values; set: (k: string, x: string) => void; dids: Record<string, string> }) {
  const groups: { name?: string; fields: Field[] }[] = []
  for (const f of call.fields) {
    const last = groups[groups.length - 1]
    if (last && last.name === f.group) last.fields.push(f)
    else groups.push({ name: f.group, fields: [f] })
  }
  return (
    <>
      <datalist id="ex-rels">
        {RELS.map((r) => (
          <option key={r} value={r} />
        ))}
      </datalist>
      {groups.map((g, i) =>
        g.name ? (
          <fieldset key={i} className="ex-side">
            <legend>{g.name}</legend>
            {g.fields.map((f) => (
              <Input key={f.k} f={f} v={v[f.k] ?? ''} set={(x) => set(f.k, x)} dids={dids} />
            ))}
          </fieldset>
        ) : (
          g.fields.map((f) => <Input key={f.k} f={f} v={v[f.k] ?? ''} set={(x) => set(f.k, x)} dids={dids} />)
        ),
      )}
    </>
  )
}

function Input({ f, v, set, dids }: { f: Field; v: string; set: (x: string) => void; dids: Record<string, string> }) {
  const id = `ex-${f.group ?? ''}${f.k}`
  const label = (
    <label htmlFor={id} title={f.tip}>
      {f.label}
      {f.tip && <span className="ex-q" aria-hidden="true" />}
    </label>
  )
  let input: ReactNode
  switch (f.kind) {
    case 'bool':
      return (
        <div className="ex-f ex-f-bool">
          <span />
          <label className="ex-check" title={f.tip}>
            <input id={id} type="checkbox" checked={v === '1'} onChange={(e) => set(e.target.checked ? '1' : '')} />
            {f.label}
          </label>
        </div>
      )
    case 'dir':
    case 'op': {
      const opts = f.kind === 'dir' ? [['out', 'out →'], ['in', '← in']] : [['and', 'and'], ['andnot', 'and not']]
      input = (
        <div className="ex-seg" role="radiogroup" aria-label={f.label} id={id}>
          {opts.map(([k, l]) => (
            <button key={k} type="button" role="radio" aria-checked={v === k} onClick={() => set(k)}>
              {l}
            </button>
          ))}
        </div>
      )
      break
    }
    case 'actors':
    case 'pairs':
      input = <textarea id={id} className="cx-inp mono" rows={f.kind === 'pairs' ? 3 : 3} value={v} spellCheck={false} placeholder={f.kind === 'pairs' ? 'did:… did:…' : 'handles or DIDs'} onChange={(e) => set(e.target.value)} />
      break
    case 'int':
      input = <input id={id} className="cx-inp mono ex-int" inputMode="numeric" value={v} onChange={(e) => set(e.target.value.replace(/[^0-9]/g, ''))} placeholder="default" />
      break
    case 'rel':
      input = <input id={id} className="cx-inp mono" list="ex-rels" value={v} spellCheck={false} onChange={(e) => set(e.target.value)} />
      break
    default:
      input = (
        <input
          id={id}
          className="cx-inp mono"
          value={v}
          spellCheck={false}
          autoCapitalize="off"
          autoCorrect="off"
          placeholder={f.placeholder ?? (f.kind === 'actor' ? 'handle or DID' : f.kind === 'cursor' ? 'first page' : '')}
          onChange={(e) => set(e.target.value)}
        />
      )
  }
  const t = v.trim()
  const aside = f.kind !== 'actor' || !t ? undefined : t.startsWith('did:') ? NAMES[t] : t.startsWith('at://') ? undefined : dids[t.replace(/^@/, '').toLowerCase()]
  return (
    <div className="ex-f">
      {label}
      <div className="ex-in">
        {input}
        {aside && <span className="ex-did mono">{aside}</span>}
      </div>
    </div>
  )
}

// JSON, pretty-printed and colored the way the docs' code is (highlight.js classes)
function Json({ value }: { value: unknown }) {
  const text = JSON.stringify(value, null, 2)
  const out: ReactNode[] = []
  const re = /("(?:[^"\\]|\\.)*")(\s*:)?|\b(true|false|null)\b|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)/g
  let last = 0
  let m: RegExpExecArray | null
  let i = 0
  while ((m = re.exec(text))) {
    if (m.index > last) out.push(text.slice(last, m.index))
    if (m[1] && m[2]) out.push(<span key={i++} className="hljs-attr">{m[1]}</span>, m[2])
    else if (m[1]) out.push(<span key={i++} className="hljs-string">{m[1]}</span>)
    else if (m[3]) out.push(<span key={i++} className="hljs-literal">{m[3]}</span>)
    else out.push(<span key={i++} className="hljs-number">{m[4]}</span>)
    last = re.lastIndex
  }
  out.push(text.slice(last))
  return <>{out}</>
}
