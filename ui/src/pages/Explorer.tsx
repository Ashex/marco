import { useEffect, useMemo, useState } from 'react'
import { DataTable } from '../components/DataTable'
import { confirmAction } from '../components/dialogs'
import { Chip, CopyBtn, Did, Empty, ErrorState, KV, Legend, Loaded, PageHead, Pairs, Panel, PanelBody, Seg, Strip, Updated, Glyph, Arrow } from '../components/kit'
import { openPanel } from '../components/nav'
import { graph, reqJson, timed } from '../lib/api'
import { day, fmtCount, n, shortDid, utc } from '../lib/fmt'
import { navigate, useSearch } from '../lib/router'
import { setActorStatus, useActor, useEdges, useOverview, useResolve } from '../lib/queries'
import { actorTone } from '../lib/tone'
import { Dir } from '../gen/marco/v1/graph_pb'
import type { RelationshipsResponse, IntersectResponse, FilterResponse } from '../gen/marco/v1/graph_pb'
import { listLabel, shardName } from './common'

const DID = /^did:[a-z]+:[a-zA-Z0-9._:%-]+$/

/** Rel and direction of the table's tab: "follow:out". */
type Tab = string

export function storedChip(s: string) {
  if (s === 'delta') return <Chip k="sig" g="+">delta</Chip>
  if (s === 'merged') return <Chip k="info">merged</Chip>
  if (!s) return <span className="muted">—</span>
  return <span className="muted">base</span>
}

export function setStatusDialog(did: string, current: string) {
  const next = current === 'active' ? 'takendown' : 'active'
  return confirmAction({
    tone: 'warn',
    title: `Set ${did} ${next}?`,
    items: [
      `${current} → ${next}`,
      next === 'takendown' ? 'hidden as actor and subject' : 'shown again',
      'kept until restart',
    ],
    action: `Set ${next}`,
    button: next === 'takendown' ? 'warn' : 'primary',
    call: `POST /marco.v1.Admin/SetActorStatus\n${reqJson({ did, status: next })}`,
    run: () => setActorStatus(did, next),
    done: `${did} is ${next}`,
  })
}

function ActorPanel({ did, rels, onPage }: { did: string; rels: string[]; onPage: (dids: string[]) => void }) {
  const a = useActor(did)
  const [tab, setTab] = useState<Tab>(`${rels[0] ?? 'follow'}:out`)
  const [cursors, setCursors] = useState<string[]>([''])
  const [rel, dirS] = tab.split(':')
  const dir = dirS === 'in' ? Dir.IN : Dir.OUT
  const cursor = cursors[cursors.length - 1]
  const e = useEdges(did, rel, dir, cursor)
  useEffect(() => setCursors(['']), [did, tab])
  useEffect(() => {
    if (e.data) onPage(e.data.edges.map((x) => x.did))
  }, [e.data, onPage])
  return (
    <Loaded load={a}>
      {(x) => {
        if (!x.known)
          return (
            <Panel title="Actor">
              <Empty title={x.did}>Not in the directory</Empty>
            </Panel>
          )
        const r0 = x.rels[0]
        const strip: [React.ReactNode, string, string?][] = []
        for (const r of x.rels) {
          strip.push([fmtCount(r.out), `${listLabel(r.rel, 'out', false)} →`], [fmtCount(r.in), `${listLabel(r.rel, 'in', false)} ←`])
          if (r === r0) strip.push([fmtCount(r.mutual), 'mutuals', 'subjects that name this actor back'])
        }
        if (r0) strip.push([r0.firstUs ? day(r0.firstUs) : '—', `first ${r0.rel}`, 'oldest rkey'])
        const opts = x.rels.flatMap((r) => [
          { v: `${r.rel}:out`, label: `${listLabel(r.rel, 'out')} →`, n: fmtCount(r.out) },
          { v: `${r.rel}:in`, label: `${listLabel(r.rel, 'in')} ←`, n: fmtCount(r.in) },
        ])
        const total = n(e.data?.total)
        const from = (cursors.length - 1) * 8
        return (
          <section className="cx-pn">
            <div className="cx-pn-h">
              <h3>Actor</h3>
              <div className="r">
                <Updated l={a} />
              </div>
            </div>
            <div className="cx-pn-b" style={{ display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
              <div style={{ minWidth: 0 }}>
                <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
                  <span className="cx-handle mono" style={{ fontSize: 15 }}>
                    {x.did}
                  </span>
                  <Chip k={actorTone(x.status)}>{x.status}</Chip>
                </div>
                <div className="cx-did" style={{ display: 'flex', gap: 10 }}>
                  <span>id {fmtCount(x.id)}</span>
                  <a href="#" onClick={(ev) => (ev.preventDefault(), openPanel('shard', String(x.shard)))}>
                    {shardName(x.shard)}
                  </a>
                </div>
              </div>
              <div style={{ marginLeft: 'auto', display: 'flex', gap: 6 }}>
                <CopyBtn text={x.did} label="Copy DID" />
                <button type="button" className="cx-btn sm" onClick={() => setStatusDialog(x.did, x.status)}>
                  Set status…
                </button>
              </div>
            </div>
            <div style={{ padding: '0 12px 12px' }}>
              <Strip items={strip} />
            </div>
            <div className="cx-pn-h" style={{ borderTop: '1px solid var(--rule2)' }}>
              <Seg label="List" value={tab} options={opts} onChange={setTab} />
              <div className="r">{dir === Dir.OUT ? 'newest first · cursor = rkey' : 'id order · cursor = DID'}</div>
            </div>
            {e.error ? (
              <ErrorState error={e.error} retry={e.reload} />
            ) : (
              <DataTable
                rows={e.data?.edges ?? []}
                rowKey={(r) => r.uri || r.did}
                open={(r) => (r.uri ? { type: 'edge', id: r.uri } : undefined)}
                empty={<Empty>{e.loading ? 'Loading…' : 'No records'}</Empty>}
                cols={[
                  { id: 'did', label: dir === Dir.OUT ? 'Subject' : 'Actor', fill: true, render: (r) => <><div className="mono" style={{ fontSize: 12 }}>{r.did}</div><div className="cx-did">id {fmtCount(r.id)}</div></> },
                  { id: 'rkey', label: 'rkey', render: (r) => <span className="mono">{r.rkey || '—'}</span> },
                  { id: 'created', label: 'Created', r: true, render: (r) => utc(r.createdUs) },
                  { id: 'stored', label: 'In', render: (r) => storedChip(r.stored) },
                ]}
              />
            )}
            <div className="cx-pn-f">
              <span className="mono">{total ? `${fmtCount(from + 1)}–${fmtCount(from + (e.data?.edges.length ?? 0))} of ${fmtCount(total)}` : '0'}</span>
              <div className="r">
                {cursors.length > 1 && (
                  <button type="button" className="cx-btn sm" onClick={() => setCursors((c) => c.slice(0, -1))}>
                    ‹ Prev
                  </button>
                )}
                {e.data?.cursor && (
                  <button type="button" className="cx-btn sm" onClick={() => setCursors((c) => [...c, e.data!.cursor])}>
                    Next › <span className="mono muted">{e.data.cursor.length > 16 ? `${e.data.cursor.slice(0, 16)}…` : e.data.cursor}</span>
                  </button>
                )}
              </div>
            </div>
          </section>
        )
      }}
    </Loaded>
  )
}

function PairPanel({ a0, b0, rels }: { a0: string; b0: string; rels: string[] }) {
  const [a, setA] = useState(a0)
  const [b, setB] = useState(b0)
  const [res, setRes] = useState<{ r: RelationshipsResponse; ms: number } | { error: unknown }>()
  useEffect(() => setA(a0), [a0])
  useEffect(() => setB(b0), [b0])
  const check = async () => {
    try {
      const [r, ms] = await timed(() => graph().relationships({ actor: a.trim(), others: [b.trim()], rels }))
      setRes({ r, ms })
    } catch (e) {
      setRes({ error: e })
    }
  }
  useEffect(() => {
    if (a0 && b0) check()
    // a pair from the URL checks once on arrival
  }, [a0, b0]) // eslint-disable-line react-hooks/exhaustive-deps
  const row = res && 'r' in res ? res.r.relationships[0] : undefined
  const uriTail = (u: string) => <span className="mono trunc sm" title={u}>…/{u.split('/').slice(-2).join('/')}</span>
  return (
    <Panel title="Pair" right={res && 'ms' in res ? <span className="mono">Relationships · {res.ms.toFixed(1)} ms</span> : undefined}>
      <PanelBody style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
        <form style={{ display: 'flex', gap: 6, flexWrap: 'wrap' }} onSubmit={(e) => (e.preventDefault(), check())}>
          <input className="cx-inp mono" style={{ flex: '1 1 150px' }} value={a} onChange={(e) => setA(e.target.value)} placeholder="DID" aria-label="Actor A" />
          <input className="cx-inp mono" style={{ flex: '1 1 150px' }} value={b} onChange={(e) => setB(e.target.value)} placeholder="DID" aria-label="Actor B" />
          <button type="submit" className="cx-btn" disabled={!DID.test(a.trim()) || !DID.test(b.trim())}>
            Check
          </button>
        </form>
        {res && 'error' in res && <ErrorState error={res.error} />}
        {row && (
          <Pairs
            rows={row.rels.flatMap((r) => [
              [<>{r.rel} <Arrow dir="out" /></>, r.outUri ? uriTail(r.outUri) : <span className="muted">—</span>, r.outUri ? <Chip k="ok">yes</Chip> : <Chip k="idle">no</Chip>],
              [<>{r.rel} <Arrow dir="in" /></>, r.inUri ? uriTail(r.inUri) : <span className="muted">—</span>, r.inUri ? <Chip k="ok">yes</Chip> : <Chip k="idle">no</Chip>],
            ] as [React.ReactNode, React.ReactNode, React.ReactNode][])}
          />
        )}
      </PanelBody>
    </Panel>
  )
}

function UriPanel({ initial }: { initial: string }) {
  const [u, setU] = useState(initial)
  const [q, setQ] = useState(initial)
  useEffect(() => {
    setU(initial)
    setQ(initial)
  }, [initial])
  const r = useResolve(q)
  return (
    <Panel title="URI">
      <PanelBody style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
        <form style={{ display: 'flex', gap: 6 }} onSubmit={(e) => (e.preventDefault(), setQ(u.trim()))}>
          <input className="cx-inp mono" style={{ flex: 1 }} value={u} onChange={(e) => setU(e.target.value)} placeholder="at://did/collection/rkey" aria-label="at:// URI" />
          <button type="submit" className="cx-btn" disabled={!u.trim().startsWith('at://')}>
            Resolve
          </button>
        </form>
        {q && (
          <Loaded load={r}>
            {(x) => (
              <KV
                rows={[
                  ['Relation', <b>{x.rel}</b>],
                  ['Actor', <Did did={x.actor} />],
                  ['Subject', x.subject ? <Did did={x.subject} /> : <span className="muted">{x.tombstoned ? 'deleted before its create' : 'no such record'}</span>],
                  ['rkey', <span className="mono">{x.rkey}</span>, <Chip k="plain" g="◇">{x.tid ? 'TID' : 'not a TID'}</Chip>],
                  ['Created', <span className="mono">{x.tid ? `${utc(x.createdUs, true)} UTC` : '—'}</span>],
                  [
                    'Out log',
                    <span className="mono">
                      {shardName(x.shard)}
                      {x.position ? ` · pos ${fmtCount(x.position)} of ${fmtCount(x.outCount)}` : ` · ${fmtCount(x.outCount)} records`}
                    </span>,
                    x.subject ? storedChip(x.stored) : undefined,
                  ],
                ]}
              />
            )}
          </Loaded>
        )}
      </PanelBody>
    </Panel>
  )
}

function IntersectPanel({ a0, rels }: { a0: string; rels: string[] }) {
  const rel = rels[0] ?? 'follow'
  const [a, setA] = useState(a0)
  const [b, setB] = useState('')
  const [da, setDa] = useState<Dir>(Dir.OUT)
  const [db, setDb] = useState<Dir>(Dir.IN)
  const [res, setRes] = useState<{ r: IntersectResponse; ms: number } | { error: unknown }>()
  useEffect(() => setA(a0), [a0])
  const body = { a: { rel, dir: da, actor: a.trim() }, b: { rel, dir: db, actor: b.trim() }, limit: 100 }
  const json = reqJson({ a: { ...body.a, dir: da === Dir.OUT ? 'DIR_OUT' : 'DIR_IN' }, b: { ...body.b, dir: db === Dir.OUT ? 'DIR_OUT' : 'DIR_IN' }, limit: 100 })
  const go = async () => {
    try {
      const [r, ms] = await timed(() => graph().intersect(body))
      setRes({ r, ms })
    } catch (e) {
      setRes({ error: e })
    }
  }
  const side = (d: Dir, set: (d: Dir) => void) => (
    <button type="button" className={`cx-chip ${d === Dir.OUT ? 'out' : 'in'}`} style={{ border: 0, cursor: 'pointer' }} title="switch direction" onClick={() => set(d === Dir.OUT ? Dir.IN : Dir.OUT)}>
      {d === Dir.OUT ? `→ ${rel}.out` : `← ${rel}.in`}
    </button>
  )
  return (
    <Panel title="Intersect" right={<CopyBtn text={`POST /marco.v1.Graph/Intersect\n${json}`} label="Copy request" />}>
      <PanelBody style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
        <form style={{ display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center' }} onSubmit={(e) => (e.preventDefault(), go())}>
          {side(da, setDa)}
          <input className="cx-inp mono" style={{ flex: '1 1 120px' }} value={a} onChange={(e) => setA(e.target.value)} placeholder="DID" aria-label="Side A actor" />
          <span className="muted">∩</span>
          {side(db, setDb)}
          <input className="cx-inp mono" style={{ flex: '1 1 120px' }} value={b} onChange={(e) => setB(e.target.value)} placeholder="DID" aria-label="Side B actor" />
          <button type="submit" className="cx-btn" disabled={!DID.test(a.trim()) || !DID.test(b.trim())}>
            Intersect
          </button>
        </form>
        {res && 'error' in res && <ErrorState error={res.error} />}
        {res && 'r' in res && (
          <>
            <div style={{ display: 'flex', gap: 10, alignItems: 'baseline' }}>
              <span className="n" style={{ fontSize: 18, fontWeight: 620 }}>
                {res.r.actors.length >= 100 ? '≥ 100' : res.r.actors.length}
              </span>
              <span className="muted">
                of {fmtCount(res.r.aLen)} ∩ {fmtCount(res.r.bLen)} · {res.ms.toFixed(1)} ms
              </span>
            </div>
            <DataTable compact rows={res.r.actors.slice(0, 8)} rowKey={(d) => d} onRow={(d) => navigate(`/admin/explorer?actor=${encodeURIComponent(d)}`)} cols={[{ id: 'd', label: 'Actor', fill: true, render: (d) => <span className="mono sm" title={d}>{shortDid(d)}</span> }]} />
          </>
        )}
      </PanelBody>
    </Panel>
  )
}

function FilterPanel({ viewer0, actors, rels }: { viewer0: string; actors: string[]; rels: string[] }) {
  const [viewer, setViewer] = useState(viewer0)
  useEffect(() => setViewer(viewer0), [viewer0])
  const opts = rels.flatMap((r) => [`${r}.out`, `${r}.in`])
  const [picked, setDrop] = useState('')
  const drop = opts.includes(picked) ? picked : (opts.find((o) => o === 'block.out') ?? opts[0] ?? '')
  const [res, setRes] = useState<{ r: FilterResponse; ms: number; n: number } | { error: unknown }>()
  const go = async () => {
    const [rel, d] = drop.split('.')
    try {
      const [r, ms] = await timed(() => graph().filter({ viewer: viewer.trim(), actors, drop: [{ rel, dir: d === 'in' ? Dir.IN : Dir.OUT }] }))
      setRes({ r, ms, n: actors.length })
    } catch (e) {
      setRes({ error: e })
    }
  }
  return (
    <Panel title="Filter">
      <PanelBody style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
        <form style={{ display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center' }} onSubmit={(e) => (e.preventDefault(), go())}>
          <span className="muted sm">viewer</span>
          <input className="cx-inp mono" style={{ flex: '1 1 140px' }} value={viewer} onChange={(e) => setViewer(e.target.value)} placeholder="DID" aria-label="Viewer" />
          <Seg label="Drop" value={drop} options={opts.map((o) => ({ v: o, label: o }))} onChange={setDrop} />
          <button type="submit" className="cx-btn" disabled={!DID.test(viewer.trim()) || !actors.length} title="the actors on the list's page">
            Filter {actors.length}
          </button>
        </form>
        {res && 'error' in res && <ErrorState error={res.error} />}
        {res && 'r' in res && (
          <>
            <Legend
              items={[
                [null, `${res.n} in`],
                [<Glyph k="ok" />, `${res.r.actors.length} kept`],
                [<Glyph k="err" />, `${res.r.dropped.length} dropped`],
                [null, <span className="mono muted">{res.ms.toFixed(1)} ms</span>],
              ]}
            />
            {res.r.dropped.length > 0 && (
              <DataTable
                compact
                rows={res.r.dropped}
                rowKey={(d) => d.actor}
                cols={[
                  { id: 'a', label: 'Actor', fill: true, render: (d) => <span className="mono sm" title={d.actor}>{shortDid(d.actor)}</span> },
                  { id: 'why', label: '', render: (d) => <Chip k="err">{d.rel === 'status' ? 'status' : `${d.rel} ${d.dir === Dir.IN ? '←' : '→'}`}</Chip> },
                ]}
              />
            )}
          </>
        )}
      </PanelBody>
    </Panel>
  )
}

export function Explorer() {
  const q = useSearch()
  const actor = q.get('actor') ?? ''
  const b = q.get('b') ?? ''
  const uri = q.get('uri') ?? ''
  const ov = useOverview()
  const rels = useMemo(() => ov.data?.relations.map((r) => r.name) ?? [], [ov.data])
  const [text, setText] = useState(actor || uri)
  const [err, setErr] = useState('')
  const [page, setPage] = useState<string[]>([])
  useEffect(() => setText(uri || actor), [actor, uri])
  const look = () => {
    const t = text.trim()
    setErr('')
    if (t.startsWith('at://')) {
      const did = t.slice(5).split('/')[0]
      navigate(`/admin/explorer?actor=${encodeURIComponent(did)}&uri=${encodeURIComponent(t)}`)
      openPanel('edge', t)
    } else if (DID.test(t)) navigate(`/admin/explorer?actor=${encodeURIComponent(t)}`)
    else setErr('DID or at:// URI only')
  }
  const o = ov.data
  return (
    <>
      <PageHead
        title="Explorer"
        sub={
          o && (
            <>
              <span>
                snapshot {o.snapshot?.build.slice(0, 8)} + {fmtCount(o.writeSeq)} writes since start
              </span>
              <Updated l={ov} />
            </>
          )
        }
      />
      <div className="cx-pn" style={{ marginBottom: 14 }}>
        <form className="cx-pn-b" style={{ display: 'flex', gap: 8, flexWrap: 'wrap', alignItems: 'center' }} onSubmit={(e) => (e.preventDefault(), look())}>
          <label className="sr" htmlFor="q">
            DID or at:// URI
          </label>
          <input id="q" data-search className="cx-inp big mono" style={{ flex: '1 1 420px' }} value={text} onChange={(e) => setText(e.target.value)} placeholder="did:plc:… or at://did/collection/rkey" spellCheck={false} />
          <button type="submit" className="cx-btn primary" style={{ height: 38 }}>
            Look up
          </button>
          <span className="muted sm" style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
            <kbd>/</kbd> focus
          </span>
          {err && <span className="s-warn sm" style={{ width: '100%' }}>{err}</span>}
        </form>
      </div>
      <div className="cx-ov" style={{ gridTemplateColumns: 'minmax(0,1fr) 440px' }}>
        <div className="cx-stack">
          {actor ? (
            <ActorPanel did={actor} rels={rels} onPage={setPage} />
          ) : (
            <Panel title="Actor">
              <Empty>Look up a DID or an at:// URI</Empty>
            </Panel>
          )}
        </div>
        <div className="cx-stack">
          <PairPanel a0={actor} b0={b} rels={rels} />
          <UriPanel initial={uri} />
          <IntersectPanel a0={actor} rels={rels} />
          <FilterPanel viewer0={actor} actors={page} rels={rels} />
        </div>
      </div>
    </>
  )
}

