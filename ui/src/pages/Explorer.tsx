import { useEffect, useMemo, useState } from 'react'
import { DataTable } from '../components/DataTable'
import { confirmAction } from '../components/dialogs'
import { Chip, CopyBtn, Did, Empty, ErrorState, KV, Legend, Loaded, PageHead, Pairs, Panel, PanelBody, Seg, Strip, Updated, Glyph, Arrow } from '../components/kit'
import { openPanel } from '../components/nav'
import { graph, reqJson, timed } from '../lib/api'
import { day, fmtCount, n, shortDid, utc } from '../lib/fmt'
import { navigate, useSearch } from '../lib/router'
import { setActorStatus, useActor, useEdges, useOverview, useRelations, useResolve } from '../lib/queries'
import { actorTone } from '../lib/tone'
import { Dir, SetOp } from '../gen/marco/v1/graph_pb'
import type { RelationshipsResponse, IntersectResponse, FilterResponse } from '../gen/marco/v1/graph_pb'
import { isListUri, listLabel, shardName } from './common'

const DID = /^did:[a-z]+:[a-zA-Z0-9._:%-]+$/
/** A DID or a list's AT URI: what a set operand's actor takes. */
const NODE = (s: string) => DID.test(s) || isListUri(s)

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

function ActorPanel({ did, rels, listRel, onPage }: { did: string; rels: string[]; listRel: string; onPage: (dids: string[]) => void }) {
  const a = useActor(did)
  const first = isListUri(did) && listRel ? listRel : (rels[0] ?? 'follow')
  const [tab, setTab] = useState<Tab>(`${first}:out`)
  useEffect(() => setTab(`${first}:out`), [first])
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
        // a list node has only its members; a DID shows a relation's sides it has, follow's always
        const shown = (r: (typeof x.rels)[number], d: 'out' | 'in') =>
          x.list ? r.rel === listRel && d === 'out' : r === x.rels[0] || (d === 'out' ? n(r.out) : n(r.in)) > 0
        const r0 = x.list ? x.rels.find((r) => r.rel === listRel) : x.rels[0]
        const strip: [React.ReactNode, string, string?][] = []
        for (const r of x.rels) {
          if (shown(r, 'out')) strip.push([fmtCount(r.out), `${listLabel(r.rel, 'out', false)} →`])
          if (x.list && r === r0) strip.push([fmtCount(r.outSubjects), 'distinct', 'members less repeat records'])
          if (shown(r, 'in')) strip.push([fmtCount(r.in), `${listLabel(r.rel, 'in', false)} ←`])
          if (r === r0 && !x.list) strip.push([fmtCount(r.mutual), 'mutuals', 'subjects that name this actor back'])
        }
        if (r0) strip.push([r0.firstUs ? day(r0.firstUs) : '—', `first ${x.list ? 'member' : r0.rel}`, 'oldest rkey'])
        const opts = x.rels.flatMap((r) => [
          ...(shown(r, 'out') ? [{ v: `${r.rel}:out`, label: `${listLabel(r.rel, 'out')} →`, n: fmtCount(r.out) }] : []),
          ...(shown(r, 'in') ? [{ v: `${r.rel}:in`, label: `${listLabel(r.rel, 'in')} ←`, n: fmtCount(r.in) }] : []),
        ])
        const total = n(e.data?.total)
        const from = (cursors.length - 1) * 8
        return (
          <section className="cx-pn">
            <div className="cx-pn-h">
              <h3>{x.list ? 'List' : 'Actor'}</h3>
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
                  {x.list && <Chip k="info">list</Chip>}
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
                <CopyBtn text={x.did} label={x.list ? 'Copy URI' : 'Copy DID'} />
                {x.list ? (
                  <button type="button" className="cx-btn sm" onClick={() => navigate(`/admin/explorer?actor=${encodeURIComponent(x.did.slice(5).split('/')[0])}`)}>
                    Owner
                  </button>
                ) : (
                  <button type="button" className="cx-btn sm" onClick={() => setStatusDialog(x.did, x.status)}>
                    Set status…
                  </button>
                )}
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
                onRow={isListUri(e.data?.edges[0]?.did ?? '') ? (r) => navigate(`/admin/explorer?actor=${encodeURIComponent(r.did)}`) : undefined}
                empty={<Empty>{e.loading ? 'Loading…' : 'No records'}</Empty>}
                cols={[
                  { id: 'did', label: isListUri(e.data?.edges[0]?.did ?? '') ? 'List' : dir === Dir.OUT ? (x.list ? 'Member' : 'Subject') : 'Actor', fill: true, render: (r) => <><div className="mono" style={{ fontSize: 12 }}>{r.did}</div><div className="cx-did">id {fmtCount(r.id)}</div></> },
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
                  ...(x.list ? [['List', <a href="#" className="mono sm" onClick={(ev) => (ev.preventDefault(), navigate(`/admin/explorer?actor=${encodeURIComponent(x.list)}`))}>{x.list}</a>] as [React.ReactNode, React.ReactNode]] : []),
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

function IntersectPanel({ a0, rels, listRel }: { a0: string; rels: string[]; listRel: string }) {
  const [a, setA] = useState(a0)
  const [b, setB] = useState('')
  const [ra, setRa] = useState('')
  const [rb, setRb] = useState('')
  const [da, setDa] = useState<Dir>(Dir.OUT)
  const [db, setDb] = useState<Dir>(Dir.IN)
  const [op, setOp] = useState<SetOp>(SetOp.AND)
  const [res, setRes] = useState<{ r: IntersectResponse; ms: number } | { error: unknown }>()
  useEffect(() => {
    setA(a0)
    setRa(isListUri(a0) ? listRel : '')
  }, [a0, listRel])
  const relA = ra || rels[0] || 'follow'
  const relB = rb || rels[0] || 'follow'
  const body = { a: { rel: relA, dir: da, actor: a.trim() }, b: { rel: relB, dir: db, actor: b.trim() }, op, limit: 100 }
  const dirName = (d: Dir) => (d === Dir.OUT ? 'DIR_OUT' : 'DIR_IN')
  const json = reqJson({ a: { ...body.a, dir: dirName(da) }, b: { ...body.b, dir: dirName(db) }, op: op === SetOp.AND_NOT ? 'SET_OP_AND_NOT' : 'SET_OP_AND', limit: 100 })
  const go = async () => {
    try {
      const [r, ms] = await timed(() => graph().intersect(body))
      setRes({ r, ms })
    } catch (e) {
      setRes({ error: e })
    }
  }
  const sideOf = (rel: string, setRel: (r: string) => void, d: Dir, set: (d: Dir) => void, label: string) => (
    <>
      <select className="cx-inp sm" value={rel} onChange={(e) => setRel(e.target.value)} aria-label={`Side ${label} relation`}>
        {rels.map((r) => (
          <option key={r} value={r}>
            {r}
          </option>
        ))}
      </select>
      <button type="button" className={`cx-chip ${d === Dir.OUT ? 'out' : 'in'}`} style={{ border: 0, cursor: 'pointer' }} title="switch direction" onClick={() => set(d === Dir.OUT ? Dir.IN : Dir.OUT)}>
        {d === Dir.OUT ? `→ ${listLabel(rel, 'out', false)}` : `← ${listLabel(rel, 'in', false)}`}
      </button>
    </>
  )
  return (
    <Panel title="Intersect" right={<CopyBtn text={`POST /marco.v1.Graph/Intersect\n${json}`} label="Copy request" />}>
      <PanelBody style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
        <form style={{ display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center' }} onSubmit={(e) => (e.preventDefault(), go())}>
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center', width: '100%' }}>
            {sideOf(relA, setRa, da, setDa, 'A')}
            <input className="cx-inp mono" style={{ flex: '1 1 120px' }} value={a} onChange={(e) => setA(e.target.value)} placeholder="DID or list URI" aria-label="Side A actor" />
          </div>
          <button type="button" className="cx-btn sm" title="and / and not" onClick={() => setOp(op === SetOp.AND_NOT ? SetOp.AND : SetOp.AND_NOT)}>
            {op === SetOp.AND_NOT ? '−' : '∩'}
          </button>
          <div style={{ display: 'flex', gap: 6, flexWrap: 'wrap', alignItems: 'center', width: '100%' }}>
            {sideOf(relB, setRb, db, setDb, 'B')}
            <input className="cx-inp mono" style={{ flex: '1 1 120px' }} value={b} onChange={(e) => setB(e.target.value)} placeholder="DID or list URI" aria-label="Side B actor" />
            <button type="submit" className="cx-btn" disabled={!NODE(a.trim()) || !NODE(b.trim())}>
              {op === SetOp.AND_NOT ? 'Subtract' : 'Intersect'}
            </button>
          </div>
        </form>
        {res && 'error' in res && <ErrorState error={res.error} />}
        {res && 'r' in res && (
          <>
            <div style={{ display: 'flex', gap: 10, alignItems: 'baseline' }}>
              <span className="n" style={{ fontSize: 18, fontWeight: 620 }}>
                {res.r.cursor ? '≥ 100' : res.r.actors.length}
              </span>
              <span className="muted">
                of {fmtCount(res.r.aLen)} {op === SetOp.AND_NOT ? '−' : '∩'} {fmtCount(res.r.bLen)} · {res.ms.toFixed(1)} ms
              </span>
            </div>
            <DataTable compact rows={res.r.actors.slice(0, 8)} rowKey={(d) => d} onRow={(d) => navigate(`/admin/explorer?actor=${encodeURIComponent(d)}`)} cols={[{ id: 'd', label: 'Actor', fill: true, render: (d) => <span className="mono sm" title={d}>{shortDid(d)}</span> }]} />
          </>
        )}
      </PanelBody>
    </Panel>
  )
}

function FilterPanel({ viewer0, actors, rels, listRel }: { viewer0: string; actors: string[]; rels: string[]; listRel: string }) {
  const [viewer, setViewer] = useState(viewer0)
  const [list, setList] = useState('')
  useEffect(() => setViewer(viewer0), [viewer0])
  const opts = [...rels.flatMap((r) => [`${r}.out`, `${r}.in`]), ...(listRel ? ['on list'] : [])]
  const [picked, setDrop] = useState('')
  const drop = opts.includes(picked) ? picked : (opts.find((o) => o === 'block.out') ?? opts[0] ?? '')
  const [res, setRes] = useState<{ r: FilterResponse; ms: number; n: number } | { error: unknown }>()
  const go = async () => {
    const [rel, d] = drop.split('.')
    const rule = drop === 'on list' ? { memberOf: { rel: listRel, dir: Dir.OUT, actor: list.trim() } } : { rel, dir: d === 'in' ? Dir.IN : Dir.OUT }
    try {
      const [r, ms] = await timed(() => graph().filter({ viewer: viewer.trim(), actors, drop: [rule] }))
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
          {drop === 'on list' && (
            <input className="cx-inp mono" style={{ flex: '1 1 100%' }} value={list} onChange={(e) => setList(e.target.value)} placeholder="at://did/app.bsky.graph.list/rkey" aria-label="List" />
          )}
          <button type="submit" className="cx-btn" disabled={!DID.test(viewer.trim()) || !actors.length || (drop === 'on list' && !isListUri(list.trim()))} title="the actors on the table's page">
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
  const rl = useRelations()
  const listRel = rl.data?.relations.find((r) => r.list)?.name ?? ''
  // Relationships and Filter's viewer rules are between actors; set operands take any relation but companions
  const actorRels = useMemo(() => rl.data?.relations.filter((r) => !r.list && !r.ownerOf).map((r) => r.name) ?? rels.filter((r) => !r.includes('.') && r !== 'listitem'), [rl.data, rels])
  const setRels = useMemo(() => rl.data?.relations.filter((r) => !r.ownerOf).map((r) => r.name) ?? rels, [rl.data, rels])
  const recordColls = useMemo(() => new Set(rl.data?.relations.map((r) => r.collection) ?? []), [rl.data])
  const [text, setText] = useState(actor || uri)
  const [err, setErr] = useState('')
  const [page, setPage] = useState<string[]>([])
  useEffect(() => setText(uri || actor), [actor, uri])
  const look = () => {
    const t = text.trim()
    setErr('')
    if (isListUri(t) && rl.data && !recordColls.has(t.split('/')[3])) navigate(`/admin/explorer?actor=${encodeURIComponent(t)}`)
    else if (t.startsWith('at://')) {
      const did = t.slice(5).split('/')[0]
      navigate(`/admin/explorer?actor=${encodeURIComponent(did)}&uri=${encodeURIComponent(t)}`)
      openPanel('edge', t)
    } else if (DID.test(t)) navigate(`/admin/explorer?actor=${encodeURIComponent(t)}`)
    else setErr('DID, at:// record or list URI only')
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
            DID, at:// URI or list
          </label>
          <input id="q" data-search className="cx-inp big mono" style={{ flex: '1 1 420px' }} value={text} onChange={(e) => setText(e.target.value)} placeholder="did:plc:…, at://did/collection/rkey or a list's at:// URI" spellCheck={false} />
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
            <ActorPanel did={actor} rels={rels} listRel={listRel} onPage={setPage} />
          ) : (
            <Panel title="Actor">
              <Empty>Look up a DID or an at:// URI</Empty>
            </Panel>
          )}
        </div>
        <div className="cx-stack">
          <PairPanel a0={isListUri(actor) ? '' : actor} b0={b} rels={actorRels} />
          <UriPanel initial={uri} />
          <IntersectPanel a0={actor} rels={setRels} listRel={listRel} />
          <FilterPanel viewer0={isListUri(actor) ? '' : actor} actors={page.filter((d) => !isListUri(d))} rels={actorRels} listRel={listRel} />
        </div>
      </div>
    </>
  )
}

