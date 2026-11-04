import { useState, useSyncExternalStore } from 'react'
import { DataTable } from '../components/DataTable'
import { confirmAction } from '../components/dialogs'
import { Chip, KV, PageHead, Panel, PanelBody, Seg, Spark, Tiles, Updated, Dot } from '../components/kit'
import { registerPalette } from '../components/Palette'
import { ops, reqJson } from '../lib/api'
import { bytes, clock, lat, latParts, pct, rate } from '../lib/fmt'
import { keys, last, series, setTraceRatio, sum, useOverview, useSeries, weighted, READ_OPS } from '../lib/queries'
import { queryClient } from '../lib/query'
import { useWindow, windowLabel } from './common'
import type { GetOverviewResponse, GetSeriesResponse } from '../gen/marco/v1/admin_pb'

const RPC_OP: [string, string][] = [
  ['Has', 'has'],
  ['Relationships', 'relationships'],
  ['Counts', 'counts'],
  ['ListOut', 'out_list'],
  ['ListIn', 'in_list'],
  ['Intersect', 'intersect'],
  ['Filter', 'filter'],
  ['WriteBatch', 'add'],
  ['Write', 'add'],
]

function rpcRows(s: GetSeriesResponse | undefined, rel: string) {
  const rows: { rpc: string; rel: string; rate: number[]; p50: number[]; p99: number[]; wire: number[] }[] = []
  for (const [rpc, op] of RPC_OP) {
    const wire = sum(s, { metric: 'rpc_p99', rpc: `Graph/${rpc}` })
    if (op === 'add') {
      // a write RPC carries adds and deletes, over every relation
      if (rel !== 'all' && rel !== 'multi') continue
      rows.push({ rpc, rel: rpc === 'Write' ? 'stream' : 'multi', rate: sum(s, { metric: 'rpc_rate', rpc: `Graph/${rpc}` }), p50: weighted(s, 'op_p50', { op: 'add' }), p99: weighted(s, 'op_p99', { op: 'add' }), wire })
      continue
    }
    const rels = [...new Set(series(s, { metric: 'op_rate', op }).map((x) => x.rel))]
    for (const r of rels) {
      if (rel !== 'all' && r !== rel) continue
      const rt = sum(s, { metric: 'op_rate', op, rel: r })
      if (!rt.some((x) => x > 0) && rels.length > 1) continue
      rows.push({ rpc, rel: r, rate: rt, p50: sum(s, { metric: 'op_p50', op, rel: r }), p99: sum(s, { metric: 'op_p99', op, rel: r }), wire })
    }
  }
  return rows
}

const totalOps = (s?: GetSeriesResponse) => [...READ_OPS, 'add', 'delete'].reduce((a, op) => a + (last(sum(s, { metric: 'op_rate', op })) || 0), 0)

function traceDialog(ratio: number, cur: number, opsPerSec: number) {
  const label = ratio === 0 ? 'off' : pct(ratio, ratio < 0.01 ? 1 : 0)
  return confirmAction({
    tone: 'info',
    title: ratio === 0 ? 'Turn tracing off?' : `Trace ${label} of ops?`,
    items: [`${cur === 0 ? 'off' : cur} → ${ratio === 0 ? 'off' : ratio}`, ratio > 0 ? `≈ ${rate(ratio * opsPerSec)} spans/s at ${rate(opsPerSec)} ops/s` : 'no spans', 'resets on restart'],
    action: ratio === 0 ? 'Turn off' : `Set ${label}`,
    call: `POST /marco.v1.Admin/SetTraceRatio\n${reqJson({ ratio })}`,
    run: () => setTraceRatio(ratio),
    done: ratio === 0 ? 'Tracing off' : `Tracing ${label}`,
  })
}

type Capture = { at: number; kind: string; size: number; url: string; file: string }
let captures: Capture[] = []
const capSubs = new Set<() => void>()
const useCaptures = () =>
  useSyncExternalStore(
    (l) => {
      capSubs.add(l)
      return () => void capSubs.delete(l)
    },
    () => captures,
  )

function captureDialog(kind: 'cpu' | 'heap', secs: number) {
  const path = kind === 'cpu' ? `/debug/pprof/profile?seconds=${secs}` : '/debug/pprof/heap'
  return confirmAction({
    tone: 'info',
    title: kind === 'cpu' ? `Profile CPU for ${secs} s?` : 'Capture the heap?',
    items: kind === 'cpu' ? ['SIGPROF at 99 Hz while it runs, one profile at a time', 'pprof protobuf, a download'] : ['jemalloc heap in use, sampled every 512 KiB', 'pprof protobuf, a download'],
    action: kind === 'cpu' ? `Profile ${secs} s` : 'Capture heap',
    call: `GET ${path}`,
    run: async () => {
      const r = await ops(path)
      const b = await r.blob()
      const file = kind === 'cpu' ? 'profile.pb.gz' : 'heap.pb.gz'
      const c = { at: Date.now(), kind: kind === 'cpu' ? `CPU ${secs} s` : 'heap', size: b.size, url: URL.createObjectURL(b), file }
      captures = [c, ...captures].slice(0, 8)
      const a = document.createElement('a')
      a.href = c.url
      a.download = file
      a.click()
      capSubs.forEach((l) => l())
    },
    done: 'Captured',
  })
}

export function Requests() {
  const ov = useOverview()
  const [win, winSeg] = useWindow()
  const se = useSeries(win)
  const s = se.data
  const o = ov.data
  const [rel, setRel] = useState('all')
  const [cpu, setCpu] = useState('30')
  const [custom, setCustom] = useState('')
  const caps = useCaptures()
  const rels = o?.relations.map((r) => r.name) ?? []
  const rpcs = sum(s, { metric: 'rpc_rate' })
  const errs = sum(s, { metric: 'rpc_errors' })
  const opsRate = totalOps(s)
  const tr = o?.traceRatio ?? 0
  const wp = latParts(last(weighted(s, 'op_p99', { op: 'add' })))
  const errRpc = series(s, { metric: 'rpc_errors' })
    .map((x) => [x.rpc, last(x.values)] as const)
    .sort((a, b) => b[1] - a[1])[0]
  const rows = rpcRows(s, rel)
  const ratios = ['0', '0.001', '0.01', '0.1', '1']
  const trSeg = ratios.includes(String(tr)) ? String(tr) : ''
  return (
    <>
      <PageHead
        title="Requests"
        sub={
          o && (
            <>
              <span className="mono">marco.v1.Graph</span> on {o.apiListen}
              <Dot />
              <span>ops on {o.adminListen}</span>
              <Updated l={se} />
            </>
          )
        }
        actions={winSeg}
      />
      <Tiles
        cols={4}
        tiles={[
          { label: 'RPCs', value: rate(last(rpcs)), unit: '/s', spark: <Spark data={rpcs} color="accent" /> },
          { label: 'Errors', value: rate(last(errs)), unit: '/s', sec: errRpc && errRpc[1] > 0 ? errRpc[0] : undefined, spark: <Spark data={errs} color="err" /> },
          { label: 'Write p99', value: wp[0], unit: wp[1], sec: 'engine', spark: <Spark data={weighted(s, 'op_p99', { op: 'add' })} /> },
          { label: 'Spans', value: rate(tr * opsRate), unit: '/s', sec: tr ? `ratio ${tr}` : 'tracing off' },
        ]}
      />
      <div className="cx-ov" style={{ gridTemplateColumns: 'minmax(0,1fr) 400px' }}>
        <div className="cx-stack">
          <Panel title="RPCs" right={<Seg label="Relation" value={rel} options={[{ v: 'all', label: 'all' }, ...rels.map((r) => ({ v: r, label: r })), { v: 'multi', label: 'multi' }]} onChange={setRel} />}>
            <DataTable
              compact
              rows={rows}
              rowKey={(r) => `${r.rpc}:${r.rel}`}
              cols={[
                { id: 'rpc', label: 'RPC', render: (r) => <span className="mono">{r.rpc}</span> },
                { id: 'rel', label: 'Relation', fill: true, render: (r) => <span className="t2">{r.rel}</span> },
                { id: 'rate', label: '/s', r: true, render: (r) => rate(last(r.rate)) },
                { id: 'p50', label: 'engine p50', r: true, render: (r) => lat(last(r.p50)) },
                { id: 'p99', label: 'engine p99', r: true, render: (r) => lat(last(r.p99)) },
                { id: 'wire', label: 'wire p99', r: true, title: 'in the server, decode to encode', render: (r) => lat(last(r.wire)) },
                { id: 'sp', label: windowLabel(win), className: 'hide-phone', render: (r) => <span style={{ display: 'block', width: 110 }}><Spark data={r.rate} color="accent" /></span> },
              ]}
            />
          </Panel>
        </div>
        <div className="cx-stack">
          <Panel title="Tracing">
            <PanelBody style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
              <form style={{ display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap' }} onSubmit={(e) => {
                e.preventDefault()
                const r = parseFloat(custom)
                if (r >= 0 && r <= 1) traceDialog(r, tr, opsRate)
              }}>
                <Seg
                  label="Trace ratio"
                  value={trSeg}
                  options={[
                    { v: '0', label: 'off' },
                    { v: '0.001', label: '0.1%' },
                    { v: '0.01', label: '1%' },
                    { v: '0.1', label: '10%' },
                    { v: '1', label: '100%' },
                  ]}
                  onChange={(v) => traceDialog(Number(v), tr, opsRate)}
                />
                <input className="cx-inp mono" style={{ width: 90 }} value={custom} placeholder="0.01" onChange={(e) => setCustom(e.target.value)} aria-label="Custom ratio" />
                <button type="submit" className="cx-btn primary" disabled={!(parseFloat(custom) >= 0 && parseFloat(custom) <= 1)}>
                  Apply…
                </button>
              </form>
              <KV
                rows={[
                  ['Ratio', <span className="mono">{tr}</span>, <Chip k={tr ? 'info' : 'idle'}>{tr ? 'on' : 'off'}</Chip>],
                  ['Spans', <span className="mono">{rate(tr * opsRate)} /s</span>],
                  ['At 1%', <span className="mono">≈ {rate(0.01 * opsRate)} /s</span>, <span className="muted sm">est.</span>],
                  ['Endpoint', <span className="mono">POST /marco.v1.Admin/SetTraceRatio</span>],
                ]}
              />
            </PanelBody>
          </Panel>
          <Panel title="Profiling">
            <PanelBody style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
              <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
                <span style={{ width: 44 }}>CPU</span>
                <Seg label="CPU seconds" value={cpu} options={['10', '30', '60'].map((v) => ({ v, label: `${v} s` }))} onChange={setCpu} />
                <button type="button" className="cx-btn" onClick={() => captureDialog('cpu', Number(cpu))}>
                  Capture…
                </button>
              </div>
              <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
                <span style={{ width: 44 }}>Heap</span>
                <span className="mono muted sm">jemalloc · 512 KiB sample</span>
                <button type="button" className="cx-btn" onClick={() => captureDialog('heap', 0)}>
                  Capture…
                </button>
              </div>
            </PanelBody>
            <DataTable
              compact
              rows={caps}
              rowKey={(c) => c.url}
              empty={<span className="cx-empty">No captures</span>}
              cols={[
                { id: 'at', label: 'Taken', render: (c) => clock(c.at) },
                { id: 'kind', label: 'Kind', fill: true, render: (c) => c.kind },
                { id: 'size', label: 'Size', r: true, render: (c) => bytes(c.size) },
                { id: 'dl', label: '', r: true, render: (c) => <a href={c.url} download={c.file}>{c.file}</a> },
              ]}
            />
          </Panel>
        </div>
      </div>
    </>
  )
}

const verb = (label: string, kind: string, run: () => void) => ({ id: label, glyph: <span className="cx-g muted">›</span>, label, kind, run })

registerPalette('requests', (q) => {
  const t = q.trim().toLowerCase()
  const o = queryClient.getQueryData<GetOverviewResponse>(keys.overview)
  const s = queryClient.getQueryData<GetSeriesResponse>(keys.series(3600)) ?? queryClient.getQueryData<GetSeriesResponse>(keys.series(180))
  const tr = o?.traceRatio ?? 0
  const out = []
  const m = t.match(/^trace\s+(off|[\d.]+%?)$/)
  if (m) {
    const r = m[1] === 'off' ? 0 : m[1].endsWith('%') ? parseFloat(m[1]) / 100 : parseFloat(m[1])
    if (r >= 0 && r <= 1) out.push(verb(`trace ${m[1]}`, 'SetTraceRatio', () => traceDialog(r, tr, totalOps(s))))
  }
  const c = t.match(/^cpu\s+(\d+)\s*s?$/)
  if (c) out.push(verb(`cpu ${c[1]}s`, '/debug/pprof/profile', () => captureDialog('cpu', Math.min(300, Number(c[1])))))
  if (t && 'heap'.startsWith(t)) out.push(verb('heap', '/debug/pprof/heap', () => captureDialog('heap', 0)))
  return out
})
