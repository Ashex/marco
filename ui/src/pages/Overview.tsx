import { useState } from 'react'
import { DataTable } from '../components/DataTable'
import {
  Banners,
  Dot,
  Glyph,
  HealthLine,
  KV,
  Legend,
  Loaded,
  Meter,
  PageHead,
  Panel,
  PanelBody,
  Seg,
  Spark,
  Swatch,
  Tiles,
  Updated,
  type BannerSpec,
  type HealthCell,
  type TileSpec,
} from '../components/kit'
import { Link } from '../lib/router'
import { bytes, bytesParts, clock, dur, fmtCount, lat, latParts, n, pct, rate, si } from '../lib/fmt'
import { useLiveState } from '../lib/live'
import { last, mean, READ_OPS, sum, useOverview, useRecent, useSeries, useShards, useSources, weighted, WRITE_OPS } from '../lib/queries'
import { OUTCOMES, outcomeColor } from '../lib/tone'
import { setAttention } from '../components/Palette'
import { openPanel } from '../components/nav'
import { cursorText, lagText, shardName, sourceNamed, useWindow, windowLabel } from './common'
import type { Source, GetOverviewResponse, GetSeriesResponse, ListShardsResponse, RecentWrite } from '../gen/marco/v1/admin_pb'
import { togglePaused } from '../lib/live'

// ---------------------------------------------------------------- shared with other pages

/** The banners every page that shows them derives the same way. */
export function banners(o?: GetOverviewResponse, _sh?: ListShardsResponse): BannerSpec[] {
  const b: BannerSpec[] = []
  const job = o?.job
  if (job?.running)
    b.push({
      id: 'job',
      tone: 'info',
      title: job.kind === 'checkpoint' ? 'Checkpointing' : 'Verifying checksums',
      desc: `${job.done} / ${job.total} files · started ${clock(n(job.startedMs))}`,
      right: <Link to="/admin/storage">Storage ›</Link>,
    })
  else if (job?.error)
    b.push({ id: 'jobfail', tone: 'err', title: job.kind === 'checkpoint' ? 'Checkpoint failed' : 'Checksums failed', desc: job.error, right: <Link to="/admin/storage">Storage ›</Link> })
  const du = o?.durability
  if (du?.segmentError) b.push({ id: 'segerr', tone: 'err', title: 'Segment write failing', desc: du.segmentError, right: <Link to="/admin/storage">Storage ›</Link> })
  if (du?.checkpointError) b.push({ id: 'ckerr', tone: 'warn', title: 'Checkpoint error', desc: du.checkpointError, right: <Link to="/admin/storage">Storage ›</Link> })
  if (o && !o.ready)
    b.push({
      id: 'notready',
      tone: 'warn',
      title: 'Catching up',
      desc: `${o.readyReason || 'not ready'} · ${fmtCount(o.readsBeforeReady)} reads before ready`,
      right: <Link to="/admin/ingest">Ingest ›</Link>,
    })
  if (o?.readOnly) b.push({ id: 'ro', tone: 'idle', title: 'Read-only', desc: '--read-only · writes refused' })
  return b
}

export function useAttention(o?: GetOverviewResponse, sh?: ListShardsResponse) {
  const items = banners(o, sh)
  setAttention(
    items.map((x) => ({
      id: x.id,
      glyph: <Glyph k={x.tone} />,
      label: x.title,
      kind: typeof x.desc === 'string' ? x.desc : undefined,
      run: () => void (location.href = x.id === 'notready' ? '/admin/ingest' : '/admin/storage'),
    })),
  )
  return items
}

const READ_RPCS = ['Has', 'Relationships', 'Counts', 'ListOut', 'ListIn', 'Intersect', 'Filter'].map((m) => `Graph/${m}`)
export const readRate = (s?: GetSeriesResponse) =>
  READ_RPCS.reduce<number[]>((acc, rpc) => {
    const v = sum(s, { metric: 'rpc_rate', rpc })
    return v.length ? v.map((x, i) => x + (acc[i] ?? 0)) : acc
  }, [])
export const writeRate = (s?: GetSeriesResponse) => sum(s, { metric: 'applied' })

export function ShardMap({ sh, small }: { sh: ListShardsResponse; small?: boolean }) {
  const ratio = sh.mergeRatio || 0.125
  return (
    <div className="cx-map" style={small ? { gap: 2 } : undefined}>
      {sh.shards.map((s) => {
        const d = n(s.deltaEntries) / Math.max(1, n(s.baseEntries))
        const st = s.verify === 'mismatch' ? 'mismatch' : s.state === 'clean' ? '' : s.state
        const g = { writing: '●', dirty: '◆', mismatch: '■', '': '' }[st] ?? ''
        const title = `${shardName(s.shard)} · delta ${pct(d)} of base · ${s.state}${s.verify !== 'unverified' ? ` · xxh3 ${s.verify}` : ''}`
        return small ? (
          <span key={s.shard} className={`cx-cell ${st}`} style={{ height: 16, padding: '0 2px', fontSize: 9, lineHeight: '16px', justifyContent: 'center', cursor: 'pointer' }} title={title} onClick={() => openPanel('shard', String(s.shard))}>
            {g}
          </span>
        ) : (
          <span key={s.shard} className={`cx-cell ${st}`} title={title} style={{ cursor: 'pointer' }} onClick={() => openPanel('shard', String(s.shard))}>
            <span>{String(s.shard).padStart(2, '0')}</span>
            <span>{g}</span>
            <i>
              <s style={{ width: `${Math.min(1, d / ratio) * 100}%` }} />
            </i>
          </span>
        )
      })}
    </div>
  )
}

export function shardCounts(sh: ListShardsResponse) {
  const c = { clean: 0, dirty: 0, writing: 0, mismatch: 0 }
  for (const s of sh.shards) {
    c[s.state as keyof typeof c] = (c[s.state as keyof typeof c] ?? 0) + 1
    if (s.verify === 'mismatch') c.mismatch++
  }
  return c
}

// ---------------------------------------------------------------- the page

function WritesChart({ s, rel, win }: { s?: GetSeriesResponse; rel: string; win: number }) {
  const h = 120
  const w = 1000
  // at most 60 bars: a window's points averaged into groups
  const raw = s?.tMs.length ?? 0
  const g = Math.max(1, Math.ceil(raw / 60))
  const group = (v: number[]) => Array.from({ length: Math.ceil(v.length / g) }, (_, i) => { const xs = v.slice(i * g, i * g + g); return xs.reduce((a, b) => a + b, 0) / Math.max(1, xs.length) })
  const per = OUTCOMES.map((o) => ({ o, v: group(sum(s, { metric: 'applied', outcome: o, ...(rel === 'all' ? {} : { rel }) }).map((x) => x * 60)) }))
  const pts = Math.ceil(raw / g)
  const tot = Array.from({ length: pts }, (_, i) => per.reduce((a, p) => a + (p.v[i] ?? 0), 0))
  const mx = Math.max(1, ...tot) * 1.1
  const bw = pts ? w / pts : w
  return (
    <>
      <div style={{ padding: '10px 12px 4px' }}>
        <svg viewBox={`0 0 ${w} ${h + 18}`} preserveAspectRatio="none" style={{ display: 'block', width: '100%', height: h + 18 }} role="img" aria-label="Writes per minute by outcome">
          {[0, 0.33, 0.66, 1].map((k) => (
            <line key={k} x1="0" x2={w} y1={h - k * h} y2={h - k * h} style={{ stroke: 'var(--rule2)', strokeWidth: 1 }} vectorEffect="non-scaling-stroke" />
          ))}
          {tot.map((_, i) => {
            let y = h
            return per.map((p) => {
              const hh = ((p.v[i] ?? 0) / mx) * h
              y -= hh
              return hh > 0 ? <rect key={`${i}-${p.o}`} x={i * bw + 1} y={y} width={Math.max(1, bw - 2)} height={hh} style={{ fill: `var(--${outcomeColor(p.o)})` }} /> : null
            })
          })}
        </svg>
      </div>
      <div style={{ padding: '0 12px 10px' }}>
        <Legend items={OUTCOMES.map((o) => [<Swatch color={outcomeColor(o)} />, o])} right={<span className="mono muted">{pts ? `−${windowLabel(win)} … now · ${rate(Math.max(0, ...tot) / 60)} ops/s max` : 'no writes yet'}</span>} />
      </div>
    </>
  )
}

function Tail({ writes, rel }: { writes: RecentWrite[]; rel: string }) {
  const rows = writes.filter((w) => rel === 'all' || w.rel === rel).slice(0, 9)
  if (!rows.length) return <div className="cx-tail" style={{ padding: '0 12px' }}><span className="muted">no writes since start</span></div>
  const tail = (d: string) => d.slice(-8)
  return (
    <div className="cx-tail">
      {rows.map((w) => (
        <div key={String(w.seq)} className="cx-tl" title={w.uri} onClick={() => w.uri && openPanel('edge', w.uri)} style={{ cursor: 'pointer' }}>
          <span className="muted">{clock(n(w.atMs))}</span>
          {w.add ? <span className="s-in">+</span> : <span className="s-sig">−</span>}
          <span className="t2">{w.rel}</span>
          <span>
            …{tail(w.actor)} → {w.subject ? `…${tail(w.subject)}` : '?'}
          </span>
          <span className="muted" style={{ textAlign: 'right' }}>
            {w.outcome}
          </span>
        </div>
      ))}
    </div>
  )
}

export function Overview() {
  const ov = useOverview()
  const [win, winSeg] = useWindow()
  const se = useSeries(win)
  const sh = useShards()
  const rc = useRecent()
  const src = useSources()
  const [wrel, setWrel] = useState('all')
  const [trel, setTrel] = useState('all')
  const { paused } = useLiveState()
  const o = ov.data
  const s = se.data
  const items = useAttention(o, sh.data)
  const rels = o?.relations.map((r) => r.name) ?? []
  const relOpts = [{ v: 'all', label: 'all' }, ...rels.map((r) => ({ v: r, label: r }))]

  return (
    <>
      <PageHead
        title="Overview"
        sub={
          o && (
            <>
              <span>{si(n(o.directory?.base) + n(o.directory?.live))} actors</span>
              <Dot />
              <span>{si(o.relations.reduce((a, r) => a + n(r.records), 0))} edges</span>
              <Dot />
              <span>snapshot {dur(Date.now() - n(o.snapshot?.writtenMs))}</span>
              <Dot />
              <span>up {dur(Date.now() - n(o.startedMs))}</span>
              <Updated l={ov} />
            </>
          )
        }
        actions={winSeg}
      />
      <Banners items={items} />
      <Loaded load={ov}>
        {(o) => {
          const reads = readRate(s)
          const writes = writeRate(s)
          const added = sum(s, { metric: 'applied', outcome: 'added' })
          const removed = sum(s, { metric: 'applied', outcome: 'removed' })
          const cnt = sh.data ? shardCounts(sh.data) : undefined
          const dr = n(o.deltaEntries) / Math.max(1, n(o.baseEntries))
          const tr = o.traceRatio
          const jet = sourceNamed(src.data?.sources, 'jetstream')
          const fire = sourceNamed(src.data?.sources, 'firehose')
          const lagCell = (label: string, x?: Source): HealthCell =>
            x
              ? { label, tone: x.caughtUp ? 'ok' : 'warn', value: lagText(x.lagSeconds), sub: `${rate(x.opsPerSecond)} ops/s`, to: '/admin/ingest', title: 'lag: now − cursor time_us' }
              : { label, tone: 'idle', value: '—', sub: 'not configured', to: '/admin/ingest' }
          const health: HealthCell[] = [
            { label: 'Reads', tone: 'ok', value: rate(last(reads)), unit: 'rpc/s', sub: `p99 ${lat(last(weighted(s, 'op_p99', {})))}`, to: '/admin/requests', title: 'all read ops' },
            { label: 'Writes', tone: 'ok', value: rate(last(writes)), unit: 'ops/s', sub: `added ${rate(last(added))} · removed ${rate(last(removed))}`, to: '/admin/ingest' },
            lagCell('Jetstream', jet),
            lagCell('Firehose', fire),
            { label: 'Durability', tone: 'ok', value: o.durability?.segmentMs ? dur(Date.now() - n(o.durability.segmentMs)) : '—', sub: `${fmtCount(o.durability?.segments)} segs · ckpt ${dur(Date.now() - n(o.durability?.checkpointMs))}`, title: `${fmtCount(o.durability?.segments)} segments since the checkpoint ${dur(Date.now() - n(o.durability?.checkpointMs))} ago; last segment age`, to: "/admin/storage" },
            {
              label: 'Snapshot',
              tone: cnt && cnt.dirty ? 'info' : 'ok',
              value: dur(Date.now() - n(o.snapshot?.writtenMs)),
              sub: `${n(o.dirtyShards)} / ${o.snapshot?.shards ?? 0} dirty`,
              to: '/admin/storage',
            },
            { label: 'Deltas', tone: dr > o.mergeRatio ? 'warn' : 'ok', value: (dr * 100).toFixed(dr < 0.1 ? 2 : 1), unit: '%', sub: `${si(o.deltaEntries)} entries`, to: '/admin/storage', title: 'delta entries ÷ base entries, all lists' },
            { label: 'Tracing', tone: tr > 0 ? 'info' : 'idle', value: tr > 0 ? pct(tr, tr < 0.01 ? 1 : 0) : 'off', sub: `ratio ${tr}`, to: '/admin/requests' },
          ]
          const perDay = (rel: string) => (mean(sum(s, { metric: 'applied', outcome: 'added', rel })) - mean(sum(s, { metric: 'applied', outcome: 'removed', rel }))) * 86400
          const [rss, rssU] = bytesParts(o.process?.rssBytes)
          const has = latParts(last(weighted(s, 'op_p99', { op: 'has' })))
          const rel50 = latParts(last(weighted(s, 'op_p50', { op: 'relationships' })))
          const tiles: TileSpec[] = [
            ...o.relations.slice(0, 2).map<TileSpec>((r) => {
              const d = perDay(r.name)
              return {
                label: r.name === 'follow' ? 'Follows' : r.name === 'block' ? 'Blocks' : r.name,
                value: fmtCount(r.records),
                right: isFinite(d) && s?.tMs.length ? `${d >= 0 ? '+' : ''}${si(d)}/d` : undefined,
                title: 'records (out logs)',
                spark: <Spark data={sum(s, { metric: 'applied', outcome: 'added', rel: r.name })} color="out" />,
              }
            }),
            { label: 'Actors', value: fmtCount(n(o.directory?.base) + n(o.directory?.live)), right: n(o.directory?.live) ? `+${si(o.directory?.live)} live` : undefined, title: 'directory DIDs' },
            { label: 'Reads', value: rate(last(reads)), unit: 'rpc/s', spark: <Spark data={reads} color="accent" /> },
            { label: 'Writes', value: rate(last(writes)), unit: 'ops/s', sec: s?.tMs.length ? `${rate(mean(writes))} avg ${windowLabel(win)}` : undefined, spark: <Spark data={writes} color="signal" /> },
            { label: 'has p99', value: has[0], unit: has[1], spark: <Spark data={weighted(s, 'op_p99', { op: 'has' })} /> },
            { label: 'relationships p50', value: rel50[0], unit: rel50[1], sec: `p99 ${lat(last(weighted(s, 'op_p99', { op: 'relationships' })))}`, spark: <Spark data={weighted(s, 'op_p50', { op: 'relationships' })} /> },
            { label: 'RSS', value: rss, unit: rssU, sec: `of ${bytes(o.process?.memTotalBytes)}`, spark: <Spark data={sum(s, { metric: 'rss_bytes' })} /> },
          ]
          const opsRows = [...READ_OPS, ...WRITE_OPS].map((op) => ({
            op,
            kind: WRITE_OPS.includes(op) ? 'write' : 'read',
            rate: sum(s, { metric: 'op_rate', op }),
            p50: weighted(s, 'op_p50', { op }),
            p99: weighted(s, 'op_p99', { op }),
          }))
          const rel = (
            <Panel title="Edges by relation" to="/admin/relations" right={<Updated l={ov} />}>
              <DataTable
                rows={o.relations}
                rowKey={(r) => r.name}
                cols={[
                  { id: 'name', label: 'Relation', fill: true, render: (r) => (<><b>{r.name}</b><div className="cx-did">{r.collection}</div></>) },
                  { id: 'records', label: 'Records', r: true, render: (r) => fmtCount(r.records) },
                  { id: 'pairs', label: 'Pairs', r: true, className: 'hide-phone', title: 'at the snapshot', render: (r) => fmtCount(r.snapshotPairs) },
                  { id: 'actors', label: 'Actors', r: true, className: 'hide-phone', title: 'actors with records, at the snapshot', render: (r) => fmtCount(r.outLists) },
                  { id: 'subjects', label: 'Subjects', r: true, className: 'hide-phone', title: 'actors named, at the snapshot', render: (r) => fmtCount(r.inLists) },
                  { id: 'dup', label: 'Dup', r: true, className: 'hide-phone', title: 'records less pairs', render: (r) => fmtCount(n(r.snapshotRecords) - n(r.snapshotPairs)) },
                  { id: 'net', label: `Net ${windowLabel(win)}`, r: true, render: (r) => { const d = (mean(sum(s, { metric: 'applied', outcome: 'added', rel: r.name })) - mean(sum(s, { metric: 'applied', outcome: 'removed', rel: r.name }))) * win; return isFinite(d) ? `${d >= 0 ? '+' : ''}${si(d)}` : '—' } },
                ]}
              />
            </Panel>
          )
          const wchart = (
            <Panel title="Writes / min by outcome" right={<Seg label="Relation" value={wrel} options={relOpts} onChange={setWrel} />}>
              <WritesChart s={s} rel={wrel} win={win} />
            </Panel>
          )
          const ops = (
            <Panel title="Ops" to="/admin/requests" right={<span>engine time · all relations</span>}>
              <DataTable
                rows={opsRows}
                rowKey={(r) => r.op}
                cols={[
                  { id: 'op', label: 'Op', render: (r) => <span className="mono">{r.op}</span> },
                  { id: 'kind', label: 'Kind', fill: true, className: 'hide-phone', render: (r) => <span className="muted">{r.kind}</span> },
                  { id: 'rate', label: '/s', r: true, render: (r) => rate(last(r.rate)) },
                  { id: 'p50', label: 'p50', r: true, render: (r) => lat(last(r.p50)) },
                  { id: 'p99', label: 'p99', r: true, render: (r) => lat(last(r.p99)) },
                  { id: 'sp', label: windowLabel(win), className: 'hide-phone', render: (r) => <span style={{ display: 'block', width: 120 }}><Spark data={r.rate} color={r.kind === 'write' ? 'signal' : 'out'} /></span> },
                ]}
              />
            </Panel>
          )
          const ingest = (
            <Panel title="Ingest" to="/admin/ingest">
              <Loaded load={src}>
                {(d) => (
                  <DataTable
                    compact
                    rows={d.sources}
                    rowKey={(x) => x.name}
                    cols={[
                      { id: 'name', label: 'Source', fill: true, render: (x) => <span className="mono">{x.name}</span> },
                      { id: 'cur', label: 'Cursor', className: 'hide-phone', render: (x) => <span className="mono t2">{cursorText(x.cursor, x.cursorKind)}</span> },
                      { id: 'lag', label: 'Lag', r: true, render: (x) => (x.cursorKind === 'time_us' ? lagText(x.lagSeconds) : '—') },
                      { id: 'ops', label: 'ops/s', r: true, render: (x) => rate(x.opsPerSecond) },
                      { id: 'up', label: 'Caught up', render: (x) => <Glyph k={x.caughtUp ? 'ok' : 'warn'} /> },
                    ]}
                  />
                )}
              </Loaded>
            </Panel>
          )
          const snap = (
            <Panel title="Snapshot" to="/admin/storage">
              <PanelBody style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                {sh.data && (
                  <>
                    <ShardMap sh={sh.data} small />
                    <Legend
                      items={[
                        [<Glyph k="info" />, `dirty ${cnt?.dirty ?? 0}`],
                        [<Glyph k="acc" />, `writing ${cnt?.writing ?? 0}`],
                        [<span className="muted">○ </span>, <span className="muted">clean {cnt?.clean ?? 0}</span>],
                      ]}
                    />
                  </>
                )}
                <KV
                  rows={[
                    ['Written', <>{dur(Date.now() - n(o.snapshot?.writtenMs))} ago <span className="muted mono">{o.snapshot?.build.slice(0, 8)}</span></>],
                    ['Size', <><span className="mono">{bytes(o.snapshot?.sizeBytes)}</span> <span className="muted">{o.snapshot?.shards} shards</span></>],
                    ['Segments', <><span className="mono">{fmtCount(o.durability?.segments)}</span> <span className="muted">{bytes(o.durability?.segmentBytes)} · {si(o.durability?.segmentOps)} ops</span></>],
                  ]}
                />
              </PanelBody>
            </Panel>
          )
          const mt = n(o.process?.memTotalBytes)
          const mem = (
            <Panel title="Memory">
              <PanelBody>
                <KV
                  rows={[
                    ['RSS', <span className="mono">{bytes(o.process?.rssBytes)}</span>, <Meter v={n(o.process?.rssBytes)} max={mt} k="acc" />],
                    ['Merged lists', <span className="mono">{bytes(o.mergedBytes)}</span>, <Meter v={n(o.mergedBytes)} max={mt} />],
                  ]}
                />
              </PanelBody>
            </Panel>
          )
          const live = (
            <Panel
              title="Live writes"
              right={
                <>
                  <Seg label="Relation" value={trel} options={relOpts} onChange={setTrel} />
                  <button type="button" className="cx-btn sm quiet" onClick={togglePaused}>
                    <kbd>space</kbd> {paused ? 'resume' : 'pause'}
                  </button>
                </>
              }
            >
              <Loaded load={rc}>{(r) => <Tail writes={r.writes} rel={trel} />}</Loaded>
            </Panel>
          )
          return (
            <>
              <HealthLine cells={health} />
              <Tiles tiles={tiles} cols={4} />
              <div className="cx-ov">
                <div className="cx-stack">
                  {rel}
                  {wchart}
                  {ops}
                </div>
                <div className="cx-stack">
                  {ingest}
                  {snap}
                  {mem}
                  {live}
                </div>
              </div>
            </>
          )
        }}
      </Loaded>
    </>
  )
}

