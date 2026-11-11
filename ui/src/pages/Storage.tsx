import { useState, type ReactNode } from 'react'
import { DataTable } from '../components/DataTable'
import { confirmAction } from '../components/dialogs'
import { Banners, Chip, Glyph, KV, Legend, Loaded, Meter, PageHead, Panel, PanelBody, Seg, Spark, Swatch, Tiles, Updated, Arrow, Dot, oldest } from '../components/kit'
import { bytes, bytesParts, dur, fmtCount, n, pct, si, ago, lat, shortDid } from '../lib/fmt'
import { last, sum, series, useOverview, useSeries, useShards, useStorage, verifyShards, checkpoint } from '../lib/queries'
import { SHARD_WORD, shardTone, verifyTone } from '../lib/tone'
import { ShardMap, shardCounts, useAttention } from './Overview'
import { dirOf, shardName, windowLabel } from './common'
import type { Cache, Durability, GetOverviewResponse, GetSeriesResponse, ListShardsResponse, Section } from '../gen/marco/v1/admin_pb'

const PART: Record<string, [string, string]> = {
  'out.small': ['small out logs', 'out'],
  'out.subj': ['out subjects', 'out'],
  'out.rkey': ['out rkeys', 'out'],
  'out.bysubj': ['out by subject', 'out'],
  'out.head': ['out heads', 'c4'],
  'out.idx': ['out index', 'c4'],
  'in.small': ['small in sets', 'in'],
  'in.blocks': ['in set blocks', 'in'],
  'in.head': ['in heads', 'c4'],
  'in.idx': ['in index', 'c4'],
  side: ['non-TID rkeys', 'c3'],
}

function partOf(part: string): [string, string] | undefined {
  return part.endsWith('.sums') ? ['page sums', 'c4'] : PART[part]
}

function sectionRows(secs: Section[], o: GetOverviewResponse) {
  const rels = secs.filter((s) => s.rel !== 'directory')
  const dir = secs.filter((s) => s.rel === 'directory')
  const dirBytes = dir.reduce((a, s) => a + n(s.sizeBytes), 0)
  const rows = rels.map((s) => {
    const part = s.name.split('.').slice(1).join('.')
    const r = o.relations.find((x) => x.name === s.rel)
    const per = part.startsWith('in') ? n(r?.snapshotPairs) : n(r?.snapshotRecords)
    const p = partOf(part)
    return { name: s.name, rel: s.rel, part: p?.[0] ?? part, color: p?.[1] ?? 'c6', b: n(s.sizeBytes), per: per ? n(s.sizeBytes) / per : NaN, unit: 'B/edge' }
  })
  rows.push({ name: 'dir.*', rel: 'directory', part: `${si(o.directory?.base)} DIDs`, color: 'c6', b: dirBytes, per: dirBytes / Math.max(1, n(o.directory?.base)), unit: 'B/DID' })
  return { rows, dir }
}

export function checkpointDialog(sh: ListShardsResponse, d?: Durability) {
  const c = shardCounts(sh)
  const dirtyIds = sh.shards.filter((s) => s.state !== 'clean').map((s) => String(s.shard).padStart(3, '0'))
  return confirmAction({
    tone: 'info',
    title: 'Checkpoint now?',
    items: [
      dirtyIds.length ? `dirty: shard-${dirtyIds.slice(0, 12).join(' ')}${dirtyIds.length > 12 ? ` +${dirtyIds.length - 12}` : ''}` : 'no dirty shards',
      `folds ${d?.segments ?? 0} segments · ${fmtCount(d?.segmentOps)} ops`,
      `rewrites ${c.dirty + c.writing} dirty shards · manifest renamed atomically`,
      'reads and writes continue',
    ],
    action: 'Checkpoint',
    call: 'POST /marco.v1.Admin/Checkpoint\n{}',
    run: () => checkpoint(),
    done: 'Checkpoint started',
  })
}

export function verifyDialog(sh: ListShardsResponse) {
  const size = sh.shards.reduce((a, s) => a + n(s.sizeBytes), 0) + n(sh.directoryBytes)
  return confirmAction({
    tone: 'info',
    title: `Verify ${sh.shards.length + 1} files?`,
    items: [`every section and page sum · ${bytes(size)} read`, 'reads and writes continue'],
    action: 'Verify checksums',
    call: 'POST /marco.v1.Admin/VerifyShards\n{}',
    run: () => verifyShards(),
    done: 'Verify started',
  })
}

export function Storage() {
  const ov = useOverview()
  const sh = useShards()
  const [by, setBy] = useState<'ratio' | 'size'>('ratio')
  const st = useStorage(by)
  const se = useSeries(3600)
  const [sort, setSort] = useState<'delta' | 'bytes' | 'written'>('delta')
  const [all, setAll] = useState(false)
  const o = ov.data
  const items = useAttention(o, sh.data)
  const s = se.data
  return (
    <>
      <PageHead
        title="Storage"
        sub={
          o && (
            <>
              <span className="mono">{o.snapshot?.path}</span>
              <Dot />
              <span>build {o.snapshot?.build.slice(0, 8)}</span>
              <Dot />
              <span>format {o.snapshot?.format}</span>
              <Dot />
              <span>written {ago(o.snapshot?.writtenMs)}</span>
              <Updated l={oldest(ov, sh)} />
            </>
          )
        }
        actions={
          sh.data && (
            <>
              <button type="button" className="cx-btn" disabled={sh.data.job?.running} onClick={() => verifyDialog(sh.data!)}>
                Verify checksums…
              </button>
              <button type="button" className="cx-btn primary" disabled={sh.data.job?.running || o?.readOnly} title={o?.readOnly ? '--read-only' : undefined} onClick={() => checkpointDialog(sh.data!, o?.durability)}>
                Checkpoint now…
              </button>
            </>
          )
        }
      />
      <Banners items={items} />
      <Loaded load={ov}>
        {(o) => {
          const cnt = sh.data ? shardCounts(sh.data) : undefined
          const [sz, szU] = bytesParts(o.snapshot?.sizeBytes)
          const [mb, mbU] = bytesParts(o.mergedBytes)
          const verifyJob = o.job?.kind === 'verify' && !o.job.running && o.job.finishedMs ? dur(n(o.job.finishedMs) - n(o.job.startedMs)) : undefined
          return (
            <Tiles
              cols={4}
              tiles={[
                { label: 'Snapshot', value: sz, unit: szU, sec: `${o.snapshot?.shards} shards`, right: o.snapshot?.build.slice(0, 8) },
                { label: 'Directory', value: fmtCount(n(o.directory?.base) + n(o.directory?.live)), unit: 'DIDs', right: n(o.directory?.live) ? `+${fmtCount(o.directory?.live)} live` : undefined, title: `base ${fmtCount(o.directory?.base)} · live ${fmtCount(o.directory?.live)}` },
                { label: 'Dirty shards', value: String(n(o.dirtyShards)), unit: `/ ${o.snapshot?.shards}`, sec: cnt?.writing ? `${cnt.writing} writing` : undefined },
                { label: 'Delta entries', value: si(o.deltaEntries), sec: pct(n(o.deltaEntries) / Math.max(1, n(o.baseEntries)), 2), spark: <Spark data={sum(s, { metric: 'delta_entries' })} color="signal" /> },
                { label: 'Merged since snapshot', value: mb, unit: mbU, sec: `${fmtCount(o.mergedLists)} lists`, title: 'marco_override_bytes', spark: <Spark data={sum(s, { metric: 'merges' })} /> },
                { label: 'Tombstones', value: fmtCount(o.tombstones), spark: <Spark data={sum(s, { metric: 'tombstones' })} /> },
                { label: 'Segments', value: fmtCount(o.durability?.segments), sec: `${bytes(o.durability?.segmentBytes)} · ${si(o.durability?.segmentOps)} ops`, right: o.durability?.segmentMs ? ago(o.durability.segmentMs) : undefined, title: 'change segments since the checkpoint' },
                { label: 'Open', value: dur(o.snapshot!.openSeconds * 1000), sec: verifyJob ? `verify ${verifyJob}` : undefined, title: 'pinned sections read and checked, pages loaded, segments replayed' },
              ]}
            />
          )
        }}
      </Loaded>
      {o?.cache && <CachePanel c={o.cache} o={o} s={s} />}
      {o && st.data && (
        <BytesPanel secs={st.data.sections} o={o} />
      )}
      <div style={{ height: 14 }} />
      <div className="cx-grid2" style={{ gridTemplateColumns: 'minmax(0,1.4fr) minmax(0,1fr)' }}>
        <div className="cx-stack">
          <Panel
            title="Shards"
            right={<Seg label="Sort" value={sort} options={[{ v: 'delta', label: 'delta' }, { v: 'bytes', label: 'bytes' }, { v: 'written', label: 'written' }]} onChange={setSort} />}
            foot={
              sh.data && (
                <>
                  <span className="mono">
                    {all ? sh.data.shards.length : Math.min(10, sh.data.shards.length)} of {sh.data.shards.length}
                  </span>
                  <span>sorted by {sort}</span>
                  <div className="r">
                    <button type="button" className="cx-btn sm" onClick={() => setAll((a) => !a)}>
                      {all ? 'Top 10' : `All ${sh.data.shards.length} ›`}
                    </button>
                  </div>
                </>
              )
            }
          >
            <Loaded load={sh}>
              {(d) => {
                const c = shardCounts(d)
                const ratio = d.mergeRatio || 0.125
                const key = (x: (typeof d.shards)[number]) => (sort === 'delta' ? n(x.deltaEntries) / Math.max(1, n(x.baseEntries)) : sort === 'bytes' ? n(x.sizeBytes) : n(x.writtenMs))
                const rows = [...d.shards].sort((a, b) => key(b) - key(a)).slice(0, all ? undefined : 10)
                return (
                  <>
                    <PanelBody style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                      <ShardMap sh={d} />
                      <Legend
                        items={[
                          [<Glyph k="idle" />, `clean ${c.clean}`],
                          [<Glyph k="info" />, `dirty ${c.dirty}`],
                          [<Glyph k="acc" />, `writing ${c.writing}`],
                          [<Glyph k="err" />, `checksum mismatch ${c.mismatch}`],
                        ]}
                        right={<span className="muted">bar: delta ÷ {pct(ratio)}</span>}
                      />
                    </PanelBody>
                    <DataTable
                      compact
                      rows={rows}
                      rowKey={(x) => String(x.shard)}
                      open={(x) => ({ type: 'shard', id: String(x.shard) })}
                      cols={[
                        { id: 'k', label: 'Shard', render: (x) => <span className="mono">{shardName(x.shard)}</span> },
                        { id: 'ids', label: 'ids', fill: true, className: 'hide-phone', render: (x) => <span className="mono t2">{si(x.lo)}–{si(x.hi)}</span> },
                        { id: 'file', label: 'File', className: 'hide-phone', title: 'empty: new ids, no file yet', render: (x) => (x.file ? <span className="mono t2">{x.file}</span> : <span className="muted">—</span>) },
                        { id: 'ck', label: 'Ckpt', r: true, className: 'hide-phone', render: (x) => <span className="mono">{x.file ? String(x.checkpoint) : '—'}</span> },
                        { id: 'b', label: 'Bytes', r: true, render: (x) => bytes(x.sizeBytes) },
                        { id: 'd', label: 'Delta', r: true, render: (x) => pct(n(x.deltaEntries) / Math.max(1, n(x.baseEntries))) },
                        { id: 'm', label: '', render: (x) => <Meter v={n(x.deltaEntries) / Math.max(1, n(x.baseEntries))} max={ratio} /> },
                        { id: 'st', label: 'State', render: (x) => <Chip k={shardTone(x.state)}>{SHARD_WORD[x.state] ?? x.state}</Chip> },
                        { id: 'v', label: 'xxh3', title: 'the last verify', render: (x) => <span title={x.verify}><Glyph k={verifyTone(x.verify)} /> <span className="sm t2">{x.verify === 'unverified' ? '—' : x.verify}</span></span> },
                        { id: 'w', label: 'Written', r: true, render: (x) => (x.state === 'writing' ? 'writing' : dur(Date.now() - n(x.writtenMs))) },
                      ]}
                    />
                  </>
                )
              }}
            </Loaded>
          </Panel>
          <Panel title="Delta vs base" right={<Seg label="Sort" value={by} options={[{ v: 'ratio', label: 'ratio' }, { v: 'size', label: 'size' }]} onChange={setBy} />} foot={o && <span>merge at {pct(o.mergeRatio)} · subject index over {fmtCount(o.snapshot?.subjIndex)}</span>}>
            <Loaded load={st}>
              {(d) => (
                <DataTable
                  compact
                  rows={d.deltas}
                  rowKey={(x) => `${x.did}:${x.rel}:${x.dir}`}
                  onRow={(x) => (location.href = `/admin/explorer?actor=${encodeURIComponent(x.did)}`)}
                  empty={<span className="cx-empty">No deltas</span>}
                  cols={[
                    { id: 'did', label: 'List', fill: true, render: (x) => <span className="mono sm" title={x.did}>{shortDid(x.did)}</span> },
                    { id: 'rel', label: 'Rel', render: (x) => <>{x.rel} <Arrow dir={dirOf(x.dir)} /></> },
                    { id: 'b', label: 'Base', r: true, render: (x) => fmtCount(x.base) },
                    { id: 'a', label: 'Adds', r: true, render: (x) => `+${fmtCount(x.adds)}` },
                    { id: 'd', label: 'Tombs', r: true, title: 'deletes of base records', render: (x) => `−${fmtCount(x.dels)}` },
                    { id: 'r', label: 'Ratio', r: true, render: (x) => pct((n(x.adds) + n(x.dels)) / Math.max(1, n(x.base))) },
                    { id: 'm', label: '', render: (x) => <Meter v={(n(x.adds) + n(x.dels)) / Math.max(1, n(x.base))} max={o?.mergeRatio ?? 0.125} k={(n(x.adds) + n(x.dels)) / Math.max(1, n(x.base)) >= (o?.mergeRatio ?? 0.125) * 0.8 ? 'warn' : undefined} /> },
                  ]}
                />
              )}
            </Loaded>
          </Panel>
        </div>
        <div className="cx-stack">
          {o?.durability && <DurabilityPanel d={o.durability} />}
          <Panel title="Merges" right={<span className="mono">last {windowLabel(3600)}</span>}>
            <PanelBody>
              <div style={{ display: 'flex', gap: 16 }}>
                <div style={{ flex: 1 }}>
                  <div className="cx-eyebrow">merges / min</div>
                  <Spark data={sum(s, { metric: 'merges', dir: 'out' }).map((x) => x * 60)} l2={sum(s, { metric: 'merges', dir: 'in' }).map((x) => x * 60)} color="out" />
                </div>
                <div style={{ flex: 1 }}>
                  <div className="cx-eyebrow">merge p99</div>
                  <Spark data={sum(s, { metric: 'merge_p99', dir: 'out' })} l2={sum(s, { metric: 'merge_p99', dir: 'in' })} />
                </div>
              </div>
            </PanelBody>
            {o && (
              <DataTable
                compact
                rows={o.relations.flatMap((r) => (['out', 'in'] as const).map((d) => ({ rel: r.name, d, merged: series(s, { metric: 'merges', rel: r.name, dir: d })[0]?.values ?? [] })))}
                rowKey={(x) => `${x.rel}:${x.d}`}
                cols={[
                  { id: 'rel', label: 'Rel', render: (x) => x.rel },
                  { id: 'dir', label: 'Dir', fill: true, render: (x) => <><Arrow dir={x.d} /> {x.d}</> },
                  { id: 'n', label: 'Merges', r: true, render: (x) => fmtCount(x.merged.reduce((a, v) => a + v, 0) * (n(s?.stepMs) / 1000)) },
                  { id: 'p99', label: 'p99', r: true, render: (x) => lat(last(sum(s, { metric: 'merge_p99', dir: x.d }))) },
                ]}
              />
            )}
          </Panel>
          {o && (
            <Panel title="Tombstones">
              <PanelBody>
                <KV
                  rows={[
                    ...o.relations.map((r) => [r.name, <span className="mono">{fmtCount(r.tombstones)}</span>, <Meter v={n(r.tombstones)} max={Math.max(1, n(o.tombstones))} k="out" />] as [React.ReactNode, React.ReactNode, React.ReactNode]),
                  ]}
                />
              </PanelBody>
            </Panel>
          )}
        </div>
      </div>
    </>
  )
}

const STRUCT_LABEL: Record<string, string> = {
  out_small: 'out.small',
  out_subj: 'out.subj',
  out_rkey: 'out.rkey',
  out_bysubj: 'bysubj',
  in_small: 'in.small',
  in_blocks: 'in.blocks',
}

/** 1 - misses/lookups, pointwise; NaN where nothing was looked up. */
function ratio(look: number[], miss: number[]) {
  return look.map((l, i) => (l > 0 ? 1 - (miss[i] ?? 0) / l : NaN))
}

const lastOk = (v: number[]) => {
  for (let i = v.length - 1; i >= 0; i--) if (isFinite(v[i])) return v[i]
  return NaN
}

export function CachePanel({ c, o, s }: { c: Cache; o: GetOverviewResponse; s?: GetSeriesResponse }) {
  const budget = n(c.budgetBytes)
  const pinned = n(c.pinnedDirBytes) + n(c.pinnedIndexBytes) + n(c.pinnedHeadsBytes) + n(c.pinnedTablesBytes)
  const parts: [string, number, string, string][] = [
    ['pinned', pinned, 'c4', `directory ${bytes(c.pinnedDirBytes)} · index ${bytes(c.pinnedIndexBytes)} · heads ${bytes(c.pinnedHeadsBytes)} · sums, slots and tables ${bytes(c.pinnedTablesBytes)}`],
    ['main', n(c.mainBytes), 'out', 'main queue'],
    ['small', n(c.smallBytes), 'in', 'small queue'],
    ['retired', n(c.retiredBytes), 'c3', 'frames waiting on the epoch'],
    ['borrowed', n(c.borrowedBytes), 'c6', "a checkpoint's new files before their switch"],
  ]
  const used = parts.reduce((a, p) => a + p[1], 0)
  const rest = Math.max(0, n(o.process?.rssBytes) - used)
  const look = sum(s, { metric: 'store_lookups' })
  const miss = sum(s, { metric: 'store_misses' })
  const hit = ratio(look, miss)
  const reads = sum(s, { metric: 'store_reads', dir: 'request' })
  const ioB = sum(s, { metric: 'store_io_bytes' })
  const errs = n(c.checksumErrors) + n(c.eioErrors) + n(c.shortErrors)
  const d = o.durability
  const rpcs = series(s, { metric: 'store_pages_p99' }).map((x) => x.rpc)
  const rounds = Math.max(...series(s, { metric: 'store_rounds_p99' }).map((x) => lastOk(x.values)), 0)
  return (
    <Panel
      title="Cache"
      right={
        <span className="mono">
          {bytes(used)} of {bytes(budget)}
          {c.resident && <> · <Chip k="ok" title="open loaded every page">resident</Chip></>}
        </span>
      }
      foot={
        <>
          <span className="mono" title="paged bytes no index names, as of the last checkpoint or compaction">garbage {bytes(c.garbageBytes)}</span>
          <span className="mono" title="cached pages requests hold">pins {bytes(c.pinsBytes)}</span>
          <span className="mono" title="process RSS outside the store's budget: deltas, live DIDs, request heap">rest of RSS {bytes(rest)}</span>
          <div className="r">
            <span className="mono" title="pages checkpoints and compactions wrote into the cache · let go">
              written through {si(c.writtenThrough)}
              {n(c.writeThroughSkipped) > 0 && ` · ${si(c.writeThroughSkipped)} let go`}
            </span>
          </div>
        </>
      }
    >
      <PanelBody style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
        <div className="cx-bytes" role="img" aria-label="the store's budget by part">
          {parts.map(([name, b, color, title]) => (b > 0 ? <span key={name} title={`${name} ${bytes(b)} · ${title}`} style={{ flex: `${(b / budget).toFixed(4)} 1 0`, background: `var(--${color})` }} /> : null))}
          <span title={`free ${bytes(c.freeBytes)}`} style={{ flex: `${(n(c.freeBytes) / budget).toFixed(4)} 1 0` }} />
        </div>
        <Legend
          items={[
            ...parts.filter((p) => p[1] > 0 || p[0] !== 'borrowed').map(([name, b, color, title]): [ReactNode, ReactNode] => [<Swatch color={color} />, <span title={title}>{name} {bytes(b)}</span>]),
            [<Swatch color="rule2" />, `free ${bytes(c.freeBytes)}`],
          ]}
        />
      </PanelBody>
      <Tiles
        cols={4}
        tiles={[
          { label: 'Hit ratio', value: pct(lastOk(hit)), sec: `${si(last(miss))} misses/s`, title: 'page lookups that found the page in memory', spark: <Spark data={hit} color="ok" /> },
          { label: 'Device reads', value: si(last(reads)), unit: '/s', sec: `${bytes(last(ioB))}/s`, title: 'read calls on the request lane · bytes on both lanes', spark: <Spark data={reads} /> },
          {
            label: 'Read latency',
            value: lat(last(sum(s, { metric: 'store_read_p99', dir: 'request' }))),
            unit: 'p99',
            sec: `p50 ${lat(last(sum(s, { metric: 'store_read_p50', dir: 'request' })))}`,
            title: 'per read call on the request lane',
            spark: <Spark data={sum(s, { metric: 'store_read_p99', dir: 'request' })} />,
          },
          {
            label: 'Miss pool',
            value: fmtCount(last(sum(s, { metric: 'store_pool_busy' }))),
            unit: `/ ${c.missThreads}`,
            sec: `wait p99 ${lat(last(sum(s, { metric: 'store_pool_wait_p99' })))} · I/O queue ${fmtCount(last(sum(s, { metric: 'store_io_queue' })))}`,
            title: 'requests that missed, on the pool’s threads now',
            spark: <Spark data={sum(s, { metric: 'store_pool_busy' })} />,
          },
        ]}
      />
      <DataTable
        compact
        rows={c.structures}
        rowKey={(x) => x.name}
        cols={[
          { id: 'name', label: 'Structure', render: (x) => <span className="mono">{STRUCT_LABEL[x.name] ?? x.name}</span> },
          {
            id: 'hit',
            label: 'Hit ratio',
            r: true,
            title: 'the last 5 s',
            render: (x) => pct(lastOk(ratio(sum(s, { metric: 'store_lookups', op: x.name }), sum(s, { metric: 'store_misses', op: x.name }))), 1),
          },
          { id: 'spark', label: '', fill: true, className: 'hide-phone', render: (x) => <Spark data={ratio(sum(s, { metric: 'store_lookups', op: x.name }), sum(s, { metric: 'store_misses', op: x.name }))} color="ok" /> },
          { id: 'miss', label: 'Misses/s', r: true, render: (x) => si(last(sum(s, { metric: 'store_misses', op: x.name }))) },
          { id: 'tot', label: 'Since start', r: true, className: 'hide-phone', title: 'lookups · misses', render: (x) => <span className="t2">{si(x.lookups)} · {si(x.misses)}</span> },
        ]}
      />
      <PanelBody>
        <KV
          rows={[
            [
              'Pages per call',
              <span className="mono">{rpcs.map((r) => `${(r ?? '').replace('Graph/', '')} ${fmtCount(last(sum(s, { metric: 'store_pages_p99', rpc: r })))}`).join(' · ') || '—'}</span>,
              <span className="muted" title="pages a request read from disk, and the rounds it waited, p99">p99 · rounds {fmtCount(rounds)}</span>,
            ],
            [
              'Errors',
              <span className="mono">
                bad pages {fmtCount(c.badPages)} · read errors {fmtCount(errs)} · retried {fmtCount(c.eioRetried)}
              </span>,
              n(c.badPages) > 0 || c.failingPages >= 64 ? <Chip k="err" title="64 pages failing in 60 s takes /readyz down">{c.failingPages >= 64 ? 'storm' : 'bad pages'}</Chip> : <Chip k="ok">ok</Chip>,
            ],
            [
              'Last checkpoint',
              <span className="mono" title="R: the changed lists and blocks · P_new: their pinned sections">
                patch {bytes(d?.checkpointPatchBytes)} · pinned {bytes(d?.checkpointPinnedBytes)}
              </span>,
              <span className="muted">compactions 24 h {c.compactions24h}</span>,
            ],
          ]}
        />
      </PanelBody>
    </Panel>
  )
}

function BytesPanel({ secs, o }: { secs: Section[]; o: GetOverviewResponse }) {
  const { rows, dir } = sectionRows(secs, o)
  const tot = rows.reduce((a, r) => a + r.b, 0)
  return (
    <Panel
      title="Bytes by section"
      right={<span className="mono">{o.snapshot?.files} files · {bytes(o.snapshot?.sizeBytes)}</span>}
      foot={
        <>
          <span>xxh3 per pinned section · a sum per 4 KiB page</span>
          <div className="r">
            <span className="mono">{dir.map((d) => `${d.name} ${bytes(d.sizeBytes)}`).join(' · ')}</span>
          </div>
        </>
      }
    >
      <PanelBody>
        <div className="cx-bytes">
          {rows.map((r) => (
            <span key={r.name} title={`${r.name} · ${bytes(r.b)}`} style={{ flex: `${(r.b / tot).toFixed(4)} 1 0`, background: `var(--${r.color})` }} />
          ))}
        </div>
      </PanelBody>
      <DataTable
        compact
        rows={rows}
        rowKey={(r) => r.name}
        cols={[
          { id: 'name', label: 'Section', render: (r) => <span style={{ display: 'inline-flex', alignItems: 'center', gap: 2 }}><Swatch color={r.color} /><span className="mono">{r.name}</span></span> },
          { id: 'rel', label: 'Relation', render: (r) => r.rel },
          { id: 'part', label: 'Part', fill: true, render: (r) => <span className="t2">{r.part}</span> },
          { id: 'b', label: 'Bytes', r: true, render: (r) => bytes(r.b) },
          { id: 'per', label: 'Per', r: true, render: (r) => <>{isFinite(r.per) ? r.per.toFixed(r.unit === 'B/DID' ? 1 : 2) : '—'} <span className="muted">{r.unit}</span></> },
          { id: 'share', label: 'Share', r: true, render: (r) => pct(r.b / tot) },
          { id: 'm', label: '', render: (r) => <Meter v={r.b} max={tot} /> },
        ]}
      />
    </Panel>
  )
}

export function DurabilityPanel({ d }: { d: Durability }) {
  const next = (
    <span className="mono">
      {si(d.checkpointOps)} ops · {dur(d.checkpointAgeSeconds * 1000)}
    </span>
  )
  return (
    <Panel title="Durability" right={<span className="mono">segment every {d.segmentIntervalSeconds} s</span>}>
      <PanelBody>
        <KV
          rows={[
            ['Segment interval', <span className="mono">{d.segmentIntervalSeconds} s</span>],
            [
              'Last segment',
              <>
                <span className="mono">#{String(d.segment)}</span> {ago(d.segmentMs)} <span className="muted mono">{lat(d.segmentWriteSeconds)}</span>
              </>,
            ],
            ['Since checkpoint', <span className="mono">{fmtCount(d.segments)} segments · {bytes(d.segmentBytes)} · {fmtCount(d.segmentOps)} ops</span>],
            ['Pending ops', <span className="mono">{fmtCount(d.pendingOps)}</span>, <span className="muted sm">not in a segment yet</span>],
            [
              'Checkpoint',
              <>
                <span className="mono">#{String(d.checkpoint)}</span> {ago(d.checkpointMs)}
              </>,
            ],
            ['Checkpoint took', <span className="mono">{dur(d.checkpointSeconds * 1000)} · {bytes(d.checkpointBytes)} · {d.checkpointShards} shards</span>],
            ['Next checkpoint at', next, <span className="muted sm">whichever first</span>],
            ['Replay at start', <span className="mono">{fmtCount(d.replayedSegments)} segments · {fmtCount(d.replayedOps)} ops · {d.replaySeconds.toFixed(2)} s</span>],
            ...(d.segmentError ? [['Segment error', <span className="err">{d.segmentError}</span>, <span className="muted sm">retrying</span>] as [ReactNode, ReactNode, ReactNode]] : []),
            ...(d.checkpointError
              ? [['Checkpoint error', <span className="err">{d.checkpointError}</span>, <span className="muted sm">{fmtCount(d.checkpointFailures)} failed</span>] as [ReactNode, ReactNode, ReactNode]]
              : []),
          ]}
        />
      </PanelBody>
    </Panel>
  )
}
