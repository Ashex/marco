import { useState } from 'react'
import { DataTable } from '../components/DataTable'
import { confirmAction } from '../components/dialogs'
import { Banners, Chip, Glyph, KV, Legend, Loaded, Meter, PageHead, Panel, PanelBody, Seg, Spark, Swatch, Tiles, Updated, Arrow, Dot, oldest } from '../components/kit'
import { bytes, bytesParts, dur, fmtCount, n, pct, si, ago, lat, shortDid } from '../lib/fmt'
import { last, sum, series, useOverview, useSeries, useShards, useStorage, verifyShards, checkpoint } from '../lib/queries'
import { SHARD_WORD, shardTone, verifyTone } from '../lib/tone'
import { ShardMap, shardCounts, useAttention } from './Overview'
import { dirOf, shardName, windowLabel } from './common'
import type { Durability, GetOverviewResponse, ListShardsResponse, Section } from '../gen/marco/v1/admin_pb'

const PART: Record<string, [string, string]> = {
  'out.dat': ['out log', 'out'],
  'out.idx': ['out index', 'c4'],
  'in.dat': ['in set', 'in'],
  'in.idx': ['in index', 'c4'],
  side: ['non-TID rkeys', 'c3'],
}

function sectionRows(secs: Section[], o: GetOverviewResponse) {
  const rels = secs.filter((s) => s.rel !== 'directory')
  const dir = secs.filter((s) => s.rel === 'directory')
  const dirBytes = dir.reduce((a, s) => a + n(s.sizeBytes), 0)
  const rows = rels.map((s) => {
    const part = s.name.split('.').slice(1).join('.')
    const r = o.relations.find((x) => x.name === s.rel)
    const per = part.startsWith('in') ? n(r?.snapshotPairs) : n(r?.snapshotRecords)
    return { name: s.name, rel: s.rel, part: PART[part]?.[0] ?? part, color: PART[part]?.[1] ?? 'c6', b: n(s.sizeBytes), per: per ? n(s.sizeBytes) / per : NaN, unit: 'B/edge' }
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
    items: [`xxh3 of every section · ${bytes(size)} read`, 'reads and writes continue'],
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
                { label: 'Cold open', value: (o.snapshot!.openSeconds * 1000).toFixed(0), unit: 'ms', sec: verifyJob ? `verify ${verifyJob}` : undefined, title: 'mmap + footer checks, no full xxh3' },
              ]}
            />
          )
        }}
      </Loaded>
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

function BytesPanel({ secs, o }: { secs: Section[]; o: GetOverviewResponse }) {
  const { rows, dir } = sectionRows(secs, o)
  const tot = rows.reduce((a, r) => a + r.b, 0)
  return (
    <Panel
      title="Bytes by section"
      right={<span className="mono">{o.snapshot?.files} files · {bytes(o.snapshot?.sizeBytes)}</span>}
      foot={
        <>
          <span>xxh3 per section · 64-byte aligned</span>
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
          ]}
        />
      </PanelBody>
    </Panel>
  )
}
