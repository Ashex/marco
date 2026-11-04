import { useState } from 'react'
import { DataTable } from '../components/DataTable'
import { Loaded, Loading, PageHead, Panel, PanelBody, Seg, Strip, Updated, Arrow, Dot } from '../components/kit'
import { fmtCount, n, pct, si, ago, shortDid } from '../lib/fmt'
import { useListStats, useOverview, useRelations } from '../lib/queries'
import { navigate } from '../lib/router'
import { Dir } from '../gen/marco/v1/graph_pb'
import type { ListDist } from '../gen/marco/v1/admin_pb'

function Hist({ d, color, threshold }: { d?: ListDist; color: string; threshold: number }) {
  const counts = (d?.log2Counts ?? []).map(n)
  const nb = Math.max(20, counts.length)
  const vals = Array.from({ length: nb }, (_, i) => counts[i] ?? 0)
  const mx = Math.max(1, ...vals)
  const w = 600
  const h = 110
  const bw = w / nb
  const tx = Math.log2(threshold) * bw
  const label = (i: number) => (2 ** i < 1000 ? String(2 ** i) : 2 ** i < 1_000_000 ? `${2 ** i / 1024}k` : `${2 ** i / 1048576}M`)
  return (
    <>
      <svg className="cx-hist" viewBox={`0 0 ${w} ${h + 16}`} preserveAspectRatio="none" style={{ display: 'block', width: '100%', height: h + 16 }} role="img" aria-label="List length histogram, log2 buckets">
        {vals.map((v, i) => {
          const hh = (v / mx) * h
          return v > 0 ? (
            <rect key={i} x={i * bw + 1} y={h - hh} width={bw - 2} height={hh} style={{ fill: `var(--${color})` }}>
              <title>{`${label(i)}–${label(i + 1)}: ${fmtCount(v)} lists`}</title>
            </rect>
          ) : null
        })}
        <line x1={tx} x2={tx} y1="0" y2={h} style={{ stroke: 'var(--ink)', strokeWidth: 1.2, strokeDasharray: '3 3' }} vectorEffect="non-scaling-stroke" />
      </svg>
      <div className="mono muted sm" style={{ display: 'flex', justifyContent: 'space-between' }}>
        {Array.from({ length: Math.ceil(nb / 2) }, (_, j) => (
          <span key={j}>{label(j * 2)}</span>
        ))}
      </div>
    </>
  )
}

export function Relations() {
  const rl = useRelations()
  const ov = useOverview()
  const names = rl.data?.relations.map((r) => r.name) ?? []
  const [rel, setRel] = useState('')
  const cur = rel || names[0] || ''
  const ls = useListStats(cur)
  const first = useListStats(names[0] ?? '')
  const o = ov.data
  const actors = n(o?.directory?.base)
  return (
    <>
      <PageHead
        title="Relations"
        sub={
          <>
            <span>{names.length} relations</span>
            <Dot />
            <span>loaded from the snapshot at start</span>
            <Updated l={rl} />
          </>
        }
      />
      <Loaded load={rl}>
        {(d) => (
          <Panel title="Relations">
            <DataTable
              rows={d.relations}
              rowKey={(r) => r.name}
              cols={[
                { id: 'name', label: 'Name', render: (r) => <b>{r.name}</b> },
                { id: 'coll', label: 'Collection', fill: true, render: (r) => <span className="mono t2">{r.collection}</span> },
                { id: 'subj', label: 'Subject path', render: (r) => <span className="mono">{r.subject}</span> },
                {
                  id: 'from',
                  label: 'From',
                  title: 'a list relation keeps edges from the record\'s list; its companion holds the records by owner',
                  render: (r) => (r.list ? <span className="mono">{r.list}</span> : r.ownerOf ? <span className="muted">records of {r.ownerOf}</span> : <span className="muted">actor</span>),
                },
                { id: 'rec', label: 'Records', r: true, render: (r) => fmtCount(r.records) },
                { id: 'dup', label: 'Duplicates', r: true, title: 'records less pairs', render: (r) => fmtCount(r.duplicates) },
                { id: 'big', label: `Lists > ${fmtCount(ls.data?.threshold ?? first.data?.threshold ?? 4096)}`, r: true, title: 'out lists with a by-subject index', render: (r) => (r.name === cur && ls.data?.ready ? fmtCount(ls.data.out?.overThreshold) : r.name === names[0] && first.data?.ready ? fmtCount(first.data.out?.overThreshold) : '—') },
                { id: 'nt', label: 'Non-TID', r: true, render: (r) => fmtCount(r.nonTid) },
              ]}
            />
          </Panel>
        )}
      </Loaded>
      <div style={{ height: 14 }} />
      <Panel
        title={`List lengths · ${cur}`}
        right={
          <>
            <Seg label="Relation" value={cur} options={names.map((x) => ({ v: x, label: x }))} onChange={setRel} />
            <span className="mono">dashed: {fmtCount(ls.data?.threshold ?? 4096)}</span>
          </>
        }
        foot={ls.data?.ready ? <span>at the snapshot · computed {ago(ls.data.computedMs)}</span> : undefined}
      >
        {ls.data && !ls.data.ready ? (
          <Loading label={`Reading lists · ${pct(ls.data.progress, 0)}`} />
        ) : (
          <Loaded load={ls}>
            {(x) => (
              <>
                <PanelBody style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(min(380px,100%),1fr))', gap: 18 }}>
                  <div>
                    <div className="cx-eyebrow" style={{ marginBottom: 6 }}>
                      <Arrow dir="out" /> out · records per actor
                    </div>
                    <Hist d={x.out} color="out" threshold={x.threshold} />
                  </div>
                  <div>
                    <div className="cx-eyebrow" style={{ marginBottom: 6 }}>
                      <Arrow dir="in" /> in · actors per subject
                    </div>
                    <Hist d={x.in} color="in" threshold={x.threshold} />
                  </div>
                </PanelBody>
                <div style={{ padding: '0 12px 12px' }}>
                  <Strip
                    items={[
                      [fmtCount(x.out?.p50), 'p50 out'],
                      [fmtCount(x.out?.p90), 'p90'],
                      [fmtCount(x.out?.p99), 'p99'],
                      [fmtCount(x.out?.p999), 'p99.9'],
                      [fmtCount(x.out?.max), 'max out'],
                      [si(x.in?.max), 'max in'],
                    ]}
                  />
                </div>
              </>
            )}
          </Loaded>
        )}
      </Panel>
      <div style={{ height: 14 }} />
      <div className="cx-grid2" style={{ gridTemplateColumns: 'minmax(0,.75fr) minmax(0,1.25fr) minmax(0,.9fr)' }}>
        <Panel title="Config" right={<span className="mono">read-only · in the snapshot</span>}>
          <PanelBody>
            <Loaded load={rl}>{(d) => <pre className="cx-code">{d.config.trim()}</pre>}</Loaded>
          </PanelBody>
        </Panel>
        <Panel title={`Lists over ${fmtCount(ls.data?.threshold ?? 4096)}`} right={ls.data?.ready ? <span className="mono">{fmtCount(ls.data.out?.overThreshold)} out · {pct(n(ls.data.out?.overThreshold) / Math.max(1, actors), 2)} of actors</span> : undefined}>
          {ls.data?.ready ? (
            <DataTable
              compact
              rows={ls.data.big}
              rowKey={(b) => `${b.did}:${b.dir}`}
              onRow={(b) => navigate(`/admin/explorer?actor=${encodeURIComponent(b.did)}`)}
              empty={<span className="cx-empty">None</span>}
              cols={[
                { id: 'did', label: 'List', fill: true, render: (b) => <span className="mono sm" title={b.did}>{shortDid(b.did)}</span> },
                { id: 'rel', label: 'Rel', render: (b) => <>{cur} <Arrow dir={b.dir === Dir.IN ? 'in' : 'out'} /></> },
                { id: 'len', label: 'Length', r: true, render: (b) => fmtCount(b.length) },
              ]}
            />
          ) : (
            <Loading />
          )}
        </Panel>
        <Panel title="Duplicates">
          <Loaded load={rl}>
            {(d) => (
              <PanelBody>
                <Strip
                  items={d.relations.flatMap(
                    (r) =>
                      [
                        [si(r.duplicates), r.name, `${fmtCount(r.duplicates)} records`],
                        [pct(n(r.duplicates) / Math.max(1, n(r.records)), 2), `of ${r.name}s`],
                      ] as [string, string, string?][],
                  )}
                />
              </PanelBody>
            )}
          </Loaded>
        </Panel>
      </div>
    </>
  )
}
