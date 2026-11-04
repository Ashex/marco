import { DataTable } from '../components/DataTable'
import { Loaded, NeedsVersion, PageHead, Panel, Spark, Tiles, Updated, Dot } from '../components/kit'
import { fmtCount, rate } from '../lib/fmt'
import { last, mean, sum, unsupported, useOverview, useSeries, useSources, useWal } from '../lib/queries'
import { OUTCOMES } from '../lib/tone'
import { useWindow, windowLabel } from './common'

export function Ingest() {
  const ov = useOverview()
  const src = useSources()
  const wal = useWal()
  const [win, winSeg] = useWindow()
  const se = useSeries(win)
  const s = se.data
  const o = ov.data
  const applied = sum(s, { metric: 'applied' })
  const noSources = src.data && unsupported(src.data)
  return (
    <>
      <PageHead
        title="Ingest"
        sub={
          <>
            <span>writes through marco.v1.Graph</span>
            <Dot />
            <span>{noSources ? 'no sources of its own yet' : ''}</span>
            {wal.data && unsupported(wal.data) && (
              <>
                <Dot />
                <span>no WAL yet</span>
              </>
            )}
            <Updated l={se} />
          </>
        }
        actions={winSeg}
      />
      <Tiles
        cols={4}
        tiles={[
          { label: 'Jetstream lag', value: '—', sec: 'needs a newer marco' },
          { label: 'Firehose lag', value: '—', sec: 'needs a newer marco' },
          { label: 'Applied', value: rate(last(applied)), unit: 'ops/s', sec: s?.tMs.length ? `${rate(mean(applied))} avg ${windowLabel(win)}` : undefined, spark: <Spark data={applied} color="signal" /> },
          { label: 'Non-TID rkeys', value: fmtCount(o?.relations.reduce((a, r) => a + Number(r.nonTid), 0)), sec: 'in the snapshot' },
        ]}
      />
      <Panel title="Sources" right={<span title="key (actor, rel, rkey) · repeats land as exists">one write path</span>}>
        {noSources ? <NeedsVersion what="Jetstream and firehose sources" rpc="ListSources" /> : <Loaded load={src}>{() => null}</Loaded>}
      </Panel>
      <div style={{ height: 14 }} />
      <div className="cx-grid2">
        <Panel title="Ops by outcome" right={<span className="mono">ops/s · {windowLabel(win)} mean</span>}>
          <DataTable
            compact
            rows={o?.relations.map((r) => r.name) ?? []}
            rowKey={(r) => r}
            cols={[
              { id: 'rel', label: 'Relation', fill: true, render: (r) => r },
              ...OUTCOMES.map((oc) => ({ id: oc, label: oc, r: true, render: (r: string) => rate(mean(sum(s, { metric: 'applied', rel: r, outcome: oc }))) })),
            ]}
          />
        </Panel>
        <Panel title="Non-TID rkeys" right={<span className="mono">{fmtCount(o?.relations.reduce((a, r) => a + Number(r.nonTid), 0))} stored · side tables</span>}>
          <DataTable
            compact
            rows={o?.relations ?? []}
            rowKey={(r) => r.name}
            cols={[
              { id: 'rel', label: 'Relation', fill: true, render: (r) => r.name },
              { id: 'n', label: 'Stored', r: true, render: (r) => fmtCount(r.nonTid) },
              { id: 'k', label: '1 ‖ hash63', r: true, title: 'a non-TID rkey keys as 1 ‖ hash63(rkey); the string is in the side table', render: () => <span className="muted">side table</span> },
            ]}
          />
        </Panel>
      </div>
    </>
  )
}
