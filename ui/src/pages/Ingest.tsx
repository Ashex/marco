import { DataTable } from '../components/DataTable'
import { Chip, Loaded, Glyph, PageHead, Panel, Spark, Tiles, Updated } from '../components/kit'
import { fmtCount, rate } from '../lib/fmt'
import { last, mean, sum, useOverview, useSeries, useSources } from '../lib/queries'
import { OUTCOMES } from '../lib/tone'
import { cursorText, lagText, sourceNamed, useWindow, windowLabel } from './common'

export function Ingest() {
  const ov = useOverview()
  const src = useSources()
  const [win, winSeg] = useWindow()
  const se = useSeries(win)
  const s = se.data
  const o = ov.data
  const applied = sum(s, { metric: 'applied' })
  const lagTile = (label: string, name: string) => {
    const x = sourceNamed(src.data?.sources, name)
    return x ? { label, value: lagText(x.lagSeconds), sec: `${rate(x.opsPerSecond)} ops/s`, title: 'now − cursor time_us' } : { label, value: '—', sec: 'not configured' }
  }
  return (
    <>
      <PageHead
        title="Ingest"
        sub={
          <>
            <span>writes through marco.v1.Graph</span>
            <Updated l={se} />
          </>
        }
        actions={winSeg}
      />
      <Tiles
        cols={4}
        tiles={[
          lagTile('Jetstream lag', 'jetstream'),
          lagTile('Firehose lag', 'firehose'),
          { label: 'Applied', value: rate(last(applied)), unit: 'ops/s', sec: s?.tMs.length ? `${rate(mean(applied))} avg ${windowLabel(win)}` : undefined, spark: <Spark data={applied} color="signal" /> },
          { label: 'Non-TID rkeys', value: fmtCount(o?.relations.reduce((a, r) => a + Number(r.nonTid), 0)), sec: 'in the snapshot' },
        ]}
      />
      <Panel title="Sources" right={<span title="key (actor, rel, rkey) · repeats land as exists">one write path</span>}>
        <Loaded load={src}>
          {(d) => (
            <DataTable
              compact
              rows={d.sources}
              rowKey={(x) => x.name}
              cols={[
                { id: 'name', label: 'Source', fill: true, render: (x) => <span title={x.url}><b className="mono">{x.name}</b>{x.url && <div className="cx-did">{x.url}</div>}</span> },
                { id: 'st', label: 'State', render: (x) => <Chip k={x.state === 'connected' || x.state === 'serving' ? 'ok' : x.state === 'idle' ? 'idle' : 'warn'}>{x.state}</Chip> },
                { id: 'cur', label: 'Cursor', className: 'hide-phone', title: 'last applied', render: (x) => <span className="mono t2" title={x.cursor}>{cursorText(x.cursor, x.cursorKind)}</span> },
                { id: 'dur', label: 'Durable', className: 'hide-phone', title: 'in the newest segment: what a restart resumes from', render: (x) => <span className="mono t2" title={x.durableCursor}>{cursorText(x.durableCursor, x.cursorKind)}</span> },
                { id: 'lag', label: 'Lag', r: true, render: (x) => (x.cursorKind === 'time_us' ? lagText(x.lagSeconds) : '—') },
                { id: 'ops', label: 'ops/s', r: true, render: (x) => rate(x.opsPerSecond) },
                { id: 'err', label: 'Errors', r: true, className: 'hide-phone', render: (x) => fmtCount(x.errors) },
                { id: 'rc', label: 'Reconn.', r: true, className: 'hide-phone', render: (x) => fmtCount(x.reconnects) },
                { id: 'ex', label: 'Expected', className: 'hide-phone', title: 'readiness waits for it', render: (x) => (x.expected ? 'yes' : '—') },
                { id: 'cu', label: 'Caught up', render: (x) => <Glyph k={x.caughtUp ? 'ok' : 'warn'} /> },
              ]}
            />
          )}
        </Loaded>
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
