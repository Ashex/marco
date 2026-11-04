import { confirmAction } from '../components/dialogs'
import { registerDetail } from '../components/Drawer'
import { Chip, CopyBtn, Did, Empty, KV, Sec, Strip, Arrow } from '../components/kit'
import { closePanel } from '../components/nav'
import { graph, reqJson } from '../lib/api'
import { bytes, day, fmtCount, n, pct, utc, ago } from '../lib/fmt'
import { useLive } from '../lib/query'
import { deleteEdge, useResolve, useShards } from '../lib/queries'
import { navigate } from '../lib/router'
import { SHARD_WORD, shardTone, verifyTone } from '../lib/tone'
import { storedChip } from './Explorer'
import { listLabel, shardName } from './common'

export function deleteEdgeDialog(uri: string, actor: string, subject: string, rel: string, lastRecord: boolean) {
  const rkey = uri.split('/').pop()
  return confirmAction({
    tone: 'warn',
    title: `Delete ${actor} → ${subject}?`,
    items: [
      <>
        {rel} · rkey <span className="mono">{rkey}</span>
      </>,
      lastRecord ? <>last record for the pair · {rel}.in(subject) drops the actor</> : <>the pair keeps its other records</>,
      'in memory until the next snapshot write',
    ],
    action: 'Delete edge',
    button: 'warn',
    call: `POST /marco.v1.Graph/WriteBatch\n${reqJson({ ops: [{ delete: { uri } }] })}`,
    run: () => deleteEdge(uri),
    done: 'Deleted',
  }).then((ok) => ok && closePanel())
}

registerDetail('edge', {
  kind: 'Edge',
  section: 'explorer',
  use: (uri) => {
    const r = useResolve(uri)
    const x = r.data
    const pair = useLive(['pair', x?.actor, x?.subject], () => graph().relationships({ actor: x!.actor, others: [x!.subject] }), { enabled: !!x?.subject })
    if (r.error) return { title: uri, text: uri, body: null, missing: String((r.error as Error).message ?? r.error) }
    if (!x) return { title: uri, text: uri, body: null, loading: true }
    const title = (
      <>
        <span className="mono" style={{ fontSize: 13 }}>…{x.actor.slice(-8)}</span> <Arrow dir="out" /> <span className="mono" style={{ fontSize: 13 }}>{x.subject ? `…${x.subject.slice(-8)}` : '?'}</span>
      </>
    )
    if (!x.subject)
      return {
        title,
        text: uri,
        fresh: r,
        body: (
          <Empty title="No such record">
            {x.tombstoned ? 'deleted before its create' : `no ${x.rel} record ${x.rkey}`}
          </Empty>
        ),
      }
    const pr = pair.data?.relationships[0]
    const curl = `curl -X POST ${location.host}/marco.v1.Graph/Has \\\n  -H 'content-type: application/json' \\\n  -d '${reqJson({ rel: x.rel, from: x.actor, to: x.subject })}'`
    return {
      title,
      text: `${x.rel} ${x.actor} → ${x.subject}`,
      fresh: r,
      body: (
        <>
          <Strip
            items={[
              [x.rkey, 'rkey'],
              [x.tid ? day(x.createdUs) : '—', 'created'],
              [shardName(x.shard), 'out log'],
              [x.position ? `${fmtCount(x.position)} / ${fmtCount(x.outCount)}` : fmtCount(x.outCount), 'position', 'newest first'],
            ]}
          />
          <Sec title="Edge" digest={`${x.rel} · ${x.collection}`}>
            <KV
              rows={[
                ['URI', <span className="mono sm trunc" title={uri}>{uri}</span>, <CopyBtn text={uri} />],
                ['Actor', <><Did did={x.actor} /> <span className="cx-did">id {fmtCount(x.actorId)}</span></>],
                ['Subject', <><Did did={x.subject} /> <span className="cx-did">id {fmtCount(x.subjectId)}</span></>],
                ['rkey', <span className="mono">{x.rkey}</span>, <Chip k="plain" g="◇">{x.tid ? 'TID' : 'not a TID'}</Chip>],
                ['Created', <span className="mono">{x.tid ? `${utc(x.createdUs, true)} UTC` : '—'}</span>],
                ['Stored', <>{storedChip(x.stored)}</>],
              ]}
            />
          </Sec>
          <Sec title="Reverse" digest="subject in set">
            <KV
              rows={[
                [`${x.rel}.in(subject)`, <span className="mono">{x.inContains ? 'contains actor' : 'missing actor'}</span>, x.inContains ? <Chip k="ok">yes</Chip> : <Chip k="err">no</Chip>],
                [listLabel(x.rel, 'in'), <span className="mono">{fmtCount(x.subjectInCount)}</span>],
                ['Stored', <>{storedChip(x.subjectInStored)} <span className="mono muted">· {shardName(x.subjectShard)}</span></>],
              ]}
            />
          </Sec>
          <Sec title="Pair" digest={`${fmtCount(x.pairRecords)} ${x.pairRecords === 1n ? 'record' : 'records'} naming the subject`}>
            {pr ? (
              <KV
                rows={pr.rels.flatMap((e) => [
                  [<>{e.rel} <Arrow dir="out" /></>, e.outUri ? <span className="mono sm trunc">…/{e.outUri.split('/').slice(-2).join('/')}</span> : <span className="muted">—</span>, e.outUri ? <Chip k="ok">yes</Chip> : <Chip k="idle">no</Chip>],
                  [<>{e.rel} <Arrow dir="in" /></>, e.inUri ? <span className="mono sm trunc">…/{e.inUri.split('/').slice(-2).join('/')}</span> : <span className="muted">—</span>, e.inUri ? <Chip k="ok">yes</Chip> : <Chip k="idle">no</Chip>],
                ] as [React.ReactNode, React.ReactNode, React.ReactNode][])}
              />
            ) : (
              <Empty>Loading…</Empty>
            )}
          </Sec>
          <Sec title="Request" digest="curl" right={<CopyBtn text={curl} />}>
            <pre className="cx-code">{curl}</pre>
          </Sec>
          <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap' }}>
            <button type="button" className="cx-btn danger" onClick={() => deleteEdgeDialog(uri, x.actor, x.subject, x.rel, n(x.pairRecords) <= 1)}>
              Delete edge…
            </button>
            <button type="button" className="cx-btn" onClick={() => navigate(`/admin/explorer?actor=${encodeURIComponent(x.actor)}`)}>
              Open actor
            </button>
            <button type="button" className="cx-btn" onClick={() => navigate(`/admin/explorer?actor=${encodeURIComponent(x.subject)}`)}>
              Open subject
            </button>
          </div>
        </>
      ),
    }
  },
})

registerDetail('shard', {
  kind: 'Shard',
  section: 'storage',
  use: (id) => {
    const sh = useShards()
    const s = sh.data?.shards.find((x) => String(x.shard) === id)
    if (!sh.data) return { title: shardName(Number(id)), body: null, loading: true }
    if (!s) return { title: shardName(Number(id)), body: null, missing: `${sh.data.shards.length} shards in the snapshot` }
    const r = n(s.deltaEntries) / Math.max(1, n(s.baseEntries))
    return {
      title: shardName(s.shard),
      chip: <Chip k={shardTone(s.state)}>{SHARD_WORD[s.state] ?? s.state}</Chip>,
      fresh: sh,
      body: (
        <>
          <Strip
            items={[
              [bytes(s.sizeBytes), 'bytes'],
              [fmtCount(s.baseEntries), 'base entries', 'records and pairs'],
              [pct(r, 2), 'delta', `threshold ${pct(sh.data.mergeRatio)}`],
              [fmtCount(s.tombstones), 'tombstones'],
            ]}
          />
          <Sec title="Shard" digest={`ids ${fmtCount(s.lo)}–${fmtCount(s.hi)}`}>
            <KV
              rows={[
                ['Ids', <span className="mono">{fmtCount(s.lo)} – {fmtCount(s.hi)}</span>, <span className="muted sm">{fmtCount(s.hi - s.lo)} actors</span>],
                ['Delta', <span className="mono">{fmtCount(s.deltaEntries)} entries · {fmtCount(s.deltaLists)} lists</span>],
                ['Merged', <span className="mono">{fmtCount(s.mergedLists)} lists · {bytes(s.mergedBytes)}</span>],
                ['Written', <span className="mono">{ago(s.writtenMs)}</span>],
                ['xxh3', <Chip k={verifyTone(s.verify)}>{s.verify}</Chip>, s.verifyError ? <span className="s-err sm">{s.verifyError}</span> : undefined],
                ['File', <span className="mono sm">{shardName(s.shard)}.marco</span>],
              ]}
            />
          </Sec>
        </>
      ),
    }
  },
})

