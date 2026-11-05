import { Code, ConnectError, createRouterTransport } from '@connectrpc/connect'
import { Admin } from '../gen/marco/v1/admin_pb'
import { Dir, Graph } from '../gen/marco/v1/graph_pb'

// `npm run dev:mock` and the shots: a whole-network-sized marco with fake DIDs, answering
// every Admin and Graph RPC the way the server does. A real build never loads this file.

const B32 = '234567abcdefghijklmnopqrstuvwxyz'
function hash(s: string): number {
  let h = 2166136261
  for (let i = 0; i < s.length; i++) h = Math.imul(h ^ s.charCodeAt(i), 16777619)
  return h >>> 0
}
function rng(seed: number) {
  let x = seed || 1
  return () => {
    x ^= x << 13
    x ^= x >>> 17
    x ^= x << 5
    return (x >>> 0) / 4294967296
  }
}
export function did(seed: string): string {
  const r = rng(hash(seed))
  let s = ''
  for (let i = 0; i < 24; i++) s += B32[Math.floor(r() * 32)]
  return `did:plc:${s}`
}
function tid(us: number, clock = 7): string {
  let v = (BigInt(Math.floor(us)) << 10n) | BigInt(clock)
  let s = ''
  for (let i = 0; i < 13; i++) {
    s = B32[Number(v & 31n)] + s
    v >>= 5n
  }
  return s
}
const tidUs = (t: string) => {
  let v = 0n
  for (const c of t) v = (v << 5n) | BigInt(B32.indexOf(c))
  return Number(v >> 10n)
}

const NOW = Date.now()
const START = NOW - (6 * 86400 + 4 * 3600) * 1000
const SNAP_AT = NOW - (6 * 3600 + 5 * 60) * 1000
const SHARDS = 64
const W = 709_577
const FOLLOW = 'app.bsky.graph.follow'
const BLOCK = 'app.bsky.graph.block'
const LISTITEM = 'app.bsky.graph.listitem'
const LIST = 'app.bsky.graph.list'
const COLL: Record<string, string> = { follow: FOLLOW, block: BLOCK, listitem: LISTITEM, 'listitem.owner': LISTITEM }
const isList = (x: string) => x.startsWith('at://')
const ownerOf = (l: string) => l.slice(5).split('/')[0]
export const A = 'did:plc:k3ltx7rq2a5m6uoyzwbnf4hc'

const rels = [
  { name: 'follow', collection: FOLLOW, records: 2_740_118_442, pairs: 2_736_830_301, outLists: 33_812_447, inLists: 41_203_118, nonTid: 172_412 },
  { name: 'block', collection: BLOCK, records: 227_034_918, pairs: 226_980_112, outLists: 6_104_330, inLists: 9_871_206, nonTid: 2_010 },
  { name: 'listitem', collection: LISTITEM, records: 60_833_013, pairs: 56_294_155, outLists: 1_262_107, inLists: 7_906_218, nonTid: 313_899, list: 'list' },
  { name: 'listitem.owner', collection: LISTITEM, records: 60_833_013, pairs: 1_262_107, outLists: 693_326, inLists: 1_262_107, nonTid: 313_899, ownerOf: 'listitem' },
]
const base = rels.reduce((a, r) => a + r.records + r.pairs, 0)
const DIRTY: Record<number, number> = { 3: 0.041, 9: 0.022, 17: 0.098, 22: 0.014, 30: 0.06, 41: 0.131, 47: 0.033, 55: 0.009, 60: 0.027 }

const state = {
  trace: 0,
  statuses: new Map<string, string>(),
  deleted: new Set<string>(),
  job: { kind: '', running: false, startedMs: 0n, finishedMs: 0n, error: '', done: 0, total: SHARDS + 1 },
  dirty: { ...DIRTY } as Record<number, number>,
  verify: new Map<number, string>(),
  written: SNAP_AT,
  ckpt: 212,
  segBase: 40,
  segAt: NOW,
  seq: 48_203_117,
  writes: [] as { seq: bigint; atMs: bigint; rel: string; add: boolean; actor: string; subject: string; outcome: string; uri: string }[],
}

function shardBase(k: number) {
  return Math.round(base / SHARDS) + ((hash(`b${k}`) % 2_000_000) - 1_000_000)
}
function deltaOf(k: number) {
  return Math.round(shardBase(k) * (state.dirty[k] ?? 0))
}
const deltaTotal = () => Array.from({ length: SHARDS }, (_, k) => deltaOf(k)).reduce((a, b) => a + b, 0)

function tickWrites() {
  const now = Date.now()
  const last = state.writes[0] ? Number(state.writes[0].atMs) : now - 20_000
  const r = rng(Math.floor(now / 700))
  let t = last
  while (t < now) {
    t += 300 + r() * 3900
    if (t > now) break
    const rel = r() < 0.14 ? 'block' : 'follow'
    const add = r() < 0.82
    let outcome = add ? 'added' : 'removed'
    if (r() < 0.08) outcome = add ? 'exists' : 'not_found'
    const actor = did(`a${Math.floor(r() * 1e6)}`)
    const subject = did(`s${Math.floor(r() * 1e6)}`)
    state.seq++
    state.writes.unshift({ seq: BigInt(state.seq), atMs: BigInt(Math.floor(t)), rel, add, actor, subject: outcome === 'not_found' ? '' : subject, outcome, uri: `at://${actor}/${rel === 'follow' ? FOLLOW : BLOCK}/${tid(t * 1000)}` })
  }
  state.writes = state.writes.slice(0, 256)
}

function jobTick() {
  const j = state.job
  if (!j.running) return
  const el = Date.now() - Number(j.startedMs)
  const dur = j.kind === 'checkpoint' ? 6000 : 3000
  j.done = Math.min(j.total, Math.floor((el / dur) * j.total))
  if (j.kind === 'verify') for (let k = 0; k < Math.max(0, j.done - 1); k++) state.verify.set(k, 'ok')
  if (el >= dur) {
    j.running = false
    j.finishedMs = BigInt(Date.now())
    j.done = j.total
    if (j.kind === 'checkpoint') {
      state.dirty = {}
      state.written = Date.now()
      state.ckpt++
      state.segBase = 0
      state.segAt = Date.now()
      state.verify.clear()
    } else for (let k = 0; k < SHARDS; k++) state.verify.set(k, 'ok')
  }
}

function shardState(k: number) {
  if (state.job.running && state.job.kind === 'checkpoint') return 'writing'
  return state.dirty[k] === undefined ? 'clean' : 'dirty'
}

// ---------------------------------------------------------------- series

const OPS: [string, string, number, number, number][] = [
  ['has', 'follow', 11904, 0.18e-6, 0.41e-6],
  ['has', 'block', 406, 0.16e-6, 0.38e-6],
  ['relationships', 'multi', 1842, 37e-6, 88e-6],
  ['counts', 'follow', 2600, 0.9e-6, 3.1e-6],
  ['counts', 'block', 351, 0.8e-6, 2.9e-6],
  ['out_list', 'follow', 388, 6.2e-6, 41e-6],
  ['out_list', 'block', 24, 4.1e-6, 30e-6],
  ['in_list', 'follow', 233, 9.8e-6, 120e-6],
  ['intersect', 'follow', 96, 210e-6, 2.4e-3],
  ['filter', 'multi', 641, 18e-6, 140e-6],
  ['add', 'multi', 26.1, 1.9e-6, 4.4e-6],
  ['delete', 'multi', 4.9, 2.2e-6, 4.8e-6],
]
const RPCS: [string, number, number][] = [
  ['Graph/Has', 12310, 31e-6],
  ['Graph/Relationships', 1842, 140e-6],
  ['Graph/Counts', 2951, 44e-6],
  ['Graph/ListOut', 412, 96e-6],
  ['Graph/ListIn', 233, 210e-6],
  ['Graph/Intersect', 96, 2.6e-3],
  ['Graph/Filter', 641, 190e-6],
  ['Graph/WriteBatch', 0.4, 1.4e-3],
  ['Graph/Write', 2, 0],
]
const OUT: [string, string, number][] = [
  ['follow', 'added', 20.2],
  ['follow', 'removed', 3.1],
  ['follow', 'deleted', 0.5],
  ['follow', 'exists', 1.1],
  ['follow', 'not_found', 0.4],
  ['block', 'added', 1.8],
  ['block', 'removed', 0.5],
  ['block', 'deleted', 0.1],
  ['block', 'exists', 0.2],
  ['block', 'not_found', 0.1],
]

function walk(seed: string, nPts: number, base: number, vol: number, t0: number) {
  const r = rng(hash(seed) ^ Math.floor(t0 / 5000))
  let v = base
  const out: number[] = []
  for (let i = 0; i < nPts; i++) {
    v = Math.max(0, v * (1 + (r() * 2 - 1) * vol))
    v = v * 0.9 + base * 0.1
    out.push(v)
  }
  return out
}

function getSeries(windowSeconds: number) {
  const step = 5000 * Math.max(1, Math.ceil(windowSeconds / 5 / 180))
  const pts = Math.min(180, Math.ceil((windowSeconds * 1000) / step))
  const end = Math.floor(Date.now() / step) * step
  const tMs = Array.from({ length: pts }, (_, i) => BigInt(end - (pts - 1 - i) * step))
  const series: { metric: string; op?: string; rel?: string; outcome?: string; dir?: string; rpc?: string; values: number[] }[] = []
  for (const [op, rel, rt, p50, p99] of OPS) {
    series.push({ metric: 'op_rate', op, rel, values: walk(`r${op}${rel}`, pts, rt, 0.07, end) })
    series.push({ metric: 'op_p50', op, rel, values: walk(`p5${op}${rel}`, pts, p50, 0.05, end) })
    series.push({ metric: 'op_p99', op, rel, values: walk(`p9${op}${rel}`, pts, p99, 0.06, end) })
  }
  for (const [rpc, rt, p99] of RPCS) {
    series.push({ metric: 'rpc_rate', rpc, values: walk(`rr${rpc}`, pts, rt, 0.07, end) })
    series.push({ metric: 'rpc_p99', rpc, values: walk(`rp${rpc}`, pts, p99, 0.06, end) })
  }
  series.push({ metric: 'rpc_errors', rpc: 'Graph/Has', values: walk('err', pts, 0.02, 0.3, end) })
  for (const [rel, outcome, rt] of OUT) {
    const v = walk(`ap${rel}${outcome}`, pts, rt, 0.25, end)
    if (outcome === 'added') for (let i = Math.floor(pts * 0.63); i < Math.floor(pts * 0.7); i++) v[i] *= 1.9
    series.push({ metric: 'applied', rel, outcome, values: v })
  }
  for (const rel of ['follow', 'block'])
    for (const dir of ['out', 'in']) series.push({ metric: 'merges', rel, dir, values: walk(`m${rel}${dir}`, pts, rel === 'follow' ? (dir === 'out' ? 0.05 : 0.027) : 0.004, 0.15, end) })
  series.push({ metric: 'merge_p99', dir: 'out', values: walk('mpo', pts, 6.1e-3, 0.08, end) })
  series.push({ metric: 'merge_p99', dir: 'in', values: walk('mpi', pts, 38e-3, 0.08, end) })
  series.push({ metric: 'rss_bytes', values: walk('rss', pts, 14.2e9, 0.01, end) })
  series.push({ metric: 'cpu_cores', values: walk('cpu', pts, 3.1, 0.1, end) })
  const dt = deltaTotal()
  series.push({ metric: 'delta_entries', values: walk('de', pts, dt, 0.02, end).map((x, i) => x * (0.7 + (0.3 * i) / pts)) })
  series.push({ metric: 'tombstones', values: walk('tb', pts, 1204, 0.1, end) })
  return { stepMs: BigInt(step), tMs, series }
}

// ---------------------------------------------------------------- explorer

const COUNTS: Record<string, [number, number, number]> = { follow: [1204, 88312, 931], block: [37, 412, 0], listitem: [0, 214, 0], 'listitem.owner': [57, 0, 0] }
function countsOf(d: string, rel: string): [number, number, number] {
  if (isList(d)) return rel === 'listitem' ? [(hash(d) % 900) + 12, 0, 0] : [0, 0, 0]
  if (d === A) return COUNTS[rel] ?? [0, 0, 0]
  const h = hash(d + rel)
  if (rel === 'listitem') return [0, h % 30, 0]
  if (rel === 'listitem.owner') return [h % 4 ? 0 : h % 200, 0, 0]
  return rel === 'follow' ? [(h % 3000) + 3, (h >> 8) % 20000, (h >> 4) % 600] : [h % 40, (h >> 8) % 300, 0]
}
const idOf = (d: string) => hash(`id${d}`) % 45_391_076
function edgesOf(actor: string, rel: string, dir: Dir, cursor: string, limit: number) {
  const [out, inn] = countsOf(actor, rel)
  const total = dir === Dir.OUT ? out : inn
  const start = cursor ? Number(cursor.split('.')[1] ?? 0) : 0
  const r = rng(hash(actor + rel + dir))
  let t = NOW * 1000 - 3 * 3600e6 - 4 * 60e6
  const edges = []
  for (let i = 0; i < Math.min(total, start + limit); i++) {
    const rk = tid(t, Math.floor(r() * 1024))
    // listitem in: the lists naming the actor; listitem.owner out: the owner's lists
    const listy = (rel === 'listitem' && dir === Dir.IN) || (rel === 'listitem.owner' && dir === Dir.OUT)
    const other = listy ? `at://${dir === Dir.OUT ? actor : did(`${actor}o${i % 7}`)}/${LIST}/${tid(t - 9e9, i % 5)}` : did(`${actor}${rel}${dir}${i}`)
    const owner = isList(actor) ? ownerOf(actor) : actor
    const recOwner = dir === Dir.OUT ? owner : isList(other) ? ownerOf(other) : other
    if (i >= start)
      edges.push({ did: other, id: idOf(other), rkey: rk, uri: `at://${recOwner}/${COLL[rel] ?? FOLLOW}/${rk}`, createdUs: BigInt(Math.floor(t)), stored: i < 2 && dir === Dir.OUT ? 'delta' : 'base' })
    t -= (5 + r() * 295) * 3600e6
  }
  const next = start + limit < total ? `${edges[edges.length - 1]?.rkey}.${start + limit}` : ''
  return { edges: edges.filter((e) => !state.deleted.has(e.uri)), cursor: next, total: BigInt(total) }
}

function resolve(uri: string) {
  const m = uri.match(/^at:\/\/([^/]+)\/([^/]+)\/(.+)$/)
  if (!m) throw new ConnectError(`"${uri}" isn't a record URI`, Code.InvalidArgument)
  const [, actor, coll, rkey] = m
  const rel = coll === FOLLOW ? 'follow' : coll === BLOCK ? 'block' : coll === LISTITEM ? 'listitem' : ''
  if (!rel) throw new ConnectError(`no relation for ${coll}`, Code.InvalidArgument)
  const gone = state.deleted.has(uri)
  const list = rel === 'listitem' ? `at://${actor}/${LIST}/${tid(NOW * 1000 - 9e10, 3)}` : ''
  const page = edgesOf(list || actor, rel, Dir.OUT, '', 8)
  const pos = page.edges.findIndex((e) => e.rkey === rkey)
  const subject = gone ? '' : (page.edges[pos]?.did ?? did(`subj${uri}`))
  const isTid = /^[234567a-j][234567a-z]{12}$/.test(rkey)
  return {
    rel,
    collection: coll,
    actor,
    actorId: idOf(actor),
    subject,
    subjectId: subject ? idOf(subject) : 0,
    rkey,
    tid: isTid,
    createdUs: isTid ? BigInt(tidUs(rkey)) : 0n,
    stored: pos >= 0 && pos < 2 ? 'delta' : 'base',
    position: BigInt(pos >= 0 ? pos + 1 : 3),
    outCount: BigInt(countsOf(actor, rel)[0]),
    shard: Math.floor(idOf(actor) / W),
    tombstoned: gone,
    pairRecords: 1n,
    inContains: !gone,
    subjectInCount: 9002n,
    subjectShard: subject ? Math.floor(idOf(subject) / W) : 0,
    subjectInStored: 'base',
    list,
  }
}

// ---------------------------------------------------------------- the transport

const SECTIONS: [string, string, number][] = [
  ['r0.out.dat', 'follow', 24.2e9],
  ['r0.out.idx', 'follow', 1.12e9],
  ['r0.in.dat', 'follow', 8.33e9],
  ['r0.in.idx', 'follow', 0.71e9],
  ['r0.side', 'follow', 6.1e6],
  ['r1.out.dat', 'block', 1.86e9],
  ['r1.out.idx', 'block', 0.11e9],
  ['r1.in.dat', 'block', 0.66e9],
  ['r1.in.idx', 'block', 0.09e9],
  ['r1.side', 'block', 0.1e6],
  ['r2.out.dat', 'listitem', 0.55e9],
  ['r2.out.idx', 'listitem', 0.05e9],
  ['r2.in.dat', 'listitem', 0.14e9],
  ['r2.in.idx', 'listitem', 0.03e9],
  ['r3.out.dat', 'listitem.owner', 0.5e9],
  ['r3.in.dat', 'listitem.owner', 0.01e9],
  ['dir.plc', 'directory', 0.92e9],
  ['dir.byid', 'directory', 0.36e9],
  ['dir.plc.ids', 'directory', 0.12e9],
  ['dir.oth', 'directory', 0.02e9],
]
const snapBytes = SECTIONS.reduce((a, s) => a + s[2], 0)

const FO = [2.1e6, 3.0e6, 3.8e6, 4.4e6, 4.6e6, 4.3e6, 3.7e6, 2.9e6, 2.0e6, 1.2e6, 0.6e6, 0.21e6, 52e3, 11e3, 2.1e3, 410, 80, 14, 3, 1]
const FI = [6.0e6, 6.5e6, 6.6e6, 6.2e6, 5.4e6, 4.3e6, 3.1e6, 2.0e6, 1.2e6, 0.7e6, 0.4e6, 0.21e6, 98e3, 41e3, 15e3, 5.2e3, 1.6e3, 420, 90, 12]

function durability() {
  const now = Date.now()
  const segs = state.segBase + Math.floor((now - state.segAt) / 5000)
  const segMs = Math.floor(now / 5000) * 5000
  return {
    segmentIntervalSeconds: 5,
    segment: BigInt(Math.floor(segMs / 5000) % 100_000),
    segmentMs: BigInt(segMs),
    segmentWriteSeconds: 0.004,
    segments: segs,
    segmentBytes: BigInt(segs * 1800),
    segmentOps: BigInt(segs * 150),
    pendingOps: BigInt(Math.floor(((now - segMs) / 1000) * 30)),
    checkpoint: BigInt(state.ckpt),
    checkpointMs: BigInt(state.written),
    checkpointOps: 3_000_000n,
    checkpointAgeSeconds: 86_400,
    checkpointSeconds: 40,
    checkpointBytes: 38_000_000_000n,
    checkpointShards: SHARDS,
    replayedSegments: 12,
    replayedOps: 1_800n,
    replaySeconds: 0.06,
  }
}

function sources() {
  const now = Date.now()
  const cur = (ago: number) => String((now - ago) * 1000)
  return [
    { name: 'jetstream', url: 'wss://jetstream.example/subscribe', state: 'connected', cursor: cur(300), cursorKind: 'time_us', lagSeconds: 0.3, opsPerSecond: 30.4, errors: 0n, reconnects: 2n, durableCursor: cur(5300), expected: true, caughtUp: true, lastMs: BigInt(now - 300), ops: 17_204_118n },
    { name: 'api', url: '', state: 'serving', cursor: String(state.seq), cursorKind: 'seq', lagSeconds: 0, opsPerSecond: 0.2, errors: 0n, reconnects: 0n, durableCursor: String(state.seq - 3), expected: false, caughtUp: true, lastMs: BigInt(now - 4000), ops: 1_204n },
    { name: 'admin', url: '', state: 'idle', cursor: '', cursorKind: 'seq', lagSeconds: 0, opsPerSecond: 0, errors: 0n, reconnects: 0n, durableCursor: '', expected: false, caughtUp: true, lastMs: 0n, ops: 3n },
  ]
}

function overview() {
  jobTick()
  tickWrites()
  const dirty = Object.keys(state.dirty).length
  return {
    version: '0.1.0',
    commit: '3f9c2a1',
    startedMs: BigInt(START),
    readOnly: false,
    snapshot: { path: '/data/marco/snap', build: 'b7e41c09d2a6f310', format: 2, shards: SHARDS, sizeBytes: BigInt(Math.round(snapBytes)), writtenMs: BigInt(state.written), openSeconds: 0.018, subjIndex: 4096, files: SHARDS + 1 },
    directory: { base: 45_391_076n, live: 21_804n },
    relations: rels.map((r) => ({
      name: r.name,
      collection: r.collection,
      subject: 'subject',
      records: BigInt(r.records + (r.name === 'follow' ? 412_331 : 61_208)),
      snapshotRecords: BigInt(r.records),
      snapshotPairs: BigInt(r.pairs),
      outLists: BigInt(r.outLists),
      inLists: BigInt(r.inLists),
      nonTid: BigInt(r.nonTid),
      deltaOut: BigInt(Math.round(deltaTotal() * (r.name === 'follow' ? 0.45 : 0.04))),
      deltaIn: BigInt(Math.round(deltaTotal() * (r.name === 'follow' ? 0.47 : 0.04))),
      tombstones: BigInt(r.name === 'follow' ? 1118 : 86),
      mergedBytes: BigInt(r.name === 'follow' ? 172e6 : 14e6),
    })),
    process: { rssBytes: 14_200_000_000n, memTotalBytes: 64_000_000_000n, cpuSeconds: 68_211, threads: 41, snapshotCachedBytes: 31_000_000_000n },
    traceRatio: state.trace,
    job: { ...state.job },
    mergeRatio: 0.125,
    dirtyShards: state.job.running && state.job.kind === 'checkpoint' ? 0 : dirty,
    overShards: 0,
    durability: durability(),
    ready: true,
    readyReason: '',
    readsBeforeReady: 0n,
    deltaEntries: BigInt(deltaTotal()),
    baseEntries: BigInt(base),
    tombstones: 1204n,
    mergedBytes: 186_000_000n,
    mergedLists: 1912,
    writeCursor: '1793781726704112',
    writeSeq: BigInt(state.seq),
    apiListen: '0.0.0.0:7300',
    adminListen: '127.0.0.1:7301',
  }
}

const FLAGS: [string, string, string][] = [
  ['--listen', '0.0.0.0:7300', 'default'],
  ['--admin-listen', '127.0.0.1:7301', 'default'],
  ['--snapshot', '/data/marco/snap', 'flag'],
  ['--admin-token-file', '/etc/marco/admin-token', 'flag'],
  ['--admin-proxy-header', '—', 'unset'],
  ['--admin-proxy-from', '—', 'unset'],
  ['--admin-operators', '—', 'unset'],
  ['--proxy-protocol-from', '—', 'unset'],
  ['--admin-proxy-peers', '—', 'unset'],
  ['--merge-ratio', '0.125', 'default'],
  ['--compact-min', '64', 'default'],
  ['--trace-ratio', '0', 'default'],
  ['--otlp', '—', 'unset'],
  ['--read-only', 'false', 'default'],
  ['--ui-dir', '/usr/share/marco/ui', 'env'],
]

export function mockTransport() {
  return createRouterTransport(({ service }) => {
    service(Admin, {
      getSession: () => ({ auth: 'token' }),
      getOverview: () => overview(),
      getSeries: (r) => getSeries(r.windowSeconds || 3600),
      listRecentWrites: (r) => {
        tickWrites()
        return { writes: state.writes.filter((w) => w.seq > r.after).slice(0, r.limit || 50), lastSeq: BigInt(state.seq) }
      },
      listShards: () => {
        jobTick()
        return {
          mergeRatio: 0.125,
          job: { ...state.job },
          directoryBytes: 1_420_000_000n,
          directoryVerify: state.verify.size === SHARDS ? 'ok' : state.job.running && state.job.kind === 'verify' ? 'verifying' : 'unverified',
          shards: Array.from({ length: SHARDS }, (_, k) => {
            const b = shardBase(k)
            const st = shardState(k)
            const v = state.verify.get(k) ?? (state.job.running && state.job.kind === 'verify' ? 'verifying' : 'unverified')
            return {
              shard: k,
              lo: k * W,
              hi: (k + 1) * W,
              sizeBytes: BigInt(Math.round(snapBytes / SHARDS + (hash(`z${k}`) % 30_000_000))),
              writtenMs: BigInt(state.dirty[k] !== undefined || k === 12 ? state.written : state.written - 27 * 3600e3),
              baseEntries: BigInt(b),
              deltaEntries: BigInt(deltaOf(k)),
              deltaLists: Math.round(deltaOf(k) / 40),
              mergedLists: st === 'clean' ? 0 : 30,
              mergedBytes: BigInt(st === 'clean' ? 0 : 2_900_000),
              tombstones: st === 'clean' ? 0 : 19,
              file: `shard-${String(k).padStart(3, '0')}.shd`,
              checkpoint: BigInt(state.ckpt - (state.dirty[k] === undefined && k !== 12 ? 3 : 0)),
              state: st,
              verify: v,
            }
          }),
        }
      },
      getStorage: (r) => {
        const dl = [
          [did('carol'), 'follow', Dir.IN, 6_012_334, 412_900, 21_044],
          [did('erin'), 'follow', Dir.IN, 1_820_112, 118_220, 4_002],
          [did('victor'), 'follow', Dir.OUT, 276_497, 30_102, 812],
          [did('walter'), 'follow', Dir.IN, 904_221, 98_114, 2_300],
          [did('heidi'), 'block', Dir.IN, 512_880, 51_004, 77],
          [did('bob'), 'follow', Dir.OUT, 61_022, 6_610, 140],
        ] as const
        const rows = dl.map(([d, rel, dir, b, a, x]) => ({ did: d, rel, dir, base: BigInt(b), adds: BigInt(a), dels: BigInt(x) }))
        if (r.by === 'size') rows.sort((p, q) => Number(q.adds + q.dels - (p.adds + p.dels)))
        else rows.sort((p, q) => Number(q.adds + q.dels) / Number(q.base) - Number(p.adds + p.dels) / Number(p.base))
        return { sections: SECTIONS.map(([name, rel, b]) => ({ name, rel, sizeBytes: BigInt(Math.round(b)), files: rel === 'directory' ? 1 : SHARDS })), deltas: rows, deltaLists: 61_220 }
      },
      listSources: () => ({ sources: sources() }),
      listRelations: () => ({
        config: rels
          .filter((r) => !r.ownerOf)
          .map((r) => `[[relation]]\nname = "${r.name}"\ncollection = "${r.collection}"\nsubject = "subject"${r.list ? `\nlist = "${r.list}"` : ''}`)
          .join('\n\n'),
        relations: rels.map((r) => ({ name: r.name, collection: r.collection, subject: r.ownerOf ? 'list' : 'subject', records: BigInt(r.records), pairs: BigInt(r.pairs), duplicates: BigInt(r.records - r.pairs), nonTid: BigInt(r.nonTid), list: r.list ?? '', ownerOf: r.ownerOf ?? '' })),
      }),
      getListStats: (r) => {
        const k = r.rel === 'block' ? 0.08 : 1
        const d = (xs: number[], p: number[], over: number) => ({ log2Counts: xs.map((x) => BigInt(Math.round(x * k))), lists: BigInt(Math.round(xs.reduce((a, b) => a + b, 0) * k)), p50: BigInt(p[0]), p90: BigInt(p[1]), p99: BigInt(p[2]), p999: BigInt(p[3]), max: BigInt(p[4]), overThreshold: BigInt(Math.round(over * k)) })
        return {
          ready: true,
          progress: 1,
          computedMs: BigInt(START + 60_000),
          threshold: 4096,
          out: d(FO, [5, 97, 1011, 5774, 942_118], 31_880),
          in: d(FI, [7, 160, 2210, 14_002, 6_012_334], 120_331),
          big: [
            [did('carol'), Dir.IN, 6_012_334],
            [did('erin'), Dir.IN, 1_820_112],
            [did('victor'), Dir.OUT, 942_118],
            [did('walter'), Dir.IN, 904_221],
            [did('rupert'), Dir.OUT, 276_497],
            [did('bob'), Dir.OUT, 61_022],
          ].map(([d, dir, len]) => ({ did: d as string, dir: dir as Dir, length: BigInt(len as number) })),
        }
      },
      getActor: (r) => {
        if (!/^did:/.test(r.actor) && !isList(r.actor)) throw new ConnectError(`"${r.actor}" isn't a DID or a list's AT URI`, Code.InvalidArgument)
        const id = isList(r.actor) ? 45_391_076 + (hash(r.actor) % 1_262_107) : idOf(r.actor)
        return {
          did: r.actor,
          known: true,
          id,
          list: isList(r.actor),
          shard: Math.floor(id / W),
          status: state.statuses.get(isList(r.actor) ? ownerOf(r.actor) : r.actor) ?? 'active',
          rels: rels.map((x) => {
            const [o, i, m] = countsOf(r.actor, x.name)
            const firstUs = (x.name === 'follow' && !isList(r.actor)) || (x.name === 'listitem' && isList(r.actor)) ? BigInt(Date.UTC(2023, 3, 29) * 1000) : 0n
            return { rel: x.name, out: BigInt(o), in: BigInt(i), mutual: BigInt(m), firstUs, outStored: 'delta', inStored: 'base', outSubjects: BigInt(x.name === 'listitem' ? Math.round(o * 0.93) : o) }
          }),
        }
      },
      listEdges: (r) => edgesOf(r.actor, r.rel, r.dir, r.cursor, r.limit || 50),
      resolveUri: (r) => resolve(r.uri),
      getSettings: () => ({ flags: FLAGS.map(([name, value, source]) => ({ name, value, source })), build: { version: '0.1.0', commit: '3f9c2a1', format: 2, features: ['metrics', 'jemalloc', 'cpu-profile'] } }),
      checkpoint: () => {
        if (state.job.running) throw new ConnectError('a job is running', Code.FailedPrecondition)
        state.job = { kind: 'checkpoint', running: true, startedMs: BigInt(Date.now()), finishedMs: 0n, error: '', done: 0, total: SHARDS + 1 }
        return { job: { ...state.job } }
      },
      verifyShards: () => {
        if (state.job.running) throw new ConnectError('a job is running', Code.FailedPrecondition)
        state.verify.clear()
        state.job = { kind: 'verify', running: true, startedMs: BigInt(Date.now()), finishedMs: 0n, error: '', done: 0, total: SHARDS + 1 }
        return { job: { ...state.job } }
      },
      setTraceRatio: (r) => {
        state.trace = r.ratio
        return { ratio: r.ratio }
      },
      setActorStatus: (r) => {
        if (r.status === 'active') state.statuses.delete(r.did)
        else state.statuses.set(r.did, r.status)
        return { did: r.did, status: r.status, removed: 0n }
      },
    })
    service(Graph, {
      has: (r) => ({ uri: hash(r.from + r.to) % 3 ? `at://${r.from}/${FOLLOW}/${tid(NOW * 1000 - 9e9)}` : '' }),
      relationships: (r) => ({
        relationships: r.others.map((o, j) => ({
          other: o,
          rels: (r.rels.length ? r.rels : ['follow', 'block']).map((rel) => ({
            rel,
            outUri: rel === 'follow' ? `at://${r.actor}/${FOLLOW}/${tid(NOW * 1000 - (j + 3) * 1e10)}` : '',
            inUri: rel === 'follow' ? `at://${o}/${FOLLOW}/${tid(NOW * 1000 - 4e11)}` : '',
          })),
        })),
      }),
      listOut: () => ({ edges: [], cursor: '' }),
      listIn: () => ({ actors: [], cursor: '' }),
      counts: (r) => ({ counts: r.actors.map((a) => ({ actor: a, rels: rels.map((x) => ({ rel: x.name, outRecords: BigInt(countsOf(a, x.name)[0]), inActors: BigInt(countsOf(a, x.name)[1]), outSubjects: BigInt(countsOf(a, x.name)[0]) })) })) }),
      intersect: (r) => {
        const n = 41
        const len = (x?: { rel: string; actor: string; dir: Dir }) => BigInt(Math.abs(x ? countsOf(x.actor, x.rel)[x.dir === Dir.IN ? 1 : 0] : 0))
        return { actors: Array.from({ length: n }, (_, i) => did(`x${r.a?.actor}${r.b?.actor}${r.op}${i}`)), aLen: len(r.a), bLen: len(r.b), cursor: '' }
      },
      filter: (r) => {
        const drop = r.actors.filter((a) => hash(a + r.viewer) % 6 === 0).slice(0, 2)
        const d = r.drop[0]
        const rel = d?.memberOf?.rel || d?.rel || 'block'
        return { actors: r.actors.filter((a) => !drop.includes(a)), dropped: drop.map((a) => ({ actor: a, rel, dir: d?.memberOf?.dir ?? d?.dir ?? Dir.OUT })) }
      },
      write: async () => ({ results: [], cursor: '' }),
      writeBatch: (r) => ({
        results: r.ops.map((op) => {
          if (op.op.case === 'delete') {
            state.deleted.add(op.op.value.uri)
            return { outcome: { case: 'removed' as const, value: did(`subj${op.op.value.uri}`) } }
          }
          return { outcome: { case: 'added' as const, value: true } }
        }),
        cursor: r.cursor,
      }),
    })
  })
}

export async function mockFetch(path: string): Promise<Response> {
  await new Promise((r) => setTimeout(r, 600))
  if (path.startsWith('/debug/pprof/')) {
    const b = new Uint8Array(path.includes('heap') ? 1_900_000 : 412_000)
    return new Response(new Blob([b]), { status: 200 })
  }
  return new Response('not in the mock', { status: 404 })
}
