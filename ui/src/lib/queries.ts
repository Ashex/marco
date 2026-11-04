import { admin, graph } from './api'
import { invalidate, useLive } from './query'
import type { GetSeriesResponse, Series } from '../gen/marco/v1/admin_pb'
import { Dir } from '../gen/marco/v1/graph_pb'

// Every query the console makes, on structured keys, and the writes with what each
// invalidates. Pages never call a write on the client directly.

export const keys = {
  session: ['session'],
  overview: ['overview'],
  series: (w: number) => ['series', w],
  recent: ['recent'],
  shards: ['shards'],
  storage: (by: string) => ['storage', by],
  relations: ['relations'],
  listStats: (rel: string) => ['liststats', rel],
  settings: ['settings'],
  sources: ['sources'],
  actor: (did: string) => ['actor', did],
  edges: (did: string, rel: string, dir: Dir, cursor: string) => ['edges', did, rel, dir, cursor],
  uri: (u: string) => ['uri', u],
}

export const useSession = () => useLive(keys.session, () => admin().getSession({}))
export const useOverview = () => useLive(keys.overview, () => admin().getOverview({}), { poll: 2000, heartbeat: true })
export const useSeries = (window: number) => useLive(keys.series(window), () => admin().getSeries({ windowSeconds: window }), { poll: window <= 180 ? 5000 : 15000 })
export const useRecent = () => useLive(keys.recent, () => admin().listRecentWrites({ limit: 40 }), { poll: 1000 })
export const useShards = () => useLive(keys.shards, () => admin().listShards({}), { poll: (d) => (d?.job?.running ? 1000 : 4000) })
export const useStorage = (by: string) => useLive(keys.storage(by), () => admin().getStorage({ by }), { poll: 5000 })
export const useRelations = () => useLive(keys.relations, () => admin().listRelations({}))
export const useListStats = (rel: string) => useLive(keys.listStats(rel), () => admin().getListStats({ rel }), { poll: (d) => (d?.ready ? false : 1000), enabled: !!rel })
export const useSettings = () => useLive(keys.settings, () => admin().getSettings({}))
export const useActor = (did: string) => useLive(keys.actor(did), () => admin().getActor({ actor: did }), { enabled: !!did, poll: 10000 })
export const useEdges = (did: string, rel: string, dir: Dir, cursor: string, limit = 8) =>
  useLive(keys.edges(did, rel, dir, cursor), () => admin().listEdges({ actor: did, rel, dir, cursor, limit }), { enabled: !!did && !!rel })
export const useResolve = (u: string) => useLive(keys.uri(u), () => admin().resolveUri({ uri: u }), { enabled: !!u })

export const useSources = () => useLive(keys.sources, () => admin().listSources({}), { poll: 2000 })

// ---------------------------------------------------------------- series

export type SeriesPick = Partial<Record<'metric' | 'op' | 'rel' | 'outcome' | 'dir' | 'rpc', string>>

export function series(s: GetSeriesResponse | undefined, p: SeriesPick): Series[] {
  if (!s) return []
  return s.series.filter((x) => (Object.keys(p) as (keyof SeriesPick)[]).every((k) => p[k] === undefined || x[k] === p[k]))
}

/** The pointwise sum of every series matching `p`. */
export function sum(s: GetSeriesResponse | undefined, p: SeriesPick): number[] {
  const m = series(s, p)
  if (!s || !m.length) return []
  return s.tMs.map((_, i) => m.reduce((a, x) => a + (x.values[i] ?? 0), 0))
}

/** The last value of the sum. */
export const last = (v: number[]) => (v.length ? v[v.length - 1] : NaN)
export const mean = (v: number[]) => (v.length ? v.reduce((a, b) => a + b, 0) / v.length : NaN)

/** A latency series weighted by its op's rate (several relations or ops into one line). */
export function weighted(s: GetSeriesResponse | undefined, metric: 'op_p50' | 'op_p99', p: SeriesPick): number[] {
  const lats = series(s, { ...p, metric })
  if (!s || !lats.length) return []
  return s.tMs.map((_, i) => {
    let w = 0
    let v = 0
    for (const l of lats) {
      const r = series(s, { metric: 'op_rate', op: l.op, rel: l.rel })[0]?.values[i] ?? 0
      if (r > 0 && l.values[i] > 0) {
        w += r
        v += r * l.values[i]
      }
    }
    return w ? v / w : 0
  })
}

export const READ_OPS = ['has', 'relationships', 'counts', 'out_list', 'in_list', 'intersect', 'filter']
export const WRITE_OPS = ['add', 'delete']

// ---------------------------------------------------------------- writes

export async function checkpoint() {
  const r = await admin().checkpoint({})
  await invalidate(keys.shards, keys.overview)
  return r
}
export async function verifyShards() {
  const r = await admin().verifyShards({})
  await invalidate(keys.shards, keys.overview)
  return r
}
export async function setTraceRatio(ratio: number) {
  const r = await admin().setTraceRatio({ ratio })
  await invalidate(keys.overview)
  return r
}
export async function setActorStatus(did: string, status: string) {
  const r = await admin().setActorStatus({ did, status })
  await invalidate(keys.actor(did), keys.overview, ['edges'], ['uri'])
  return r
}
export async function deleteEdge(uri: string) {
  const r = await graph().writeBatch({ ops: [{ op: { case: 'delete', value: { uri } } }] })
  await invalidate(keys.overview, ['actor'], ['edges'], ['uri'], keys.recent, keys.shards)
  return r
}

export const dirName = (d: Dir) => (d === Dir.OUT ? 'out' : d === Dir.IN ? 'in' : '')
