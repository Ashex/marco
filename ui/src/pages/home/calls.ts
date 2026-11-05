// The explorer's calls: marco.v1.Graph's reads only. Each has flat string fields and builds its
// Connect JSON body from them; actor fields take a DID, a list's AT URI or a handle (resolved
// through the server's /resolve before the call).

const BSKY = 'did:plc:z72i7hdynmk6r22z27h6tvur' // bsky.app
const JAY = 'did:plc:oky5czdrnfjpqslsw2a5iclo' // jay.bsky.team
const BSKY_FOLLOW = 'did:plc:lysqukqdu6hsrhet5v2brjgo' // one of bsky.app's follows

/** The defaults' handles, shown beside their DIDs. */
export const NAMES: Record<string, string> = { [BSKY]: 'bsky.app', [JAY]: 'jay.bsky.team' }

export type FieldKind = 'actor' | 'actors' | 'pairs' | 'rel' | 'rels' | 'int' | 'cursor' | 'bool' | 'dir' | 'op' | 'list'

export type Field = {
  k: string
  label: string
  kind: FieldKind
  tip?: string
  /** Groups fields under a small heading (Intersect's sides). */
  group?: string
  placeholder?: string
}

export type Values = Record<string, string>

export type Call = {
  name: string
  hint: string
  tip: string
  fields: Field[]
  defaults: Values
  presets?: { label: string; values: Values }[]
  build: (v: Values, did: (s: string) => string) => unknown
}

export const RELS = ['follow', 'block', 'listitem', 'listitem.owner']

export const lines = (s: string) =>
  s
    .split(/[\s,]+/)
    .map((x) => x.trim())
    .filter(Boolean)

const int = (s: string) => (s.trim() ? Number(s) : undefined)
const opt = <T,>(x: T | undefined | '' | false) => (x === '' || x === false || x === undefined ? undefined : x)
const list = <T,>(xs: T[]) => (xs.length ? xs : undefined)
const dir = (s: string) => (s === 'in' ? 'DIR_IN' : 'DIR_OUT')

export const CALLS: Call[] = [
  {
    name: 'Has',
    hint: 'a → b',
    tip: 'The newest `rel` record of `from` naming `to`: its AT URI, or empty',
    fields: [
      { k: 'rel', label: 'rel', kind: 'rel' },
      { k: 'from', label: 'from', kind: 'actor' },
      { k: 'to', label: 'to', kind: 'actor' },
    ],
    defaults: { rel: 'follow', from: JAY, to: BSKY },
    build: (v, did) => ({ rel: v.rel, from: did(v.from), to: did(v.to) }),
  },
  {
    name: 'Relationships',
    hint: '1 × n',
    tip: 'Per other and relation: the actor’s record naming it and its record naming the actor. At most 100 others',
    fields: [
      { k: 'actor', label: 'actor', kind: 'actor' },
      { k: 'others', label: 'others', kind: 'actors', tip: 'At most 100' },
      { k: 'rels', label: 'rels', kind: 'rels', placeholder: 'all' },
    ],
    defaults: { actor: BSKY, others: `${JAY}\n${BSKY_FOLLOW}`, rels: '' },
    build: (v, did) => ({ actor: did(v.actor), others: lines(v.others).map(did), rels: list(lines(v.rels)) }),
  },
  {
    name: 'BlockPairs',
    hint: 'n pairs',
    tip: 'Per pair: whether a blocks b and whether b blocks a. At most 1,000 pairs',
    fields: [
      { k: 'pairs', label: 'pairs', kind: 'pairs', tip: 'One pair per line: a b' },
      { k: 'rels', label: 'rels', kind: 'rels', placeholder: 'block' },
    ],
    defaults: { pairs: `${BSKY} ${JAY}\n${JAY} ${BSKY_FOLLOW}`, rels: '' },
    build: (v, did) => ({
      pairs: v.pairs
        .split('\n')
        .map(lines)
        .filter((p) => p.length)
        .map(([a, b]) => ({ a: did(a ?? ''), b: did(b ?? '') })),
      rels: list(lines(v.rels)),
    }),
  },
  {
    name: 'ListOut',
    hint: 'out →',
    tip: 'An actor’s records, newest first: its follows, or a list’s members',
    fields: [
      { k: 'rel', label: 'rel', kind: 'rel' },
      { k: 'actor', label: 'actor', kind: 'actor', tip: 'A DID, a handle, or a list’s AT URI' },
      { k: 'limit', label: 'limit', kind: 'int', tip: 'Default 50, at most 1,000' },
      { k: 'cursor', label: 'cursor', kind: 'cursor' },
      { k: 'distinct', label: 'distinct', kind: 'bool', tip: 'One record per subject, its oldest' },
    ],
    defaults: { rel: 'follow', actor: BSKY, limit: '25', cursor: '', distinct: '' },
    build: (v, did) => ({ rel: v.rel, actor: did(v.actor), limit: int(v.limit), cursor: opt(v.cursor), distinct: opt(v.distinct === '1') }),
  },
  {
    name: 'ListIn',
    hint: '← in',
    tip: 'The actors naming a subject, in id order: its followers',
    fields: [
      { k: 'rel', label: 'rel', kind: 'rel' },
      { k: 'subject', label: 'subject', kind: 'actor' },
      { k: 'limit', label: 'limit', kind: 'int', tip: 'Default 50, at most 1,000' },
      { k: 'cursor', label: 'cursor', kind: 'cursor' },
    ],
    defaults: { rel: 'follow', subject: BSKY, limit: '25', cursor: '' },
    build: (v, did) => ({ rel: v.rel, subject: did(v.subject), limit: int(v.limit), cursor: opt(v.cursor) }),
  },
  {
    name: 'Counts',
    hint: 'totals',
    tip: 'Per actor and relation: out records, in actors and distinct out subjects. At most 100 actors',
    fields: [
      { k: 'actors', label: 'actors', kind: 'actors', tip: 'At most 100' },
      { k: 'rels', label: 'rels', kind: 'rels', placeholder: 'all' },
    ],
    defaults: { actors: `${BSKY}\n${JAY}`, rels: '' },
    build: (v, did) => ({ actors: lines(v.actors).map(did), rels: list(lines(v.rels)) }),
  },
  {
    name: 'Intersect',
    hint: 'a ∩ b',
    tip: 'The actors in both sides, or in a and not b. A side can be a list',
    fields: [
      { k: 'aRel', label: 'rel', kind: 'rel', group: 'a' },
      { k: 'aDir', label: 'dir', kind: 'dir', group: 'a' },
      { k: 'aActor', label: 'actor', kind: 'actor', group: 'a' },
      { k: 'bRel', label: 'rel', kind: 'rel', group: 'b' },
      { k: 'bDir', label: 'dir', kind: 'dir', group: 'b' },
      { k: 'bActor', label: 'actor', kind: 'actor', group: 'b' },
      { k: 'op', label: 'op', kind: 'op' },
      { k: 'limit', label: 'limit', kind: 'int', tip: 'Default 100, at most 10,000' },
      { k: 'count', label: 'count', kind: 'bool', tip: 'Also count the whole result' },
      { k: 'cursor', label: 'cursor', kind: 'cursor' },
    ],
    defaults: { aRel: 'follow', aDir: 'out', aActor: BSKY, bRel: 'follow', bDir: 'in', bActor: JAY, op: 'and', limit: '10', count: '1', cursor: '' },
    presets: [
      { label: 'Known followers', values: { aRel: 'follow', aDir: 'out', aActor: BSKY, bRel: 'follow', bDir: 'in', bActor: JAY, op: 'and' } },
      { label: 'Mutuals', values: { aRel: 'follow', aDir: 'out', aActor: BSKY, bRel: 'follow', bDir: 'in', bActor: BSKY, op: 'and' } },
      { label: 'Not following back', values: { aRel: 'follow', aDir: 'out', aActor: BSKY, bRel: 'follow', bDir: 'in', bActor: BSKY, op: 'andnot' } },
    ],
    build: (v, did) => ({
      a: { rel: v.aRel, dir: dir(v.aDir), actor: did(v.aActor) },
      b: { rel: v.bRel, dir: dir(v.bDir), actor: did(v.bActor) },
      op: v.op === 'andnot' ? 'SET_OP_AND_NOT' : undefined,
      limit: int(v.limit),
      count: opt(v.count === '1'),
      cursor: opt(v.cursor),
    }),
  },
  {
    name: 'Filter',
    hint: 'drop',
    tip: 'The actors, less any a drop rule matches for the viewer',
    fields: [
      { k: 'viewer', label: 'viewer', kind: 'actor' },
      { k: 'actors', label: 'actors', kind: 'actors', tip: 'At most 1,000' },
      { k: 'out', label: 'viewer blocks', kind: 'bool', tip: 'block, DIR_OUT' },
      { k: 'in', label: 'blocks viewer', kind: 'bool', tip: 'block, DIR_IN' },
      { k: 'list', label: 'member of', kind: 'list', tip: 'A list’s AT URI: drops its members', placeholder: 'at://…/app.bsky.graph.list/…' },
    ],
    defaults: { viewer: BSKY, actors: `${JAY}\n${BSKY_FOLLOW}`, out: '1', in: '1', list: '' },
    build: (v, did) => {
      const drop: unknown[] = []
      if (v.out === '1') drop.push({ rel: 'block', dir: 'DIR_OUT' })
      if (v.in === '1') drop.push({ rel: 'block', dir: 'DIR_IN' })
      if (v.list.trim()) drop.push({ memberOf: { rel: 'listitem', dir: 'DIR_OUT', actor: v.list.trim() } })
      return { viewer: did(v.viewer), actors: lines(v.actors).map(did), drop: list(drop) }
    },
  },
]

/** The tokens in a call's actor fields that need /resolve: not a DID, not an AT URI. */
export function handlesIn(c: Call, v: Values): string[] {
  const out = new Set<string>()
  for (const f of c.fields) {
    const raw = v[f.k] ?? ''
    const toks = f.kind === 'actor' ? [raw.trim()] : f.kind === 'actors' || f.kind === 'pairs' ? lines(raw) : []
    for (const t of toks) if (t && !t.startsWith('did:') && !t.startsWith('at://')) out.add(t.replace(/^@/, '').toLowerCase())
  }
  return [...out]
}

/** `JSON.stringify` without the undefined fields, for the request and its curl. */
export const body = (x: unknown) => JSON.stringify(x, (_k, val) => (val === undefined ? undefined : val))

export function curl(origin: string, method: string, json: string) {
  const q = json.replace(/'/g, `'\\''`)
  return `curl -s ${origin}/marco.v1.Graph/${method} \\\n  -H 'content-type: application/json' \\\n  -d '${q}'`
}
