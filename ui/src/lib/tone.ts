// One status vocabulary: the tone each state word gets, and so its glyph (● ok, ▲ warn,
// ■ err, ◆ info, ○ idle). Chips, glyphs, banners and badges read these, so a shard or an
// actor looks the same on every page.

export type Tone = 'ok' | 'warn' | 'err' | 'info' | 'idle'
export const GLYPH: Record<Tone | 'acc' | 'out' | 'in' | 'sig', string> = { ok: '●', warn: '▲', err: '■', info: '◆', idle: '○', acc: '●', out: '→', in: '←', sig: '●' }

const SHARD: Record<string, Tone | 'acc'> = { clean: 'idle', dirty: 'info', writing: 'acc' }
export const shardTone = (s: string) => SHARD[s] ?? 'idle'
export const SHARD_WORD: Record<string, string> = { clean: 'clean', dirty: 'dirty', writing: 'writing' }

const VERIFY: Record<string, Tone> = { ok: 'ok', mismatch: 'err', verifying: 'info', unverified: 'idle' }
export const verifyTone = (s: string): Tone => VERIFY[s] ?? 'idle'

const ACTOR: Record<string, Tone> = { active: 'ok', takendown: 'err', deleted: 'idle' }
export const actorTone = (s: string): Tone => ACTOR[s] ?? 'idle'

const OUTCOME: Record<string, string> = { added: 'in', removed: 'c5', deleted: 'c4', exists: 'c3', not_found: 'c6', error: 'err' }
/** The colour token of a write outcome. */
export const outcomeColor = (o: string) => OUTCOME[o] ?? 'ink3'
export const OUTCOMES = ['added', 'removed', 'deleted', 'exists', 'not_found'] as const
