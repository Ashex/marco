// Formatting. Everything returns "—" for unknown values. uint64s arrive as bigints.

export type Num = number | bigint | undefined | null
export const n = (v: Num): number => (v === undefined || v === null ? NaN : Number(v))
const ok = (v: number) => isFinite(v)

/** 2,740,118,442 */
export const fmtCount = (v: Num) => (ok(n(v)) ? Math.round(n(v)).toLocaleString('en-US') : '—')

/** 18.5k, 2.97B, 412 */
export function si(v: Num, digits = 3): string {
  const x = n(v)
  if (!ok(x)) return '—'
  const a = Math.abs(x)
  const p = (d: number, s: string) => `${Number((x / d).toPrecision(digits))}${s}`
  if (a >= 1e9) return p(1e9, 'B')
  if (a >= 1e6) return p(1e6, 'M')
  if (a >= 1e4) return p(1e3, 'k')
  if (a >= 100 || Number.isInteger(x)) return Math.round(x).toLocaleString('en-US')
  return Number(x.toPrecision(digits)).toString()
}

/** A rate: 18.5k, 31, 0.4, 0. */
export function rate(v: Num): string {
  const x = n(v)
  if (!ok(x)) return '—'
  if (x === 0) return '0'
  if (x < 0.01) return '<0.01'
  if (x < 10) return x.toFixed(x < 1 ? 2 : 1).replace(/\.0$/, '')
  return si(x)
}

/** Decimal bytes, as the mockup: 38.5 GB, 412 MB, 120 KB. */
export function bytes(v: Num): string {
  const x = n(v)
  if (!ok(x)) return '—'
  if (x >= 1e12) return `${(x / 1e12).toFixed(2)} TB`
  if (x >= 1e9) return `${(x / 1e9).toFixed(x >= 1e11 ? 0 : x >= 1e10 ? 1 : 2)} GB`
  if (x >= 1e6) return `${(x / 1e6).toFixed(x >= 1e8 ? 0 : 1)} MB`
  if (x >= 1e3) return `${(x / 1e3).toFixed(0)} KB`
  return `${x} B`
}

/** Bytes split into number and unit, for tiles. */
export function bytesParts(v: Num): [string, string] {
  const s = bytes(v)
  const i = s.lastIndexOf(' ')
  return i > 0 ? [s.slice(0, i), s.slice(i + 1)] : [s, '']
}

/** Seconds as a latency: 0.41 µs, 37 µs, 2.4 ms, 1.20 s. */
export function lat(sec: Num): string {
  const s = n(sec)
  if (!ok(s) || s <= 0) return '—'
  const us = s * 1e6
  if (us < 10) return `${us.toFixed(2).replace(/0$/, '')} µs`
  if (us < 1000) return `${Math.round(us)} µs`
  const ms = us / 1000
  if (ms < 10) return `${ms.toFixed(1)} ms`
  if (ms < 1000) return `${Math.round(ms)} ms`
  return `${(ms / 1000).toFixed(2)} s`
}

/** [value, unit] of a latency, for tiles. */
export function latParts(sec: Num): [string, string] {
  const s = lat(sec)
  const i = s.lastIndexOf(' ')
  return i > 0 ? [s.slice(0, i), s.slice(i + 1)] : [s, '']
}

/** A duration: 42s, 3m 10s, 3h 12m, 6d 4h. */
export function dur(ms: Num): string {
  const x = n(ms)
  if (!ok(x)) return '—'
  const s = Math.max(0, Math.round(x / 1000))
  if (s < 60) return `${s}s`
  if (s < 3600) return `${Math.floor(s / 60)}m ${s % 60}s`
  if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`
  return `${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h`
}

export function ago(ms: Num): string {
  const x = n(ms)
  if (!ok(x) || x <= 0) return '—'
  const d = Date.now() - x
  if (d < 1500) return 'just now'
  return `${dur(d)} ago`
}

export const pct = (r: Num, digits = 1) => (ok(n(r)) ? `${(n(r) * 100).toFixed(digits)}%` : '—')

const pad = (x: number) => String(x).padStart(2, '0')
/** 15:42:08, local. */
export const clock = (ms: Num) => {
  const d = new Date(n(ms))
  return ok(n(ms)) ? `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}` : '—'
}
/** 2026-11-04 15:42 UTC from microseconds. */
export function utc(us: Num, secs = false): string {
  const x = n(us)
  if (!ok(x) || x <= 0) return '—'
  const iso = new Date(x / 1000).toISOString()
  return `${iso.slice(0, 10)} ${iso.slice(11, secs ? 19 : 16)}`
}
export const day = (us: Num) => utc(us).slice(0, 10)

/** did:plc:abcdefgh…wxyz */
export const shortDid = (d: string) => (d.length > 24 ? `${d.slice(0, 16)}…${d.slice(-4)}` : d)

export const plural = (x: number, w: string, p?: string) => `${x.toLocaleString('en-US')} ${x === 1 ? w : (p ?? `${w}s`)}`
