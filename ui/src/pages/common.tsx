import { useState } from 'react'
import { Seg } from '../components/kit'
import { Dir } from '../gen/marco/v1/graph_pb'

// What several pages share: the time window, relation labels, shard names.

export const WINDOWS = [
  { v: '180', label: '3 min' },
  { v: '3600', label: '1 h' },
  { v: '86400', label: '24 h' },
] as const
export type WindowV = (typeof WINDOWS)[number]['v']

let shared: WindowV = '3600'
export function useWindow(): [number, React.ReactNode] {
  const [w, setW] = useState<WindowV>(shared)
  const seg = (
    <Seg
      label="Window"
      value={w}
      options={WINDOWS.map((x) => ({ v: x.v, label: x.label }))}
      onChange={(v) => {
        shared = v
        setW(v)
      }}
    />
  )
  return [Number(w), seg]
}
export const windowLabel = (w: number) => WINDOWS.find((x) => Number(x.v) === w)?.label ?? `${w}s`

export const shardName = (k: number) => `shard-${String(k).padStart(3, '0')}`

/** "follows →" style labels for the two built-in relations, `rel →` for others. */
export function listLabel(rel: string, dir: 'out' | 'in', cap = true): string {
  const w: Record<string, [string, string]> = { follow: ['follows', 'followers'], block: ['blocks', 'blocked by'] }
  const s = w[rel]?.[dir === 'out' ? 0 : 1] ?? `${rel} ${dir}`
  return cap ? s[0].toUpperCase() + s.slice(1) : s
}

export const dirOf = (d: Dir) => (d === Dir.IN ? 'in' : 'out') as 'out' | 'in'

/** Spark of a series with its values in display units. */
export const scale = (v: number[], k: number) => v.map((x) => x * k)
