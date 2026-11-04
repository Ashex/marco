import { useSyncExternalStore } from 'react'

// The console's live state: paused (space), stale (the heartbeat query is failing), and the
// toggle that shows where each panel's data comes from. One store, read with useLiveState().

export type LiveState = {
  paused: boolean
  stale: boolean
  lastOkAt: number
  staleError?: string
  showSources: boolean
}

const SRC_KEY = 'marco.console.sources'
let state: LiveState = {
  paused: false,
  stale: false,
  lastOkAt: 0,
  showSources: (() => {
    try {
      return localStorage.getItem(SRC_KEY) === '1'
    } catch {
      return false
    }
  })(),
}
const listeners = new Set<() => void>()
function set(p: Partial<LiveState>) {
  state = { ...state, ...p }
  listeners.forEach((l) => l())
}
const subscribe = (l: () => void) => {
  listeners.add(l)
  return () => {
    listeners.delete(l)
  }
}
export const getLive = () => state
export const useLiveState = () => useSyncExternalStore(subscribe, getLive)

export const togglePaused = () => set({ paused: !state.paused })
export function setSources(v: boolean) {
  try {
    localStorage.setItem(SRC_KEY, v ? '1' : '0')
  } catch {
    /* per-tab */
  }
  set({ showSources: v })
}
export function heartbeatOk() {
  if (state.stale || !state.lastOkAt || Date.now() - state.lastOkAt > 1000) set({ stale: false, staleError: undefined, lastOkAt: Date.now() })
}
export function heartbeatFailed(e: unknown) {
  set({ stale: true, staleError: e instanceof Error ? e.message : String(e) })
}
