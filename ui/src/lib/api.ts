import { Code, ConnectError, createClient, type Client, type Interceptor, type Transport } from '@connectrpc/connect'
import { createConnectTransport } from '@connectrpc/connect-web'
import { Admin } from '../gen/marco/v1/admin_pb'
import { Graph } from '../gen/marco/v1/graph_pb'

// The one way into marco: Connect clients for marco.v1.Admin and marco.v1.Graph on this
// origin (the admin listener), with the admin token from sessionStorage. `npm run dev:mock`
// swaps in the mock transport (lib/mock.ts), which a real build never loads.

export const MOCK = !!import.meta.env.VITE_MARCO_MOCK
const KEY = 'marco.console.token'

export function getToken(): string {
  try {
    return sessionStorage.getItem(KEY) ?? ''
  } catch {
    return ''
  }
}
export function setToken(t: string) {
  try {
    if (t) sessionStorage.setItem(KEY, t)
    else sessionStorage.removeItem(KEY)
  } catch {
    /* per page load */
  }
}

const auth: Interceptor = (next) => async (req) => {
  const t = getToken()
  if (t) req.header.set('Authorization', `Bearer ${t}`)
  return next(req)
}

let transport: Transport | undefined
let adminC: Client<typeof Admin>
let graphC: Client<typeof Graph>
let fetchRaw: (path: string, init?: RequestInit) => Promise<Response> = (path, init) => {
  const t = getToken()
  return fetch(path, { ...init, headers: { ...(init?.headers ?? {}), ...(t ? { Authorization: `Bearer ${t}` } : {}) } })
}

export async function initApi() {
  if (transport) return
  if (MOCK) {
    const m = await import('./mock')
    transport = m.mockTransport()
    fetchRaw = m.mockFetch
  } else {
    transport = createConnectTransport({ baseUrl: location.origin, interceptors: [auth] })
  }
  adminC = createClient(Admin, transport!)
  graphC = createClient(Graph, transport!)
}

export const admin = () => adminC
export const graph = () => graphC

/** An ops route on the admin listener (/debug/pprof/*, /debug/tracing), with the token. */
export async function ops(path: string, init?: RequestInit): Promise<Response> {
  const r = await fetchRaw(path, init)
  if (!r.ok) throw new Error(`${path}: ${r.status} ${(await r.text()).trim() || r.statusText}`)
  return r
}

export function errText(e: unknown): string {
  if (e instanceof ConnectError) return e.rawMessage || e.message
  if (e instanceof Error) return e.message
  return String(e)
}

export const isUnimplemented = (e: unknown) => e instanceof ConnectError && e.code === Code.Unimplemented
export const isUnauthenticated = (e: unknown) => e instanceof ConnectError && (e.code === Code.Unauthenticated || e.code === Code.PermissionDenied)

/** Runs a call and says how long it took, for the panels that show a round trip. */
export async function timed<T>(f: () => Promise<T>): Promise<[T, number]> {
  const t = performance.now()
  const r = await f()
  return [r, performance.now() - t]
}

/** The JSON a call's request is on the wire, for confirms and "Copy request". */
export const reqJson = (v: unknown) => JSON.stringify(v, (_, x) => (typeof x === 'bigint' ? x.toString() : x))
