import { useEffect, useRef, useState, useSyncExternalStore, type ReactNode } from 'react'
import { errText } from '../lib/api'
import { Glyph, Spinner } from './kit'
import { toast } from './toast'

// One modal at a time, opened imperatively: confirmAction() for anything that changes marco
// (what will happen, the exact request it sends), openDialog() for anything else.

type Render = (close: () => void) => ReactNode
let current: { id: number; render: Render; className?: string } | null = null
let n = 0
const subs = new Set<() => void>()
const emit = () => subs.forEach((l) => l())

export function openDialog(render: Render, className?: string) {
  current = { id: ++n, render, className }
  emit()
}
export function closeDialog() {
  current = null
  emit()
}
export const isDialogOpen = () => current !== null

export function DialogHost() {
  const d = useSyncExternalStore(
    (l) => {
      subs.add(l)
      return () => {
        subs.delete(l)
      }
    },
    () => current,
  )
  if (!d) return null
  return (
    <div
      className={`cx-scrim${d.className ? ` ${d.className}` : ''}`}
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) closeDialog()
      }}
      onKeyDown={(e) => {
        if (e.key === 'Escape') {
          e.stopPropagation()
          closeDialog()
        }
      }}
    >
      <div key={d.id} style={{ display: 'contents' }}>
        {d.render(closeDialog)}
      </div>
    </div>
  )
}

export type Field = { id: string; label: string; placeholder?: string; initial?: string }

export type ConfirmSpec = {
  tone: 'warn' | 'info' | 'err'
  title: string
  /** What will happen, one consequence per item. */
  items: ReactNode[]
  fields?: Field[]
  /** Must be typed exactly to enable the button. */
  word?: string
  action: string
  /** primary (violet) for routine actions, warn for a delete, danger for what can't be undone. */
  button?: 'primary' | 'warn' | 'danger'
  /** The request: its first line (method and path) in bold, then the body. */
  call: string | ((values: Record<string, string>) => string)
  run: (values: Record<string, string>) => Promise<unknown>
  done?: string | ((result: unknown) => string)
}

/** Resolves true once `run` succeeded, false if cancelled. Errors stay in the dialog. */
export function confirmAction(spec: ConfirmSpec): Promise<boolean> {
  return new Promise((resolve) => {
    let settled = false
    const finish = (v: boolean) => {
      if (!settled) {
        settled = true
        resolve(v)
      }
    }
    openDialog((close) => (
      <ConfirmDialog
        spec={spec}
        onCancel={() => {
          close()
          finish(false)
        }}
        onDone={() => {
          finish(true)
          close()
        }}
      />
    ))
    const unsub = () => {
      if (!current) {
        finish(false)
        subs.delete(unsub)
      }
    }
    subs.add(unsub)
  })
}

function Call({ text }: { text: string }) {
  const [head, ...rest] = text.split('\n')
  return (
    <div className="cx-call">
      <b>{head}</b>
      {rest.length > 0 && `\n${rest.join('\n')}`}
    </div>
  )
}

function ConfirmDialog({ spec, onCancel, onDone }: { spec: ConfirmSpec; onCancel: () => void; onDone: () => void }) {
  const [vals, setVals] = useState<Record<string, string>>(() => Object.fromEntries((spec.fields ?? []).map((f) => [f.id, f.initial ?? ''])))
  const [word, setWord] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<unknown>()
  const focus = useRef<HTMLButtonElement | HTMLInputElement | null>(null)
  useEffect(() => {
    setTimeout(() => focus.current?.focus(), 20)
  }, [])
  const can = (!spec.word || word.trim() === spec.word) && !busy
  const callText = typeof spec.call === 'function' ? spec.call(vals) : spec.call
  const go = async () => {
    if (!can) return
    setBusy(true)
    setError(undefined)
    try {
      const r = await spec.run(vals)
      const msg = typeof spec.done === 'function' ? spec.done(r) : spec.done
      if (msg) toast(msg)
      onDone()
    } catch (e) {
      setError(e)
    } finally {
      setBusy(false)
    }
  }
  const cls = spec.button === 'warn' ? 'warnb' : spec.button === 'danger' ? 'solid-danger' : 'primary'
  let first = true
  const ref = (el: HTMLButtonElement | HTMLInputElement | null) => {
    if (first && el) {
      focus.current = el
      first = false
    }
  }
  return (
    <form
      className="cx-dlg"
      role="alertdialog"
      aria-modal="true"
      aria-labelledby="cx-dlg-t"
      onSubmit={(e) => {
        e.preventDefault()
        go()
      }}
    >
      <header>
        <Glyph k={spec.tone} />
        <h2 id="cx-dlg-t">{spec.title}</h2>
      </header>
      <div className="db">
        <ul>
          {spec.items.map((x, i) => (
            <li key={i}>{x}</li>
          ))}
        </ul>
        {(spec.fields ?? []).map((f) => (
          <div key={f.id}>
            <label className="cx-lbl" htmlFor={`cf_${f.id}`}>
              {f.label}
            </label>
            <input ref={ref} id={`cf_${f.id}`} className="cx-inp mono" style={{ width: '100%' }} placeholder={f.placeholder} autoComplete="off" value={vals[f.id] ?? ''} onChange={(e) => setVals((v) => ({ ...v, [f.id]: e.target.value }))} />
          </div>
        ))}
        {spec.word && (
          <div>
            <label className="cx-lbl" htmlFor="cf_word">
              Type <b className="mono">{spec.word}</b> to confirm
            </label>
            <input ref={ref} id="cf_word" className="cx-inp mono" style={{ width: '100%' }} autoComplete="off" spellCheck={false} value={word} onChange={(e) => setWord(e.target.value)} />
          </div>
        )}
        {!!error && (
          <div className="dlg-err" role="alert">
            <span className="cx-g">■</span>
            <span>{errText(error)}</span>
          </div>
        )}
      </div>
      <Call text={callText} />
      <footer>
        <button type="button" className="cx-btn" onClick={onCancel}>
          Cancel
        </button>
        <button ref={spec.word || spec.fields?.length ? undefined : ref} type="submit" className={`cx-btn ${cls}`} disabled={!can}>
          {busy && <Spinner />}
          {spec.action}
        </button>
      </footer>
    </form>
  )
}

/** A plain dialog: a title, a body and a close button (the keyboard help). */
export function InfoDialog({ title, children, onClose }: { title: string; children: ReactNode; onClose: () => void }) {
  return (
    <div className="cx-dlg" role="dialog" aria-modal="true" aria-labelledby="cx-dlg-t">
      <header>
        <Glyph k="info" />
        <h2 id="cx-dlg-t">{title}</h2>
      </header>
      <div className="db">{children}</div>
      <footer>
        <button type="button" className="cx-btn" onClick={onClose} autoFocus>
          Close
        </button>
      </footer>
    </div>
  )
}
