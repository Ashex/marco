// Screenshots of every section against the mock (src/lib/mock.ts): `npm run shots [outDir]`
// (default shots/, gitignored). Desktop and 390 px phone, light and dark, plus the slide-over,
// the confirms and ⌘K. Exits 1 on page errors, console errors or failed requests.
// `--url http://host:port` shoots a running server instead (MARCO_TOKEN for its admin token).

import { chromium } from 'playwright'
import { spawn } from 'node:child_process'
import { mkdirSync } from 'node:fs'
import { resolve } from 'node:path'

const args = process.argv.slice(2)
const urlAt = args.indexOf('--url')
const remote = urlAt >= 0 ? args.splice(urlAt, 2)[1] : undefined
const out = resolve(args[0] ?? 'shots')
mkdirSync(out, { recursive: true })
const UI = new URL('..', import.meta.url).pathname
const ACTOR = process.env.MARCO_ACTOR ?? 'did:plc:k3ltx7rq2a5m6uoyzwbnf4hc'

let server
async function serve(port) {
  const p = spawn(process.execPath, ['node_modules/vite/bin/vite.js', '--port', String(port), '--strictPort'], { cwd: UI, env: { ...process.env, VITE_MARCO_MOCK: '1' }, stdio: ['ignore', 'pipe', 'inherit'] })
  p.stdout.resume()
  await new Promise((ok, no) => {
    p.on('exit', (c) => no(new Error(`vite exited ${c}`)))
    const tick = () => fetch(`http://localhost:${port}/admin/`).then(() => ok(), () => setTimeout(tick, 200))
    tick()
  })
  return p
}

const base = remote ?? 'http://localhost:5731'
if (!remote) server = await serve(5731)
const stop = () => server && server.pid > 1 && server.kill()
process.on('exit', stop)
process.on('uncaughtException', (e) => (console.error(e), stop(), process.exit(1)))

const browser = await chromium.launch()
let failures = 0
const fail = (...m) => (failures++, console.error('FAIL', ...m))

async function page(w, theme) {
  const ctx = await browser.newContext({ viewport: { width: w, height: 900 }, colorScheme: theme, deviceScaleFactor: 1 })
  if (process.env.MARCO_TOKEN) await ctx.addInitScript((t) => sessionStorage.setItem('marco.console.token', t), process.env.MARCO_TOKEN)
  const pg = await ctx.newPage()
  pg.on('pageerror', (e) => fail('pageerror', e.message))
  pg.on('console', (m) => {
    // GetWal and ListSources answer Unimplemented (501) by design: the console shows a placeholder
    if (m.type() === 'error' && !/status of 501/.test(m.text())) fail('console', m.text())
  })
  pg.on('requestfailed', (r) => fail('request', r.url(), r.failure()?.errorText))
  pg.on('response', (r) => {
    if (r.status() >= 400 && r.status() !== 501) fail('http', r.status(), r.url())
  })
  return pg
}
async function shot(pg, name, sel) {
  const f = `${out}/${name}.png`
  await pg.waitForTimeout(400)
  if (sel) await pg.locator(sel).first().screenshot({ path: f })
  else {
    await pg.evaluate(() => scrollTo(0, 0))
    await pg.screenshot({ path: f, fullPage: true })
  }
  console.log(f)
}

// the right-hand panels with an answer each, as an operator would leave them
async function fillExplorer(pg) {
  const rows = pg.locator('main tr[data-open]')
  await rows.first().waitFor()
  const other = (await rows.nth(2).locator('td').first().locator('.mono').textContent()).trim()
  const uri = decodeURIComponent((await rows.nth(2).getAttribute('data-open')).slice('edge:'.length))
  await pg.getByLabel('Actor B').fill(other)
  await pg.getByRole('button', { name: 'Check' }).click()
  await pg.getByText('Relationships ·').waitFor()
  await pg.getByLabel('at:// URI', { exact: true }).fill(uri)
  await pg.getByRole('button', { name: 'Resolve' }).click()
  await pg.getByText('Out log').waitFor()
  await pg.getByLabel('Side B actor').fill(other)
  await pg.getByRole('button', { name: 'Intersect', exact: true }).click()
  await pg.getByText(/ ∩ /).first().waitFor()
  await pg.getByRole('button', { name: /^Filter \d+/ }).click()
  await pg.getByText('kept').waitFor()
}

const ROUTES = [
  ['overview', '/admin/', 'Edges by relation'],
  ['explorer', `/admin/explorer?actor=${encodeURIComponent(ACTOR)}`, 'Followers ←'],
  ['relations', '/admin/relations', 'List lengths'],
  ['storage', '/admin/storage', 'Bytes by section'],
  ['ingest', '/admin/ingest', 'Ops by outcome'],
  ['requests', '/admin/requests', 'Profiling'],
  ['settings', '/admin/settings', 'Flags'],
]

for (const [w, theme, tag] of [[1440, 'light', 'desktop-light'], [1440, 'dark', 'desktop-dark'], [390, 'light', 'phone-light'], [390, 'dark', 'phone-dark']]) {
  const pg = await page(w, theme)
  for (const [name, path, wait] of ROUTES) {
    await pg.goto(base + path)
    await pg.getByText(wait).first().waitFor({ timeout: 15000 })
    if (name === 'explorer') await fillExplorer(pg)
    await shot(pg, `${name}-${tag}`)
  }
  await pg.context().close()
}

{
  const pg = await page(1440, 'light')
  await pg.goto(`${base}/admin/explorer?actor=${encodeURIComponent(ACTOR)}`)
  await pg.locator('main tr[data-open]').nth(2).click()
  await pg.getByText('Reverse').first().waitFor()
  await shot(pg, 'edge-drawer', null)
  await pg.getByRole('button', { name: 'Delete edge…' }).click()
  await pg.locator('.cx-dlg').waitFor()
  await shot(pg, 'confirm-delete-edge', '.cx-dlg')
  await pg.keyboard.press('Escape')
  await pg.keyboard.press('Escape')
  await pg.goto(`${base}/admin/requests`)
  await pg.getByText('Tracing').first().waitFor()
  await pg.getByRole('button', { name: '1%', exact: true }).click()
  await pg.locator('.cx-dlg').waitFor()
  await shot(pg, 'confirm-trace', '.cx-dlg')
  await pg.keyboard.press('Escape')
  await pg.goto(`${base}/admin/storage`)
  const write = pg.getByRole('button', { name: 'Write dirty shards…' })
  // a --read-only server disables it
  if (await write.isEnabled()) {
    await write.click()
    await pg.locator('.cx-dlg').waitFor()
    await shot(pg, 'confirm-write-shards', '.cx-dlg')
    await pg.keyboard.press('Escape')
  }
  await pg.keyboard.press('Meta+k')
  if (!(await pg.locator('.cx-pal').count())) await pg.keyboard.press('Control+k')
  await pg.locator('.cx-pal').waitFor()
  await shot(pg, 'palette', '.cx-pal')
  await pg.context().close()
}
{
  const pg = await page(390, 'dark')
  await pg.goto(`${base}/admin/explorer?actor=${encodeURIComponent(ACTOR)}`)
  await pg.locator('main tr[data-open]').nth(0).click()
  await pg.getByText('Reverse').first().waitFor()
  await shot(pg, 'edge-drawer-phone-dark', null)
  await pg.context().close()
}

await browser.close()
stop()
console.log(failures ? `${failures} failures` : 'ok')
process.exit(failures ? 1 : 0)
