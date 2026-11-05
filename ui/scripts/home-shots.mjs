// Screenshots of the landing page and its explorer: `npm run home-shots [outDir]` (default
// shots/home, gitignored). Desktop and 390 px phone, light and dark, after running the default
// call, plus Intersect's known followers. Exits 1 on page errors, console errors (a CSP
// violation is one) or failed requests.
// `--url http://host:port` shoots a running marco's API listener instead of the dev server.
// `--api https://marco.example.com` sends the explorer's calls (and /readyz, /resolve) there.

import { chromium } from 'playwright'
import { spawn } from 'node:child_process'
import { mkdirSync } from 'node:fs'
import { resolve } from 'node:path'

const args = process.argv.slice(2)
const flag = (name) => {
  const at = args.indexOf(name)
  return at >= 0 ? args.splice(at, 2)[1] : undefined
}
const remote = flag('--url')
const api = flag('--api')
const out = resolve(args[0] ?? 'shots/home')
mkdirSync(out, { recursive: true })
const UI = new URL('..', import.meta.url).pathname

let server
if (!remote) {
  server = spawn(process.execPath, ['node_modules/vite/bin/vite.js', '--port', '5733', '--strictPort'], { cwd: UI, stdio: ['ignore', 'pipe', 'inherit'] })
  server.stdout.resume()
  await new Promise((ok, no) => {
    server.on('exit', (c) => no(new Error(`vite exited ${c}`)))
    const tick = () => fetch('http://localhost:5733/').then(() => ok(), () => setTimeout(tick, 200))
    tick()
  })
}
const base = remote ?? 'http://localhost:5733'
const stop = () => server && server.pid > 1 && server.kill()
process.on('exit', stop)
process.on('uncaughtException', (e) => (console.error(e), stop(), process.exit(1)))

const browser = await chromium.launch()
let failures = 0
const fail = (...m) => (failures++, console.error('FAIL', ...m))

async function page(w, theme) {
  const ctx = await browser.newContext({ viewport: { width: w, height: 900 }, colorScheme: theme, deviceScaleFactor: 1 })
  const pg = await ctx.newPage()
  pg.on('pageerror', (e) => fail('pageerror', e.message))
  pg.on('console', (m) => m.type() === 'error' && fail('console', m.text()))
  pg.on('requestfailed', (r) => fail('request', r.url(), r.failure()?.errorText))
  pg.on('response', (r) => r.status() >= 400 && fail('http', r.status(), r.url()))
  if (api)
    await pg.route(/\/(marco\.v1\.Graph\/[A-Za-z]+|readyz|resolve)(\?|$)/, async (route) => {
      const u = new URL(route.request().url())
      const response = await route.fetch({ url: api + u.pathname + u.search })
      await route.fulfill({ response })
    })
  return pg
}

async function runCall(pg, name) {
  if (name) await pg.locator('.ex-call', { hasText: name }).first().click()
  await pg.locator('.ex-run button[type=submit]').click()
  await pg.locator('.ex-stats').waitFor({ timeout: 15000 })
  await pg.waitForTimeout(200)
  // a full-page shot draws the sticky bar where the window is scrolled to
  await pg.evaluate(() => window.scrollTo(0, 0))
  await pg.waitForTimeout(100)
}

for (const [w, theme, tag] of [[1440, 'light', 'desktop-light'], [1440, 'dark', 'desktop-dark'], [390, 'light', 'phone-light'], [390, 'dark', 'phone-dark']]) {
  const pg = await page(w, theme)
  await pg.goto(`${base}/`)
  await pg.locator('#explorer').waitFor({ timeout: 15000 })
  await runCall(pg)
  const f = `${out}/home-${tag}.png`
  await pg.screenshot({ path: f, fullPage: true })
  console.log(f)
  await pg.context().close()
}
for (const [w, theme, tag] of [[1440, 'dark', 'desktop-dark'], [390, 'light', 'phone-light']]) {
  const pg = await page(w, theme)
  await pg.goto(`${base}/`)
  await pg.locator('#explorer').waitFor({ timeout: 15000 })
  await runCall(pg, 'Intersect')
  const f = `${out}/intersect-${tag}.png`
  await pg.screenshot({ path: f, fullPage: true })
  console.log(f)
  await pg.context().close()
}

await browser.close()
stop()
console.log(failures ? `${failures} failures` : 'ok')
process.exit(failures ? 1 : 0)
