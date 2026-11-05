// Screenshots of every docs page: `npm run docs-shots [outDir]` (default shots/docs, gitignored).
// Desktop light and dark, 390 px phone light and dark, plus the phone's open nav and one diagram's
// lightbox. Exits 1 on page errors, console errors or failed requests.
// `--url http://host:port` shoots a running marco instead of the dev server.

import { chromium } from 'playwright'
import { spawn } from 'node:child_process'
import { mkdirSync } from 'node:fs'
import { resolve } from 'node:path'
import { loadDocs } from '../docs-build/docs.mjs'

const args = process.argv.slice(2)
const urlAt = args.indexOf('--url')
const remote = urlAt >= 0 ? args.splice(urlAt, 2)[1] : undefined
const out = resolve(args[0] ?? 'shots/docs')
mkdirSync(out, { recursive: true })
const UI = new URL('..', import.meta.url).pathname

let server
if (!remote) {
  server = spawn(process.execPath, ['node_modules/vite/bin/vite.js', '--port', '5732', '--strictPort'], { cwd: UI, stdio: ['ignore', 'pipe', 'inherit'] })
  server.stdout.resume()
  await new Promise((ok, no) => {
    server.on('exit', (c) => no(new Error(`vite exited ${c}`)))
    const tick = () => fetch('http://localhost:5732/docs').then(() => ok(), () => setTimeout(tick, 200))
    tick()
  })
}
const base = remote ?? 'http://localhost:5732'
const stop = () => server && server.pid > 1 && server.kill()
process.on('exit', stop)
process.on('uncaughtException', (e) => (console.error(e), stop(), process.exit(1)))

const { pages } = loadDocs()
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
  return pg
}

const name = (slug) => slug.replace(/\//g, '-')

for (const [w, theme, tag] of [[1440, 'light', 'desktop-light'], [1440, 'dark', 'desktop-dark'], [390, 'light', 'phone-light'], [390, 'dark', 'phone-dark']]) {
  const pg = await page(w, theme)
  for (const p of pages) {
    await pg.goto(`${base}/docs/${p.slug}`)
    await pg.locator('.doc-body').waitFor({ timeout: 15000 })
    await pg.waitForTimeout(300)
    const f = `${out}/${name(p.slug)}-${tag}.png`
    await pg.screenshot({ path: f, fullPage: true })
    console.log(f)
  }
  await pg.context().close()
}

{
  const pg = await page(390, 'light')
  await pg.goto(`${base}/docs/${pages[0].slug}`)
  await pg.locator('.doc-body').waitFor()
  await pg.locator('.docs-nav-toggle').click()
  await pg.waitForTimeout(200)
  await pg.screenshot({ path: `${out}/nav-phone-light.png` })
  console.log(`${out}/nav-phone-light.png`)
  await pg.context().close()
}
{
  const pg = await page(1440, 'dark')
  await pg.goto(`${base}/docs/${pages[0].slug}`)
  await pg.locator('.doc-body svg.dg').first().click()
  await pg.locator('.dgl').waitFor()
  await pg.waitForTimeout(300)
  await pg.screenshot({ path: `${out}/lightbox-desktop-dark.png` })
  console.log(`${out}/lightbox-desktop-dark.png`)
  await pg.context().close()
}

await browser.close()
stop()
console.log(failures ? `${failures} failures` : 'ok')
process.exit(failures ? 1 : 0)
