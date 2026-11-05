// The site's 1200×630 social card (og:image, twitter:image): `npm run og-card` draws it in
// Chromium and writes public/og/marco.png, which is committed. Rerun it when the card changes.

import { chromium } from 'playwright'
import { mkdirSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'

const UI = new URL('..', import.meta.url).pathname
const out = resolve(UI, 'public/og/marco.png')
mkdirSync(resolve(UI, 'public/og'), { recursive: true })
const font = readFileSync(resolve(UI, 'node_modules/@fontsource-variable/recursive/files/recursive-latin-full-normal.woff2')).toString('base64')

const C = { paper: '#eceae4', sheet: '#f8f7f3', ink: '#1c1a16', ink2: '#4b473f', ink3: '#625d53', rule: '#cfcbc1', out: '#3550b0', inn: '#0e776a', accent: '#5b3fb0' }

// The console's mark, woven wide: indigo warp (out logs) and teal weft (in sets), over and under.
function loom(x0, y0, cols, rows, pitch, w) {
  const parts = []
  const H = rows * pitch
  const W = cols * pitch
  for (let c = 0; c < cols; c++) parts.push(`<rect x="${x0 + c * pitch + (pitch - w) / 2}" y="${y0}" width="${w}" height="${H}" rx="3" fill="${C.inn}"/>`)
  for (let r = 0; r < rows; r++) parts.push(`<rect x="${x0}" y="${y0 + r * pitch + (pitch - w) / 2}" width="${W}" height="${w}" rx="3" fill="${C.out}" stroke="${C.sheet}" stroke-width="3"/>`)
  for (let r = 0; r < rows; r++)
    for (let c = 0; c < cols; c++)
      if ((r + c) % 2 === 0) {
        const cx = x0 + c * pitch + (pitch - w) / 2
        const cy = y0 + r * pitch + (pitch - w) / 2
        parts.push(`<rect x="${cx}" y="${cy - 6}" width="${w}" height="${w + 12}" fill="${C.inn}" stroke="${C.sheet}" stroke-width="3"/>`)
      }
  return parts.join('')
}

const svg = `<svg xmlns="http://www.w3.org/2000/svg" width="1200" height="630" viewBox="0 0 1200 630">
<defs>
  <linearGradient id="fade" x1="0" x2="1" y1="0" y2="0"><stop offset="0" stop-color="${C.sheet}" stop-opacity="1"/><stop offset=".5" stop-color="${C.sheet}" stop-opacity="0"/></linearGradient>
</defs>
<rect width="1200" height="630" fill="${C.paper}"/>
<rect x="36" y="36" width="1128" height="558" rx="18" fill="${C.sheet}" stroke="${C.rule}" stroke-width="2"/>
<clipPath id="sheet"><rect x="36" y="36" width="1128" height="558" rx="18"/></clipPath>
<g clip-path="url(#sheet)">${loom(790, 20, 7, 10, 64, 26)}<rect x="790" y="36" width="410" height="558" fill="url(#fade)"/></g>
<rect x="36" y="36" width="1128" height="558" rx="18" fill="none" stroke="${C.rule}" stroke-width="2"/>
<g transform="translate(96 132) scale(4)">
  <rect x="5" y="2" width="3" height="16" rx="1" fill="${C.inn}"/><rect x="12" y="2" width="3" height="16" rx="1" fill="${C.inn}"/>
  <rect x="2" y="5" width="16" height="3" rx="1" fill="${C.out}" stroke="${C.sheet}"/><rect x="2" y="12" width="16" height="3" rx="1" fill="${C.out}" stroke="${C.sheet}"/>
  <rect x="4.5" y="11" width="4" height="5" fill="${C.inn}" stroke="${C.sheet}"/><rect x="11.5" y="4" width="4" height="5" fill="${C.inn}" stroke="${C.sheet}"/>
</g>
<text x="190" y="199" class="mark">marco</text>
<text x="96" y="292" class="tag">A graph database for atproto</text>
<text x="96" y="344" class="tag">actor-to-actor edges</text>
<text x="96" y="398" class="sub">follows · blocks · list items · any DID subject</text>
<g class="facts">
  <text x="96" y="506" class="v">2.97B</text><text x="96" y="536" class="l">edges, whole network</text>
  <text x="352" y="506" class="v">13 B</text><text x="352" y="536" class="l">per edge</text>
  <text x="540" y="506" class="v">~1M</text><text x="540" y="536" class="l">Has/s on 16 threads</text>
</g>
</svg>`

const html = `<!doctype html><html><head><style>
@font-face{font-family:R;src:url(data:font/woff2;base64,${font}) format('woff2');font-weight:300 1000}
html,body{margin:0;background:${C.paper}}
text{font-family:R}
.mark{font-size:84px;font-weight:780;letter-spacing:-.03em;fill:${C.ink};font-variation-settings:'MONO' 0,'CASL' 1}
.tag{font-size:46px;font-weight:620;letter-spacing:-.015em;fill:${C.ink};font-variation-settings:'MONO' 0,'CASL' .4}
.sub{font-size:24px;font-weight:450;fill:${C.ink2};font-variation-settings:'MONO' 1,'CASL' 0}
.v{font-size:52px;font-weight:760;fill:${C.accent};font-variation-settings:'MONO' 1,'CASL' 0;letter-spacing:-.02em}
.l{font-size:19px;font-weight:500;fill:${C.ink3};font-variation-settings:'MONO' 1,'CASL' 0}
</style></head><body>${svg}</body></html>`

const browser = await chromium.launch()
const pg = await browser.newPage({ viewport: { width: 1200, height: 630 }, deviceScaleFactor: 1 })
await pg.setContent(html)
await pg.evaluate(() => document.fonts.ready)
await pg.screenshot({ path: out, clip: { x: 0, y: 0, width: 1200, height: 630 } })
await browser.close()
console.log(out)
