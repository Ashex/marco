// The docs' read examples on the public instance. A code block's API-listener
// origin (`localhost:7300`) is wrapped in a span at build time when the path
// after it is a Graph read, /readyz, /healthz or /docs, unless the fence is
// marked `local` (```bash local): writes, the admin listener and examples
// about your own instance. When `marco serve --public-url` puts the origin in
// <meta name="marco-public-url">, DocsApp swaps it into those spans.

export const READ_RPCS = ['Has', 'Relationships', 'BlockPairs', 'ListOut', 'ListIn', 'Counts', 'Intersect', 'Filter']
const PUBLIC_PATH = new RegExp(`^/(?:marco\\.v1\\.Graph/(?:${READ_RPCS.join('|')})|readyz|healthz|docs)(?![\\w.])`)
// highlighted HTML: a quote is an entity, so a path stops at `&`
const ORIGIN = /(?:https?:\/\/)?localhost:7300(?=(\/[^\s&<"]*)?)/g
const SPAN = /<span class="pub-origin">[^<]*<\/span>/g

export function markOrigins(html) {
  return html.replace(ORIGIN, (m, path) => (path && PUBLIC_PATH.test(path) ? `<span class="pub-origin">${m}</span>` : m))
}

const esc = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;')

export function applyPublicUrl(html, url) {
  if (!url) return html
  const u = esc(url.replace(/\/+$/, ''))
  return html.replace(SPAN, `<span class="pub-origin">${u}</span>`)
}

/** Problems with a page's code blocks once `url` is swapped in (check-docs). */
export function checkPublicExamples(file, html, url) {
  const errors = []
  const out = applyPublicUrl(html, url)
  const blocks = out.match(/<div class="code-block"[^>]*>[\s\S]*?<\/div>/g) ?? []
  for (const b of blocks) {
    const text = b.replace(/<[^>]+>/g, '').replace(/&#x27;/g, "'").replace(/&quot;/g, '"').replace(/&amp;/g, '&')
    const first = text.trim().split('\n')[0].slice(0, 60)
    const local = b.startsWith('<div class="code-block" data-local')
    if (local) {
      if (text.includes(url)) errors.push(`${file}: a \`local\` block points at the public instance: "${first}"`)
      continue
    }
    if (/localhost:7300/.test(text)) errors.push(`${file}: an example on localhost:7300 that isn't a Graph read, /readyz, /healthz or /docs; mark its fence \`local\` if it's about your own instance: "${first}"`)
    if (/:7301\b|marco\.v1\.Admin\/|marco\.v1\.Graph\/Write/.test(text)) errors.push(`${file}: a write or admin-listener example needs a \`local\` fence (they 404 on the public instance): "${first}"`)
  }
  return errors
}
