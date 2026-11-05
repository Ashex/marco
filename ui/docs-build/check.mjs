// `npm run check-docs`: validates docs/ without a full build (front matter,
// a hero first on every page, diagrams, links and anchors), checks the
// published pages describe only the current version, and prints the nav.
import fs from 'node:fs'
import path from 'node:path'
import { loadDocs, publishedFiles, DOCS_DIR } from './docs.mjs'

const { pages, nav, errors, internal } = loadDocs()

// Tracker, review and phase numbers belong to the project record (the
// standalone HTML pages), not to the docs (_style.md, "Current version only").
const BOOKKEEPING = [/\btask [0-9]+/i, /\bCR [0-9]+/, /\bdecision [0-9]+/i, /\bP[0-9]\b/]
for (const file of publishedFiles()) {
  fs.readFileSync(file, 'utf8')
    .split('\n')
    .forEach((line, i) => {
      for (const re of BOOKKEEPING) {
        const m = line.match(re)
        if (m) errors.push(`${path.relative(DOCS_DIR, file)}:${i + 1}: "${m[0]}": the docs describe the current version only (no task, CR, decision or phase numbers)`)
      }
    })
}
for (const s of nav) {
  console.log(s.name)
  for (const slug of s.pages) {
    const p = pages.find((x) => x.slug === slug)
    console.log(`  /docs/${slug}${p.status !== 'ready' ? ` (${p.status})` : ''}`)
  }
}
if (internal.length) console.log(`\nNot published (docs/_internal.txt): ${internal.join(', ')}`)
if (errors.length) {
  console.error(`\n${errors.length} problem(s):\n  ${errors.join('\n  ')}`)
  process.exit(1)
}
console.log(`\n${pages.length} pages OK`)
