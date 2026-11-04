// The console's sections, in rail order. `key` is the letter after `g`.

export type SectionId = 'overview' | 'explorer' | 'relations' | 'storage' | 'ingest' | 'requests' | 'settings'
export type Section = { id: SectionId; label: string; key: string; group: 'Graph' | 'Engine' | 'System'; path: string }

export const SECTIONS: Section[] = [
  { id: 'overview', label: 'Overview', key: 'o', group: 'Graph', path: '/admin' },
  { id: 'explorer', label: 'Explorer', key: 'e', group: 'Graph', path: '/admin/explorer' },
  { id: 'relations', label: 'Relations', key: 'r', group: 'Graph', path: '/admin/relations' },
  { id: 'storage', label: 'Storage', key: 's', group: 'Engine', path: '/admin/storage' },
  { id: 'ingest', label: 'Ingest', key: 'i', group: 'Engine', path: '/admin/ingest' },
  { id: 'requests', label: 'Requests', key: 'q', group: 'Engine', path: '/admin/requests' },
  { id: 'settings', label: 'Settings', key: ',', group: 'System', path: '/admin/settings' },
]

export const SECTION = Object.fromEntries(SECTIONS.map((x) => [x.id, x])) as Record<SectionId, Section>

export function sectionOf(path: string): Section {
  let best = SECTION.overview
  for (const s of SECTIONS) if (s.path !== '/admin' && (path === s.path || path.startsWith(`${s.path}/`))) best = s
  return best
}

export const TABBAR: SectionId[] = ['overview', 'explorer', 'storage', 'ingest']
