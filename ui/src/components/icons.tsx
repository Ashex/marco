// Stroke icons, 16-unit box, from the approved mockup.

const P: Record<string, string> = {
  overview: '<rect x="2" y="2" width="5" height="5" rx="1"/><rect x="9" y="2" width="5" height="5" rx="1"/><rect x="2" y="9" width="5" height="5" rx="1"/><rect x="9" y="9" width="5" height="5" rx="1"/>',
  explorer: '<circle cx="4" cy="4" r="2"/><circle cx="12" cy="5" r="2"/><circle cx="7" cy="12" r="2"/><path d="M6 4.5l4 .4M5 5.6l1.4 4.6M11 7l-2.8 3.6"/>',
  relations: '<path d="M2 5h9M8 2l3 3-3 3"/><path d="M14 11H5M8 8l-3 3 3 3"/>',
  storage: '<ellipse cx="8" cy="3.5" rx="5.5" ry="2"/><path d="M2.5 3.5v9c0 1.1 2.5 2 5.5 2s5.5-.9 5.5-2v-9M2.5 8c0 1.1 2.5 2 5.5 2s5.5-.9 5.5-2"/>',
  ingest: '<path d="M8 1.5v8M4.5 6.5L8 10l3.5-3.5"/><path d="M2 11v2.5h12V11"/>',
  requests: '<path d="M1.5 8h3l2-5 3 10 2-5h3"/>',
  settings: '<path d="M2 4h7M12 4h2M2 12h2M7 12h7"/><circle cx="10.5" cy="4" r="1.5"/><circle cx="5.5" cy="12" r="1.5"/>',
  search: '<circle cx="7" cy="7" r="4.5"/><path d="M10.5 10.5L14 14"/>',
  theme: '<circle cx="8" cy="8" r="6"/><path d="M8 2a6 6 0 0 0 0 12z" fill="currentColor"/>',
  more: '<circle cx="3" cy="8" r="1"/><circle cx="8" cy="8" r="1"/><circle cx="13" cy="8" r="1"/>',
  close: '<path d="M4 4l8 8M12 4l-8 8"/>',
  ext: '<path d="M6 3H3v10h10v-3M9 2h5v5M14 2L7.5 8.5"/>',
}

export function Icon({ name, size = 14 }: { name: keyof typeof P | string; size?: number }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 16 16"
      style={{ fill: 'none', stroke: 'currentColor', strokeWidth: 1.5, strokeLinecap: 'round', strokeLinejoin: 'round' }}
      aria-hidden="true"
      dangerouslySetInnerHTML={{ __html: P[name] ?? '' }}
    />
  )
}

export function Mark() {
  return (
    <svg width="20" height="20" viewBox="0 0 20 20" aria-hidden="true">
      <rect x="5" y="2" width="3" height="16" rx="1" style={{ fill: 'var(--in)' }} />
      <rect x="12" y="2" width="3" height="16" rx="1" style={{ fill: 'var(--in)' }} />
      <rect x="2" y="5" width="16" height="3" rx="1" style={{ fill: 'var(--out)', stroke: 'var(--sheet)', strokeWidth: 1 }} />
      <rect x="2" y="12" width="16" height="3" rx="1" style={{ fill: 'var(--out)', stroke: 'var(--sheet)', strokeWidth: 1 }} />
      <rect x="4.5" y="11" width="4" height="5" style={{ fill: 'var(--in)', stroke: 'var(--sheet)', strokeWidth: 1 }} />
      <rect x="11.5" y="4" width="4" height="5" style={{ fill: 'var(--in)', stroke: 'var(--sheet)', strokeWidth: 1 }} />
    </svg>
  )
}
