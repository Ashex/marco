import { useState } from 'react'
import { DataTable } from '../components/DataTable'
import { Chip, KV, Loaded, PageHead, Panel, PanelBody, Seg, Toggle, Updated, Dot } from '../components/kit'
import { setTheme, useTheme } from '../components/Shell'
import { setToken } from '../lib/api'
import { setSources, useLiveState } from '../lib/live'
import { useSession, useSettings } from '../lib/queries'

export function Settings() {
  const st = useSettings()
  const sess = useSession()
  const [theme] = useTheme()
  const live = useLiveState()
  const [only, setOnly] = useState<'all' | 'changed'>('all')
  const b = st.data?.build
  return (
    <>
      <PageHead
        title="Settings"
        sub={
          b && (
            <>
              <span>marco {b.version}</span>
              <Dot />
              <span className="mono">{b.commit}</span>
              <Dot />
              <span>format {b.format}</span>
              <Updated l={st} />
            </>
          )
        }
      />
      <div className="cx-grid2" style={{ gridTemplateColumns: 'minmax(0,1fr) minmax(0,1.3fr)' }}>
        <div className="cx-stack">
          <Panel title="Console">
            <PanelBody>
              <KV
                rows={[
                  ['Theme', <Seg label="Theme" value={theme} options={[{ v: 'system', label: 'system' }, { v: 'light', label: 'light' }, { v: 'dark', label: 'dark' }]} onChange={setTheme} />],
                  ['Data sources', <Toggle on={live.showSources} onChange={setSources} label="Show data sources" />],
                  [
                    'Session',
                    <span className="mono">{sess.data?.auth === 'proxy' ? sess.data.operator : sess.data?.auth === 'token' ? 'admin token' : '—'}</span>,
                    sess.data?.auth === 'token' ? (
                      <button
                        type="button"
                        className="cx-btn sm"
                        onClick={() => {
                          setToken('')
                          location.reload()
                        }}
                      >
                        Lock console
                      </button>
                    ) : undefined,
                  ],
                ]}
              />
            </PanelBody>
          </Panel>
          <Panel title="Build">
            <Loaded load={st}>
              {(d) => (
                <PanelBody>
                  <KV
                    rows={[
                      ['Version', <span className="mono">{d.build?.version}</span>],
                      ['Commit', <span className="mono">{d.build?.commit}</span>],
                      ['Snapshot format', <span className="mono">{d.build?.format}</span>],
                      ['Features', <span className="mono">{d.build?.features.join(' ')}</span>],
                    ]}
                  />
                </PanelBody>
              )}
            </Loaded>
          </Panel>
        </div>
        <div className="cx-stack">
          <Panel title="Flags" right={<Seg label="Show" value={only} options={[{ v: 'all', label: 'all' }, { v: 'changed', label: 'changed' }]} onChange={setOnly} />}>
            <Loaded load={st}>
              {(d) => (
                <DataTable
                  compact
                  rows={d.flags.filter((f) => only === 'all' || (f.source !== 'default' && f.source !== 'unset'))}
                  rowKey={(f) => f.name}
                  cols={[
                    { id: 'name', label: 'Flag', render: (f) => <span className="mono">{f.name}</span> },
                    { id: 'v', label: 'Value', fill: true, render: (f) => <span className="mono">{f.value}</span> },
                    { id: 's', label: 'Source', render: (f) => (f.source === 'default' ? <Chip k="plain" g="○">default</Chip> : f.source === 'unset' ? <Chip k="idle">unset</Chip> : <Chip k="acc">{f.source}</Chip>) },
                  ]}
                />
              )}
            </Loaded>
          </Panel>
        </div>
      </div>
    </>
  )
}
