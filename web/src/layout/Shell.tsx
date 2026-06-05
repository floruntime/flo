import { useState } from 'react'
import type { ReactNode } from 'react'
import { NavLink } from 'react-router-dom'
import { NAMESPACES, useNamespace } from '@/lib/namespace'
import { useNamespaces, useClusterStats } from '@/lib/api/hooks'
import { Icon, NAV_ICONS, CheckIcon, ChevronVIcon, CubeIcon, PlusIcon } from '@/lib/icons'
import { Dot } from '@/components/feedback'
import { cx } from '@/lib/cx'
import './Shell.css'

type NavRow = { label: string; to?: string; tail?: string; id: string }
type NavGroup = { grp: string; rows: NavRow[] }

const NAV: NavGroup[] = [
  { grp: 'Observe', rows: [{ label: 'Overview', to: '/', id: 'overview' }] },
  {
    grp: 'Data layer',
    rows: [
      { label: 'Streams', to: '/streams', id: 'streams' },
      { label: 'KV Store', to: '/kv', tail: '4', id: 'kv' },
      { label: 'Queues', to: '/queues', tail: '8', id: 'queues' },
      { label: 'Time Series', to: '/timeseries', id: 'ts' },
    ],
  },
  {
    grp: 'Compute',
    rows: [
      { label: 'Actions', to: '/actions', id: 'actions' },
      { label: 'Workers', to: '/workers', id: 'workers' },
      { label: 'Processing', to: '/processing', id: 'processing' },
      { label: 'Workflows', to: '/workflows', id: 'workflows' },
    ],
  },
]

function NamespaceSwitcher() {
  const { ns, setNs } = useNamespace()
  const [open, setOpen] = useState(false)
  const { data } = useNamespaces()
  const names = data && data.length ? data.map((n) => n.name) : [...NAMESPACES]
  return (
    <div style={{ position: 'relative' }}>
      <button className="ns-btn" onClick={() => setOpen((o) => !o)}>
        <CubeIcon />
        <span>{ns}</span>
        <ChevronVIcon />
      </button>
      {open && (
        <>
          <div className="ns-scrim" onClick={() => setOpen(false)} />
          <div className="ns-menu">
            <div className="ns-menu-h">Namespace</div>
            {names.map((n) => (
              <button
                key={n}
                className={cx('ns-item', n === ns && 'on')}
                onClick={() => {
                  setNs(n)
                  setOpen(false)
                }}
              >
                {n}
                {n === ns && (
                  <span style={{ marginLeft: 'auto', color: 'var(--accent)', display: 'flex' }}>
                    <CheckIcon />
                  </span>
                )}
              </button>
            ))}
            <div className="ns-div" />
            <button className="ns-item ns-new">
              <PlusIcon />
              New namespace
            </button>
          </div>
        </>
      )}
    </div>
  )
}

function SideItem({ row }: { row: NavRow }) {
  const inner = (
    <>
      <span className="navi">
        <Icon paths={NAV_ICONS[row.id] || ''} size={16} />
      </span>
      <span>{row.label}</span>
      {row.tail && <span className="tail">{row.tail}</span>}
    </>
  )
  if (!row.to) return <div className="side-i inert">{inner}</div>
  return (
    <NavLink to={row.to} end={row.to === '/'} className={({ isActive }) => cx('side-i', isActive && 'on')}>
      {inner}
    </NavLink>
  )
}

export function Shell({ children }: { children: ReactNode }) {
  const { data: stats } = useClusterStats()
  const nodes = stats?.nodes ?? []
  const healthy = nodes.filter((n) => n.status === 'healthy').length
  const allOk = nodes.length > 0 && healthy === nodes.length
  return (
    <div className="app">
      <header className="hdr">
        <div className="brand">
          <span className="wm">flo</span>
          <span className="sl">/</span>
          <NamespaceSwitcher />
        </div>
        <div className="hdr-r">
          <span className="lnk">Search</span>
          <span style={{ display: 'flex', alignItems: 'center', gap: 7, fontSize: 13, color: 'var(--tx-2)' }}>
            <Dot color={allOk ? 'var(--accent)' : 'var(--warn)'} />
            {nodes.length ? `${healthy} / ${nodes.length} node${nodes.length > 1 ? 's' : ''} healthy` : 'connecting…'}
          </span>
          <span className="lnk">Docs</span>
        </div>
      </header>
      <div className="body">
        <nav className="side">
          {NAV.map((s) => (
            <div className="side-grp" key={s.grp}>
              <div className="side-h">{s.grp}</div>
              {s.rows.map((row) => (
                <SideItem key={row.label} row={row} />
              ))}
            </div>
          ))}
        </nav>
        <main className="main">{children}</main>
      </div>
    </div>
  )
}
