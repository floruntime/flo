import { useEffect, useState } from 'react'
import type { ReactNode } from 'react'
import { NavLink } from 'react-router-dom'
import { NAMESPACES, useNamespace } from '@/lib/namespace'
import { useNamespaces, useClusterStats } from '@/lib/api/hooks'
import { Icon, NAV_ICONS, CheckIcon, ChevronVIcon, CubeIcon, PlusIcon, SearchIcon, BookIcon } from '@/lib/icons'
import { Dot } from '@/components/feedback'
import { LogoMark } from '@/components/brand/Logo'
import { CommandPalette, DOCS_URL } from './CommandPalette'
import { NAV } from './nav'
import type { NavRow } from './nav'
import { cx } from '@/lib/cx'
import './Shell.css'

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
  const [cmdkOpen, setCmdkOpen] = useState(false)

  // ⌘K / Ctrl-K toggles the command palette anywhere in the console.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && (e.key === 'k' || e.key === 'K')) {
        e.preventDefault()
        setCmdkOpen((o) => !o)
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [])

  return (
    <div className="app">
      <header className="hdr">
        <div className="brand">
          <span className="flo-mark" aria-label="Flo">
            <LogoMark />
          </span>
          <span className="sl">/</span>
          <NamespaceSwitcher />
        </div>
        <div className="hdr-r">
          <button className="hdr-search" onClick={() => setCmdkOpen(true)}>
            <SearchIcon size={14} />
            <span>Search</span>
            <span className="hdr-kbd">⌘K</span>
          </button>
          <span className="health-pill">
            <Dot color={allOk ? 'var(--accent)' : 'var(--warn)'} />
            {nodes.length ? `${healthy} / ${nodes.length} node${nodes.length > 1 ? 's' : ''} healthy` : 'connecting…'}
          </span>
          <a className="icon-btn" href={DOCS_URL} target="_blank" rel="noopener noreferrer" title="Docs" aria-label="Docs">
            <BookIcon />
          </a>
        </div>
      </header>
      <CommandPalette open={cmdkOpen} onClose={() => setCmdkOpen(false)} />
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
