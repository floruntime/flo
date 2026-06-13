/* Flo Console — command palette (⌘K).
   Search + jump across screens and namespaces. Opened from the header trigger
   or the ⌘K / Ctrl-K shortcut (wired in Shell). Mirrors the Console v2 design:
   every nav row + every namespace flattened into one ranked, grouped command list. */
import { useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useNamespace, NAMESPACES } from '@/lib/namespace'
import { useNamespaces } from '@/lib/api/hooks'
import { Icon, NAV_ICONS, SEARCH_PATH, CUBE_PATH, ARROW_PATH } from '@/lib/icons'
import { NAV } from './nav'
import './CommandPalette.css'

export const DOCS_URL = 'https://docs.runestack.io'

type CmdItem = {
  kind: 'nav' | 'ns'
  key: string
  label: string
  group: string
  icon: string
  current?: boolean
  run: () => void
}

export function CommandPalette({ open, onClose }: { open: boolean; onClose: () => void }) {
  const navigate = useNavigate()
  const { ns, setNs } = useNamespace()
  const { data: nsData } = useNamespaces()
  const [q, setQ] = useState('')
  const [active, setActive] = useState(0)
  const inputRef = useRef<HTMLInputElement>(null)
  const listRef = useRef<HTMLDivElement>(null)

  const names = nsData && nsData.length ? nsData.map((n) => n.name) : [...NAMESPACES]

  // Flatten everything searchable into a single command list: nav rows keep
  // their sidebar group, namespaces fall under "Switch namespace".
  const commands = useMemo<CmdItem[]>(() => {
    const nav: CmdItem[] = NAV.flatMap((g) =>
      g.rows
        .filter((r) => r.to)
        .map((r) => ({
          kind: 'nav' as const,
          key: 'nav:' + r.id,
          label: r.label,
          group: g.grp,
          icon: NAV_ICONS[r.id] || '',
          run: () => navigate(r.to!),
        })),
    )
    const nsCmds: CmdItem[] = names.map((n) => ({
      kind: 'ns' as const,
      key: 'ns:' + n,
      label: n,
      group: 'Switch namespace',
      icon: CUBE_PATH,
      current: n === ns,
      run: () => setNs(n),
    }))
    return [...nav, ...nsCmds]
  }, [names, ns, navigate, setNs])

  // Rank: label startsWith (0) > label includes (1) > group includes (2).
  const ql = q.trim().toLowerCase()
  const results = useMemo<CmdItem[]>(() => {
    if (!ql) return commands
    return commands
      .map((c) => {
        const l = c.label.toLowerCase()
        const g = c.group.toLowerCase()
        let score = -1
        if (l.startsWith(ql)) score = 0
        else if (l.includes(ql)) score = 1
        else if (g.includes(ql)) score = 2
        return { c, score }
      })
      .filter((x) => x.score >= 0)
      .sort((a, b) => a.score - b.score)
      .map((x) => x.c)
  }, [ql, commands])

  // Reset + focus on open.
  useEffect(() => {
    if (open) {
      setQ('')
      setActive(0)
      const t = setTimeout(() => inputRef.current?.focus(), 30)
      return () => clearTimeout(t)
    }
  }, [open])
  useEffect(() => setActive(0), [ql])
  useEffect(() => {
    const el = listRef.current?.querySelector('.cmd-row.on')
    if (el) el.scrollIntoView({ block: 'nearest' })
  }, [active, results])

  const choose = (c?: CmdItem) => {
    if (!c) return
    c.run()
    onClose()
  }
  const onKey = (e: React.KeyboardEvent) => {
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      setActive((a) => Math.min(a + 1, results.length - 1))
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      setActive((a) => Math.max(a - 1, 0))
    } else if (e.key === 'Enter') {
      e.preventDefault()
      choose(results[active])
    } else if (e.key === 'Escape') {
      e.preventDefault()
      onClose()
    }
  }

  if (!open) return null

  // Group results for display while preserving one flat active index.
  let idx = -1
  const grouped: { group: string; rows: CmdItem[] }[] = []
  results.forEach((c) => {
    let g = grouped.find((x) => x.group === c.group)
    if (!g) {
      g = { group: c.group, rows: [] }
      grouped.push(g)
    }
    g.rows.push(c)
  })

  return (
    <div className="cmd-scrim" onMouseDown={onClose}>
      <div className="cmd" onMouseDown={(e) => e.stopPropagation()} onKeyDown={onKey}>
        <div className="cmd-search">
          <Icon paths={SEARCH_PATH} size={17} strokeWidth={1.6} />
          <input
            ref={inputRef}
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Search screens, namespaces…"
            spellCheck={false}
          />
          <span className="cmd-esc">esc</span>
        </div>
        <div className="cmd-list" ref={listRef}>
          {results.length === 0 ? (
            <div className="cmd-empty">
              No matches for <span className="mono">"{q}"</span>
            </div>
          ) : (
            grouped.map((g) => (
              <div className="cmd-grp" key={g.group}>
                <div className="cmd-grp-h">{g.group}</div>
                {g.rows.map((c) => {
                  idx++
                  const i = idx
                  return (
                    <div
                      key={c.key}
                      className={'cmd-row' + (i === active ? ' on' : '')}
                      onMouseEnter={() => setActive(i)}
                      onMouseDown={() => choose(c)}
                    >
                      <span className="cmd-ico">
                        <Icon paths={c.icon} size={16} strokeWidth={1.6} />
                      </span>
                      <span className="cmd-label">{c.label}</span>
                      {c.kind === 'ns' && c.current && <span className="cmd-badge">current</span>}
                      <span className="cmd-go">
                        {c.kind === 'nav' ? 'Jump to' : 'Switch'}
                        <Icon paths={ARROW_PATH} size={13} strokeWidth={1.7} />
                      </span>
                    </div>
                  )
                })}
              </div>
            ))
          )}
        </div>
        <div className="cmd-foot">
          <span>
            <span className="cmd-k">↑</span>
            <span className="cmd-k">↓</span> navigate
          </span>
          <span>
            <span className="cmd-k">↵</span> select
          </span>
          <span>
            <span className="cmd-k">esc</span> close
          </span>
        </div>
      </div>
    </div>
  )
}
