/* Flo Console — command palette (⌘K).
   Search + jump across screens, namespaces, and quick actions. Opened from the
   header trigger or the ⌘K / Ctrl-K shortcut (wired in Shell). */
import { useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useNamespace, NAMESPACES } from '@/lib/namespace'
import { useNamespaces } from '@/lib/api/hooks'
import { Icon, NAV_ICONS, SEARCH_PATH, BOOK_PATH, TERMINAL_PATH, CUBE_PATH } from '@/lib/icons'
import { NAV } from './nav'
import './CommandPalette.css'

export const DOCS_URL = 'https://docs.runestack.io'

/** Short descriptor shown on the right of each navigate item. */
const SCREEN_META: Record<string, string> = {
  overview: 'Cluster health',
  streams: 'Commit logs',
  kv: 'Versioned store',
  queues: 'Priority queues',
  ts: 'Metrics & FloQL',
  actions: 'Durable execution',
  workers: 'Action runners',
  processing: 'Submitted jobs',
  workflows: 'Orchestration',
}

type CmdItem = {
  key: string
  name: string
  group: string
  icon: string
  meta?: string
  run: () => void
}

export function CommandPalette({ open, onClose }: { open: boolean; onClose: () => void }) {
  const navigate = useNavigate()
  const { setNs } = useNamespace()
  const { data: nsData } = useNamespaces()
  const [q, setQ] = useState('')
  const [active, setActive] = useState(0)
  const inputRef = useRef<HTMLInputElement>(null)

  const names = nsData && nsData.length ? nsData.map((n) => n.name) : [...NAMESPACES]

  const all = useMemo<CmdItem[]>(() => {
    const nav: CmdItem[] = NAV.flatMap((g) =>
      g.rows
        .filter((r) => r.to)
        .map((r) => ({
          key: 'nav:' + r.id,
          name: r.label,
          group: 'Navigate',
          icon: NAV_ICONS[r.id] || '',
          meta: SCREEN_META[r.id],
          run: () => navigate(r.to!),
        })),
    )
    const ns: CmdItem[] = names.map((n) => ({
      key: 'ns:' + n,
      name: 'Switch to ' + n,
      group: 'Namespace',
      icon: CUBE_PATH,
      run: () => setNs(n),
    }))
    const actions: CmdItem[] = [
      { key: 'act:floql', name: 'Run FloQL query', group: 'Actions', icon: TERMINAL_PATH, run: () => navigate('/timeseries') },
      { key: 'act:docs', name: 'Open documentation', group: 'Actions', icon: BOOK_PATH, run: () => window.open(DOCS_URL, '_blank', 'noopener,noreferrer') },
    ]
    return [...nav, ...ns, ...actions]
  }, [names, navigate, setNs])

  const filtered = q ? all.filter((i) => i.name.toLowerCase().includes(q.toLowerCase())) : all

  // Reset + focus on open.
  useEffect(() => {
    if (open) {
      setQ('')
      setActive(0)
      const t = setTimeout(() => inputRef.current?.focus(), 30)
      return () => clearTimeout(t)
    }
  }, [open])
  useEffect(() => setActive(0), [q])

  const run = (item?: CmdItem) => {
    if (!item) return
    item.run()
    onClose()
  }

  // Arrow / enter / esc while open.
  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose()
      else if (e.key === 'ArrowDown') {
        e.preventDefault()
        setActive((a) => Math.min(filtered.length - 1, a + 1))
      } else if (e.key === 'ArrowUp') {
        e.preventDefault()
        setActive((a) => Math.max(0, a - 1))
      } else if (e.key === 'Enter') {
        e.preventDefault()
        run(filtered[active])
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [open, filtered, active])

  if (!open) return null

  let lastGroup: string | null = null
  let i = -1
  return (
    <div className="cmdk-scrim" onClick={onClose}>
      <div className="cmdk" onClick={(e) => e.stopPropagation()}>
        <div className="cmdk-search">
          <Icon paths={SEARCH_PATH} size={18} strokeWidth={1.6} />
          <input
            ref={inputRef}
            placeholder="Search screens, namespaces, actions…"
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
          <span className="kbd">esc</span>
        </div>
        <div className="cmdk-list">
          {filtered.length === 0 && <div className="cmdk-empty">No results for “{q}”</div>}
          {filtered.map((item) => {
            i++
            const idx = i
            const showGroup = item.group !== lastGroup
            lastGroup = item.group
            return (
              <div key={item.key}>
                {showGroup && <div className="cmdk-group">{item.group}</div>}
                <button
                  className={'cmdk-item' + (idx === active ? ' active' : '')}
                  onMouseEnter={() => setActive(idx)}
                  onClick={() => run(item)}
                >
                  <span className="ci-ic">
                    <Icon paths={item.icon} size={16} />
                  </span>
                  <span>{item.name}</span>
                  {item.meta && <span className="ci-tail">{item.meta}</span>}
                </button>
              </div>
            )
          })}
        </div>
        <div className="cmdk-foot">
          <span>
            <span className="kbd">↑</span> <span className="kbd">↓</span> navigate
          </span>
          <span>
            <span className="kbd">↵</span> select
          </span>
          <span>
            <span className="kbd">esc</span> close
          </span>
        </div>
      </div>
    </div>
  )
}
