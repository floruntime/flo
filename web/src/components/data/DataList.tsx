import { useEffect, useRef, useState } from 'react'
import { tfmt } from '@/lib/format'
import './data.css'

export type DataRecord = {
  id: string
  ts: number
  size: number
}

export type DataListProps<T extends DataRecord> = {
  records: T[]
  picked?: T | null
  onPick: (r: T) => void
}

/** Infinite-scroll record list with drag-to-scroll + flick momentum (`.dlist`). */
export function DataList<T extends DataRecord>({ records, picked, onPick }: DataListProps<T>) {
  const [count, setCount] = useState(60)
  const boxRef = useRef<HTMLDivElement>(null)
  const drag = useRef<{ y: number; top: number; vy: number; ly: number; lt: number; moved: boolean } | null>(null)

  useEffect(() => {
    setCount(60)
    if (boxRef.current) boxRef.current.scrollTop = 0
  }, [records])

  const onScroll = () => {
    const el = boxRef.current
    if (!el) return
    if (el.scrollHeight - el.scrollTop - el.clientHeight < 240)
      setCount((c) => Math.min(records.length, c + 60))
  }

  const down = (e: React.MouseEvent) => {
    const el = boxRef.current
    if (!el) return
    drag.current = { y: e.clientY, top: el.scrollTop, vy: 0, ly: e.clientY, lt: performance.now(), moved: false }
    el.classList.add('grabbing')
  }

  useEffect(() => {
    const move = (e: MouseEvent) => {
      const d = drag.current
      if (!d) return
      const el = boxRef.current
      if (!el) return
      const dy = e.clientY - d.y
      if (Math.abs(dy) > 3) d.moved = true
      el.scrollTop = d.top - dy
      const now = performance.now()
      const dt = now - d.lt
      if (dt > 0) {
        d.vy = (e.clientY - d.ly) / dt
        d.ly = e.clientY
        d.lt = now
      }
    }
    const up = () => {
      const d = drag.current
      if (!d) return
      const el = boxRef.current
      if (el) el.classList.remove('grabbing')
      let v = d.vy * 16
      const decay = () => {
        if (Math.abs(v) < 0.4 || !boxRef.current) return
        boxRef.current.scrollTop -= v
        v *= 0.92
        onScroll()
        requestAnimationFrame(decay)
      }
      if (Math.abs(v) > 1) requestAnimationFrame(decay)
      drag.current = null
    }
    window.addEventListener('mousemove', move)
    window.addEventListener('mouseup', up)
    return () => {
      window.removeEventListener('mousemove', move)
      window.removeEventListener('mouseup', up)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  return (
    <div className="dlist" ref={boxRef} onScroll={onScroll} onMouseDown={down}>
      <div className="li head dl-row" style={{ position: 'sticky', top: 0, zIndex: 2 }}>
        <div className="c">Entry</div>
        <div className="c r">Size</div>
      </div>
      {records.slice(0, count).map((r) => (
        <div
          className="li dl-row"
          key={r.id}
          style={{ background: picked && picked.id === r.id ? 'var(--hover)' : undefined }}
          onClick={() => {
            if (!drag.current || !drag.current.moved) onPick(r)
          }}
        >
          <div>
            <div style={{ fontSize: 12.5, color: 'var(--tx-2)' }}>{tfmt(r.ts)}</div>
            <div className="mono" style={{ fontSize: 11, color: 'var(--tx-faint)', marginTop: 2 }}>
              {r.id}
            </div>
          </div>
          <div className="c r dim mono">{r.size} B</div>
        </div>
      ))}
      {count < records.length && (
        <div className="dl-more">{(records.length - count).toLocaleString()} more · scroll or drag to load</div>
      )}
    </div>
  )
}
