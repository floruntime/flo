import { useEffect, useMemo, useRef, useState } from 'react'
import { cfmt, jhl, tfmt } from '@/lib/format'
import { useNamespace } from '@/lib/namespace'
import { useStreamDetail, useStreamMessages } from '@/lib/api/streams'
import { Crumb, Stats, Stat } from '@/components/layout'
import { Button } from '@/components/buttons/Button'
import { Pill, Dot } from '@/components/feedback'
import { DataList } from '@/components/data'

type Rec = { id: string; ts: number; seq: number; size: number; payload: string; bucket: number }
type Tab = 'data' | 'groups' | 'pel'

const CATS = ['var(--cat-2)', 'var(--cat-3)', 'var(--cat-4)', 'var(--cat-5)', 'var(--cat-6)', 'var(--cat-1)']

export function StreamDetail({ name, onBack }: { name: string; onBack: () => void }) {
  const { ns } = useNamespace()
  const detailQ = useStreamDetail(ns, name)
  const msgsQ = useStreamMessages(ns, name)
  const [tab, setTab] = useState<Tab>('data')
  const [cursor, setCursor] = useState(0)
  const [picked, setPicked] = useState<Rec | null>(null)
  const seekRef = useRef<HTMLDivElement>(null)
  const dragRef = useRef<'s' | 'e' | 'c' | null>(null)

  const detail = detailQ.data
  const recordCount = detail?.partitions?.[0]?.record_count ?? msgsQ.data?.total_count ?? 0

  // Build records (ascending) + bucket them by TIME (a real activity histogram:
  // bursts → tall bars, quiet stretches → short). Bar height = total bytes per
  // bucket (data volume), so both arrival rate and payload size drive the shape.
  // Falls back to even index spacing when all records share one timestamp.
  const { records, N, buckets, bucketBytes } = useMemo(() => {
    const msgs = [...(msgsQ.data?.messages ?? [])].sort((a, b) => a.id_ms - b.id_ms || a.id_seq - b.id_seq)
    const n = Math.max(1, Math.min(80, msgs.length))
    const t0 = msgs.length ? msgs[0].id_ms : 0
    const span = msgs.length ? msgs[msgs.length - 1].id_ms - t0 : 0
    const recs: Rec[] = msgs.map((m, i) => ({
      id: `${m.id_ms}-${m.id_seq}`,
      ts: m.id_ms,
      seq: m.id_seq,
      size: m.size,
      payload: m.payload,
      bucket:
        span > 0
          ? Math.min(n - 1, Math.floor(((m.id_ms - t0) / span) * (n - 1)))
          : msgs.length <= 1
            ? 0
            : Math.min(n - 1, Math.floor((i / (msgs.length - 1)) * (n - 1))),
    }))
    const bkts = new Array(n).fill(0)
    const bytes = new Array(n).fill(0)
    recs.forEach((r) => {
      bkts[r.bucket] += 1
      bytes[r.bucket] += r.size
    })
    return { records: recs, N: n, buckets: bkts, bucketBytes: bytes }
  }, [msgsQ.data])

  const [range, setRange] = useState({ start: 0, end: 0 })
  useEffect(() => {
    setRange({ start: 0, end: N - 1 })
    if (records.length) {
      setCursor(records[records.length - 1].bucket)
      setPicked(records[records.length - 1])
    } else {
      setPicked(null)
    }
  }, [records, N])

  // Map each consumer group onto a bucket by its last-delivered timestamp.
  const gpos = useMemo(() => {
    const groups = detail?.consumer_groups ?? []
    return groups.map((g, k) => {
      let bucket = 0
      for (let i = 0; i < records.length; i++) {
        if (records[i].ts <= g.last_delivered_ms) bucket = records[i].bucket
        else break
      }
      if (g.last_delivered_ms === 0) bucket = 0
      const lag = records.filter((r) => r.ts > g.last_delivered_ms).length
      return { ...g, color: CATS[k % CATS.length], bucket, lag, pct: ((bucket + 0.5) / N) * 100 }
    })
  }, [detail, records, N])

  const maxBytes = Math.max(1, ...bucketBytes)
  const maxLag = Math.max(0, ...gpos.map((g) => g.lag))
  const curPct = ((cursor + 0.5) / N) * 100
  const startPct = N > 1 ? (range.start / (N - 1)) * 100 : 0
  const endPct = N > 1 ? (range.end / (N - 1)) * 100 : 100
  const fullRange = range.start === 0 && range.end === N - 1
  const inRange = useMemo(() => records.filter((r) => r.bucket >= range.start && r.bucket <= range.end), [records, range])
  const listView = useMemo(() => [...inRange].reverse(), [inRange])

  const idxFromX = (clientX: number) => {
    const r = seekRef.current!.getBoundingClientRect()
    return Math.max(0, Math.min(N - 1, Math.round(((clientX - r.left) / r.width) * (N - 1))))
  }
  useEffect(() => {
    const mv = (e: MouseEvent) => {
      if (!dragRef.current) return
      const i = idxFromX(e.clientX)
      if (dragRef.current === 's') setRange((r) => ({ start: Math.min(i, r.end - 1), end: r.end }))
      else if (dragRef.current === 'e') setRange((r) => ({ start: r.start, end: Math.max(i, r.start + 1) }))
      else setCursor(i)
    }
    const up = () => (dragRef.current = null)
    window.addEventListener('mousemove', mv)
    window.addEventListener('mouseup', up)
    return () => {
      window.removeEventListener('mousemove', mv)
      window.removeEventListener('mouseup', up)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [N])

  const pickBucket = (b: number) => {
    setCursor(b)
    const inB = records.filter((r) => r.bucket === b)
    if (inB.length) setPicked(inB[inB.length - 1])
  }
  const payloadObj = (() => {
    if (!picked) return null
    try {
      return JSON.parse(picked.payload)
    } catch {
      return null
    }
  })()

  return (
    <div className="wrap fade">
      <Crumb root="Streams" onRoot={onBack} current={name} />
      <div className="ph">
        <h1 className="ph-t">{name}</h1>
        <div className="ph-s">
          {detail?.namespace ?? ns} · {gpos.length} consumer group{gpos.length === 1 ? '' : 's'} · append-only log
        </div>
      </div>

      <Stats>
        <Stat label="Records" value={cfmt(recordCount)} sub="in stream" />
        <Stat label="Consumer groups" value={gpos.length} />
        <Stat label="Max lag" value={cfmt(maxLag)} valueColor={maxLag > 0 ? 'var(--warn)' : undefined} sub={maxLag > 0 ? 'behind' : 'caught up'} />
        <Stat label="Loaded" value={records.length} sub="recent window" />
      </Stats>

      {/* activity tape — group cursors + playhead + draggable range seeker */}
      <div className="sec">
        <div className="sec-h">
          <h2>Activity</h2>
          <span className="meta">{records.length} records · click or drag to inspect</span>
        </div>
        <div className="tape2">
          <div className="tape2-bars">
            {buckets.map((c, i) => (
              <i
                key={i}
                className={i === cursor ? 'a' : ''}
                style={{ height: Math.max(6, (bucketBytes[i] / maxBytes) * 100) + '%' }}
                onClick={() => pickBucket(i)}
                title={`${c} record(s) · ${bucketBytes[i]} B`}
              />
            ))}
          </div>
          <div className="tape2-ov">
            {range.start > 0 && <div className="tape-dim" style={{ left: 0, width: (range.start / N) * 100 + '%' }} />}
            {range.end < N - 1 && <div className="tape-dim" style={{ right: 0, width: (1 - (range.end + 1) / N) * 100 + '%' }} />}
            {gpos.map((g) => (
              <div key={g.name} className="gc" style={{ left: g.pct + '%', ['--gc' as string]: g.color }} title={`${g.name} · lag ${g.lag}`} />
            ))}
            <div className="ph2" style={{ left: curPct + '%' }} />
          </div>
        </div>
        <div
          className="seeker"
          ref={seekRef}
          onMouseDown={(e) => {
            if ((e.target as HTMLElement).closest('.seeker-h')) return
            dragRef.current = 'c'
            setCursor(idxFromX(e.clientX))
          }}
        >
          <div className="seeker-dim" style={{ left: 0, width: startPct + '%' }} />
          <div className="seeker-dim" style={{ right: 0, width: 100 - endPct + '%' }} />
          <div className="seeker-win" style={{ left: startPct + '%', width: endPct - startPct + '%' }} />
          {gpos.map((g) => (
            <span key={g.name} className="seeker-tick" style={{ left: g.pct + '%', background: g.color }} />
          ))}
          <span className="seeker-cur" style={{ left: curPct + '%' }} />
          <div className="seeker-h" style={{ left: startPct + '%' }} onMouseDown={(e) => { e.stopPropagation(); dragRef.current = 's' }} />
          <div className="seeker-h" style={{ left: endPct + '%' }} onMouseDown={(e) => { e.stopPropagation(); dragRef.current = 'e' }} />
        </div>
        {records.length > 0 && (
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginTop: 9, fontSize: 12, color: 'var(--tx-faint)', fontFamily: 'var(--mono)' }}>
            <span>{tfmt(records[Math.min(range.start, records.length - 1)]?.ts ?? records[0].ts)}</span>
            {fullRange ? (
              <span>{tfmt(records[records.length - 1].ts)}</span>
            ) : (
              <Button variant="quiet" style={{ padding: '2px 8px', fontSize: 11.5, whiteSpace: 'nowrap' }} onClick={() => setRange({ start: 0, end: N - 1 })}>
                reset range · {inRange.length}/{records.length}
              </Button>
            )}
          </div>
        )}
      </div>

      {/* tabs */}
      <div className="sec">
        <div style={{ display: 'flex', gap: 22, borderBottom: '1px solid var(--line-soft)', marginBottom: 18 }}>
          {(
            [
              ['data', 'Stream data' + (fullRange ? '' : ' · ' + inRange.length)],
              ['groups', 'Consumer groups · ' + gpos.length],
              ['pel', 'Pending'],
            ] as [Tab, string][]
          ).map(([id, lbl]) => (
            <button
              key={id}
              onClick={() => setTab(id)}
              style={{
                background: 'none', border: 'none', font: 'inherit', fontSize: 13.5, cursor: 'pointer',
                padding: '0 0 12px', whiteSpace: 'nowrap', color: tab === id ? 'var(--tx)' : 'var(--tx-3)',
                fontWeight: tab === id ? 600 : 400, borderBottom: tab === id ? '2px solid var(--accent)' : '2px solid transparent', marginBottom: -1,
              }}
            >
              {lbl}
            </button>
          ))}
        </div>

        {tab === 'data' && (
          <div style={{ display: 'grid', gridTemplateColumns: '1.5fr 1fr', gap: 20, alignItems: 'start' }}>
            <div>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 10 }}>
                <span className="mono" style={{ fontSize: 12, color: 'var(--tx-3)' }}>
                  {msgsQ.isLoading ? 'loading…' : `${listView.length.toLocaleString()} records${fullRange ? '' : ' in range'}`}
                </span>
                <span className="mono" style={{ fontSize: 11, color: 'var(--tx-faint)' }}>newest first</span>
              </div>
              {records.length === 0 ? (
                <div className="card card-p" style={{ textAlign: 'center', color: 'var(--tx-3)', padding: '40px 0' }}>
                  {msgsQ.isLoading ? 'Loading…' : 'No records.'}
                </div>
              ) : (
                <DataList records={listView} picked={picked} onPick={(r) => { setPicked(r); setCursor(r.bucket) }} />
              )}
            </div>
            {picked && (
              <div className="card card-p">
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginBottom: 4 }}>
                  <span className="mono" style={{ fontSize: 12.5, color: 'var(--tx-2)' }}>{picked.id}</span>
                  <Pill tone="ok">record</Pill>
                </div>
                <div style={{ fontSize: 11.5, color: 'var(--tx-faint)', marginBottom: 14 }}>{tfmt(picked.ts)} · {picked.size} bytes</div>
                {payloadObj ? (
                  <pre className="code" dangerouslySetInnerHTML={{ __html: jhl(payloadObj) }} />
                ) : (
                  <pre className="code" style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>{picked.payload || '(empty)'}</pre>
                )}
              </div>
            )}
          </div>
        )}

        {tab === 'groups' && (
          <div className="card card-p" style={{ padding: '6px 20px 12px' }}>
            {gpos.length === 0 ? (
              <div style={{ padding: '28px 0', textAlign: 'center', color: 'var(--tx-3)', fontSize: 13 }}>No consumer groups.</div>
            ) : (
              <>
                <div className="grow head" style={{ gridTemplateColumns: 'minmax(150px,1.6fr) 90px 90px 90px 130px' }}>
                  <div>Group</div>
                  <div className="r">Members</div>
                  <div className="r">Pending</div>
                  <div className="r">Lag</div>
                  <div className="r">Last delivered</div>
                </div>
                {gpos.map((g) => (
                  <div className="grow" key={g.name} style={{ gridTemplateColumns: 'minmax(150px,1.6fr) 90px 90px 90px 130px' }}>
                    <span style={{ display: 'flex', alignItems: 'center', gap: 12, minWidth: 0 }}>
                      <Dot color={g.color} style={{ flex: 'none' }} />
                      <span className="gn" style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{g.name}</span>
                    </span>
                    <span className="r mono" style={{ color: 'var(--tx-2)', fontSize: 13 }}>{g.members}</span>
                    <span className="r mono" style={{ fontSize: 13, color: g.pending_count > 0 ? 'var(--warn)' : 'var(--tx-3)' }}>{g.pending_count}</span>
                    <span className="r mono" style={{ fontSize: 13, color: g.lag > 0 ? 'var(--warn)' : 'var(--accent)' }}>{g.lag}</span>
                    <span className="r mono" style={{ fontSize: 11.5, color: 'var(--tx-3)' }}>
                      {g.last_delivered_ms ? tfmt(g.last_delivered_ms) : '—'}
                    </span>
                  </div>
                ))}
              </>
            )}
          </div>
        )}

        {tab === 'pel' && (
          <div className="card card-p" style={{ color: 'var(--tx-3)', fontSize: 13.5, textAlign: 'center', padding: '40px 24px' }}>
            {gpos.reduce((a, g) => a + g.pending_count, 0) > 0
              ? `${gpos.reduce((a, g) => a + g.pending_count, 0)} pending entries across ${gpos.length} group(s).`
              : 'No pending entries — all groups have acknowledged.'}
          </div>
        )}
      </div>
    </div>
  )
}
