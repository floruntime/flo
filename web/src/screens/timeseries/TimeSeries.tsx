/* Flo Console v2 — Time Series screen (live).
   Columnar engine · line-protocol ingest · FloQL pipelines.

   Wired to the dashboard API. The TS projection keys write buffers by
   `measurement\0field\0tag_hash`, so tags are a real series dimension: this
   screen renders measurements, their fields, per-field point counts, and raw
   point charts from /data (add `?tags=k=v` to scope to one tag-series). FloQL
   executes server-side (parser + pipeline executor) and its source tag filters
   are fully applied — partial sets, `!=`, and `=~`/`!~` globs all select series
   via the projection's tag dictionary.
   Ingest is command-only (no write endpoint); measurements are namespace-scoped
   server-side (`?namespace=`). See API_INTEGRATION.md. */
import { useState, useEffect, useMemo, useRef } from 'react'
import { Modal } from '@/components/overlay/Modal'
import { Crumb, Stats, Stat } from '@/components/layout'
import { Button } from '@/components/buttons/Button'
import { cfmt } from '@/lib/format'
import { useNamespace } from '@/lib/namespace'
import { useMeasurements, useMeasurementDetail, useSeriesData, useFloql } from '@/lib/api/ts'
import type { TsMeasurement } from '@/lib/api/types'
import './timeseries.css'

/* ---- local formatters ---- */
const vfmt = (v: number): string =>
  v >= 1000 ? (v / 1000).toFixed(1) + 'k' : v >= 100 ? String(Math.round(v)) : v >= 10 ? v.toFixed(0) : v.toFixed(1)
const hhmm = (ms: number): string =>
  new Date(ms).toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' })
const CATS = ['var(--cat-1)', 'var(--cat-2)', 'var(--cat-3)', 'var(--cat-4)', 'var(--cat-5)', 'var(--cat-6)']

type ChartDataset = { label: string; color: string; values: number[]; dots?: boolean }

/* ======================= chart ======================= */
function useW(): [React.RefObject<HTMLDivElement | null>, number] {
  const ref = useRef<HTMLDivElement>(null)
  const [w, setW] = useState(760)
  useEffect(() => {
    const el = ref.current
    if (!el) return
    const ro = new ResizeObserver((es) => {
      for (const e of es) setW(e.contentRect.width)
    })
    ro.observe(el)
    setW(el.clientWidth || 760)
    return () => ro.disconnect()
  }, [])
  return [ref, w]
}

type TSChartProps = { datasets: ChartDataset[]; times?: number[]; unit: string; height?: number }

function TSChart({ datasets, times, unit, height = 236 }: TSChartProps) {
  const [ref, w] = useW()
  const [hi, setHi] = useState<number | null>(null)
  const W = Math.max(w, 320),
    H = height,
    L = 48,
    R = 16,
    T = 16,
    B = 28
  const pw = W - L - R,
    ph = H - T - B
  const n = datasets[0]?.values.length || 0
  let mn = Infinity,
    mx = -Infinity
  datasets.forEach((d) =>
    d.values.forEach((v) => {
      if (v < mn) mn = v
      if (v > mx) mx = v
    }),
  )
  if (!isFinite(mn)) {
    mn = 0
    mx = 1
  }
  const span = mx - mn || 1
  mn = Math.max(0, mn - span * 0.12)
  mx = mx + span * 0.12
  const X = (i: number) => L + (n <= 1 ? 0 : (i / (n - 1)) * pw)
  const Y = (v: number) => T + (1 - (v - mn) / (mx - mn)) * ph
  const grid = [0, 0.25, 0.5, 0.75, 1].map((f) => mn + (mx - mn) * f)
  const xticks = n ? [0, Math.floor((n - 1) * 0.25), Math.floor((n - 1) * 0.5), Math.floor((n - 1) * 0.75), n - 1] : []
  const onMove = (e: React.MouseEvent) => {
    if (!ref.current) return
    const r = ref.current.getBoundingClientRect()
    const px = e.clientX - r.left
    let idx = Math.round(((px - L) / pw) * (n - 1))
    idx = Math.max(0, Math.min(n - 1, idx))
    setHi(idx)
  }
  return (
    <div ref={ref} style={{ position: 'relative', width: '100%' }} onMouseLeave={() => setHi(null)} onMouseMove={onMove}>
      <svg width={W} height={H} style={{ display: 'block' }}>
        {grid.map((g, i) => (
          <g key={i}>
            <line x1={L} x2={W - R} y1={Y(g)} y2={Y(g)} stroke="var(--line-soft)" strokeWidth="1" />
            <text x={L - 9} y={Y(g) + 3.5} textAnchor="end" fontSize="10.5" fontFamily="var(--mono)" fill="var(--tx-faint)">
              {vfmt(g)}
            </text>
          </g>
        ))}
        {xticks.map((xi, i) => (
          <text
            key={i}
            x={X(xi)}
            y={H - 9}
            textAnchor={i === 0 ? 'start' : i === xticks.length - 1 ? 'end' : 'middle'}
            fontSize="10.5"
            fontFamily="var(--mono)"
            fill="var(--tx-faint)"
          >
            {times ? hhmm(times[xi]) : xi}
          </text>
        ))}
        {datasets.map((d, di) => {
          if (d.dots) {
            return (
              <g key={di}>
                {d.values.map((v, i) => (
                  <circle key={i} cx={X(i)} cy={Y(v)} r="3" fill={d.color} />
                ))}
              </g>
            )
          }
          const path = d.values.map((v, i) => `${i ? 'L' : 'M'}${X(i).toFixed(1)} ${Y(v).toFixed(1)}`).join(' ')
          const area = `${path} L${X(n - 1).toFixed(1)} ${(T + ph).toFixed(1)} L${X(0).toFixed(1)} ${(T + ph).toFixed(1)} Z`
          return (
            <g key={di}>
              {di === 0 && datasets.length === 1 && (
                <path d={area} fill={`color-mix(in srgb, ${d.color} 13%, transparent)`} />
              )}
              <path d={path} fill="none" stroke={d.color} strokeWidth="1.6" strokeLinejoin="round" strokeLinecap="round" />
            </g>
          )
        })}
        {hi != null && <line x1={X(hi)} x2={X(hi)} y1={T} y2={T + ph} stroke="var(--line)" strokeWidth="1" />}
        {hi != null &&
          datasets
            .filter((d) => !d.dots)
            .map((d, di) => (
              <circle key={di} cx={X(hi)} cy={Y(d.values[hi])} r="3.2" fill="var(--card)" stroke={d.color} strokeWidth="1.6" />
            ))}
      </svg>
      {hi != null && times && (
        <div
          style={{
            position: 'absolute',
            top: 8,
            left: Math.min(Math.max(X(hi) + 10, L), W - 176),
            background: 'var(--card-2)',
            border: '1px solid var(--line)',
            borderRadius: 'var(--r-sm)',
            padding: '8px 10px',
            pointerEvents: 'none',
            minWidth: 120,
            boxShadow: '0 4px 16px rgba(0,0,0,0.35)',
          }}
        >
          <div style={{ fontSize: 11, fontFamily: 'var(--mono)', color: 'var(--tx-faint)', marginBottom: 5 }}>
            {hhmm(times[hi])}
          </div>
          {datasets.map((d, di) => (
            <div
              key={di}
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: 14,
                fontSize: 12,
                marginTop: di ? 3 : 0,
              }}
            >
              <span style={{ display: 'flex', alignItems: 'center', gap: 6, color: 'var(--tx-2)' }}>
                <span style={{ width: 7, height: 7, borderRadius: 2, background: d.color }}></span>
                {d.label}
              </span>
              <span className="mono" style={{ color: 'var(--tx)' }}>
                {vfmt(d.values[hi])}
                {unit && <span style={{ color: 'var(--tx-faint)' }}> {unit}</span>}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

/* ======================= FloQL console ======================= */
function floHL(q: string): string {
  let s = q.replace(/&/g, '&amp;').replace(/</g, '&lt;')
  s = s.replace(/^(\w+)/, '<span style="color:var(--tx);font-weight:600">$1</span>')
  s = s.replace(/(\{[^}]*\})/g, '<span style="color:var(--info)">$1</span>')
  s = s.replace(/(\[[^\]]*\])/g, '<span style="color:var(--accent)">$1</span>')
  s = s.replace(/\|\s*([a-z_]+)/g, '| <span style="color:var(--cat-3)">$1</span>')
  return s
}

function FloQLConsole({ ns, measurements }: { ns: string; measurements: TsMeasurement[] }) {
  const m0 = measurements[0]?.name ?? 'cpu_usage'
  const samples = useMemo(
    () => [
      `${m0}[1h] | window(5m) | avg()`,
      `${m0}{host=web-01}[1h] | rate(1m)`,
      `${m0}[1h] | group_by(host) | window(5m) | max()`,
    ],
    [m0],
  )
  const [q, setQ] = useState(samples[0])
  const floql = useFloql(ns)
  useEffect(() => setQ(samples[0]), [samples])

  const run = (text?: string) => {
    const query = text ?? q
    if (text) setQ(text)
    floql.mutate(query)
  }

  const res = floql.data
  // The server percent-decodes `?q=` before parsing and echoes the decoded
  // text, so this is already the original query.
  const echoed = res?.query ?? ''
  return (
    <div className="card" style={{ overflow: 'hidden', marginBottom: 20 }}>
      <div className="card-p" style={{ paddingBottom: 14, borderBottom: '1px solid var(--line-soft)' }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
          <span style={{ display: 'flex', alignItems: 'center', gap: 9, fontSize: 13, fontWeight: 600, color: 'var(--tx)' }}>
            <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="var(--accent)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
              <path d="M4 17l6-6-6-6" />
              <path d="M12 19h8" />
            </svg>
            FloQL
          </span>
          <span className="mono" style={{ fontSize: 11, color: 'var(--tx-faint)' }}>
            read-only · ns={ns}
          </span>
        </div>
        <div style={{ display: 'flex', gap: 7, flexWrap: 'wrap', marginBottom: 12 }}>
          {samples.map((s, i) => (
            <button key={i} className={'ts-chip' + (q === s ? ' on' : '')} onClick={() => run(s)}>
              {['avg', 'rate', 'group_by'][i]}
            </button>
          ))}
        </div>
        <div className="ts-query">
          <code
            className="mono"
            style={{ flex: 1, minWidth: 0 }}
            dangerouslySetInnerHTML={{ __html: floHL(q) }}
          ></code>
          <button className="btn acc" style={{ flex: 'none', height: 32 }} onClick={() => run()}>
            {floql.isPending ? (
              <span className="ts-spin"></span>
            ) : (
              <svg viewBox="0 0 24 24" width="14" height="14" fill="none" stroke="currentColor" strokeWidth="1.7" strokeLinecap="round" strokeLinejoin="round">
                <path d="M5 4l14 8-14 8z" />
              </svg>
            )}
            Run
          </button>
        </div>
      </div>
      <div className="card-p">
        {floql.isError ? (
          <div className="mono" style={{ fontSize: 12.5, color: 'var(--crit)' }}>
            {String((floql.error as Error)?.message ?? 'Query failed')}
          </div>
        ) : res?.error ? (
          <div className="mono" style={{ fontSize: 12.5, color: 'var(--crit)' }}>
            {res.error}
          </div>
        ) : res ? (
          <div>
            <div className="mono" style={{ fontSize: 12.5, color: 'var(--tx-2)', marginBottom: 10 }}>
              {res.series.length} series ·{' '}
              {res.series.reduce((a, s) => a + s.point_count, 0)} points ·{' '}
              <span style={{ color: 'var(--tx)' }}>{echoed}</span>
            </div>
            {res.series.length === 0 ? (
              <div className="mono" style={{ fontSize: 12.5, color: 'var(--tx-faint)' }}>
                No series matched.
              </div>
            ) : (
              res.series.map((s, i) => (
                <div key={`${s.key}-${s.field}-${i}`} style={{ marginBottom: 12 }}>
                  <div className="mono" style={{ fontSize: 12, color: 'var(--tx)', marginBottom: 5 }}>
                    {s.key}
                    <span style={{ color: 'var(--tx-faint)' }}>·{s.field}</span>
                    {s.tags.map((t) => (
                      <span key={t.key} style={{ color: 'var(--info)' }}>
                        {' '}
                        {t.key}={t.value}
                      </span>
                    ))}
                  </div>
                  <div
                    style={{
                      maxHeight: 190,
                      overflowY: 'auto',
                      background: 'var(--card-2)',
                      border: '1px solid var(--line-soft)',
                      borderRadius: 'var(--r-sm)',
                      padding: '8px 11px',
                    }}
                  >
                    {s.points.slice(0, 200).map((p, j) => (
                      <div
                        key={j}
                        className="mono"
                        style={{ display: 'flex', justifyContent: 'space-between', gap: 16, fontSize: 12, color: 'var(--tx-3)' }}
                      >
                        <span>{new Date(p.timestamp).toISOString()}</span>
                        <span style={{ color: 'var(--tx)' }}>{p.value}</span>
                      </div>
                    ))}
                    {s.points.length > 200 ? (
                      <div className="mono" style={{ fontSize: 11.5, color: 'var(--tx-faint)', marginTop: 5 }}>
                        … {s.points.length - 200} more
                      </div>
                    ) : null}
                  </div>
                </div>
              ))
            )}
          </div>
        ) : (
          <div className="mono" style={{ fontSize: 12.5, color: 'var(--tx-faint)' }}>
            Run a query to send it to the engine.
          </div>
        )}
      </div>
    </div>
  )
}

/* ======================= ingest composer (command-only) ======================= */
function IngestModal({ ns, meas, onClose }: { ns: string; meas: string; onClose: () => void }) {
  const [mode, setMode] = useState('single')
  const [m, setM] = useState(meas)
  const [tags, setTags] = useState('host=web-01,region=us-east')
  const [val, setVal] = useState('82.5')
  const [fields, setFields] = useState('user=72.5,system=7.4,idle=20.1')
  const [prec, setPrec] = useState('ms')
  const batch = `cpu,host=web-01 user=72.5,system=7.4 1708700400000\ncpu,host=web-02 user=44.1,system=3.2 1708700400000\nmemory,host=web-01 used=68.2,free=31.8 1708700400000`
  const cmd =
    mode === 'single'
      ? `flo ts write ${m} --tags ${tags} --value ${val} -n ${ns}`
      : mode === 'multi'
        ? `flo ts write ${m} --tags ${tags} --fields "${fields}" -n ${ns}`
        : `cat data.txt | flo ts write --batch --precision ${prec} -n ${ns}`
  return (
    <Modal
      title="Write point"
      sub="Single value, multi-field, or InfluxDB line protocol — issued via the CLI (no dashboard write endpoint)"
      width="min(640px,96vw)"
      onClose={onClose}
      foot={
        <>
          <Button onClick={onClose}>Close</Button>
          <Button variant="accent" onClick={() => navigator.clipboard?.writeText(cmd)}>
            Copy command
          </Button>
        </>
      }
    >
      <div className="seg-group" style={{ marginBottom: 18 }}>
        {[
          ['single', 'Single field'],
          ['multi', 'Multi-field'],
          ['batch', 'Batch · line protocol'],
        ].map(([id, l]) => (
          <button key={id} className={'seg-btn' + (mode === id ? ' on' : '')} onClick={() => setMode(id)}>
            {l}
          </button>
        ))}
      </div>
      {mode !== 'batch' ? (
        <>
          <div className="ts-frow">
            <div className="set-h" style={{ padding: 0 }}>
              Measurement
            </div>
            <div className="field" style={{ width: 240 }}>
              <input className="mono" value={m} onChange={(e) => setM(e.target.value)} />
            </div>
          </div>
          <div className="ts-frow">
            <div className="set-h" style={{ padding: 0 }}>
              Tags
            </div>
            <div className="field" style={{ width: 300 }}>
              <input className="mono" value={tags} onChange={(e) => setTags(e.target.value)} placeholder="host=web-01" />
            </div>
          </div>
          {mode === 'single' ? (
            <div className="ts-frow">
              <div className="set-h" style={{ padding: 0 }}>
                Value
              </div>
              <div className="field" style={{ width: 160 }}>
                <input className="mono" value={val} onChange={(e) => setVal(e.target.value)} />
              </div>
            </div>
          ) : (
            <div className="ts-frow">
              <div className="set-h" style={{ padding: 0 }}>
                Fields
              </div>
              <div className="field" style={{ width: 300 }}>
                <input className="mono" value={fields} onChange={(e) => setFields(e.target.value)} placeholder="user=72.5,system=7.4" />
              </div>
            </div>
          )}
        </>
      ) : (
        <>
          <div className="set-h" style={{ padding: 0, marginBottom: 7 }}>
            Line protocol
          </div>
          <textarea className="kv-editor mono" style={{ width: '100%', minHeight: 96, resize: 'vertical' }} defaultValue={batch}></textarea>
          <div className="ts-frow" style={{ marginTop: 14 }}>
            <div className="set-h" style={{ padding: 0 }}>
              Precision
            </div>
            <div className="seg-group">
              {['ns', 'us', 'ms', 's'].map((pp) => (
                <button key={pp} className={'seg-btn' + (prec === pp ? ' on' : '')} onClick={() => setPrec(pp)}>
                  {pp}
                </button>
              ))}
            </div>
          </div>
        </>
      )}
      <div className="set-h" style={{ padding: 0, margin: '18px 0 7px' }}>
        Command
      </div>
      <pre className="code">{cmd}</pre>
    </Modal>
  )
}

/* ======================= per-field chart (live /data) ======================= */
function FieldChart({ ns, measurement, field }: { ns: string; measurement: string; field: string }) {
  const q = useSeriesData(ns, measurement, field)
  const points = useMemo(
    () => [...(q.data?.series ?? [])].sort((a, b) => a.timestamp - b.timestamp),
    [q.data],
  )
  const times = points.map((p) => p.timestamp)
  const datasets: ChartDataset[] = [{ label: field, color: CATS[0], values: points.map((p) => p.value) }]
  const last = points.length ? points[points.length - 1].value : null

  return (
    <div className="card" style={{ overflow: 'hidden', marginBottom: 16 }}>
      <div className="card-p" style={{ paddingBottom: 8, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <span style={{ display: 'flex', alignItems: 'center', gap: 7, fontSize: 12.5, color: 'var(--tx-2)' }}>
          <span style={{ width: 8, height: 8, borderRadius: 2, background: CATS[0] }}></span>
          <span className="mono">{field}</span>
          {last != null && (
            <span className="mono" style={{ color: 'var(--tx)' }}>
              {vfmt(last)}
            </span>
          )}
        </span>
        <span className="mono" style={{ fontSize: 11, color: 'var(--tx-faint)' }}>
          {q.isLoading ? 'loading…' : `${points.length} points · raw`}
        </span>
      </div>
      <div className="card-p" style={{ paddingTop: 4 }}>
        {points.length === 0 ? (
          <div className="mono" style={{ fontSize: 12.5, color: 'var(--tx-faint)', padding: '28px 0', textAlign: 'center' }}>
            {q.isLoading ? 'Loading points…' : 'No points in the retained window.'}
          </div>
        ) : (
          <TSChart datasets={datasets} times={times} unit="" />
        )}
      </div>
    </div>
  )
}

/* ======================= measurement detail ======================= */
function MeasDetail({ ns, row, onBack }: { ns: string; row: TsMeasurement; onBack: () => void }) {
  const q = useMeasurementDetail(ns, row.name)
  const detail = q.data
  const fields = detail?.fields ?? []

  return (
    <div className="wrap fade">
      <Crumb root="Time Series" onRoot={onBack} current={row.name} />
      <div className="ph">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16, flexWrap: 'wrap' }}>
          <div>
            <h1 className="ph-t">{row.name}</h1>
            <div className="ph-s">
              {fields.length === 1 ? 'single field' : (detail?.field_count ?? row.field_count) + ' fields'} ·{' '}
              {detail ? `namespace ${detail.namespace}` : 'columnar engine'}
            </div>
          </div>
        </div>
      </div>

      <Stats>
        <Stat label="Series" value={detail?.series_count ?? row.series_count} sub={`${detail?.field_count ?? row.field_count} fields`} />
        <Stat label="Datapoints" value={cfmt(row.points)} sub="buffered" />
        <Stat label="Fields" value={detail?.field_count ?? row.field_count} sub="per measurement" />
        <Stat label="Retention" value={detail?.retention ?? '—'} sub={detail?.retention ? 'policy' : 'no policy set'} />
      </Stats>

      {q.isError ? (
        <div className="card card-p" style={{ textAlign: 'center', color: 'var(--crit)', padding: '40px 0' }}>
          Failed to load measurement. Is the server running?
        </div>
      ) : q.isLoading ? (
        <div className="card card-p" style={{ textAlign: 'center', color: 'var(--tx-3)', padding: '40px 0' }}>
          Loading…
        </div>
      ) : (
        <>
          <div className="sec">
            <div className="set-h" style={{ padding: 0, marginBottom: 8 }}>
              {fields.length === 1 ? 'Series' : 'Series · per field'}
            </div>
            {fields.map((f) => (
              <FieldChart key={f.name} ns={ns} measurement={row.name} field={f.name} />
            ))}
          </div>

          <div className="sec">
            <div className="set-h" style={{ padding: 0, marginBottom: 2 }}>
              Fields
            </div>
            <div className="list">
              <div className="li head" style={{ gridTemplateColumns: '1.7fr 120px' }}>
                <div className="c">Field</div>
                <div className="c r">Type</div>
              </div>
              {fields.map((f) => (
                <div className="li" key={f.name} style={{ gridTemplateColumns: '1.7fr 120px', cursor: 'default' }}>
                  <span className="mono nw" style={{ color: 'var(--tx)' }}>
                    {f.name}
                  </span>
                  <span className="c r">
                    <span className="tag">{f.type}</span>
                  </span>
                </div>
              ))}
            </div>
            <div className="mono" style={{ fontSize: 11, color: 'var(--tx-faint)', marginTop: 8 }}>
              Tags collapse into one series per field — the projection keys buffers by measurement + field only.
            </div>
          </div>
        </>
      )}
    </div>
  )
}

/* ======================= list screen ======================= */
export function TimeSeries() {
  const { ns } = useNamespace()
  const q = useMeasurements(ns)
  const meas = q.data ?? []
  const [open, setOpen] = useState<TsMeasurement | null>(null)
  const [ingest, setIngest] = useState(false)

  if (open) return <MeasDetail ns={ns} row={open} onBack={() => setOpen(null)} />

  const totSeries = meas.reduce((a, m) => a + m.series_count, 0)
  const totFields = meas.reduce((a, m) => a + m.field_count, 0)
  const totPoints = meas.reduce((a, m) => a + m.points, 0)

  return (
    <div className="wrap fade">
      <div className="ph">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16 }}>
          <div>
            <h1 className="ph-t">Time Series</h1>
            <div className="ph-s">Columnar engine · line-protocol ingest · FloQL pipelines</div>
          </div>
          <Button variant="accent" style={{ flex: 'none' }} onClick={() => setIngest(true)}>
            Write point
          </Button>
        </div>
      </div>

      <Stats>
        <Stat label="Measurements" value={q.isLoading ? '…' : meas.length} sub={`in ${ns}`} />
        <Stat label="Series" value={cfmt(totSeries)} sub="measurement + field" />
        <Stat label="Fields" value={cfmt(totFields)} sub="numeric columns" />
        <Stat label="Datapoints" value={cfmt(totPoints)} sub="buffered · Raft-replicated" />
      </Stats>

      <FloQLConsole ns={ns} measurements={meas} />

      <div className="sec">
        <div className="sec-h" style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 2 }}>
          <span className="set-h" style={{ padding: 0 }}>
            Measurements
          </span>
          <span className="mono meta" style={{ fontSize: 11, color: 'var(--tx-faint)' }}>
            click to inspect series
          </span>
        </div>

        {q.isError ? (
          <div className="card card-p" style={{ textAlign: 'center', color: 'var(--crit)', padding: '40px 0' }}>
            Failed to load measurements. Is the server running?
          </div>
        ) : meas.length === 0 ? (
          <div className="card card-p" style={{ textAlign: 'center', color: 'var(--tx-3)', padding: '40px 0' }}>
            {q.isLoading ? 'Loading…' : 'No measurements. Write a point with `flo ts write`.'}
          </div>
        ) : (
          <div className="list">
            <div className="li head" style={{ gridTemplateColumns: '1.7fr 90px 90px 110px' }}>
              <div className="c">Measurement</div>
              <div className="c r">Series</div>
              <div className="c r">Fields</div>
              <div className="c r">Points</div>
            </div>
            {meas.map((m) => (
              <div className="li" key={m.name} style={{ gridTemplateColumns: '1.7fr 90px 90px 110px' }} onClick={() => setOpen(m)}>
                <div style={{ minWidth: 0 }}>
                  <div className="mono nw" style={{ fontWeight: 500, color: 'var(--tx)' }}>
                    {m.name}
                  </div>
                  <div className="desc">columnar · {m.field_count === 1 ? 'single field' : m.field_count + ' fields'}</div>
                </div>
                <span className="c r mono">{m.series_count}</span>
                <span className="c r mono" style={{ color: 'var(--tx-3)' }}>
                  {m.field_count}
                </span>
                <span className="c r mono">{cfmt(m.points)}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {ingest && <IngestModal ns={ns} meas={meas[0]?.name ?? 'cpu_usage'} onClose={() => setIngest(false)} />}
    </div>
  )
}
