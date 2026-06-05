/* Flo Console v2 — Workers screen (live).
   Long-running processes · action & stream · registry, health, heartbeats.

   Wired to the dashboard API. Workers are namespace-scoped server-side. The API
   exposes the registry record, health (status + last heartbeat), throughput
   (tasks_completed/failed, current_load) and per-process metrics. There are no
   drain/deregister dashboard endpoints (use the CLI), and the seed registers only
   action workers (one process each — the register CLI arg isn't variadic). */
import { useMemo, useState } from 'react'
import { jhl } from '@/lib/format'
import { useNamespace } from '@/lib/namespace'
import { Crumb, Stats, Stat, OpPill } from '@/components'
import { useWorkers, useWorkerDetail } from '@/lib/api/workers'
import type { WorkerInfo } from '@/lib/api/types'
import { ASvc, MapPill, afmtN, aAgo, secsAgo, typeColor, W_ST } from './shared'
import './compute.css'

function metaHl(meta: string | null): { __html: string } | null {
  if (!meta) return null
  try {
    return { __html: jhl(JSON.parse(meta)) }
  } catch {
    return null
  }
}

/* ======================= detail ======================= */
function WorkerDetail({ id, onBack }: { id: string; onBack: () => void }) {
  const q = useWorkerDetail(id)
  const w = q.data
  const hb = w ? secsAgo(w.last_seen) : 0
  const mh = w ? metaHl(w.metadata) : null

  return (
    <div className="wrap fade">
      <Crumb root="Workers" onRoot={onBack} current={id} />
      <div className="ph">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16, flexWrap: 'wrap' }}>
          <div>
            <h1 className="ph-t" style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              {id} {w && <MapPill map={W_ST} s={w.status} />}
            </h1>
            <div className="ph-s mono">
              {w ? `${w.worker_type} worker · ${w.machine_id ?? 'no machine'} · ${w.namespace}` : 'loading…'}
            </div>
          </div>
        </div>
      </div>

      {q.isError ? (
        <div className="card card-p" style={{ textAlign: 'center', color: 'var(--crit)', padding: '40px 0' }}>
          Worker not found, or the server is unavailable.
        </div>
      ) : !w ? (
        <div className="card card-p" style={{ textAlign: 'center', color: 'var(--tx-3)', padding: '40px 0' }}>
          Loading…
        </div>
      ) : (
        <>
          {w.status === 'unhealthy' && (
            <div
              className="wf-wait card-p"
              style={{
                marginBottom: 18,
                background: 'color-mix(in srgb,var(--crit) 7%,var(--card))',
                borderColor: 'color-mix(in srgb,var(--crit) 30%,transparent)',
              }}
            >
              <span style={{ color: 'var(--crit)', fontWeight: 700 }}>!</span>
              <span>
                No heartbeat for {hb}s — marked unhealthy after 90s of silence. Recovers automatically on the next
                heartbeat.
              </span>
            </div>
          )}

          <Stats>
            <Stat label="Load" value={w.current_load} unit={`/ ${w.max_concurrent}`} sub="in-flight / max" />
            <Stat label="Completed" value={afmtN(w.tasks_completed)} sub="lifetime" />
            <Stat label="Failed" value={w.tasks_failed} valueColor={w.tasks_failed ? 'var(--warn)' : undefined} />
            <Stat
              label="Last heartbeat"
              value={
                <span style={{ fontSize: 18, color: hb > 90 ? 'var(--crit)' : 'var(--tx)' }}>
                  {hb}
                  <span className="u">s ago</span>
                </span>
              }
              sub="every 30s"
            />
          </Stats>

          <div className="sec" style={{ display: 'grid', gridTemplateColumns: '1.3fr 1fr', gap: 16, alignItems: 'start' }}>
            <div className="card" style={{ overflow: 'hidden' }}>
              <div className="card-p" style={{ paddingBottom: 0 }}>
                <ASvc
                  icon="list"
                  title="Processes"
                  right={
                    <span className="mono" style={{ fontSize: 11, color: 'var(--tx-faint)' }}>
                      per-{w.worker_type} metrics
                    </span>
                  }
                />
              </div>
              <div className="list" style={{ border: 'none', borderRadius: 0, margin: '0 -1px -1px' }}>
                <div className="li head" style={{ gridTemplateColumns: '1.5fr 80px 70px 90px' }}>
                  <div className="c">{w.worker_type === 'stream' ? 'Stream / group' : 'Action'}</div>
                  <div className="c r">Runs</div>
                  <div className="c r">Fails</div>
                  <div className="c r">Last run</div>
                </div>
                {w.processes.length === 0 ? (
                  <div className="li" style={{ gridTemplateColumns: '1fr', cursor: 'default' }}>
                    <span className="mono" style={{ color: 'var(--tx-faint)', fontSize: 12.5 }}>
                      No processes registered.
                    </span>
                  </div>
                ) : (
                  w.processes.map((p) => (
                    <div className="li" key={p.name} style={{ gridTemplateColumns: '1.5fr 80px 70px 90px', cursor: 'default' }}>
                      <span className="mono nw" style={{ color: 'var(--tx)' }}>
                        {p.name}
                      </span>
                      <span className="c r mono">{afmtN(p.run_count)}</span>
                      <span className="c r mono" style={{ color: p.fail_count > 20 ? 'var(--warn)' : 'var(--tx-3)' }}>
                        {p.fail_count}
                      </span>
                      <span className="c r mono" style={{ fontSize: 11.5, color: 'var(--tx-3)' }}>
                        {p.last_run_at ? aAgo(secsAgo(p.last_run_at)) : '—'}
                      </span>
                    </div>
                  ))
                )}
              </div>
            </div>
            <div className="card card-p">
              <ASvc icon="info" title="Registry record" />
              {(
                [
                  ['Worker ID', w.worker_id],
                  ['Type', w.worker_type],
                  ['Namespace', w.namespace],
                  ['Machine', w.machine_id ?? '—'],
                  ['Max concurrency', w.max_concurrent],
                  ['Registered', aAgo(secsAgo(w.registered_at))],
                ] as [string, string | number][]
              ).map(([k, v]) => (
                <div className="kvrow2" key={k}>
                  <span>{k}</span>
                  <span className="mono" style={{ color: 'var(--tx)' }}>
                    {v}
                  </span>
                </div>
              ))}
              <div style={{ marginTop: 12, paddingTop: 12, borderTop: '1px solid var(--line-soft)' }}>
                <div className="set-h" style={{ padding: 0, marginBottom: 6 }}>
                  Metadata
                </div>
                {mh ? (
                  <pre className="code" dangerouslySetInnerHTML={mh} />
                ) : w.metadata ? (
                  <pre className="code">{w.metadata}</pre>
                ) : (
                  <div className="mono" style={{ fontSize: 12.5, color: 'var(--tx-faint)' }}>
                    none
                  </div>
                )}
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  )
}

/* ======================= list ======================= */
type ListTab = 'all' | 'action' | 'stream'

function WorkersList({ ns, onOpen }: { ns: string; onOpen: (id: string) => void }) {
  const q = useWorkers(ns)
  const workers = useMemo<WorkerInfo[]>(() => q.data ?? [], [q.data])
  const [tab, setTab] = useState<ListTab>('all')

  const active = workers.filter((w) => w.status === 'active').length
  const unhealthy = workers.filter((w) => w.status === 'unhealthy').length
  const inflight = workers.reduce((acc, w) => acc + w.current_load, 0)
  const done = workers.reduce((acc, w) => acc + w.tasks_completed, 0)
  const filtered = tab === 'all' ? workers : workers.filter((w) => w.worker_type === tab)

  const grid = '1.3fr 70px 110px 90px 90px 90px'

  return (
    <div className="wrap fade">
      <div className="ph">
        <h1 className="ph-t">Workers</h1>
        <div className="ph-s">Long-running processes · action &amp; stream · registry, health, heartbeats</div>
      </div>

      <Stats>
        <Stat label="Workers" value={q.isLoading ? '…' : workers.length} sub={`${active} active · in ${ns}`} />
        <Stat label="In-flight" value={inflight} sub="tasks now" />
        <Stat label="Completed" value={afmtN(done)} sub="lifetime" />
        <Stat label="Unhealthy" value={unhealthy} valueColor={unhealthy ? 'var(--crit)' : undefined} sub="no heartbeat" />
      </Stats>

      <div className="sec">
        <div className="wtabs">
          {(
            [
              ['all', 'All · ' + workers.length],
              ['action', 'Action'],
              ['stream', 'Stream'],
            ] as [ListTab, string][]
          ).map(([id, l]) => (
            <button key={id} className={'wtab' + (tab === id ? ' on' : '')} onClick={() => setTab(id)}>
              {l}
            </button>
          ))}
        </div>

        {q.isError ? (
          <div className="card card-p" style={{ textAlign: 'center', color: 'var(--crit)', padding: '40px 0' }}>
            Failed to load workers. Is the server running?
          </div>
        ) : filtered.length === 0 ? (
          <div className="card card-p" style={{ textAlign: 'center', color: 'var(--tx-3)', padding: '40px 0' }}>
            {q.isLoading ? 'Loading…' : `No ${tab === 'all' ? '' : tab + ' '}workers in ${ns}. Register one with \`flo worker register\`.`}
          </div>
        ) : (
          <div className="list">
            <div className="li head" style={{ gridTemplateColumns: grid }}>
              <div className="c">Worker</div>
              <div className="c">Type</div>
              <div className="c">Status</div>
              <div className="c r">Load</div>
              <div className="c r">Completed</div>
              <div className="c r">Heartbeat</div>
            </div>
            {filtered.map((w) => {
              const hb = secsAgo(w.last_seen)
              return (
                <div className="li" key={w.worker_id} style={{ gridTemplateColumns: grid }} onClick={() => onOpen(w.worker_id)}>
                  <div style={{ minWidth: 0 }}>
                    <div className="mono nw" style={{ fontWeight: 500, color: 'var(--tx)' }}>
                      {w.worker_id}
                    </div>
                    <div className="desc">
                      {w.machine_id ?? 'no machine'} · {w.namespace}
                    </div>
                  </div>
                  <span>
                    <OpPill color={typeColor(w.worker_type)}>{w.worker_type}</OpPill>
                  </span>
                  <span>
                    <MapPill map={W_ST} s={w.status} />
                  </span>
                  <span className="c r mono" style={{ color: w.current_load ? '#6F9BD1' : 'var(--tx-3)' }}>
                    {w.current_load}/{w.max_concurrent}
                  </span>
                  <span className="c r mono">{afmtN(w.tasks_completed)}</span>
                  <span className="c r mono" style={{ fontSize: 11.5, color: hb > 90 ? 'var(--crit)' : 'var(--tx-3)' }}>
                    {hb}s
                  </span>
                </div>
              )
            })}
          </div>
        )}
      </div>
    </div>
  )
}

/** Workers screen — master list ↔ detail (internal state, matching the design). */
export function Workers() {
  const { ns } = useNamespace()
  const [open, setOpen] = useState<string | null>(null)
  return open ? (
    <WorkerDetail id={open} onBack={() => setOpen(null)} />
  ) : (
    <WorkersList ns={ns} onOpen={setOpen} />
  )
}
