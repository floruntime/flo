import { useMemo, useState } from 'react'
import { cfmt, jhl } from '@/lib/format'
import { cx } from '@/lib/cx'
import { useQueueDetail, useQueueMessages, useRequeueDLQ } from '@/lib/api/queues'
import { Crumb, Stats, Stat } from '@/components/layout'
import { Button } from '@/components/buttons/Button'
import { OpPill, Tag } from '@/components/feedback'
import type { QueueMessage } from '@/lib/api/types'

type Tab = 'ready' | 'inflight' | 'dlq'

function prioMeta(p: number): [string, string] {
  return p <= 1 ? ['var(--crit)', 'urgent'] : p <= 5 ? ['var(--warn)', 'high'] : ['var(--tx-3)', 'normal']
}
function PrioTag({ p }: { p: number }) {
  return <OpPill color={prioMeta(p)[0]}>P{p}</OpPill>
}
function ago(ms: number): string {
  const s = Math.max(0, Math.round((Date.now() - ms) / 1000))
  return s < 60 ? s + 's ago' : s < 3600 ? Math.floor(s / 60) + 'm ago' : s < 86400 ? Math.floor(s / 3600) + 'h ago' : Math.floor(s / 86400) + 'd ago'
}

export function QueueDetail({ name, ns, onBack }: { name: string; ns: string; onBack: () => void }) {
  const detailQ = useQueueDetail(ns, name)
  const msgsQ = useQueueMessages(ns, name)
  const requeue = useRequeueDLQ(ns)
  const [tab, setTab] = useState<Tab>('ready')
  const [sel, setSel] = useState<QueueMessage | null>(null)

  const d = detailQ.data
  const msgs = msgsQ.data?.messages ?? []
  const ready = useMemo(() => msgs.filter((m) => m.state === 'ready').sort((a, b) => a.priority - b.priority || a.seq - b.seq), [msgs])
  const inflight = useMemo(() => msgs.filter((m) => m.state === 'leased').sort((a, b) => a.lease_remaining_ms - b.lease_remaining_ms), [msgs])
  const dlq = useMemo(() => msgs.filter((m) => m.state === 'dlq'), [msgs])
  const empty = msgs.length === 0
  const sw = sel ? '1.35fr 1fr' : '1fr'

  const payloadObj = (() => {
    if (!sel) return null
    try {
      return JSON.parse(sel.payload)
    } catch {
      return null
    }
  })()

  return (
    <div className="wrap fade">
      <Crumb root="Queues" onRoot={onBack} current={name} />
      <div className="ph">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16, flexWrap: 'wrap' }}>
          <div>
            <h1 className="ph-t" style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              {name}
              <Tag>{ns}</Tag>
            </h1>
            <div className="ph-s">Priority delivery · competing consumers · lease-based · dead-letter</div>
          </div>
        </div>
      </div>

      <Stats>
        <Stat label="Ready" value={cfmt(d?.ready ?? 0)} sub="available to deliver" />
        <Stat label="In-flight" value={d?.inflight ?? 0} valueColor={d?.inflight ? 'var(--info)' : undefined} sub="leased" />
        <Stat label="Dead letters" value={d?.dlq_count ?? 0} valueColor={d?.dlq_count ? 'var(--crit)' : undefined} sub={d?.dlq_count ? 'needs triage' : 'none'} />
        <Stat label="Throughput" value={<span style={{ fontSize: 20 }}>{cfmt(d?.enqueued ?? 0)}<span className="u">/ {cfmt(d?.dequeued ?? 0)}</span></span>} sub="enqueued / dequeued" />
      </Stats>

      {empty ? (
        <div className="card card-p" style={{ textAlign: 'center', color: 'var(--tx-3)', fontSize: 13.5, padding: '40px 0' }}>
          {msgsQ.isLoading ? 'Loading…' : 'Queue is empty — blocking dequeues are parked on the waiter pool.'}
        </div>
      ) : (
        <div className="sec" style={{ display: 'grid', gridTemplateColumns: sw, gap: 20, alignItems: 'start' }}>
          <div style={{ minWidth: 0 }}>
            <div className="wtabs">
              {(
                [
                  ['ready', 'Ready · ' + cfmt(ready.length)],
                  ['inflight', 'In-flight · ' + inflight.length],
                  ['dlq', 'DLQ · ' + dlq.length],
                ] as [Tab, string][]
              ).map(([id, l]) => (
                <button key={id} className={cx('wtab', tab === id && 'on')} onClick={() => { setTab(id); setSel(null) }}>
                  {l}
                </button>
              ))}
            </div>

            {tab === 'ready' && (
              <div className="list">
                <div className="li head" style={{ gridTemplateColumns: '96px 64px 1fr 96px' }}>
                  <div className="c">Seq</div>
                  <div className="c">Prio</div>
                  <div className="c">Payload</div>
                  <div className="c r">Enqueued</div>
                </div>
                {ready.map((m) => (
                  <div className="li" key={m.seq} style={{ gridTemplateColumns: '96px 64px 1fr 96px', background: sel?.seq === m.seq ? 'var(--hover)' : undefined }} onClick={() => setSel(m)}>
                    <span className="mono nw" style={{ fontSize: 12.5, color: 'var(--tx-2)' }}>#{m.seq}</span>
                    <span><PrioTag p={m.priority} /></span>
                    <span className="mono nw" style={{ fontSize: 12, color: 'var(--tx-3)', overflow: 'hidden', textOverflow: 'ellipsis' }}>{m.payload}</span>
                    <span className="c r mono" style={{ fontSize: 11.5, color: 'var(--tx-3)' }}>{ago(m.enqueued_at)}</span>
                  </div>
                ))}
              </div>
            )}

            {tab === 'inflight' && (
              inflight.length === 0 ? (
                <div className="card card-p" style={{ color: 'var(--tx-3)', fontSize: 13, padding: '24px 0', textAlign: 'center' }}>No leased messages.</div>
              ) : (
                <div className="list">
                  <div className="li head" style={{ gridTemplateColumns: '96px 80px 70px 90px' }}>
                    <div className="c">Seq</div>
                    <div className="c r">Attempt</div>
                    <div className="c r">Prio</div>
                    <div className="c r">Lease</div>
                  </div>
                  {inflight.map((m) => (
                    <div className="li" key={m.seq} style={{ gridTemplateColumns: '96px 80px 70px 90px', background: sel?.seq === m.seq ? 'var(--hover)' : undefined }} onClick={() => setSel(m)}>
                      <span className="mono nw" style={{ fontSize: 12.5, color: 'var(--tx-2)' }}>#{m.seq}</span>
                      <span className="c r mono" style={{ color: m.attempts > 1 ? 'var(--warn)' : 'var(--tx-3)' }}>{m.attempts}×</span>
                      <span className="c r"><PrioTag p={m.priority} /></span>
                      <span className="c r mono" style={{ fontSize: 12, color: m.lease_remaining_ms < 10000 ? 'var(--crit)' : 'var(--tx-3)' }}>{Math.round(m.lease_remaining_ms / 1000)}s</span>
                    </div>
                  ))}
                </div>
              )
            )}

            {tab === 'dlq' && (
              dlq.length === 0 ? (
                <div className="card card-p" style={{ color: 'var(--tx-3)', fontSize: 13, padding: '24px 0', textAlign: 'center' }}>No dead-lettered messages.</div>
              ) : (
                <div className="list">
                  <div className="li head" style={{ gridTemplateColumns: '96px 70px 1fr 96px' }}>
                    <div className="c">Seq</div>
                    <div className="c r">Attempts</div>
                    <div className="c">Payload</div>
                    <div className="c r">Action</div>
                  </div>
                  {dlq.map((m) => (
                    <div className="li" key={m.seq} style={{ gridTemplateColumns: '96px 70px 1fr 96px', background: sel?.seq === m.seq ? 'var(--hover)' : undefined }} onClick={() => setSel(m)}>
                      <span className="mono nw" style={{ fontSize: 12.5, color: 'var(--tx-2)' }}>#{m.seq}</span>
                      <span className="c r mono" style={{ color: 'var(--crit)' }}>{m.attempts}×</span>
                      <span className="nw" style={{ fontSize: 12, color: 'var(--tx-3)', overflow: 'hidden', textOverflow: 'ellipsis' }}>{m.payload}</span>
                      <span className="c r">
                        <Button
                          style={{ flex: 'none', padding: '4px 10px', fontSize: 12 }}
                          disabled={requeue.isPending}
                          onClick={(e) => { e.stopPropagation(); requeue.mutate({ name, seq: m.seq }) }}
                        >
                          Requeue
                        </Button>
                      </span>
                    </div>
                  ))}
                </div>
              )
            )}
          </div>

          {sel && (
            <div className="card card-p" style={{ minWidth: 0 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 10, marginBottom: 12 }}>
                <span className="mono" style={{ fontSize: 13, color: 'var(--tx)' }}>#{sel.seq}</span>
                <Button variant="quiet" style={{ padding: '4px 8px' }} onClick={() => setSel(null)}>Close</Button>
              </div>
              <div className="kvrow2"><span>Priority</span><span style={{ display: 'flex', gap: 8, alignItems: 'center' }}><PrioTag p={sel.priority} /><span className="mono" style={{ fontSize: 11.5, color: 'var(--tx-faint)' }}>{prioMeta(sel.priority)[1]}</span></span></div>
              <div className="kvrow2"><span>State</span><span className="mono" style={{ color: 'var(--tx)' }}>{sel.state}</span></div>
              <div className="kvrow2"><span>Attempts</span><span className="mono" style={{ color: sel.attempts > 1 ? 'var(--warn)' : 'var(--tx)' }}>{sel.attempts}</span></div>
              {sel.state === 'leased' && <div className="kvrow2"><span>Lease expiry</span><span className="mono" style={{ color: sel.lease_remaining_ms < 10000 ? 'var(--crit)' : 'var(--tx)' }}>{Math.round(sel.lease_remaining_ms / 1000)}s</span></div>}
              <div className="kvrow2"><span>Enqueued</span><span className="mono" style={{ color: 'var(--tx)' }}>{ago(sel.enqueued_at)}</span></div>
              <div className="set-h" style={{ padding: 0, margin: '14px 0 6px' }}>Payload</div>
              {payloadObj ? (
                <pre className="code" dangerouslySetInnerHTML={{ __html: jhl(payloadObj) }} />
              ) : (
                <pre className="code" style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>{sel.payload || '(empty)'}</pre>
              )}
              {sel.state === 'dlq' && (
                <div style={{ display: 'flex', gap: 8, marginTop: 14 }}>
                  <Button variant="accent" style={{ flex: 'none' }} disabled={requeue.isPending} onClick={() => requeue.mutate({ name, seq: sel.seq })}>Requeue</Button>
                </div>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
