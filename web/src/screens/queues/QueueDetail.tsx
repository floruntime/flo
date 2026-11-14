import { useMemo, useState, type ReactNode } from 'react'
import { DASH, cfmt, jhl, tfmt } from '@/lib/format'
import { cx } from '@/lib/cx'
import { useQueueDetail, useQueueDLQ, useQueueMessages } from '@/lib/api/queues'
import { Crumb, Stats, Stat } from '@/components/layout'
import { Button } from '@/components/buttons/Button'
import { OpPill, Tag } from '@/components/feedback'
import type { QueueDLQEntry, QueueMessage } from '@/lib/api/types'

type Tab = 'ready' | 'inflight' | 'dlq'
type Sel = { kind: 'ready'; m: QueueMessage } | { kind: 'dlq'; m: QueueDLQEntry }

// The server refuses both today; the buttons stay visible so the gap is plain.
const DLQ_ACTIONS_NOTE = 'Requeue and delete aren’t supported yet'

function prioMeta(p: number): [string, string] {
  return p <= 1 ? ['var(--crit)', 'urgent'] : p <= 5 ? ['var(--warn)', 'high'] : ['var(--tx-3)', 'normal']
}
function PrioTag({ p }: { p: number }) {
  return <OpPill color={prioMeta(p)[0]}>P{p}</OpPill>
}
/** Why a list can't be shown, in the server's own words when it gave some. */
function unavailable(err: unknown, what: string): string {
  const m = (err as Error | null)?.message
  return m ? m.charAt(0).toUpperCase() + m.slice(1) : `${what} couldn’t be loaded`
}
function Note({ children }: { children: ReactNode }) {
  return <div className="card card-p" style={{ color: 'var(--tx-3)', fontSize: 13, padding: '24px 0', textAlign: 'center' }}>{children}</div>
}
function More({ children }: { children: ReactNode }) {
  return <div className="mono" style={{ fontSize: 11.5, color: 'var(--tx-faint)', padding: '10px 2px 0' }}>{children}</div>
}
function ago(ms: number): string {
  const s = Math.max(0, Math.round((Date.now() - ms) / 1000))
  return s < 60 ? s + 's ago' : s < 3600 ? Math.floor(s / 60) + 'm ago' : s < 86400 ? Math.floor(s / 3600) + 'h ago' : Math.floor(s / 86400) + 'd ago'
}
function bfmt(b: number): string {
  return b >= 1 << 20 ? (b / (1 << 20)).toFixed(1) + ' MB' : b >= 1 << 10 ? (b / (1 << 10)).toFixed(1) + ' KB' : b + ' B'
}
/** The payload as shown: the server cuts it at 4 KiB, so say how big the whole is. */
function shown(m: QueueMessage): string {
  return m.truncated ? `${m.payload}… ${bfmt(m.size)} total` : m.payload
}
function DlqActions({ compact }: { compact?: boolean }) {
  const st = compact ? { flex: 'none' as const, padding: '4px 10px', fontSize: 12 } : { flex: 'none' as const }
  return (
    <>
      <Button style={st} disabled title={DLQ_ACTIONS_NOTE}>Requeue</Button>
      {!compact && <Button variant="danger" style={st} disabled title={DLQ_ACTIONS_NOTE}>Delete</Button>}
    </>
  )
}

export function QueueDetail({ name, ns, onBack }: { name: string; ns: string; onBack: () => void }) {
  const detailQ = useQueueDetail(ns, name)
  const msgsQ = useQueueMessages(ns, name)
  const dlqQ = useQueueDLQ(ns, name)
  const [tab, setTab] = useState<Tab>('ready')
  const [sel, setSel] = useState<Sel | null>(null)

  const d = detailQ.data
  // Already in dequeue order (priority, then seq); peeked, not taken.
  const ready = msgsQ.data?.messages ?? []
  const readyMore = msgsQ.data?.more ?? false
  const msgsErr = msgsQ.isError
  const dlq = useMemo(() => dlqQ.data?.pages.flatMap((p) => p.entries) ?? [], [dlqQ.data])
  const dlqCount = dlqQ.data?.pages[0]?.count ?? d?.dlq_count ?? null
  const empty = msgsQ.isSuccess && ready.length === 0 && !d?.inflight && !dlqCount
  const sw = sel ? '1.35fr 1fr' : '1fr'
  const selSeq = sel?.m.seq

  const payloadObj = (() => {
    if (!sel || sel.m.truncated) return null
    try {
      return JSON.parse(sel.m.payload)
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
        <Stat label="Ready" value={d ? cfmt(d.ready) : '…'} sub="available to deliver" />
        <Stat label="In-flight" value={d ? d.inflight : '…'} valueColor={d?.inflight ? 'var(--info)' : undefined} sub="leased" />
        <Stat label="Dead letters" value={d?.dlq_count ?? 0} valueColor={d?.dlq_count ? 'var(--crit)' : undefined} sub={d?.dlq_count ? 'needs triage' : 'none'} />
        <Stat label="Throughput" value={<span style={{ fontSize: 20 }}>{cfmt(d?.enqueued ?? 0)}<span className="u">/ {cfmt(d?.dequeued ?? 0)}</span></span>} sub="enqueued / delivered" />
      </Stats>

      {msgsQ.isLoading ? (
        <div className="card card-p" style={{ textAlign: 'center', color: 'var(--tx-3)', fontSize: 13.5, padding: '40px 0' }}>
          Loading…
        </div>
      ) : empty ? (
        <div className="card card-p" style={{ textAlign: 'center', color: 'var(--tx-3)', fontSize: 13.5, padding: '40px 0' }}>
          Queue is empty — blocking dequeues are parked on the waiter pool.
        </div>
      ) : (
        <div className="sec" style={{ display: 'grid', gridTemplateColumns: sw, gap: 20, alignItems: 'start' }}>
          <div style={{ minWidth: 0 }}>
            <div className="wtabs">
              {(
                [
                  ['ready', 'Ready · ' + (d ? cfmt(d.ready) : DASH)],
                  ['inflight', 'In-flight · ' + (d ? d.inflight : DASH)],
                  ['dlq', 'DLQ · ' + (dlqCount ?? DASH)],
                ] as [Tab, string][]
              ).map(([id, l]) => (
                <button key={id} className={cx('wtab', tab === id && 'on')} onClick={() => { setTab(id); setSel(null) }}>
                  {l}
                </button>
              ))}
            </div>

            {tab === 'ready' && (
              msgsErr ? (
                <Note>{unavailable(msgsQ.error, 'Messages')}{d ? ` · ${cfmt(d.ready)} ready` : ''}</Note>
              ) : ready.length === 0 ? (
                <Note>No ready messages.</Note>
              ) : (
                <>
                  <div className="list">
                    <div className="li head" style={{ gridTemplateColumns: '96px 64px 1fr 96px' }}>
                      <div className="c">Seq</div>
                      <div className="c">Prio</div>
                      <div className="c">Payload</div>
                      <div className="c r">Enqueued</div>
                    </div>
                    {ready.map((m) => (
                      <div className="li" key={m.seq} style={{ gridTemplateColumns: '96px 64px 1fr 96px', background: selSeq === m.seq ? 'var(--hover)' : undefined }} onClick={() => setSel({ kind: 'ready', m })}>
                        <span className="mono nw" style={{ fontSize: 12.5, color: 'var(--tx-2)' }}>#{m.seq}</span>
                        <span><PrioTag p={m.priority} /></span>
                        <span className="mono nw" style={{ fontSize: 12, color: 'var(--tx-3)', overflow: 'hidden', textOverflow: 'ellipsis' }}>{shown(m)}</span>
                        <span className="c r mono" style={{ fontSize: 11.5, color: 'var(--tx-3)' }}>{ago(m.enqueued_at)}</span>
                      </div>
                    ))}
                  </div>
                  <More>Next to be delivered, in order · peeked, not taken{readyMore ? ` · more follow (first ${ready.length} shown)` : ''}</More>
                </>
              )
            )}

            {tab === 'inflight' && (
              <Note>
                {d?.inflight ? `${d.inflight} leased · leased messages aren’t listed yet` : 'No leased messages.'}
              </Note>
            )}

            {tab === 'dlq' && (
              dlqQ.isError ? (
                <Note>{unavailable(dlqQ.error, 'Dead letters')}{dlqCount ? ` · ${cfmt(dlqCount)} dead-lettered` : ''}</Note>
              ) : dlqQ.isLoading ? (
                <Note>Loading…</Note>
              ) : dlq.length === 0 ? (
                <Note>No dead-lettered messages.</Note>
              ) : (
                <>
                  <div className="list">
                    <div className="li head" style={{ gridTemplateColumns: '96px 70px 1fr 96px 96px' }}>
                      <div className="c">Seq</div>
                      <div className="c r">Deliveries</div>
                      <div className="c">Payload</div>
                      <div className="c r">Moved</div>
                      <div className="c r">Action</div>
                    </div>
                    {dlq.map((m) => (
                      <div className="li" key={m.seq} style={{ gridTemplateColumns: '96px 70px 1fr 96px 96px', background: selSeq === m.seq ? 'var(--hover)' : undefined }} onClick={() => setSel({ kind: 'dlq', m })}>
                        <span className="mono nw" style={{ fontSize: 12.5, color: 'var(--tx-2)' }}>#{m.seq}</span>
                        <span className="c r mono" style={{ color: 'var(--crit)' }}>{m.deliveries}×</span>
                        <span className="nw" style={{ fontSize: 12, color: 'var(--tx-3)', overflow: 'hidden', textOverflow: 'ellipsis' }}>{shown(m)}</span>
                        <span className="c r mono" style={{ fontSize: 11.5, color: 'var(--tx-3)' }}>{ago(m.moved_at)}</span>
                        <span className="c r" onClick={(e) => e.stopPropagation()}><DlqActions compact /></span>
                      </div>
                    ))}
                  </div>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 12 }}>
                    <More>Oldest first · {dlq.length} of {dlqCount != null ? cfmt(dlqCount) : DASH} · {DLQ_ACTIONS_NOTE.toLowerCase()}</More>
                    {dlqQ.hasNextPage && (
                      <Button variant="quiet" style={{ flex: 'none', marginTop: 10, padding: '4px 10px', fontSize: 12 }} disabled={dlqQ.isFetchingNextPage} onClick={() => dlqQ.fetchNextPage()}>
                        {dlqQ.isFetchingNextPage ? 'Loading…' : 'Load more'}
                      </Button>
                    )}
                  </div>
                </>
              )
            )}
          </div>

          {sel && (
            <div className="card card-p" style={{ minWidth: 0 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: 10, marginBottom: 12 }}>
                <span className="mono" style={{ fontSize: 13, color: 'var(--tx)' }}>#{sel.m.seq}</span>
                <Button variant="quiet" style={{ padding: '4px 8px' }} onClick={() => setSel(null)}>Close</Button>
              </div>
              <div className="kvrow2"><span>Priority</span><span style={{ display: 'flex', gap: 8, alignItems: 'center' }}><PrioTag p={sel.m.priority} /><span className="mono" style={{ fontSize: 11.5, color: 'var(--tx-faint)' }}>{prioMeta(sel.m.priority)[1]}</span></span></div>
              <div className="kvrow2"><span>State</span><span className="mono" style={{ color: 'var(--tx)' }}>{sel.kind === 'dlq' ? 'dead-lettered' : 'ready'}</span></div>
              <div className="kvrow2"><span>Deliveries</span><span className="mono" style={{ color: sel.m.deliveries > 1 ? 'var(--warn)' : 'var(--tx)' }}>{sel.m.deliveries}</span></div>
              <div className="kvrow2"><span>Enqueued</span><span className="mono" style={{ color: 'var(--tx)' }} title={tfmt(sel.m.enqueued_at)}>{ago(sel.m.enqueued_at)}</span></div>
              {sel.kind === 'dlq' && <div className="kvrow2"><span>Dead-lettered</span><span className="mono" style={{ color: 'var(--tx)' }} title={tfmt(sel.m.moved_at)}>{ago(sel.m.moved_at)}</span></div>}
              <div className="kvrow2"><span>Size</span><span className="mono" style={{ color: 'var(--tx)' }}>{bfmt(sel.m.size)}</span></div>
              <div className="set-h" style={{ padding: 0, margin: '14px 0 6px' }}>Payload{sel.m.truncated ? ' · first 4 KB' : ''}</div>
              {payloadObj ? (
                <pre className="code" dangerouslySetInnerHTML={{ __html: jhl(payloadObj) }} />
              ) : (
                <pre className="code" style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>{sel.m.payload ? shown(sel.m) : '(empty)'}</pre>
              )}
              {sel.kind === 'dlq' && (
                <>
                  <div style={{ display: 'flex', gap: 8, marginTop: 14 }}><DlqActions /></div>
                  <More>{DLQ_ACTIONS_NOTE}.</More>
                </>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
