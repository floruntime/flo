import { useState } from 'react'
import { cfmt } from '@/lib/format'
import { cx } from '@/lib/cx'
import { useNamespace } from '@/lib/namespace'
import { useQueues } from '@/lib/api/queues'
import { Stats, Stat } from '@/components/layout'
import { Button } from '@/components/buttons/Button'
import { QueueDetail } from './QueueDetail'
import { EnqueueModal } from './EnqueueModal'

type Tab = 'all' | 'dlq'

export function Queues() {
  const { ns } = useNamespace()
  const q = useQueues()
  const queues = q.data ?? []
  const [open, setOpen] = useState<{ name: string; ns: string } | null>(null)
  const [tab, setTab] = useState<Tab>('all')
  const [enq, setEnq] = useState(false)

  if (open) return <QueueDetail name={open.name} ns={open.ns} onBack={() => setOpen(null)} />

  const totReady = queues.reduce((a, x) => a + x.ready, 0)
  const totInflight = queues.reduce((a, x) => a + x.inflight, 0)
  const totDlq = queues.reduce((a, x) => a + x.dlq_count, 0)
  const dlqQueues = queues.filter((x) => x.dlq_count > 0)

  return (
    <div className="wrap fade">
      <div className="ph">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16 }}>
          <div>
            <h1 className="ph-t">Queues</h1>
            <div className="ph-s">Priority delivery · competing consumers · lease-based · dead-letter</div>
          </div>
          <Button variant="accent" style={{ flex: 'none' }} onClick={() => setEnq(true)}>
            Enqueue
          </Button>
        </div>
      </div>

      <Stats>
        <Stat label="Queues" value={q.isLoading ? '…' : queues.length} sub="across namespaces" />
        <Stat label="Ready" value={cfmt(totReady)} sub="available to deliver" />
        <Stat label="In-flight" value={totInflight} sub="leased now" />
        <Stat label="Dead letters" value={totDlq} valueColor={totDlq ? 'var(--crit)' : undefined} sub={totDlq ? 'needs triage' : 'all clear'} />
      </Stats>

      <div className="sec">
        <div className="wtabs">
          {(
            [
              ['all', 'All queues · ' + queues.length],
              ['dlq', 'Dead-letter triage · ' + totDlq],
            ] as [Tab, string][]
          ).map(([id, l]) => (
            <button key={id} className={cx('wtab', tab === id && 'on')} onClick={() => setTab(id)}>
              {l}
            </button>
          ))}
        </div>

        {q.isError ? (
          <div className="card card-p" style={{ textAlign: 'center', color: 'var(--crit)', padding: '40px 0' }}>
            Failed to load queues. Is the server running?
          </div>
        ) : tab === 'all' ? (
          queues.length === 0 ? (
            <div className="card card-p" style={{ textAlign: 'center', color: 'var(--tx-3)', padding: '40px 0' }}>
              {q.isLoading ? 'Loading…' : 'No queues.'}
            </div>
          ) : (
            <div className="list">
              <div className="li head" style={{ gridTemplateColumns: '1.5fr 100px 90px 90px 90px 80px' }}>
                <div className="c">Queue</div>
                <div className="c r">Ready</div>
                <div className="c r">In-flight</div>
                <div className="c r">Enqueued</div>
                <div className="c r">Dequeued</div>
                <div className="c r">DLQ</div>
              </div>
              {queues.map((x) => (
                <div
                  className="li"
                  key={x.namespace + '/' + x.name}
                  style={{ gridTemplateColumns: '1.5fr 100px 90px 90px 90px 80px' }}
                  onClick={() => setOpen({ name: x.name, ns: x.namespace })}
                >
                  <div style={{ minWidth: 0 }}>
                    <div className="mono nw" style={{ fontWeight: 500, color: 'var(--tx)' }}>
                      {x.name}
                      {x.ready === 0 && x.inflight === 0 && (
                        <span className="op-pill" style={{ marginLeft: 8, color: 'var(--tx-3)', background: 'var(--card-2)' }}>idle</span>
                      )}
                    </div>
                    <div className="desc">{x.namespace}</div>
                  </div>
                  <span className="c r mono">{cfmt(x.ready)}</span>
                  <span className="c r mono" style={{ color: x.inflight ? 'var(--info)' : 'var(--tx-3)' }}>{x.inflight}</span>
                  <span className="c r mono" style={{ color: 'var(--tx-3)' }}>{cfmt(x.enqueued)}</span>
                  <span className="c r mono" style={{ color: 'var(--tx-3)' }}>{cfmt(x.dequeued)}</span>
                  <span className="c r mono" style={{ color: x.dlq_count ? 'var(--crit)' : 'var(--tx-3)' }}>{x.dlq_count}</span>
                </div>
              ))}
            </div>
          )
        ) : totDlq === 0 ? (
          <div className="card card-p" style={{ textAlign: 'center', color: 'var(--tx-3)', fontSize: 13.5, padding: '40px 0' }}>
            No dead-lettered messages across any queue.
          </div>
        ) : (
          <div className="list">
            <div className="li head" style={{ gridTemplateColumns: '1.5fr 120px 120px' }}>
              <div className="c">Queue</div>
              <div className="c r">Dead letters</div>
              <div className="c r" />
            </div>
            {dlqQueues.map((x) => (
              <div className="li" key={x.namespace + '/' + x.name} style={{ gridTemplateColumns: '1.5fr 120px 120px' }} onClick={() => setOpen({ name: x.name, ns: x.namespace })}>
                <div style={{ minWidth: 0 }}>
                  <div className="mono nw" style={{ fontWeight: 500, color: 'var(--tx)' }}>{x.name}</div>
                  <div className="desc">{x.namespace}</div>
                </div>
                <span className="c r mono" style={{ color: 'var(--crit)' }}>{x.dlq_count}</span>
                <span className="c r">
                  <Button style={{ flex: 'none', padding: '4px 10px', fontSize: 12 }}>Triage →</Button>
                </span>
              </div>
            ))}
          </div>
        )}
      </div>

      {enq && <EnqueueModal ns={ns} onClose={() => setEnq(false)} />}
    </div>
  )
}
