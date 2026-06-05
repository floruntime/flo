import { useState } from 'react'
import { Modal } from '@/components/overlay/Modal'
import { Button } from '@/components/buttons/Button'
import { Field } from '@/components/inputs'

function prioMeta(p: number): [string, string] {
  return p <= 1 ? ['var(--crit)', 'urgent'] : p <= 5 ? ['var(--warn)', 'high'] : ['var(--tx-3)', 'normal']
}

export function EnqueueModal({ qname, ns, onClose }: { qname?: string; ns: string; onClose: () => void }) {
  const [q, setQ] = useState(qname || 'tasks')
  const [payload, setPayload] = useState('{"task":"send-email","to":"alice@example.com"}')
  const [prio, setPrio] = useState(0)
  const [delay, setDelay] = useState(0)
  const cmd =
    `flo queue enqueue ${q} '${payload}'` +
    (prio ? ` --priority ${prio}` : '') +
    (delay ? ` --delay ${delay}` : '') +
    ` -n ${ns}`
  const [pc] = prioMeta(prio)

  return (
    <Modal
      title="Enqueue message"
      sub="Run the command below — dashboard enqueue endpoint is not wired yet"
      width="min(620px,96vw)"
      onClose={onClose}
      foot={
        <>
          <Button variant="quiet" onClick={onClose}>Cancel</Button>
          <Button variant="accent" onClick={onClose}>Done</Button>
        </>
      }
    >
      <div className="ts-frow">
        <div className="set-h" style={{ padding: 0 }}>Queue</div>
        <Field className="mono" wrapStyle={{ width: 240 }} value={q} onChange={(e) => setQ(e.target.value)} />
      </div>
      <div className="set-h" style={{ padding: 0, marginBottom: 7 }}>Payload</div>
      <textarea className="kv-editor mono" style={{ width: '100%', minHeight: 84, resize: 'vertical' }} value={payload} onChange={(e) => setPayload(e.target.value)} />
      <div className="ts-frow" style={{ marginTop: 14 }}>
        <div>
          <div className="set-h" style={{ padding: 0 }}>Priority <span style={{ color: pc, fontWeight: 600, marginLeft: 6 }}>{prioMeta(prio)[1]}</span></div>
          <div className="set-d">0 = highest · u8 range 0–255</div>
        </div>
        <Field className="mono" wrapStyle={{ width: 96 }} type="number" min={0} max={255} value={prio} onChange={(e) => setPrio(Math.max(0, Math.min(255, +e.target.value || 0)))} />
      </div>
      <div className="ts-frow">
        <div>
          <div className="set-h" style={{ padding: 0 }}>Delay</div>
          <div className="set-d">Invisible for N ms after enqueue</div>
        </div>
        <Field className="mono" wrapStyle={{ width: 140 }} type="number" min={0} value={delay} onChange={(e) => setDelay(Math.max(0, +e.target.value || 0))} trailing={<span className="mono" style={{ fontSize: 11, color: 'var(--tx-faint)' }}>ms</span>} />
      </div>
      <div className="set-h" style={{ padding: 0, margin: '16px 0 7px' }}>Command</div>
      <pre className="code">{cmd}</pre>
    </Modal>
  )
}
