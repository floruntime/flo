import { useState } from 'react'
import { Modal } from '@/components/overlay/Modal'
import { Button } from '@/components/buttons/Button'
import { Field, SegGroup } from '@/components/inputs'

const MODES = [
  { id: '60s', label: '60s' },
  { id: '1h', label: '1 hour' },
  { id: '24h', label: '24 hours' },
  { id: 'custom', label: 'Custom' },
  { id: 'persist', label: 'Persist' },
]

function parseDuration(s: string): number | null {
  const m = s.trim().match(/^(\d+)\s*([smhd])$/i)
  if (!m) return null
  return parseInt(m[1], 10) * { s: 1, m: 60, h: 3600, d: 86400 }[m[2].toLowerCase() as 's' | 'm' | 'h' | 'd']
}

const PRESET_SECONDS: Record<string, number> = { '60s': 60, '1h': 3600, '24h': 86400 }

export function KVTouch({
  keyName,
  onClose,
  onSave,
}: {
  keyName: string
  onClose: () => void
  onSave: (ttlSeconds: number | null) => void
}) {
  const [mode, setMode] = useState('60s')
  const [custom, setCustom] = useState('')
  const ttlSeconds = mode === 'persist' ? null : mode === 'custom' ? parseDuration(custom) : PRESET_SECONDS[mode]

  return (
    <Modal
      title="Set TTL"
      sub={keyName}
      width="min(460px,92vw)"
      onClose={onClose}
      foot={
        <>
          <Button variant="quiet" onClick={onClose}>Cancel</Button>
          <Button variant="accent" onClick={() => onSave(ttlSeconds)}>Apply</Button>
        </>
      }
    >
      <p style={{ margin: '0 0 14px', fontSize: 13, color: 'var(--tx-2)', lineHeight: 1.55 }}>
        Re-writes the key with a new lease. <span className="mono" style={{ color: 'var(--tx)' }}>Persist</span> clears the TTL.
      </p>
      <SegGroup options={MODES} value={mode} onChange={setMode} />
      {mode === 'custom' && (
        <Field wrapStyle={{ width: 180, marginTop: 10 }} placeholder="30s / 15m / 7d" value={custom} onChange={(e) => setCustom(e.target.value)} />
      )}
    </Modal>
  )
}
