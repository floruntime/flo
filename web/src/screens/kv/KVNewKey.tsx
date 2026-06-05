import { useState } from 'react'
import { Modal } from '@/components/overlay/Modal'
import { Button } from '@/components/buttons/Button'
import { Field, TextArea, SegGroup, Checkbox } from '@/components/inputs'

export type NewKeyPayload = { key: string; value: string; ttlSeconds: number | null; nx: boolean }

const TTL_PRESETS = [
  ['none', 'No expiry'],
  ['60s', '60s'],
  ['1h', '1 hour'],
  ['24h', '24 hours'],
  ['custom', 'Custom'],
] as const

/** Parse "30s" / "15m" / "2h" / "7d" → seconds. */
function parseDuration(s: string): number | null {
  const m = s.trim().match(/^(\d+)\s*([smhd])$/i)
  if (!m) return null
  const n = parseInt(m[1], 10)
  return n * { s: 1, m: 60, h: 3600, d: 86400 }[m[2].toLowerCase() as 's' | 'm' | 'h' | 'd']
}

export function KVNewKey({
  existingKeys = [],
  initial,
  onClose,
  onSave,
}: {
  existingKeys?: string[]
  initial?: { key: string; value: string }
  onClose: () => void
  onSave: (p: NewKeyPayload) => void
}) {
  const editing = !!initial
  const [key, setKey] = useState(initial?.key ?? '')
  const [fmt, setFmt] = useState<'json' | 'raw'>('json')
  const [val, setVal] = useState(initial?.value ?? '{\n  "value": ""\n}')
  const [ttlMode, setTtlMode] = useState('none')
  const [ttlCustom, setTtlCustom] = useState('')
  const [nx, setNx] = useState(false)
  const [tried, setTried] = useState(false)

  const k = key.trim()
  const dup = !editing && !!k && existingKeys.includes(k)
  let vErr: string | null = null
  if (!val.trim()) vErr = 'Value is empty'
  else if (fmt === 'json') {
    try {
      JSON.parse(val)
    } catch {
      vErr = 'Invalid JSON'
    }
  }
  const keyErr = !k ? 'Enter a key' : dup && nx ? 'Key already exists (NX)' : null
  const valid = !keyErr && !vErr
  const ttlSeconds = ttlMode === 'none' ? null : ttlMode === 'custom' ? parseDuration(ttlCustom) : parseDuration(ttlMode)
  const bytes = new Blob([val]).size

  const save = () => {
    if (!valid) {
      setTried(true)
      return
    }
    onSave({ key: k, value: val, ttlSeconds, nx })
  }

  return (
    <Modal
      title={editing ? 'Edit value' : 'Create key'}
      sub={editing ? 'Writes a new version' : 'Strongly-consistent · versioned · linearizable'}
      width="min(600px,94vw)"
      onClose={onClose}
      foot={
        <>
          <Button variant="quiet" onClick={onClose}>Cancel</Button>
          <Button variant="accent" onClick={save} style={{ opacity: valid ? 1 : 0.5 }}>
            {editing ? 'Save' : 'Create key'}
          </Button>
        </>
      }
    >
      <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
        <div>
          <div className="set-h" style={{ padding: 0, marginBottom: 7 }}>Key</div>
          <Field
            className="mono"
            style={{ fontFamily: 'var(--mono)' }}
            placeholder="user:142:profile"
            value={key}
            onChange={(e) => setKey(e.target.value)}
            readOnly={editing}
            autoFocus={!editing}
            invalid={tried && !!keyErr}
            wrapStyle={{ opacity: editing ? 0.7 : 1 }}
          />
          <div className="kv-hint" style={{ color: (dup && !nx) || (tried && keyErr) ? 'var(--crit)' : 'var(--tx-faint)' }}>
            {editing
              ? 'Writes a new MVCC version'
              : dup && !nx
                ? 'Key exists — saving creates a new version'
                : tried && keyErr
                  ? keyErr
                  : 'Any string. : conventionally groups keys but isn’t required.'}
          </div>
        </div>
        {!editing && (
          <Checkbox checked={nx} onChange={setNx}>
            <b>Only if absent</b> <span className="mono" style={{ color: 'var(--tx-faint)' }}>--nx</span> · fail if the key already exists
          </Checkbox>
        )}
        <div>
          <div className="set-h" style={{ padding: 0, marginBottom: 7 }}>Time to live</div>
          <SegGroup options={TTL_PRESETS.map(([id, label]) => ({ id, label }))} value={ttlMode} onChange={setTtlMode} />
          {ttlMode === 'custom' && (
            <Field wrapStyle={{ width: 180, marginTop: 9 }} placeholder="30s / 15m / 7d" value={ttlCustom} onChange={(e) => setTtlCustom(e.target.value)} />
          )}
        </div>
        <div>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 7 }}>
            <div className="set-h" style={{ padding: 0, margin: 0 }}>Value</div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
              <SegGroup
                style={{ padding: 2 }}
                btnStyle={{ padding: '4px 11px' }}
                options={[{ id: 'json', label: 'JSON' }, { id: 'raw', label: 'Raw' }]}
                value={fmt}
                onChange={setFmt}
              />
              <span className="mono" style={{ fontSize: 11.5, color: vErr && fmt === 'json' ? 'var(--crit)' : 'var(--accent)' }}>
                {fmt === 'raw' ? bytes + ' B' : vErr ? 'invalid' : 'valid · ' + bytes + ' B'}
              </span>
            </div>
          </div>
          <TextArea
            invalid={fmt === 'json' && !!vErr && !!val.trim()}
            value={val}
            onChange={(e) => setVal(e.target.value)}
            placeholder={fmt === 'raw' ? 'any bytes — text, base64, a token…' : '{\n  "value": ""\n}'}
          />
        </div>
      </div>
    </Modal>
  )
}
