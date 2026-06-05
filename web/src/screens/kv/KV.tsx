import { useEffect, useState } from 'react'
import { jhl } from '@/lib/format'
import { useNamespace } from '@/lib/namespace'
import { useKVKeys, useKVKey, useKVHistory, usePutKVKey, useDeleteKVKey } from '@/lib/api/kv'
import { Stats, Stat } from '@/components/layout'
import { Button } from '@/components/buttons/Button'
import { Field } from '@/components/inputs'
import { Pill, Tag } from '@/components/feedback'
import { Modal } from '@/components/overlay/Modal'
import { TrashIcon, XIcon } from '@/lib/icons'
import { KVNewKey } from './KVNewKey'
import { KVTouch } from './KVTouch'

const isCounter = (v?: string) => !!v && /^-?\d+$/.test(v.trim())

function ttlLabel(ttl_ms?: number | null): string {
  if (ttl_ms == null) return '—'
  const secs = Math.round((ttl_ms - Date.now()) / 1000)
  if (secs <= 0) return 'expired'
  if (secs < 60) return secs + 's'
  if (secs < 3600) return Math.floor(secs / 60) + 'm'
  if (secs < 86400) return Math.floor(secs / 3600) + 'h'
  return Math.floor(secs / 86400) + 'd'
}

export function KV() {
  const { ns } = useNamespace()
  const [search, setSearch] = useState('')
  const [sel, setSel] = useState<string | null>(null)
  const [newOpen, setNewOpen] = useState(false)
  const [editOpen, setEditOpen] = useState(false)
  const [touchOpen, setTouchOpen] = useState(false)
  const [delOpen, setDelOpen] = useState(false)

  const keysQ = useKVKeys(ns, search)
  const keys = keysQ.data?.keys ?? []
  const detailQ = useKVKey(ns, sel)
  const historyQ = useKVHistory(ns, sel)
  const put = usePutKVKey(ns)
  const del = useDeleteKVKey(ns)

  // Reset selection on namespace change; default-select the first key once loaded.
  useEffect(() => setSel(null), [ns])
  useEffect(() => {
    if (!sel && keys.length) setSel(keys[0].key)
  }, [keys, sel])

  const detail = detailQ.data
  const versions = historyQ.data?.versions ?? []
  const maxLsn = keys.reduce((m, k) => Math.max(m, k.version), 0)
  const totalBytes = keys.reduce((a, k) => a + k.size, 0)
  const value = detail?.found ? detail.value ?? '' : ''
  const counter = detail?.found && isCounter(value)
  const detailObj = (() => {
    if (!detail?.found) return null
    try {
      return JSON.parse(value)
    } catch {
      return null
    }
  })()

  const doDelete = () => del.mutate(sel!, { onSuccess: () => { setDelOpen(false); setSel(null) } })
  const setTTL = (ttlSeconds: number | null) =>
    put.mutate({ key: sel!, body: { value, ttl_seconds: ttlSeconds } }, { onSuccess: () => setTouchOpen(false) })

  return (
    <div className="wrap fade">
      <div className="ph">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16, flexWrap: 'wrap' }}>
          <div>
            <h1 className="ph-t">KV Store</h1>
            <div className="ph-s">Versioned · strongly consistent · MVCC time-travel</div>
          </div>
          <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
            <Field
              wrapStyle={{ width: 220 }}
              placeholder="Search keys…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              trailing={
                search ? (
                  <button onClick={() => setSearch('')} style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--tx-3)', display: 'flex', padding: 0 }}>
                    <XIcon />
                  </button>
                ) : undefined
              }
            />
            <Button variant="accent" style={{ flex: 'none' }} onClick={() => setNewOpen(true)}>
              New key
            </Button>
          </div>
        </div>
      </div>

      <Stats>
        <Stat label="Keys" value={keysQ.isLoading ? '…' : (keysQ.data?.count ?? 0).toLocaleString()} sub={`${ns} namespace`} />
        <Stat label="Stored" value={(totalBytes / 1024).toFixed(1)} unit="KB" sub="across listed keys" />
        <Stat label="Max LSN" value={maxLsn.toLocaleString()} sub="latest write" />
        <Stat label="Namespace" value={<span className="mono" style={{ fontSize: 18 }}>{ns}</span>} />
      </Stats>

      <div className="sec" style={{ display: 'grid', gridTemplateColumns: '1.4fr 1fr', gap: 20, alignItems: 'start' }}>
        {keysQ.isError ? (
          <div className="card card-p" style={{ textAlign: 'center', color: 'var(--crit)', padding: '44px 20px' }}>
            Failed to load keys. Is the server running?
          </div>
        ) : keys.length === 0 ? (
          <div className="card card-p" style={{ textAlign: 'center', color: 'var(--tx-3)', padding: '44px 20px' }}>
            {keysQ.isLoading ? 'Loading…' : <>No keys{search ? <> match “{search}”</> : <> in <span className="mono">{ns}</span></>}</>}
          </div>
        ) : (
          <div className="list" style={{ minWidth: 0 }}>
            <div className="li head" style={{ gridTemplateColumns: '1fr 72px 60px 30px' }}>
              <div className="c">Key</div>
              <div className="c r">LSN</div>
              <div className="c r">Size</div>
              <div />
            </div>
            {keys.map((k) => (
              <div
                className="li"
                key={k.key}
                style={{ gridTemplateColumns: '1fr 72px 60px 30px', background: sel === k.key ? 'var(--hover)' : undefined }}
                onClick={() => setSel(k.key)}
              >
                <span className="mono" style={{ fontSize: 13, color: 'var(--tx)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                  {k.key}
                </span>
                <span className="c r"><Tag style={{ fontSize: 10.5, padding: '1px 6px' }}>{k.version}</Tag></span>
                <span className="c r dim mono" style={{ fontSize: 11.5 }}>{k.size} B</span>
                <button className="gtrash" title={'Delete ' + k.key} onClick={(e) => { e.stopPropagation(); setSel(k.key); setDelOpen(true) }}>
                  <TrashIcon />
                </button>
              </div>
            ))}
          </div>
        )}

        {sel && (
          <div className="card card-p" style={{ minWidth: 0 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', gap: 10, marginBottom: 4 }}>
              <span className="mono" style={{ fontSize: 13, fontWeight: 500, color: 'var(--tx)', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {sel}
              </span>
              <Pill tone={detail?.found ? 'ok' : 'default'} dot={false} style={{ flex: 'none' }}>
                <span className="d" style={{ background: detail?.found ? 'var(--accent)' : 'var(--tx-faint)' }} />
                {detailQ.isLoading ? 'loading' : detail?.found ? 'current' : 'not found'}
              </Pill>
            </div>
            <div className="mono" style={{ fontSize: 11.5, color: 'var(--tx-faint)', marginBottom: 12 }}>
              version {detail?.version ?? '—'} · {detail?.size ?? 0} B · TTL {ttlLabel(detail?.ttl_ms)}
              {counter ? ' · counter' : ''}
            </div>

            <div style={{ display: 'flex', gap: 7, marginBottom: 14, flexWrap: 'wrap' }}>
              <Button style={{ padding: '5px 11px', fontSize: 12 }} onClick={() => setEditOpen(true)} disabled={!detail?.found}>
                Edit
              </Button>
              <Button style={{ padding: '5px 11px', fontSize: 12 }} onClick={() => setTouchOpen(true)} disabled={!detail?.found}>
                Touch TTL
              </Button>
              <Button style={{ padding: '5px 11px', fontSize: 12 }} onClick={() => setTTL(null)} disabled={!detail?.found || put.isPending}>
                Persist
              </Button>
              <Button variant="danger" style={{ padding: '5px 11px', fontSize: 12 }} onClick={() => setDelOpen(true)}>
                <TrashIcon />
                Delete
              </Button>
            </div>

            {detailQ.isLoading ? (
              <div style={{ color: 'var(--tx-3)', fontSize: 13, padding: '12px 0' }}>Loading value…</div>
            ) : counter ? (
              <div style={{ background: 'var(--card-2)', border: '1px solid var(--line-soft)', borderRadius: 'var(--r)', padding: '18px' }}>
                <div className="set-h" style={{ padding: 0, marginBottom: 8 }}>Counter</div>
                <div style={{ fontSize: 30, fontWeight: 600, fontFamily: 'var(--mono)', color: 'var(--accent)' }}>{value}</div>
                <div className="kv-hint" style={{ color: 'var(--tx-faint)' }}>64-bit · atomic incr</div>
              </div>
            ) : detailObj !== null ? (
              <pre className="code" dangerouslySetInnerHTML={{ __html: jhl(detailObj) }} />
            ) : (
              <pre className="code" style={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>{value || '(empty)'}</pre>
            )}

            <div style={{ marginTop: 18 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
                <span className="set-h" style={{ padding: 0, margin: 0 }}>Version history</span>
                <span className="mono" style={{ fontSize: 11.5, color: 'var(--tx-3)' }}>{historyQ.data?.version_count ?? versions.length} version(s)</span>
              </div>
              {versions.length === 0 ? (
                <div style={{ color: 'var(--tx-faint)', fontSize: 12 }}>No history.</div>
              ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                  {versions.slice(0, 8).map((v) => (
                    <div key={v.version} className="kvrow2" style={{ borderBottom: '1px solid var(--line-soft)' }}>
                      <span className="mono" style={{ fontSize: 12, color: v.tombstone ? 'var(--crit)' : 'var(--tx-2)' }}>
                        v{v.version}{v.tombstone ? ' · tombstone' : ''}
                      </span>
                      <span className="mono" style={{ fontSize: 11.5, color: 'var(--tx-faint)' }}>
                        {new Date(v.timestamp_ms).toLocaleString('en-GB', { day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit' })} · {v.size} B
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {newOpen && (
        <KVNewKey
          existingKeys={keys.map((k) => k.key)}
          onClose={() => setNewOpen(false)}
          onSave={(p) => put.mutate({ key: p.key, body: { value: p.value, ttl_seconds: p.ttlSeconds, nx: p.nx } }, { onSuccess: () => { setNewOpen(false); setSel(p.key) } })}
        />
      )}
      {editOpen && detail?.found && (
        <KVNewKey
          initial={{ key: sel!, value }}
          onClose={() => setEditOpen(false)}
          onSave={(p) => put.mutate({ key: sel!, body: { value: p.value, ttl_seconds: p.ttlSeconds } }, { onSuccess: () => setEditOpen(false) })}
        />
      )}
      {touchOpen && <KVTouch keyName={sel!} onClose={() => setTouchOpen(false)} onSave={setTTL} />}
      {delOpen && (
        <Modal
          title="Delete key"
          sub={sel!}
          width="min(440px,92vw)"
          onClose={() => setDelOpen(false)}
          foot={
            <>
              <Button variant="quiet" onClick={() => setDelOpen(false)}>Cancel</Button>
              <Button variant="danger" onClick={doDelete} disabled={del.isPending}>
                <TrashIcon />
                Delete key
              </Button>
            </>
          }
        >
          <p style={{ margin: 0, fontSize: 13.5, color: 'var(--tx-2)', lineHeight: 1.55 }}>
            Permanently delete <span className="mono" style={{ color: 'var(--tx)' }}>{sel}</span> from{' '}
            <span className="mono" style={{ color: 'var(--tx)' }}>{ns}</span>? Writes a tombstone at the next LSN.
          </p>
        </Modal>
      )}
    </div>
  )
}
