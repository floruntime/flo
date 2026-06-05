/* Flo Console v2 — Actions screen (live).
   Durable task types · worker-hosted · retries, timeouts, dead-letter.

   Wired to the dashboard API. Actions are namespace-scoped server-side. The API
   exposes registration metadata, run counts, recent runs (input/output/error) and
   the workers handling each action — but NOT latency percentiles (p50/p99) or
   per-action invocation-rate, and `timeout_ms`/`owner`/`description` aren't persisted
   on the action record yet. Invoke is a real loopback write (async → run_id);
   Register is command-only (no dashboard endpoint). See API_INTEGRATION.md. */
import { useMemo, useState } from 'react'
import { jhl } from '@/lib/format'
import { useNamespace } from '@/lib/namespace'
import { Button, Crumb, Stats, Stat, OpPill, Modal, Field } from '@/components'
import { useActions, useActionDetail, useActionRuns, useInvokeAction } from '@/lib/api/actions'
import type { ActionInfo, ActionRun, ActionRunCounts } from '@/lib/api/types'
import { ASvc, MapPill, afmtN, A_RUNST } from './shared'
import './compute.css'

type Tab = 'overview' | 'runs'

const errCount = (r: ActionRunCounts): number => r.failed + r.timed_out
const durMs = (run: ActionRun): number | null =>
  run.completed_at != null && run.started_at != null ? run.completed_at - run.started_at : null
const fmtDur = (ms: number): string => (ms < 1000 ? ms + 'ms' : (ms / 1000).toFixed(1) + 's')
function tryJsonHl(s: string | null): { __html: string } | null {
  if (!s) return null
  try {
    return { __html: jhl(JSON.parse(s)) }
  } catch {
    return null
  }
}

/* ======================= register (command-only) ======================= */
function RegisterModal({ ns, onClose }: { ns: string; onClose: () => void }) {
  const [name, setName] = useState('my-action')
  const [timeout, setTimeout] = useState(30000)
  const [retries, setRetries] = useState(3)
  const cmd = `flo action register ${name} --timeout ${timeout} --retries ${retries} -n ${ns}`
  return (
    <Modal
      title="Register action"
      sub="Run the command below — the dashboard has no register endpoint"
      width="min(620px,96vw)"
      onClose={onClose}
      foot={
        <>
          <Button variant="quiet" onClick={onClose}>
            Cancel
          </Button>
          <Button variant="accent" onClick={() => navigator.clipboard?.writeText(cmd)}>
            Copy command
          </Button>
        </>
      }
    >
      <div className="ts-frow">
        <div className="set-h" style={{ padding: 0 }}>
          Name
        </div>
        <Field className="mono" wrapStyle={{ width: 240 }} value={name} onChange={(e) => setName(e.target.value)} />
      </div>
      <div className="ts-frow">
        <div className="set-h" style={{ padding: 0 }}>
          Timeout
        </div>
        <Field
          className="mono"
          wrapStyle={{ width: 140 }}
          type="number"
          min={0}
          value={timeout}
          onChange={(e) => setTimeout(Math.max(0, +e.target.value || 0))}
          trailing={<span className="mono" style={{ fontSize: 11, color: 'var(--tx-faint)' }}>ms</span>}
        />
      </div>
      <div className="ts-frow">
        <div className="set-h" style={{ padding: 0 }}>
          Max retries
        </div>
        <Field
          className="mono"
          wrapStyle={{ width: 96 }}
          type="number"
          min={0}
          value={retries}
          onChange={(e) => setRetries(Math.max(0, +e.target.value || 0))}
        />
      </div>
      <div className="set-h" style={{ padding: 0, margin: '16px 0 7px' }}>
        Command
      </div>
      <pre className="code">{cmd}</pre>
    </Modal>
  )
}

/* ======================= invoke (real loopback write) ======================= */
function InvokeModal({ ns, name, onClose }: { ns: string; name: string; onClose: () => void }) {
  const [payload, setPayload] = useState('{"to":"alice@example.com","subject":"Welcome!"}')
  const invoke = useInvokeAction(ns)
  const [parseErr, setParseErr] = useState<string | null>(null)

  const run = () => {
    let input: unknown
    try {
      input = JSON.parse(payload)
    } catch {
      setParseErr('Payload must be valid JSON')
      return
    }
    setParseErr(null)
    invoke.mutate({ name, input })
  }

  const result = invoke.data
  return (
    <Modal
      title={`Invoke ${name}`}
      sub="Async invocation via loopback — returns a run_id immediately"
      width="min(620px,96vw)"
      onClose={onClose}
      foot={
        <>
          <Button variant="quiet" onClick={onClose}>
            {result ? 'Close' : 'Cancel'}
          </Button>
          <Button variant="accent" onClick={run} disabled={invoke.isPending}>
            {invoke.isPending ? 'Invoking…' : 'Invoke'}
          </Button>
        </>
      }
    >
      <div className="set-h" style={{ padding: 0, marginBottom: 7 }}>
        Input · JSON
      </div>
      <textarea
        className="kv-editor mono"
        style={{ width: '100%', minHeight: 96, resize: 'vertical' }}
        value={payload}
        onChange={(e) => setPayload(e.target.value)}
      />
      {parseErr && (
        <div className="mono" style={{ fontSize: 12, color: 'var(--crit)', marginTop: 8 }}>
          {parseErr}
        </div>
      )}
      {invoke.isError && (
        <div className="mono" style={{ fontSize: 12, color: 'var(--crit)', marginTop: 8 }}>
          {String((invoke.error as Error)?.message ?? 'Invoke failed')}
        </div>
      )}
      {result && (
        <div
          style={{
            marginTop: 14,
            background: 'var(--card-2)',
            border: '1px solid var(--line-soft)',
            borderRadius: 'var(--r-sm)',
            padding: '11px 13px',
            fontSize: 12.5,
          }}
        >
          <span style={{ color: 'var(--accent)' }}>✓ {result.status}</span>{' '}
          <span className="mono" style={{ color: 'var(--tx-2)' }}>
            run_id={result.run_id}
          </span>
          <div className="mono" style={{ color: 'var(--tx-faint)', marginTop: 4 }}>
            queued · stays pending until a worker leases it
          </div>
        </div>
      )}
    </Modal>
  )
}

/* ======================= detail ======================= */
function ActionDetail({ ns, name, onBack }: { ns: string; name: string; onBack: () => void }) {
  const q = useActionDetail(ns, name)
  const runsQ = useActionRuns(ns, name)
  const [tab, setTab] = useState<Tab>('overview')
  const [runSel, setRunSel] = useState<ActionRun | null>(null)
  const [invoke, setInvoke] = useState(false)

  const a = q.data
  const runs = runsQ.data ?? []
  const counts = a?.runs
  const errs = counts ? errCount(counts) : 0
  const errRate = counts && counts.total ? ((errs / counts.total) * 100).toFixed(2) : '0'

  const inputHl = runSel ? tryJsonHl(runSel.input) : null
  const outputHl = runSel ? tryJsonHl(runSel.output) : null

  return (
    <div className="wrap fade">
      <Crumb root="Actions" onRoot={onBack} current={name} />
      <div className="ph">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16, flexWrap: 'wrap' }}>
          <div>
            <h1 className="ph-t" style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              {name}
              {a && !a.enabled && <OpPill color="var(--warn)">disabled</OpPill>}
            </h1>
            <div className="ph-s">
              {a ? `${a.type} · v${a.version} · ${a.namespace}` : 'loading…'}
            </div>
          </div>
          <div style={{ display: 'flex', gap: 9 }}>
            <Button variant="accent" style={{ flex: 'none' }} disabled={!a?.enabled} onClick={() => setInvoke(true)}>
              Invoke
            </Button>
          </div>
        </div>
      </div>

      <Stats>
        <Stat label="Invocations" value={afmtN(counts?.total ?? 0)} sub="all time" />
        <Stat label="Completed" value={afmtN(counts?.completed ?? 0)} sub={`${counts?.pending ?? 0} pending`} />
        <Stat
          label="Errors"
          value={errs}
          valueColor={errs ? 'var(--crit)' : undefined}
          sub={`${errRate}% rate`}
        />
        <Stat label="Workers" value={a?.workers?.length ?? 0} sub="handling this action" />
      </Stats>

      {q.isError ? (
        <div className="card card-p" style={{ textAlign: 'center', color: 'var(--crit)', padding: '40px 0' }}>
          Failed to load action. Is the server running?
        </div>
      ) : (
        <div className="sec">
          <div className="wtabs">
            {(
              [
                ['overview', 'Overview'],
                ['runs', 'Runs · ' + (runsQ.isLoading ? '…' : runs.length)],
              ] as [Tab, string][]
            ).map(([id, l]) => (
              <button key={id} className={'wtab' + (tab === id ? ' on' : '')} onClick={() => setTab(id)}>
                {l}
              </button>
            ))}
          </div>

          {tab === 'overview' && (
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, alignItems: 'start' }}>
              <div className="card card-p">
                <ASvc icon="info" title="Configuration" />
                {(
                  [
                    ['Type', a?.type ?? '—'],
                    ['Namespace', a?.namespace ?? ns],
                    ['Version', a ? 'v' + a.version : '—'],
                    ['Max retries', a?.max_retries ?? '—'],
                    ['Enabled', a ? (a.enabled ? 'yes' : 'no') : '—'],
                  ] as [string, string | number][]
                ).map(([k, v]) => (
                  <div className="kvrow2" key={k}>
                    <span>{k}</span>
                    <span className="mono" style={{ color: 'var(--tx)' }}>
                      {v}
                    </span>
                  </div>
                ))}
                <div className="mono" style={{ fontSize: 11, color: 'var(--tx-faint)', marginTop: 8 }}>
                  timeout / retry-delay / owner aren’t persisted on the record yet.
                </div>
              </div>
              <div className="card card-p">
                <ASvc icon="cpu" title="Execution" />
                {(
                  [
                    ['Model', 'user-hosted workers'],
                    ['Dispatch', 'long-poll await'],
                    ['Workers', String(a?.workers?.length ?? 0)],
                  ] as [string, string][]
                ).map(([k, v]) => (
                  <div className="kvrow2" key={k}>
                    <span>{k}</span>
                    <span className="mono" style={{ color: 'var(--tx)' }}>
                      {v}
                    </span>
                  </div>
                ))}
                <div className="kvrow2">
                  <span>Run states</span>
                  <span style={{ display: 'flex', gap: 6, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                    {counts &&
                      (['pending', 'running', 'completed', 'failed', 'timed_out'] as const)
                        .filter((s) => counts[s] > 0)
                        .map((s) => (
                          <span key={s} className="tag">
                            {A_RUNST[s]?.[1] ?? s} {counts[s]}
                          </span>
                        ))}
                    {counts && counts.total === 0 && (
                      <span className="mono" style={{ color: 'var(--tx-faint)' }}>
                        no runs
                      </span>
                    )}
                  </span>
                </div>
              </div>
            </div>
          )}

          {tab === 'runs' && (
            <div style={{ display: 'grid', gridTemplateColumns: runSel ? '1.3fr 1fr' : '1fr', gap: 20, alignItems: 'start' }}>
              {runs.length === 0 ? (
                <div className="card card-p" style={{ textAlign: 'center', color: 'var(--tx-3)', padding: '40px 0' }}>
                  {runsQ.isLoading ? 'Loading…' : 'No runs yet. Invoke the action to create one.'}
                </div>
              ) : (
                <div className="list">
                  <div className="li head" style={{ gridTemplateColumns: '1.4fr 110px 80px 90px' }}>
                    <div className="c">Run</div>
                    <div className="c">Status</div>
                    <div className="c r">Source</div>
                    <div className="c r">Duration</div>
                  </div>
                  {runs.map((r) => {
                    const d = durMs(r)
                    return (
                      <div
                        className="li"
                        key={r.run_id}
                        style={{
                          gridTemplateColumns: '1.4fr 110px 80px 90px',
                          background: runSel?.run_id === r.run_id ? 'var(--hover)' : undefined,
                        }}
                        onClick={() => setRunSel(r)}
                      >
                        <span className="mono nw" style={{ fontSize: 12.5, color: 'var(--tx)' }}>
                          {r.run_id}
                        </span>
                        <span>
                          <MapPill map={A_RUNST} s={r.status} />
                        </span>
                        <span className="c r mono" style={{ fontSize: 11.5, color: 'var(--tx-3)' }}>
                          {r.source}
                        </span>
                        <span className="c r mono" style={{ fontSize: 12, color: 'var(--tx-3)' }}>
                          {d != null ? fmtDur(d) : '—'}
                        </span>
                      </div>
                    )
                  })}
                </div>
              )}
              {runSel && (
                <div className="card card-p" style={{ minWidth: 0 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', gap: 10, marginBottom: 12 }}>
                    <span className="mono" style={{ fontSize: 13, color: 'var(--tx)' }}>
                      {runSel.run_id}
                    </span>
                    <MapPill map={A_RUNST} s={runSel.status} />
                  </div>
                  <div className="set-h" style={{ padding: 0, marginBottom: 6 }}>
                    Input
                  </div>
                  {inputHl ? (
                    <pre className="code" dangerouslySetInnerHTML={inputHl} />
                  ) : (
                    <pre className="code">{runSel.input ?? '—'}</pre>
                  )}
                  <div className="set-h" style={{ padding: 0, margin: '12px 0 6px' }}>
                    {runSel.status === 'failed' || runSel.status === 'timed_out' ? 'Error' : 'Output'}
                  </div>
                  {runSel.error ? (
                    <div className="code" style={{ color: 'var(--crit)' }}>
                      {runSel.error}
                    </div>
                  ) : outputHl ? (
                    <pre className="code" dangerouslySetInnerHTML={outputHl} />
                  ) : runSel.output ? (
                    <pre className="code">{runSel.output}</pre>
                  ) : (
                    <div className="code" style={{ color: 'var(--tx-faint)' }}>
                      — {runSel.status === 'pending' ? 'pending — awaiting a worker' : 'no output'} —
                    </div>
                  )}
                  {runSel.worker_id && (
                    <div className="kvrow2" style={{ marginTop: 10 }}>
                      <span>Worker</span>
                      <span className="mono" style={{ color: 'var(--tx-2)' }}>
                        {runSel.worker_id}
                      </span>
                    </div>
                  )}
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {invoke && <InvokeModal ns={ns} name={name} onClose={() => setInvoke(false)} />}
    </div>
  )
}

/* ======================= list ======================= */
type ListTab = 'all' | 'enabled' | 'disabled'

function ActionsList({ ns, onOpen }: { ns: string; onOpen: (name: string) => void }) {
  const q = useActions(ns)
  const rows = useMemo<ActionInfo[]>(() => q.data ?? [], [q.data])
  const [tab, setTab] = useState<ListTab>('all')
  const [reg, setReg] = useState(false)

  const totalInv = rows.reduce((acc, r) => acc + r.runs.total, 0)
  const totalErr = rows.reduce((acc, r) => acc + errCount(r.runs), 0)
  const enabledCount = rows.filter((r) => r.enabled).length
  const filtered = tab === 'all' ? rows : tab === 'enabled' ? rows.filter((r) => r.enabled) : rows.filter((r) => !r.enabled)

  return (
    <div className="wrap fade">
      <div className="ph">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16 }}>
          <div>
            <h1 className="ph-t">Actions</h1>
            <div className="ph-s">Durable task types · worker-hosted · retries, timeouts, dead-letter</div>
          </div>
          <Button variant="accent" style={{ flex: 'none' }} onClick={() => setReg(true)}>
            Register action
          </Button>
        </div>
      </div>

      <Stats>
        <Stat label="Actions" value={q.isLoading ? '…' : rows.length} sub={`${enabledCount} enabled · in ${ns}`} />
        <Stat label="Invocations" value={afmtN(totalInv)} sub="all time" />
        <Stat label="Errors" value={totalErr} valueColor={totalErr ? 'var(--crit)' : undefined} sub={totalErr ? 'failed + timed out' : 'all clear'} />
        <Stat label="Disabled" value={rows.length - enabledCount} sub="blocked" />
      </Stats>

      <div className="sec">
        <div className="wtabs">
          {(
            [
              ['all', 'All · ' + rows.length],
              ['enabled', 'Enabled'],
              ['disabled', 'Disabled'],
            ] as [ListTab, string][]
          ).map(([id, l]) => (
            <button key={id} className={'wtab' + (tab === id ? ' on' : '')} onClick={() => setTab(id)}>
              {l}
            </button>
          ))}
        </div>

        {q.isError ? (
          <div className="card card-p" style={{ textAlign: 'center', color: 'var(--crit)', padding: '40px 0' }}>
            Failed to load actions. Is the server running?
          </div>
        ) : filtered.length === 0 ? (
          <div className="card card-p" style={{ textAlign: 'center', color: 'var(--tx-3)', padding: '40px 0' }}>
            {q.isLoading ? 'Loading…' : `No ${tab === 'all' ? '' : tab + ' '}actions in ${ns}.`}
          </div>
        ) : (
          <div className="list">
            <div className="li head" style={{ gridTemplateColumns: '1.5fr 100px 90px 70px' }}>
              <div className="c">Action</div>
              <div className="c r">Invocations</div>
              <div className="c r">Workers</div>
              <div className="c r">Errors</div>
            </div>
            {filtered.map((r) => {
              const errs = errCount(r.runs)
              return (
                <div className="li" key={r.name} style={{ gridTemplateColumns: '1.5fr 100px 90px 70px' }} onClick={() => onOpen(r.name)}>
                  <div style={{ minWidth: 0 }}>
                    <div className="mono nw" style={{ fontWeight: 500, color: r.enabled ? 'var(--tx)' : 'var(--tx-3)' }}>
                      {r.name}
                      {!r.enabled && (
                        <span style={{ marginLeft: 8 }}>
                          <OpPill color="var(--warn)">off</OpPill>
                        </span>
                      )}
                    </div>
                    <div className="desc">
                      {r.type} · {r.runs.pending} pending
                    </div>
                  </div>
                  <span className="c r mono">{afmtN(r.runs.total)}</span>
                  <span className="c r mono" style={{ color: r.worker_count ? 'var(--info)' : 'var(--tx-3)' }}>
                    {r.worker_count}
                  </span>
                  <span className="c r mono" style={{ color: errs > 20 ? 'var(--crit)' : errs > 0 ? 'var(--warn)' : 'var(--tx-3)' }}>
                    {errs}
                  </span>
                </div>
              )
            })}
          </div>
        )}
      </div>

      {reg && <RegisterModal ns={ns} onClose={() => setReg(false)} />}
    </div>
  )
}

/** Actions screen — master list ↔ detail (internal state, matching the design). */
export function Actions() {
  const { ns } = useNamespace()
  const [open, setOpen] = useState<string | null>(null)
  return open ? (
    <ActionDetail ns={ns} name={open} onBack={() => setOpen(null)} />
  ) : (
    <ActionsList ns={ns} onOpen={setOpen} />
  )
}
