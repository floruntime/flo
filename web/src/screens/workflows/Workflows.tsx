/* Flo Console v2 — Workflows screen (live).
   Durable orchestration · steps & plans · signals · history.

   Wired to the dashboard API. Workflows are namespace-scoped server-side. Reads
   (definitions, runs, run detail, event history) are real; the run's per-step
   results and the event timeline come straight from the engine. Start / cancel /
   enable / disable are real loopback writes. The list `steps[]` is often empty
   (the start step isn't a named step) so the Definition tab shows the raw YAML
   rather than a synthesized DAG. create + signal have no dashboard endpoint yet.
   See API_INTEGRATION.md. */
import { useMemo, useState } from 'react'
import { Button } from '@/components/buttons/Button'
import { Crumb, PhSec, Stat, Stats } from '@/components/layout'
import { StatusPill, OpPill } from '@/components'
import { Modal } from '@/components/overlay/Modal'
import { jhl, tfmt } from '@/lib/format'
import { useNamespace } from '@/lib/namespace'
import {
  useWorkflows,
  useWorkflowDef,
  useRuns,
  useRunDetail,
  useRunHistory,
  useStartRun,
  useCancelRun,
  useToggleWorkflow,
} from '@/lib/api/workflows'
import type { WorkflowDefInfo, WorkflowRunInfo, WorkflowHistoryEvent } from '@/lib/api/types'
import './workflows.css'

const WICON = {
  list: 'M9 6h11|M9 12h11|M9 18h11|M4.5 6h.01|M4.5 12h.01|M4.5 18h.01',
  history: 'M3.5 12a8.5 8.5 0 1 0 2.5-6|M3.5 4v4h4|M12 8v4.5l3 2',
  info: 'M12 3.5a8.5 8.5 0 1 0 0 17 8.5 8.5 0 0 0 0-17z|M12 11v5M12 8h.01',
}

/* status → [color, label] */
const WF_ST: Record<string, [string, string]> = {
  running: ['#6F9BD1', 'running'],
  waiting: ['#9e8cfc', 'waiting'],
  completed: ['var(--accent)', 'completed'],
  failed: ['var(--crit)', 'failed'],
  timed_out: ['var(--warn)', 'timed out'],
  pending: ['var(--tx-faint)', 'pending'],
  cancelled: ['var(--tx-faint)', 'cancelled'],
}
function Pill({ s }: { s: string }) {
  const [c, l] = WF_ST[s] ?? ['var(--tx-faint)', s]
  return <StatusPill color={c} label={l} />
}

/* event_type → timeline colour */
function evColor(t: string): string {
  if (/(completed|received|matched|fired)/.test(t)) return 'var(--accent)'
  if (/(failed|not_found|disabled|timeout|timed_out|cancelled)/.test(t)) return 'var(--crit)'
  if (/(waiting|signal|awaiting|approval)/.test(t)) return '#9e8cfc'
  if (/(retry|timer)/.test(t)) return 'var(--warn)'
  if (/(started|running|scheduled|tried)/.test(t)) return '#6F9BD1'
  return 'var(--tx-3)'
}
const evLabel = (t: string): string => t.replace(/_/g, ' ').replace(/^\w/, (c) => c.toUpperCase())

const ago = (ms: number): string => {
  if (!ms) return '—'
  const d = Date.now() - ms
  if (d < 60000) return Math.floor(d / 1000) + 's ago'
  if (d < 3600000) return Math.floor(d / 60000) + 'm ago'
  if (d < 86400000) return Math.floor(d / 3600000) + 'h ago'
  return Math.floor(d / 86400000) + 'd ago'
}
const fmtDur = (ms: number | null): string =>
  ms == null ? '—' : ms < 1000 ? ms + 'ms' : (ms / 1000).toFixed(1) + 's'

/* ============ start modal (loopback) ============ */
function StartModal({ ns, workflow, onClose }: { ns: string; workflow: string; onClose: () => void }) {
  const [payload, setPayload] = useState('{"msg":"hello"}')
  const [err, setErr] = useState<string | null>(null)
  const start = useStartRun(ns)
  const result = start.data
  const run = () => {
    try {
      JSON.parse(payload)
    } catch {
      setErr('Input must be valid JSON')
      return
    }
    setErr(null)
    start.mutate({ workflow, input: payload })
  }
  return (
    <Modal
      title={`Start ${workflow}`}
      sub="Starts a run via loopback — returns a run_id immediately"
      width="min(620px,96vw)"
      onClose={onClose}
      foot={
        <>
          <Button variant="quiet" onClick={onClose}>
            {result ? 'Close' : 'Cancel'}
          </Button>
          <Button variant="accent" onClick={run} disabled={start.isPending}>
            {start.isPending ? 'Starting…' : 'Start run'}
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
      {err && <div className="mono" style={{ fontSize: 12, color: 'var(--crit)', marginTop: 8 }}>{err}</div>}
      {start.isError && (
        <div className="mono" style={{ fontSize: 12, color: 'var(--crit)', marginTop: 8 }}>
          {String((start.error as Error)?.message ?? 'Start failed')}
        </div>
      )}
      {result && (
        <div style={{ marginTop: 12, background: 'var(--card-2)', border: '1px solid var(--line-soft)', borderRadius: 'var(--r-sm)', padding: '11px 13px', fontSize: 12.5 }}>
          <span style={{ color: 'var(--accent)' }}>✓ started</span>{' '}
          <span className="mono" style={{ color: 'var(--tx-2)' }}>run_id={result.run_id}</span>
        </div>
      )}
    </Modal>
  )
}

/* ============ run detail ============ */
function RunDetail({ ns, run, onBack, backLabel }: { ns: string; run: WorkflowRunInfo; onBack: () => void; backLabel: string }) {
  const q = useRunDetail(run.run_id)
  const histQ = useRunHistory(run.run_id)
  const cancel = useCancelRun(ns)
  const d = q.data
  const status = d?.status ?? run.status
  const events: WorkflowHistoryEvent[] = histQ.data ?? []
  const stepResults = d?.step_results ? Object.entries(d.step_results) : []
  const terminal = status === 'completed' || status === 'failed' || status === 'cancelled' || status === 'timed_out'

  return (
    <div className="wrap fade">
      <Crumb root={backLabel} onRoot={onBack} current={run.run_id} />
      <div className="ph">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16, flexWrap: 'wrap' }}>
          <div>
            <h1 className="ph-t" style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              {run.run_id} <Pill s={status} />
            </h1>
            <div className="ph-s mono">
              {run.workflow} v{run.version} · {d?.triggered_by ?? run.triggered_by}
            </div>
          </div>
          <Button variant="danger" style={{ flex: 'none' }} disabled={terminal || cancel.isPending} onClick={() => cancel.mutate(run.run_id)}>
            {cancel.isPending ? 'Cancelling…' : 'Cancel'}
          </Button>
        </div>
      </div>

      <Stats>
        <Stat label="Status" value={WF_ST[status]?.[1] ?? status} valueColor={WF_ST[status]?.[0]} />
        <Stat label="Current step" value={d?.current_step ?? (terminal ? '—' : '…')} sub={d?.pending_signals ? `${d.pending_signals} pending signals` : undefined} />
        <Stat label="Duration" value={fmtDur(d?.duration_ms ?? run.duration_ms)} sub={terminal ? 'final' : 'running'} />
        <Stat label="Events" value={d?.history_event_count ?? run.history_event_count} sub="in history" />
      </Stats>

      {(d?.error_message || run.error) && (
        <div className="wf-wait card-p" style={{ marginBottom: 18, background: 'color-mix(in srgb,var(--crit) 7%,var(--card))', borderColor: 'color-mix(in srgb,var(--crit) 30%,transparent)', alignItems: 'flex-start' }}>
          <span style={{ color: 'var(--crit)', fontWeight: 700, flex: 'none' }}>!</span>
          <div className="mono" style={{ fontSize: 12.5, color: 'var(--tx-2)' }}>{d?.error_message || run.error}</div>
        </div>
      )}

      <div className="sec" style={{ display: 'grid', gridTemplateColumns: '1.1fr 1fr', gap: 16, alignItems: 'start' }}>
        <div style={{ display: 'flex', flexDirection: 'column', gap: 16, minWidth: 0 }}>
          {d?.input != null && (
            <div className="card card-p">
              <div className="set-h" style={{ padding: 0, marginBottom: 6 }}>Input</div>
              <pre className="code" dangerouslySetInnerHTML={{ __html: jhl(d.input) }} />
            </div>
          )}
          {d?.output != null && (
            <div className="card card-p">
              <div className="set-h" style={{ padding: 0, marginBottom: 6 }}>Output</div>
              <pre className="code" dangerouslySetInnerHTML={{ __html: jhl(d.output) }} />
            </div>
          )}
          {stepResults.length > 0 && (
            <div className="card card-p">
              <PhSec icon={WICON.list} title="Step results" />
              <div className="list" style={{ border: 'none' }}>
                <div className="li head" style={{ gridTemplateColumns: '1.4fr 90px 80px' }}>
                  <div className="c">Step</div>
                  <div className="c">Outcome</div>
                  <div className="c r">Duration</div>
                </div>
                {stepResults.map(([name, r]) => (
                  <div className="li" key={name} style={{ gridTemplateColumns: '1.4fr 90px 80px', cursor: 'default' }}>
                    <span className="mono nw" style={{ color: 'var(--tx)' }}>{name}</span>
                    <span className="c"><OpPill color={r.outcome === 'success' ? 'var(--accent)' : 'var(--crit)'}>{r.outcome}</OpPill></span>
                    <span className="c r mono" style={{ fontSize: 12, color: 'var(--tx-3)' }}>{fmtDur(r.duration_ms ?? null)}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>

        <div className="card card-p wf-histcard" style={{ minWidth: 0 }}>
          <PhSec icon={WICON.history} title="History" />
          {histQ.isLoading ? (
            <div className="mono" style={{ fontSize: 12.5, color: 'var(--tx-faint)' }}>Loading…</div>
          ) : events.length === 0 ? (
            <div className="mono" style={{ fontSize: 12.5, color: 'var(--tx-faint)' }}>No events.</div>
          ) : (
            <div className="wf-hist">
              {events.map((e, i) => {
                const c = evColor(e.event_type)
                return (
                  <div className="wf-hev" key={i}>
                    <div className="wf-hrail">
                      <span className="wf-hdot" style={{ borderColor: c, color: c, background: `color-mix(in srgb,${c} 16%,var(--card))` }} />
                      {i < events.length - 1 && <span className="wf-hline" />}
                    </div>
                    <div className="wf-hbody">
                      <div className="wf-hhead">
                        <span className="wf-htitle">{evLabel(e.event_type)}</span>
                        <span className="mono wf-htime" title={tfmt(e.timestamp)}>{tfmt(e.timestamp).split(' ').slice(2).join(' ')}</span>
                      </div>
                      {e.step_name && <div className="wf-hdetail mono">{e.step_name}</div>}
                    </div>
                  </div>
                )
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

/* ============ definition detail ============ */
function DefDetail({ ns, def, runs, onBack, onOpenRun }: { ns: string; def: WorkflowDefInfo; runs: WorkflowRunInfo[]; onBack: () => void; onOpenRun: (r: WorkflowRunInfo) => void }) {
  const q = useWorkflowDef(ns, def.name)
  const toggle = useToggleWorkflow(ns)
  const [tab, setTab] = useState<'runs' | 'definition'>('runs')
  const [start, setStart] = useState(false)
  const detail = q.data
  const enabled = detail?.enabled ?? def.enabled
  const myRuns = useMemo(() => runs.filter((r) => r.workflow === def.name), [runs, def.name])

  return (
    <div className="wrap fade">
      <Crumb root="Workflows" onRoot={onBack} current={def.name} />
      <div className="ph">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16, flexWrap: 'wrap' }}>
          <div>
            <h1 className="ph-t" style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              {def.name}
              {!enabled && <OpPill color="var(--warn)">disabled</OpPill>}
            </h1>
            <div className="ph-s mono">
              v{def.version} · {def.step_count} step{def.step_count === 1 ? '' : 's'}
              {def.has_schedule ? ' · scheduled' : def.has_trigger ? ' · triggered' : ' · manual'}
            </div>
          </div>
          <div style={{ display: 'flex', gap: 9 }}>
            <Button style={{ flex: 'none' }} disabled={toggle.isPending} onClick={() => toggle.mutate({ name: def.name, enable: !enabled })}>
              {enabled ? 'Disable' : 'Enable'}
            </Button>
            <Button variant="accent" style={{ flex: 'none' }} disabled={!enabled} onClick={() => setStart(true)}>
              Start run
            </Button>
          </div>
        </div>
      </div>

      <Stats>
        <Stat label="Runs" value={detail?.run_count ?? myRuns.length} sub="all time" />
        <Stat label="Steps" value={def.step_count} sub={`${def.plan_count} plans`} />
        <Stat label="Version" value={'v' + def.version} sub={detail?.status ?? '—'} />
        <Stat label="Status" value={enabled ? 'enabled' : 'disabled'} valueColor={enabled ? undefined : 'var(--warn)'} sub="accepts runs" />
      </Stats>

      <div className="sec">
        <div className="wtabs">
          {(
            [
              ['runs', 'Runs · ' + myRuns.length],
              ['definition', 'Definition'],
            ] as ['runs' | 'definition', string][]
          ).map(([id, l]) => (
            <button key={id} className={'wtab' + (tab === id ? ' on' : '')} onClick={() => setTab(id)}>
              {l}
            </button>
          ))}
        </div>

        {tab === 'runs' ? (
          myRuns.length === 0 ? (
            <div className="card card-p" style={{ textAlign: 'center', color: 'var(--tx-3)', padding: '40px 0' }}>
              No runs yet. Start one to see it here.
            </div>
          ) : (
            <div className="list">
              <div className="li head" style={{ gridTemplateColumns: '1.5fr 110px 1fr 90px 90px' }}>
                <div className="c">Run</div>
                <div className="c">Status</div>
                <div className="c">Step</div>
                <div className="c r">Duration</div>
                <div className="c r">Started</div>
              </div>
              {myRuns.map((r) => (
                <div className="li" key={r.run_id} style={{ gridTemplateColumns: '1.5fr 110px 1fr 90px 90px' }} onClick={() => onOpenRun(r)}>
                  <span className="mono nw" style={{ fontSize: 12.5, color: 'var(--tx)' }}>{r.run_id}</span>
                  <span><Pill s={r.status} /></span>
                  <span className="mono nw" style={{ fontSize: 12, color: 'var(--tx-3)' }}>{r.current_step ?? r.terminal_name ?? '—'}</span>
                  <span className="c r mono" style={{ fontSize: 12, color: 'var(--tx-3)' }}>{fmtDur(r.duration_ms)}</span>
                  <span className="c r mono" style={{ fontSize: 11.5, color: 'var(--tx-3)' }}>{ago(r.started_at)}</span>
                </div>
              ))}
            </div>
          )
        ) : (
          <div className="card card-p">
            <PhSec icon={WICON.info} title="Definition" />
            {detail?.definition_yaml ? (
              <pre className="code" dangerouslySetInnerHTML={{ __html: yamlHL(detail.definition_yaml) }} />
            ) : (
              <div className="mono" style={{ fontSize: 12.5, color: 'var(--tx-faint)' }}>{q.isLoading ? 'Loading…' : 'No definition.'}</div>
            )}
          </div>
        )}
      </div>

      {start && <StartModal ns={ns} workflow={def.name} onClose={() => setStart(false)} />}
    </div>
  )
}

function yamlHL(y: string): string {
  return y
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/^(\s*)([\w.\-]+):/gm, '$1<span class="jk">$2</span>:')
    .replace(/: (.+)$/gm, (_m, v: string) => (/^[0-9"[]/.test(v) ? `: <span class="jn">${v}</span>` : `: <span class="js">${v}</span>`))
}

/* ============ list ============ */
type ListTab = 'all' | 'enabled' | 'disabled'

function WorkflowsList({ ns, onOpen }: { ns: string; onOpen: (d: WorkflowDefInfo) => void }) {
  const q = useWorkflows(ns)
  const runsQ = useRuns(ns)
  const defs = useMemo<WorkflowDefInfo[]>(() => q.data ?? [], [q.data])
  const runs = useMemo<WorkflowRunInfo[]>(() => runsQ.data ?? [], [runsQ.data])
  const [tab, setTab] = useState<ListTab>('all')

  const runsByWf = useMemo(() => {
    const m = new Map<string, { total: number; running: number }>()
    for (const r of runs) {
      const e = m.get(r.workflow) ?? { total: 0, running: 0 }
      e.total++
      if (r.status === 'running' || r.status === 'waiting' || r.status === 'pending') e.running++
      m.set(r.workflow, e)
    }
    return m
  }, [runs])

  const enabledCount = defs.filter((d) => d.enabled).length
  const activeRuns = runs.filter((r) => r.status === 'running' || r.status === 'waiting' || r.status === 'pending').length
  const filtered = tab === 'all' ? defs : tab === 'enabled' ? defs.filter((d) => d.enabled) : defs.filter((d) => !d.enabled)
  const grid = '1.6fr 90px 90px 110px 110px'

  return (
    <div className="wrap fade">
      <div className="ph">
        <h1 className="ph-t">Workflows</h1>
        <div className="ph-s">Durable orchestration · steps &amp; plans · signals · event history</div>
      </div>

      <Stats>
        <Stat label="Workflows" value={q.isLoading ? '…' : defs.length} sub={`${enabledCount} enabled · in ${ns}`} />
        <Stat label="Runs" value={runsQ.isLoading ? '…' : runs.length} sub="all time" />
        <Stat label="Active" value={activeRuns} valueColor={activeRuns ? '#6F9BD1' : undefined} sub="running / waiting" />
        <Stat label="Disabled" value={defs.length - enabledCount} sub="blocked" />
      </Stats>

      <div className="sec">
        <div className="wtabs">
          {(
            [
              ['all', 'All · ' + defs.length],
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
            Failed to load workflows. Is the server running?
          </div>
        ) : filtered.length === 0 ? (
          <div className="card card-p" style={{ textAlign: 'center', color: 'var(--tx-3)', padding: '40px 0' }}>
            {q.isLoading ? 'Loading…' : `No ${tab === 'all' ? '' : tab + ' '}workflows in ${ns}. Create one with \`flo workflow create\`.`}
          </div>
        ) : (
          <div className="list">
            <div className="li head" style={{ gridTemplateColumns: grid }}>
              <div className="c">Workflow</div>
              <div className="c r">Version</div>
              <div className="c r">Steps</div>
              <div className="c r">Runs</div>
              <div className="c r">Active</div>
            </div>
            {filtered.map((d) => {
              const rc = runsByWf.get(d.name) ?? { total: 0, running: 0 }
              return (
                <div className="li" key={d.name} style={{ gridTemplateColumns: grid }} onClick={() => onOpen(d)}>
                  <div style={{ minWidth: 0 }}>
                    <div className="mono nw" style={{ fontWeight: 500, color: d.enabled ? 'var(--tx)' : 'var(--tx-3)' }}>
                      {d.name}
                      {!d.enabled && <span style={{ marginLeft: 8 }}><OpPill color="var(--warn)">off</OpPill></span>}
                    </div>
                    <div className="desc">{d.has_schedule ? 'scheduled' : d.has_trigger ? 'triggered' : 'manual'}</div>
                  </div>
                  <span className="c r mono" style={{ color: 'var(--tx-3)' }}>v{d.version}</span>
                  <span className="c r mono">{d.step_count}</span>
                  <span className="c r mono">{rc.total}</span>
                  <span className="c r mono" style={{ color: rc.running ? '#6F9BD1' : 'var(--tx-3)' }}>{rc.running}</span>
                </div>
              )
            })}
          </div>
        )}
      </div>
    </div>
  )
}

/** Workflows screen — definitions ↔ definition detail ↔ run detail. */
export function Workflows() {
  const { ns } = useNamespace()
  const runsQ = useRuns(ns)
  const runs = useMemo<WorkflowRunInfo[]>(() => runsQ.data ?? [], [runsQ.data])
  const [def, setDef] = useState<WorkflowDefInfo | null>(null)
  const [run, setRun] = useState<WorkflowRunInfo | null>(null)

  if (run) {
    return (
      <RunDetail
        ns={ns}
        run={run}
        backLabel={def ? def.name : 'Workflows'}
        onBack={() => setRun(null)}
      />
    )
  }
  if (def) {
    return <DefDetail ns={ns} def={def} runs={runs} onBack={() => setDef(null)} onOpenRun={setRun} />
  }
  return <WorkflowsList ns={ns} onOpen={setDef} />
}
