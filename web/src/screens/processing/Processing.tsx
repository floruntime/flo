/* Flo Console v2 — Processing screen (live).
   Stream processing · pipeline DAG · savepoints.

   Wired to the dashboard API. Jobs are namespace-scoped server-side. The API
   exposes the registry record (status, parallelism, batch_size, records_processed)
   and the full submitted pipeline YAML (parsed here into the source→operator→sink
   DAG) + savepoints. It does NOT expose live rates, e2e latency, backpressure,
   per-operator in/out counts, checkpoints or window metrics — those mock cards were
   dropped, not faked. Submit / stop / cancel are real loopback writes. See API_INTEGRATION.md. */
import { Fragment, useMemo, useState } from 'react'
import { Button } from '@/components/buttons/Button'
import { Crumb, PhSec, Stat, Stats } from '@/components/layout'
import { StatusPill, OpPill } from '@/components'
import { Modal } from '@/components/overlay/Modal'
import { useNamespace } from '@/lib/namespace'
import { useJobs, useJobDetail, useSubmitJob, useStopJob, useCancelJob } from '@/lib/api/processing'
import type { ProcessingJobInfo, ProcessingJobDetail, JobStatus } from '@/lib/api/types'
import './processing.css'

/* ---------- colour maps ---------- */
const PROC_OPC: Record<string, string> = {
  filter: '#C9A26B',
  keyby: 'var(--accent)',
  map: '#6F9BD1',
  flatmap: '#9e8cfc',
  aggregate: '#9e8cfc',
  kv_lookup: '#5FB0A6',
  classify: '#d6409f',
  passthrough: 'var(--tx-faint)',
}
const PROC_EPC: Record<string, string> = {
  stream: '#6F9BD1',
  ts: '#9e8cfc',
  kv: 'var(--accent)',
  queue: '#C9A26B',
}
const P_ST: Record<string, [string, string]> = {
  RUNNING: ['#6F9BD1', 'running'],
  STOPPED: ['var(--warn)', 'stopped'],
  FAILED: ['var(--crit)', 'failed'],
  CANCELLED: ['var(--tx-faint)', 'cancelled'],
  COMPLETED: ['var(--accent)', 'completed'],
  unknown: ['var(--tx-faint)', 'unknown'],
}
const PSEC = { layers: 'M12 3l8 4.5-8 4.5-8-4.5z|M4 12l8 4.5 8-4.5', info: 'M12 3.5a8.5 8.5 0 1 0 0 17 8.5 8.5 0 0 0 0-17z|M12 11v5M12 8h.01' }

/* ---------- formatters ---------- */
const pfmtN = (n: number): string =>
  n >= 1e6 ? (n / 1e6).toFixed(1) + 'M' : n >= 1e3 ? (n / 1e3).toFixed(1) + 'K' : Math.round(n).toLocaleString()
function pUp(createdMs: number): string {
  if (!createdMs) return '—'
  const ms = Math.max(0, Date.now() - createdMs)
  const h = Math.floor(ms / 3600000)
  if (h < 1) return Math.floor(ms / 60000) + 'm'
  if (h < 24) return h + 'h ' + Math.floor((ms % 3600000) / 60000) + 'm'
  return Math.floor(h / 24) + 'd ' + (h % 24) + 'h'
}

/* ---------- pipeline YAML → DAG (minimal, structure-specific parser) ---------- */
type Endpoint = { name: string; kind: string; target: string }
type Op = { type: string; name: string }
type Pipeline = { sources: Endpoint[]; operators: Op[]; sinks: Endpoint[] }

const KIND_KEYS = new Set(['stream', 'ts', 'kv', 'queue'])
const TARGET_KEYS = new Set(['name', 'measurement', 'key_prefix', 'key'])

/** Parse the (regular, machine-generated) pipeline YAML into source/operator/sink
    lists. Tolerant: unknown keys are ignored, malformed input yields empty lists. */
function parsePipeline(yaml: string): Pipeline {
  const sources: Endpoint[] = []
  const operators: Op[] = []
  const sinks: Endpoint[] = []
  let section: 'sources' | 'operators' | 'sinks' | null = null
  let list: Endpoint[] | Op[] | null = null
  let cur: (Endpoint & Op) | null = null
  let kindIndent = -1

  const kv = (s: string): [string, string] | null => {
    const i = s.indexOf(':')
    if (i < 0) return null
    return [s.slice(0, i).trim(), s.slice(i + 1).trim()]
  }

  for (const raw0 of yaml.split('\n')) {
    const raw = raw0.replace(/\t/g, '  ')
    const trimmed = raw.trim()
    if (!trimmed || trimmed.startsWith('#')) continue
    const indent = raw.length - raw.trimStart().length

    if (indent === 0) {
      const k = trimmed.endsWith(':') ? trimmed.slice(0, -1) : (kv(trimmed)?.[0] ?? '')
      section = k === 'sources' ? 'sources' : k === 'operators' ? 'operators' : k === 'sinks' ? 'sinks' : null
      list = section === 'sources' ? sources : section === 'operators' ? operators : section === 'sinks' ? sinks : null
      cur = null
      kindIndent = -1
      continue
    }
    if (!section || !list) continue

    if (trimmed.startsWith('- ')) {
      cur = { name: '', kind: '', target: '', type: '' }
      ;(list as (Endpoint & Op)[]).push(cur)
      kindIndent = -1
      const pair = kv(trimmed.slice(2))
      if (pair) assign(cur, pair, indent)
      continue
    }
    if (!cur) continue
    const pair = kv(trimmed)
    if (pair) assign(cur, pair, indent)
  }

  function assign(node: Endpoint & Op, [k, v]: [string, string], indent: number) {
    if (section === 'operators') {
      if (k === 'type') node.type = v
      else if (k === 'name') node.name = v
      return
    }
    // sources / sinks endpoints
    if (KIND_KEYS.has(k) && v === '') {
      node.kind = k
      kindIndent = indent
      return
    }
    if (k === 'name' && kindIndent < 0) {
      node.name = v // the endpoint's own name (on the `- name:` line)
      return
    }
    if (TARGET_KEYS.has(k) && kindIndent >= 0 && indent > kindIndent && !node.target) {
      node.target = v
      return
    }
  }

  return { sources, operators, sinks }
}

/* ---------- YAML highlight ---------- */
function pYamlHL(y: string): string {
  return y
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/^(\s*)([\w.\-]+):/gm, '$1<span class="jk">$2</span>:')
    .replace(/: (.+)$/gm, (_m, v: string) => (/^[0-9[]/.test(v) ? `: <span class="jn">${v}</span>` : `: <span class="js">${v}</span>`))
}

/* ---------- topology bits ---------- */
function EpTag({ kind }: { kind: string }) {
  return <OpPill color={PROC_EPC[kind] || 'var(--tx-faint)'}>{kind || 'endpoint'}</OpPill>
}
function OpTag({ type }: { type: string }) {
  return <OpPill color={PROC_OPC[type] || 'var(--tx-faint)'}>{type || 'op'}</OpPill>
}
function Node({ label, sub, badge }: { label: string; sub?: string; badge: React.ReactNode }) {
  return (
    <div className="ptopo-node">
      <div className="ptopo-top">{badge}</div>
      <div className="ptopo-name mono">{label}</div>
      {sub && <div className="ptopo-sub mono">{sub}</div>}
    </div>
  )
}
function Wire({ n, flow, running }: { n?: number | null; flow?: boolean; running: boolean }) {
  return (
    <div className="ptopo-wire">
      <span className="ptopo-count mono">{n != null ? pfmtN(n) : ''}</span>
      <span className={'ptopo-line' + (flow && running ? ' flow' : '')} />
    </div>
  )
}
function Topology({ pipe, records, running }: { pipe: Pipeline; records: number; running: boolean }) {
  return (
    <div className="ptopo-scroll">
      <div className="ptopo">
        <div className="ptopo-col">
          {pipe.sources.length ? (
            pipe.sources.map((s, i) => (
              <Node key={s.name + i} label={s.name || 'source'} sub={`${s.kind}:${s.target}`} badge={<EpTag kind={s.kind} />} />
            ))
          ) : (
            <Node label="source" sub="—" badge={<EpTag kind="" />} />
          )}
        </div>
        <Wire n={records} flow running={running} />
        {pipe.operators.map((op, i) => (
          <Fragment key={op.name + i}>
            <Node label={op.name || op.type} sub={op.type} badge={<OpTag type={op.type} />} />
            <Wire flow running={running} />
          </Fragment>
        ))}
        <div className="ptopo-col">
          {pipe.sinks.length ? (
            pipe.sinks.map((s, i) => (
              <Node key={s.name + i} label={s.name || 'sink'} sub={`${s.kind}:${s.target}`} badge={<EpTag kind={s.kind} />} />
            ))
          ) : (
            <Node label="sink" sub="—" badge={<EpTag kind="" />} />
          )}
        </div>
      </div>
    </div>
  )
}

/* ---------- submit modal (real loopback) ---------- */
const SUBMIT_YAML = `kind: Processing
name: my-pipeline
namespace: production
sources:
  - name: input
    stream:
      name: events
      partitions: all
operators:
  - type: filter
    name: keep
    condition: "value_contains:e"
sinks:
  - name: output
    stream:
      name: my-output`

function SubmitModal({ ns, onClose }: { ns: string; onClose: () => void }) {
  const [yaml, setYaml] = useState(SUBMIT_YAML)
  const submit = useSubmitJob(ns)
  const result = submit.data
  return (
    <Modal
      title="Submit pipeline"
      sub="Submitted via loopback — the engine starts consuming immediately"
      width="min(680px,96vw)"
      onClose={onClose}
      foot={
        <>
          <Button variant="quiet" onClick={onClose}>
            {result ? 'Close' : 'Cancel'}
          </Button>
          <Button variant="accent" onClick={() => submit.mutate(yaml)} disabled={submit.isPending}>
            {submit.isPending ? 'Submitting…' : 'Submit'}
          </Button>
        </>
      }
    >
      <div className="set-h" style={{ padding: 0, marginBottom: 7 }}>
        Pipeline definition · YAML
      </div>
      <textarea
        className="kv-editor mono"
        style={{ width: '100%', minHeight: 220, resize: 'vertical' }}
        value={yaml}
        onChange={(e) => setYaml(e.target.value)}
      />
      {submit.isError && (
        <div className="mono" style={{ fontSize: 12, color: 'var(--crit)', marginTop: 8 }}>
          {String((submit.error as Error)?.message ?? 'Submit failed')}
        </div>
      )}
      {result && (
        <div
          style={{
            marginTop: 12,
            background: 'var(--card-2)',
            border: '1px solid var(--line-soft)',
            borderRadius: 'var(--r-sm)',
            padding: '11px 13px',
            fontSize: 12.5,
          }}
        >
          <span style={{ color: 'var(--accent)' }}>✓ {result.status ?? 'submitted'}</span>{' '}
          <span className="mono" style={{ color: 'var(--tx-2)' }}>
            job_id={result.job_id}
          </span>
        </div>
      )}
    </Modal>
  )
}

/* ---------- detail ---------- */
function JobDetail({ ns, job, onBack }: { ns: string; job: ProcessingJobInfo; onBack: () => void }) {
  const q = useJobDetail(job.job_id)
  const stop = useStopJob(ns)
  const cancel = useCancelJob(ns)
  const [tab, setTab] = useState<'overview' | 'definition' | 'savepoints'>('overview')

  const d: ProcessingJobDetail | undefined = q.data
  const status: JobStatus = (d?.status ?? job.status) as JobStatus
  const records = d?.records_processed ?? job.records_processed
  const running = status === 'RUNNING'
  const pipe = useMemo(() => parsePipeline(d?.yaml ?? ''), [d?.yaml])
  const savepoints = d?.savepoints ?? []
  const [c, l] = P_ST[status] ?? P_ST.unknown

  return (
    <div className="wrap fade">
      <Crumb root="Processing" onRoot={onBack} current={job.name} />
      <div className="ph">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16, flexWrap: 'wrap' }}>
          <div>
            <h1 className="ph-t" style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
              {job.name} <StatusPill color={c} label={l} />
            </h1>
            <div className="ph-s mono">
              {job.namespace} · {job.job_id}
            </div>
          </div>
          <div style={{ display: 'flex', gap: 9 }}>
            <Button style={{ flex: 'none' }} disabled={!running || stop.isPending} onClick={() => stop.mutate(job.job_id)}>
              {stop.isPending ? 'Stopping…' : 'Stop'}
            </Button>
            <Button
              variant="danger"
              style={{ flex: 'none' }}
              disabled={status === 'CANCELLED' || cancel.isPending}
              onClick={() => cancel.mutate(job.job_id)}
            >
              {cancel.isPending ? 'Cancelling…' : 'Cancel'}
            </Button>
          </div>
        </div>
      </div>

      <Stats>
        <Stat label="Records processed" value={pfmtN(records)} sub={running ? 'climbing' : 'final'} />
        <Stat label="Parallelism" value={'×' + (d?.parallelism ?? job.parallelism)} sub="subtasks" />
        <Stat label="Batch size" value={d?.batch_size ?? job.batch_size} sub="per tick" />
        <Stat label="Uptime" value={pUp(d?.created_at ?? job.created_at)} sub="since submit" />
      </Stats>

      <div className="sec">
        <div className="wtabs">
          {(
            [
              ['overview', 'Overview'],
              ['definition', 'Definition'],
              ['savepoints', 'Savepoints · ' + savepoints.length],
            ] as ['overview' | 'definition' | 'savepoints', string][]
          ).map(([id, lab]) => (
            <button key={id} className={'wtab' + (tab === id ? ' on' : '')} onClick={() => setTab(id)}>
              {lab}
            </button>
          ))}
        </div>

        {q.isError ? (
          <div className="card card-p" style={{ textAlign: 'center', color: 'var(--crit)', padding: '40px 0' }}>
            Failed to load job.
          </div>
        ) : tab === 'overview' ? (
          <div className="card card-p">
            <PhSec icon={PSEC.layers} title="Pipeline topology" />
            <Topology pipe={pipe} records={records} running={running} />
            <div className="mono" style={{ fontSize: 11, color: 'var(--tx-faint)', marginTop: 10 }}>
              Edge counts aren’t exposed per-operator — only the source record total is live.
            </div>
          </div>
        ) : tab === 'definition' ? (
          <div className="card card-p">
            <PhSec icon={PSEC.info} title="Submitted definition" />
            {d?.yaml ? (
              <pre className="code" dangerouslySetInnerHTML={{ __html: pYamlHL(d.yaml) }} />
            ) : (
              <div className="mono" style={{ fontSize: 12.5, color: 'var(--tx-faint)' }}>
                {q.isLoading ? 'Loading…' : 'No definition available.'}
              </div>
            )}
          </div>
        ) : savepoints.length === 0 ? (
          <div className="card card-p" style={{ textAlign: 'center', color: 'var(--tx-3)', padding: '40px 0' }}>
            No savepoints. Create one with `flo processing savepoint {job.job_id}`.
          </div>
        ) : (
          <div className="list">
            <div className="li head" style={{ gridTemplateColumns: '1.6fr 120px 1fr' }}>
              <div className="c">Savepoint</div>
              <div className="c r">Records</div>
              <div className="c r">Created</div>
            </div>
            {savepoints.map((sp) => (
              <div className="li" key={sp.savepoint_id} style={{ gridTemplateColumns: '1.6fr 120px 1fr', cursor: 'default' }}>
                <span className="mono nw" style={{ color: 'var(--tx)' }}>
                  {sp.savepoint_id}
                </span>
                <span className="c r mono">{pfmtN(sp.records_at_savepoint)}</span>
                <span className="c r mono" style={{ fontSize: 11.5, color: 'var(--tx-3)' }}>
                  {pUp(sp.created_at)} ago
                </span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}

/* ---------- list ---------- */
type ListTab = 'all' | 'running' | 'stopped'

function JobsList({ ns, onOpen }: { ns: string; onOpen: (job: ProcessingJobInfo) => void }) {
  const q = useJobs(ns)
  const jobs = useMemo<ProcessingJobInfo[]>(() => q.data ?? [], [q.data])
  const [tab, setTab] = useState<ListTab>('all')
  const [submit, setSubmit] = useState(false)

  const running = jobs.filter((j) => j.status === 'RUNNING').length
  const records = jobs.reduce((a, j) => a + j.records_processed, 0)
  const notRunning = jobs.length - running
  const filtered =
    tab === 'all' ? jobs : tab === 'running' ? jobs.filter((j) => j.status === 'RUNNING') : jobs.filter((j) => j.status !== 'RUNNING')

  const grid = '1.6fr 110px 90px 110px 90px'

  return (
    <div className="wrap fade">
      <div className="ph">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 16 }}>
          <div>
            <h1 className="ph-t">Processing</h1>
            <div className="ph-s">Stream processing · pipeline DAG · checkpoints &amp; savepoints</div>
          </div>
          <Button variant="accent" style={{ flex: 'none' }} onClick={() => setSubmit(true)}>
            Submit pipeline
          </Button>
        </div>
      </div>

      <Stats>
        <Stat label="Jobs" value={q.isLoading ? '…' : jobs.length} sub={`${running} running · in ${ns}`} />
        <Stat label="Running" value={running} sub="active pipelines" />
        <Stat label="Records processed" value={pfmtN(records)} sub="across jobs" />
        <Stat label="Not running" value={notRunning} valueColor={notRunning ? 'var(--warn)' : undefined} sub="stopped / cancelled" />
      </Stats>

      <div className="sec">
        <div className="wtabs">
          {(
            [
              ['all', 'All · ' + jobs.length],
              ['running', 'Running'],
              ['stopped', 'Not running'],
            ] as [ListTab, string][]
          ).map(([id, l]) => (
            <button key={id} className={'wtab' + (tab === id ? ' on' : '')} onClick={() => setTab(id)}>
              {l}
            </button>
          ))}
        </div>

        {q.isError ? (
          <div className="card card-p" style={{ textAlign: 'center', color: 'var(--crit)', padding: '40px 0' }}>
            Failed to load jobs. Is the server running?
          </div>
        ) : filtered.length === 0 ? (
          <div className="card card-p" style={{ textAlign: 'center', color: 'var(--tx-3)', padding: '40px 0' }}>
            {q.isLoading ? 'Loading…' : `No ${tab === 'all' ? '' : tab + ' '}jobs in ${ns}. Submit a pipeline to start.`}
          </div>
        ) : (
          <div className="list">
            <div className="li head" style={{ gridTemplateColumns: grid }}>
              <div className="c">Job</div>
              <div className="c">Status</div>
              <div className="c r">Parallelism</div>
              <div className="c r">Records</div>
              <div className="c r">Uptime</div>
            </div>
            {filtered.map((j) => {
              const [c, l] = P_ST[j.status] ?? P_ST.unknown
              return (
                <div className="li" key={j.job_id} style={{ gridTemplateColumns: grid }} onClick={() => onOpen(j)}>
                  <div style={{ minWidth: 0 }}>
                    <div className="mono nw" style={{ fontWeight: 500, color: 'var(--tx)' }}>
                      {j.name}
                    </div>
                    <div className="desc">{j.job_id}</div>
                  </div>
                  <span>
                    <StatusPill color={c} label={l} />
                  </span>
                  <span className="c r mono">×{j.parallelism}</span>
                  <span className="c r mono">{pfmtN(j.records_processed)}</span>
                  <span className="c r mono" style={{ fontSize: 11.5, color: 'var(--tx-3)' }}>
                    {pUp(j.created_at)}
                  </span>
                </div>
              )
            })}
          </div>
        )}
      </div>

      {submit && <SubmitModal ns={ns} onClose={() => setSubmit(false)} />}
    </div>
  )
}

/** Processing screen — master list ↔ detail (internal state, matching the design). */
export function Processing() {
  const { ns } = useNamespace()
  const [open, setOpen] = useState<ProcessingJobInfo | null>(null)
  return open ? (
    <JobDetail ns={ns} job={open} onBack={() => setOpen(null)} />
  ) : (
    <JobsList ns={ns} onOpen={setOpen} />
  )
}
