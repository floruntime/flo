/* Flo Console v2 — Overview (live).
   Cluster health + real workload/traffic + workflow activity, from `cluster/stats`
   and `/metrics`. The event-flow strip is a client-derived command-rate series
   (sampled from `commands_total` deltas across polls). Real per-node CPU/MEM/IO and
   per-namespace storage aren't instrumented server-side, so the node list shows the
   honest shard registry (id/status/role) rather than fabricated meters. */
import { useEffect, useRef, useState } from 'react'
import { cfmt } from '@/lib/format'
import { useClusterStats, useMetrics } from '@/lib/api/hooks'
import { Card, PhSec } from '@/components/layout'
import { Pill, Dot } from '@/components/feedback'
import './Overview.css'

const ICON = {
  gauge: 'M3.5 18a8.5 8.5 0 0 1 17 0|M12 18l4.5-6',
  flow: 'M2.5 12h4l2.2-6.5 4 13 2.3-6.5h4.5',
  layers: 'M12 3l8 4.5-8 4.5-8-4.5z|M4 12l8 4.5 8-4.5',
  branch: 'M7 6.5v11|M7 8a2 2 0 1 0 0-4 2 2 0 0 0 0 4z|M7 21a2 2 0 1 0 0-4 2 2 0 0 0 0 4z|M17 9.5a2 2 0 1 0 0-4 2 2 0 0 0 0 4z|M17 7.5v1A4 4 0 0 1 13 12.5H7',
  nodes: 'M3.5 6.5h17v4h-17z|M3.5 13.5h17v4h-17z|M7 8.5h.01|M7 15.5h.01',
}

const bfmt = (b: number): string =>
  b >= 1 << 30 ? (b / (1 << 30)).toFixed(1) + ' GB' : b >= 1 << 20 ? (b / (1 << 20)).toFixed(1) + ' MB' : b >= 1 << 10 ? (b / (1 << 10)).toFixed(1) + ' KB' : b + ' B'

/** Sample `commands_total` once per poll → a real command-rate (cmd/s) series.
    Keyed on the query's `dataUpdatedAt` so it samples every refetch (a flat line
    when idle, real spikes under load), not only when the counter changes. */
function useCommandRate(commandsTotal: number | undefined, tick: number): number[] {
  const [series, setSeries] = useState<number[]>([])
  const last = useRef<{ t: number; c: number } | null>(null)
  useEffect(() => {
    if (commandsTotal == null || !tick) return
    const now = tick
    const prev = last.current
    last.current = { t: now, c: commandsTotal }
    if (prev && now > prev.t) {
      const dt = (now - prev.t) / 1000
      const rate = Math.max(0, (commandsTotal - prev.c) / dt)
      setSeries((s) => [...s, rate].slice(-48))
    }
  }, [tick, commandsTotal])
  return series
}

export function Overview() {
  const statsQ = useClusterStats()
  const stats = statsQ.data
  const { data: metrics } = useMetrics()
  const nodes = stats?.nodes ?? []
  const healthy = nodes.filter((n) => n.status === 'healthy').length
  const rateSeries = useCommandRate(stats?.commands_total, statsQ.dataUpdatedAt)
  const curRate = rateSeries.length ? rateSeries[rateSeries.length - 1] : 0
  const maxRate = Math.max(1, ...rateSeries)
  const srv = metrics?.server

  return (
    <div className="wrap fade">
      <div className="ph">
        <h1 className="ph-t">Overview</h1>
        <div className="ph-s">cluster health{stats ? ` · ${stats.version} · up ${stats.uptime}` : ''}</div>
      </div>

      {/* cluster summary */}
      <Card style={{ padding: '18px 22px 22px' }}>
        <PhSec icon={ICON.gauge} title="Cluster summary" info />
        <div className="ov-summary">
          <div className="ov-hero">
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8 }}>
              <span className="ov-lbl">Throughput</span>
              <Pill tone="ok" style={{ padding: '2px 9px' }}>Healthy</Pill>
            </div>
            <div className="ov-big" style={{ color: 'var(--accent)' }}>
              {stats ? cfmt(stats.rps) : '—'}<span className="ov-u">req/s avg</span>
            </div>
          </div>
          <div className="ov-metric">
            <div className="ov-lbl">Commands</div>
            <div className="ov-num">{stats ? cfmt(stats.commands_total) : '—'}</div>
          </div>
          <div className="ov-metric">
            <div className="ov-lbl">Shards</div>
            <div className="ov-num">{stats ? stats.num_shards : '—'}</div>
          </div>
          <div className="ov-metric">
            <div className="ov-lbl">Connections</div>
            <div className="ov-num">{stats ? stats.active_connections : '—'}</div>
          </div>
        </div>
      </Card>

      <div className="sec" style={{ display: 'grid', gridTemplateColumns: '1.5fr 1fr', gap: 16, alignItems: 'start' }}>
        {/* event flow — real sampled command rate */}
        <Card style={{ minWidth: 0 }}>
          <PhSec icon={ICON.flow} title="Command rate" info />
          <div className="strip" style={{ height: 120, border: 'none', background: 'transparent', padding: '6px 0 0' }}>
            {rateSeries.length === 0 ? (
              <div className="mono" style={{ alignSelf: 'center', margin: 'auto', color: 'var(--tx-faint)', fontSize: 12 }}>
                sampling…
              </div>
            ) : (
              rateSeries.map((v, i) => (
                <i
                  key={i}
                  style={{
                    height: Math.max(3, (v / maxRate) * 100) + '%',
                    background: i === rateSeries.length - 1 ? 'var(--accent)' : 'var(--tx-faint)',
                    opacity: i === rateSeries.length - 1 ? 0.9 : 0.5,
                  }}
                />
              ))
            )}
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: 8, fontSize: 11.5, color: 'var(--tx-faint)', fontFamily: 'var(--mono)' }}>
            <span>{rateSeries.length ? `${rateSeries.length * 5}s window` : 'live'}</span>
            <span>{cfmt(Math.round(curRate))} cmd/s · now</span>
          </div>
        </Card>

        {/* workload — real entity counts (server byte/connection counters aren't
            instrumented yet, so they're omitted rather than shown as 0). */}
        <Card style={{ minWidth: 0 }}>
          <PhSec icon={ICON.layers} title="Workload" info />
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, margin: '4px 0 14px' }}>
            <span className="ov-num" style={{ fontSize: 22 }}>
              {srv ? bfmt(srv.bytes_received + srv.bytes_sent) : '—'}
            </span>
            <span className="ov-u">traffic · in+out</span>
          </div>
          {(
            [
              ['streams', metrics ? cfmt(metrics.streams) : '—', 'var(--cat-2)'],
              ['queues', metrics ? cfmt(metrics.queues) : '—', 'var(--cat-3)'],
              ['kv namespaces', metrics ? cfmt(metrics.kv_namespaces) : '—', 'var(--accent)'],
              ['bytes received', srv ? bfmt(srv.bytes_received) : '—', 'var(--cat-4)'],
              ['bytes sent', srv ? bfmt(srv.bytes_sent) : '—', 'var(--cat-5)'],
            ] as [string, string, string][]
          ).map(([n, s, c]) => (
            <div key={n} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '6px 0' }}>
              <span style={{ display: 'flex', alignItems: 'center', gap: 9, fontFamily: 'var(--mono)', fontSize: 12.5, color: 'var(--tx-2)' }}>
                <Dot color={c} />
                {n}
              </span>
              <span className="mono" style={{ fontSize: 12, color: 'var(--tx-3)', whiteSpace: 'nowrap' }}>{s}</span>
            </div>
          ))}
        </Card>
      </div>

      {/* nodes (shards) — honest registry, no fabricated meters */}
      <Card className="sec">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <PhSec icon={ICON.nodes} title="Shards" />
          <Pill tone={healthy === nodes.length ? 'ok' : 'lag'} style={{ padding: '2px 9px' }}>
            {healthy} / {nodes.length || '—'} healthy
          </Pill>
        </div>
        {nodes.length === 0 && <div style={{ padding: '20px 0', color: 'var(--tx-3)', fontSize: 13 }}>No shards reporting.</div>}
        {nodes.map((n) => (
          <div key={n.id} className="ov-node">
            <div style={{ display: 'flex', alignItems: 'center', gap: 10, minWidth: 160 }}>
              <Dot color={n.status === 'healthy' ? 'var(--accent)' : 'var(--crit)'} />
              <span className="mono" style={{ fontSize: 13, color: 'var(--tx)' }}>{n.id}</span>
              <span className="gmode">{n.role}</span>
            </div>
            <span className="mono" style={{ fontSize: 11.5, color: 'var(--tx-faint)', flex: 1, textAlign: 'right' }}>
              {n.status}
            </span>
          </div>
        ))}
      </Card>
    </div>
  )
}
