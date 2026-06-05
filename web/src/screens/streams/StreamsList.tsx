import { cfmt } from '@/lib/format'
import { useNamespace } from '@/lib/namespace'
import { useStreams } from '@/lib/api/streams'
import { Stats, Stat } from '@/components/layout'

const GRID = '2fr 110px 120px 100px'

export function StreamsList({ onOpen }: { onOpen: (name: string) => void }) {
  const { ns } = useNamespace()
  const q = useStreams(ns)
  const rows = q.data ?? []
  const totalParts = rows.reduce((a, r) => a + r.partitions, 0)
  const totalIngest = rows.reduce((a, r) => a + r.ingest_rate, 0)

  return (
    <div className="wrap fade">
      <div className="ph">
        <h1 className="ph-t">Streams</h1>
        <div className="ph-s">Append-only commit logs · zero-copy reads from the unified log</div>
      </div>

      <Stats>
        <Stat label="Streams" value={q.isLoading ? '…' : rows.length} sub={`in ${ns}`} />
        <Stat label="Partitions" value={totalParts} sub="across streams" />
        <Stat label="Ingest" value={cfmt(totalIngest)} unit="rec/s" sub="aggregate" />
        <Stat label="Namespace" value={<span className="mono" style={{ fontSize: 18 }}>{ns}</span>} />
      </Stats>

      {q.isError ? (
        <div className="card card-p" style={{ textAlign: 'center', color: 'var(--crit)', padding: '44px 20px' }}>
          Failed to load streams. Is the server running?
        </div>
      ) : rows.length === 0 ? (
        <div className="card card-p" style={{ textAlign: 'center', color: 'var(--tx-3)', padding: '44px 20px' }}>
          {q.isLoading ? 'Loading…' : <>No streams in <span className="mono">{ns}</span>.</>}
        </div>
      ) : (
        <div className="list">
          <div className="li head" style={{ gridTemplateColumns: GRID }}>
            <div className="c">Stream</div>
            <div className="c r">Partitions</div>
            <div className="c r">Ingest /s</div>
            <div className="c r">Retention</div>
          </div>
          {rows.map((r) => (
            <div className="li" key={r.name} style={{ gridTemplateColumns: GRID }} onClick={() => onOpen(r.name)}>
              <div>
                <div className="name">{r.name}</div>
                <div className="desc">{r.namespace}</div>
              </div>
              <div className="c r muted-num">{r.partitions}</div>
              <div className="c r muted-num">{cfmt(r.ingest_rate)}</div>
              <div className="c r dim mono">{r.retention}</div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
