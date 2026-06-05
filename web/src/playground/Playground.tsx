import { useState } from 'react'
import type { ReactNode } from 'react'
import { Link } from 'react-router-dom'
import { cx } from '@/lib/cx'
import {
  Button,
  Field,
  TextArea,
  SegGroup,
  Checkbox,
  Toggle,
  Pill,
  Tag,
  StatusPill,
  OpPill,
  Bar,
  Dot,
  Card,
  Stats,
  Stat,
  PageHead,
  Section,
  PhSec,
  Crumb,
  Modal,
  Logo,
} from '@/components'
import { TrashIcon, PlusIcon } from '@/lib/icons'
import './playground.css'

type SectionDef = { id: string; label: string; render: () => ReactNode }

function Block({ title, desc, children, col }: { title: string; desc?: string; children: ReactNode; col?: boolean }) {
  return (
    <div className="pg-block">
      <div className="pg-block-h">{title}</div>
      {desc && <div className="pg-block-d">{desc}</div>}
      <div className={cx('pg-stage', col && 'col')}>{children}</div>
    </div>
  )
}

const TOKENS: [string, string][] = [
  ['--bg', '#121210'],
  ['--panel', '#171715'],
  ['--card', '#1a1a18'],
  ['--card-2', '#1f1f1c'],
  ['--hover', '#222220'],
  ['--line', '#262521'],
  ['--tx', '#ECE9E3'],
  ['--tx-2', '#A3A097'],
  ['--tx-3', '#6F6D65'],
  ['--tx-faint', '#4C4A44'],
  ['--accent', '#7FAA8A'],
  ['--warn', '#C9A26B'],
  ['--crit', '#C98787'],
  ['--info', '#6F9BD1'],
  ['--cat-1', '#7FAA8A'],
  ['--cat-2', '#6F9BD1'],
  ['--cat-3', '#B58BC9'],
  ['--cat-4', '#C9A26B'],
  ['--cat-5', '#C98787'],
  ['--cat-6', '#5FB0A6'],
]

function TokensSection() {
  return (
    <>
      <Block title="Color tokens" desc="The full calm palette — warm-dark neutrals plus the sage accent and muted status hues.">
        <div className="pg-swatches" style={{ width: '100%' }}>
          {TOKENS.map(([name, hex]) => (
            <div key={name} className="pg-swatch">
              <div className="pg-chip" style={{ background: `var(${name})` }} />
              <div className="pg-token">
                {name} <span className="hex">{hex}</span>
              </div>
            </div>
          ))}
        </div>
      </Block>
      <Block title="Type" desc="Inter · JetBrains Mono · Collier (serif display).">
        <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
          <span style={{ fontFamily: 'var(--serif)', fontStyle: 'italic', fontSize: 28 }}>flo</span>
          <span style={{ fontSize: 26, fontWeight: 600, letterSpacing: '-0.02em' }}>Inter Semibold 26 / page title</span>
          <span style={{ fontSize: 14 }}>Inter Regular 14 / body</span>
          <span className="mono" style={{ fontSize: 13 }}>JetBrains Mono 13 / 1717318872-10042</span>
        </div>
      </Block>
    </>
  )
}

function ButtonsSection() {
  return (
    <>
      <Block title="Variants">
        <Button>Default</Button>
        <Button variant="quiet">Quiet</Button>
        <Button variant="accent">Accent</Button>
        <Button variant="danger">
          <TrashIcon />
          Danger
        </Button>
      </Block>
      <Block title="With icon · disabled · sizes">
        <Button variant="accent">
          <PlusIcon />
          New key
        </Button>
        <Button disabled>Disabled</Button>
        <Button style={{ padding: '5px 11px', fontSize: 12 }}>Compact</Button>
      </Block>
    </>
  )
}

function InputsSection() {
  const [text, setText] = useState('user:142:profile')
  const [area, setArea] = useState('{\n  "value": ""\n}')
  const [seg, setSeg] = useState('json')
  const [ttl, setTtl] = useState('60s')
  const [check, setCheck] = useState(true)
  const [on, setOn] = useState(true)
  return (
    <>
      <Block title="Field" col>
        <Field wrapStyle={{ width: 280 }} value={text} onChange={(e) => setText(e.target.value)} placeholder="Search keys…" />
      </Block>
      <Block title="TextArea (mono code editor)" col>
        <TextArea value={area} onChange={(e) => setArea(e.target.value)} />
      </Block>
      <Block title="SegGroup">
        <SegGroup
          options={[
            { id: 'json', label: 'JSON' },
            { id: 'raw', label: 'Raw' },
          ]}
          value={seg}
          onChange={setSeg}
        />
        <SegGroup
          options={[
            { id: 'none', label: 'No expiry' },
            { id: '60s', label: '60s' },
            { id: '1h', label: '1 hour' },
            { id: '24h', label: '24 hours' },
          ]}
          value={ttl}
          onChange={setTtl}
        />
      </Block>
      <Block title="Checkbox · Toggle">
        <Checkbox checked={check} onChange={setCheck}>
          <b>Only if absent</b> <span className="mono" style={{ color: 'var(--tx-faint)' }}>--nx</span>
        </Checkbox>
        <span style={{ display: 'flex', alignItems: 'center', gap: 9 }}>
          <Toggle on={on} onChange={setOn} />
          <span style={{ fontSize: 13, color: 'var(--tx-2)' }}>{on ? 'enabled' : 'disabled'}</span>
        </span>
      </Block>
    </>
  )
}

function FeedbackSection() {
  return (
    <>
      <Block title="Pill">
        <Pill>default</Pill>
        <Pill tone="ok">healthy</Pill>
        <Pill tone="lag">3.0K lag</Pill>
        <Pill tone="ok" dot={false}>
          no dot
        </Pill>
      </Block>
      <Block title="Tag · StatusPill · OpPill">
        <Tag>v7</Tag>
        <Tag>32</Tag>
        <StatusPill color="var(--info)" label="running" />
        <StatusPill color="var(--accent)" label="completed" />
        <StatusPill color="var(--crit)" label="failed" />
        <OpPill color="var(--cat-3)">FILTER</OpPill>
        <OpPill color="var(--cat-2)">AGGREGATE</OpPill>
      </Block>
      <Block title="Bar · Dot" col>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <Bar value={0.45} style={{ maxWidth: 200 }} />
          <span className="mono" style={{ fontSize: 12, color: 'var(--tx-3)' }}>45%</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <Bar value={0.82} warn style={{ maxWidth: 200 }} />
          <span className="mono" style={{ fontSize: 12, color: 'var(--warn)' }}>82%</span>
        </div>
        <div style={{ display: 'flex', gap: 14 }}>
          <Dot color="var(--accent)" />
          <Dot color="var(--warn)" />
          <Dot color="var(--crit)" />
          <Dot color="var(--info)" />
        </div>
      </Block>
    </>
  )
}

function LayoutSection() {
  return (
    <>
      <Block title="Stats" col>
        <Stats>
          <Stat label="Streams" value={5} sub="in production" />
          <Stat label="Partitions" value={128} sub="across streams" />
          <Stat label="Ingest" value="12.8K" unit="rec/s" sub="aggregate" />
          <Stat label="Lagging" value={2} valueColor="var(--warn)" sub="need attention" />
        </Stats>
      </Block>
      <Block title="Card · PhSec" col>
        <Card>
          <PhSec icon="M3.5 18a8.5 8.5 0 0 1 17 0|M12 18l4.5-6" title="Cluster summary" info />
          <div style={{ fontSize: 13, color: 'var(--tx-2)' }}>A flat borderless surface card with an icon section header.</div>
        </Card>
      </Block>
      <Block title="PageHead" col>
        <PageHead title="KV Store" subtitle="Versioned · strongly consistent · MVCC time-travel" />
      </Block>
      <Block title="Section · Crumb" col>
        <Crumb root="Streams" current="user-clicks" />
        <Section title="Activity" meta="80 records · click or drag to inspect">
          <div style={{ fontSize: 13, color: 'var(--tx-3)' }}>Section body content.</div>
        </Section>
      </Block>
    </>
  )
}

function OverlaySection() {
  const [open, setOpen] = useState(false)
  return (
    <Block title="Modal">
      <Button onClick={() => setOpen(true)}>Open modal</Button>
      {open && (
        <Modal
          title="Delete key"
          sub="user:142:profile"
          width="min(440px,92vw)"
          onClose={() => setOpen(false)}
          foot={
            <>
              <Button variant="quiet" onClick={() => setOpen(false)}>
                Cancel
              </Button>
              <Button variant="danger" onClick={() => setOpen(false)}>
                <TrashIcon />
                Delete key
              </Button>
            </>
          }
        >
          <p style={{ margin: 0, fontSize: 13.5, color: 'var(--tx-2)', lineHeight: 1.55 }}>
            Permanently delete <span className="mono" style={{ color: 'var(--tx)' }}>user:142:profile</span>? Writes a
            tombstone at the next LSN.
          </p>
        </Modal>
      )}
    </Block>
  )
}

const SECTIONS: SectionDef[] = [
  { id: 'tokens', label: 'Tokens', render: TokensSection },
  { id: 'buttons', label: 'Buttons', render: ButtonsSection },
  { id: 'inputs', label: 'Inputs', render: InputsSection },
  { id: 'feedback', label: 'Feedback', render: FeedbackSection },
  { id: 'layout', label: 'Layout', render: LayoutSection },
  { id: 'overlay', label: 'Overlay', render: OverlaySection },
]

export function Playground() {
  const [active, setActive] = useState('tokens')
  const section = SECTIONS.find((s) => s.id === active)!
  return (
    <div className="pg">
      <nav className="pg-side">
        <div className="pg-brand">
          <Logo className="wm" />
          <span className="sl">/</span>
          <span className="lbl">playground</span>
        </div>
        {SECTIONS.map((s) => (
          <button key={s.id} className={cx('pg-nav', active === s.id && 'on')} onClick={() => setActive(s.id)}>
            {s.label}
          </button>
        ))}
        <div style={{ borderTop: '1px solid var(--line-soft)', margin: '14px 0' }} />
        <Link to="/" className="pg-nav">
          ← Back to console
        </Link>
      </nav>
      <main className="pg-main">
        <div className="pg-wrap fade" key={active}>
          <h1 className="pg-h">{section.label}</h1>
          <p className="pg-sub">Live component variants from the Flo Console design system.</p>
          {section.render()}
        </div>
      </main>
    </div>
  )
}
