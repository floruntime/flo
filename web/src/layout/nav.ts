/* Console navigation model — shared by the sidebar (Shell) and the command palette. */
export type NavRow = { label: string; to?: string; tail?: string; id: string }
export type NavGroup = { grp: string; rows: NavRow[] }

export const NAV: NavGroup[] = [
  { grp: 'Observe', rows: [{ label: 'Overview', to: '/', id: 'overview' }] },
  {
    grp: 'Data layer',
    rows: [
      { label: 'Streams', to: '/streams', id: 'streams' },
      { label: 'KV Store', to: '/kv', tail: '4', id: 'kv' },
      { label: 'Queues', to: '/queues', tail: '8', id: 'queues' },
      { label: 'Time Series', to: '/timeseries', id: 'ts' },
    ],
  },
  {
    grp: 'Compute',
    rows: [
      { label: 'Actions', to: '/actions', id: 'actions' },
      { label: 'Workers', to: '/workers', id: 'workers' },
      { label: 'Processing', to: '/processing', id: 'processing' },
      { label: 'Workflows', to: '/workflows', id: 'workflows' },
    ],
  },
]
