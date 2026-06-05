/* Minimal stroke-icon system.
   Icons are pipe-separated SVG path `d` strings; <Icon> splits and renders them.
   This mirrors the design's inline-SVG approach and keeps the icon set tweakable. */

export type IconProps = {
  /** One or more SVG path `d` strings joined by "|". */
  paths: string
  size?: number
  strokeWidth?: number
  className?: string
  style?: React.CSSProperties
}

export function Icon({ paths, size = 16, strokeWidth = 1.5, className, style }: IconProps) {
  return (
    <svg
      viewBox="0 0 24 24"
      width={size}
      height={size}
      fill="none"
      stroke="currentColor"
      strokeWidth={strokeWidth}
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      style={style}
    >
      {paths.split('|').map((d, i) => (
        <path key={i} d={d} />
      ))}
    </svg>
  )
}

/* ---- Navigation / section glyphs (from the design) ---- */
export const NAV_ICONS: Record<string, string> = {
  overview: 'M3.5 18a8.5 8.5 0 0 1 17 0|M12 18l4.5-6',
  streams: 'M12 3l8 4.5-8 4.5-8-4.5z|M4 12l8 4.5 8-4.5|M4 16.5l8 4.5 8-4.5',
  kv: 'M5 5.2c0 1.5 3.1 2.7 7 2.7s7-1.2 7-2.7S15.9 2.5 12 2.5 5 3.7 5 5.2|M5 5.2v13.6c0 1.5 3.1 2.7 7 2.7s7-1.2 7-2.7V5.2|M5 12c0 1.5 3.1 2.7 7 2.7s7-1.2 7-2.7',
  queues: 'M9 6h11|M9 12h11|M9 18h11|M4.5 6h.01|M4.5 12h.01|M4.5 18h.01',
  ts: 'M4 5v14h16|M7 15l3.5-4.5 3 2L20 8',
  actions: 'M13 3 5 13h6l-1 8 8-10h-6z',
  workers:
    'M8.5 8.5h7v7h-7z|M10 3v2.5M14 3v2.5M10 18.5V21M14 18.5V21M3 10h2.5M3 14h2.5M18.5 10H21M18.5 14H21',
  workflows:
    'M7 6.5v11|M7 8a2 2 0 1 0 0-4 2 2 0 0 0 0 4z|M7 21a2 2 0 1 0 0-4 2 2 0 0 0 0 4z|M17 9.5a2 2 0 1 0 0-4 2 2 0 0 0 0 4z|M17 7.5v1A4 4 0 0 1 13 12.5H7',
  processing: 'M4 7h5v5H4z|M15 12h5v5h-5z|M9 9.5h3a3 3 0 0 1 3 3v2',
}

/* ---- Overview section glyphs ---- */
export const PHI: Record<string, string> = {
  gauge: 'M3.5 18a8.5 8.5 0 0 1 17 0|M12 18l4.5-6',
  sparkle: 'M12 3.5l1.9 5.6 5.6 1.9-5.6 1.9L12 18.5l-1.9-5.6L4.5 11l5.6-1.9z',
  pulse: 'M2.5 12h4l2.2-6.5 4 13 2.3-6.5h4.5',
  database:
    'M5 5.2c0 1.5 3.1 2.7 7 2.7s7-1.2 7-2.7S15.9 2.5 12 2.5 5 3.7 5 5.2|M5 5.2v13.6c0 1.5 3.1 2.7 7 2.7s7-1.2 7-2.7V5.2|M5 12c0 1.5 3.1 2.7 7 2.7s7-1.2 7-2.7',
  drives: 'M3.5 6.5h17v4h-17z|M3.5 13.5h17v4h-17z|M7 8.5h.01|M7 15.5h.01',
}

/* ---- Common small icons ---- */
export const TrashIcon = ({ size = 15 }: { size?: number }) => (
  <Icon paths="M3 6h18M8 6V4h8v2M6 6l1 14h10l1-14M10 11v6M14 11v6" size={size} strokeWidth={1.6} />
)
export const XIcon = ({ size = 15 }: { size?: number }) => (
  <Icon paths="M6 6l12 12M18 6L6 18" size={size} strokeWidth={1.7} />
)
export const CheckIcon = ({ size = 14 }: { size?: number }) => (
  <Icon paths="M5 12l5 5L20 7" size={size} strokeWidth={2} />
)
export const PlusIcon = ({ size = 14 }: { size?: number }) => (
  <Icon paths="M12 5v14M5 12h14" size={size} strokeWidth={1.7} />
)
export const ChevronVIcon = ({ size = 13 }: { size?: number }) => (
  <Icon paths="M8 9l4-4 4 4M8 15l4 4 4-4" size={size} strokeWidth={1.6} />
)
export const CubeIcon = ({ size = 15 }: { size?: number }) => (
  <Icon paths="M12 2.5l8 4.5v9l-8 4.5-8-4.5v-9z|M12 12l8-4.5M12 12v9.5M12 12L4 7.5" size={size} strokeWidth={1.6} />
)
