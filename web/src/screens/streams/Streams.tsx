import { useState } from 'react'
import { StreamsList } from './StreamsList'
import { StreamDetail } from './StreamDetail'

/** Streams screen — master list ↔ detail (internal state, matching the design). */
export function Streams() {
  const [sel, setSel] = useState<string | null>(null)
  return sel ? <StreamDetail name={sel} onBack={() => setSel(null)} /> : <StreamsList onOpen={setSel} />
}
