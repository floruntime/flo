import { createContext, useContext, useState } from 'react'
import type { ReactNode } from 'react'

/** Fallback list shown before the live `GET namespaces` resolves. */
export const NAMESPACES = ['production', 'analytics', 'media', 'staging'] as const
export type Namespace = string

type NamespaceCtx = {
  ns: Namespace
  setNs: (ns: Namespace) => void
}

const Ctx = createContext<NamespaceCtx | null>(null)

export function NamespaceProvider({ children }: { children: ReactNode }) {
  const [ns, setNs] = useState<Namespace>('production')
  return <Ctx.Provider value={{ ns, setNs }}>{children}</Ctx.Provider>
}

export function useNamespace(): NamespaceCtx {
  const v = useContext(Ctx)
  if (!v) throw new Error('useNamespace must be used within NamespaceProvider')
  return v
}
