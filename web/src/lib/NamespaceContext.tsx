import { createContext, useContext, useState, useEffect, type ReactNode } from "react";
import { api, type NamespaceInfo } from "./api";

interface NamespaceContextValue {
  /** All namespaces fetched from the server */
  namespaces: NamespaceInfo[];
  /** Currently selected namespace name */
  selected: string;
  /** Update the selected namespace */
  setSelected: (name: string) => void;
  /** Whether namespaces are still loading */
  loading: boolean;
}

const NamespaceContext = createContext<NamespaceContextValue | null>(null);

const STORAGE_KEY = "flo:namespace";
const POLL_INTERVAL = 10_000;

export function NamespaceProvider({ children }: { children: ReactNode }) {
  const [namespaces, setNamespaces] = useState<NamespaceInfo[]>([]);
  const [selected, setSelectedRaw] = useState<string>(
    () => localStorage.getItem(STORAGE_KEY) || "default"
  );
  const [loading, setLoading] = useState(true);

  const setSelected = (name: string) => {
    setSelectedRaw(name);
    localStorage.setItem(STORAGE_KEY, name);
  };

  useEffect(() => {
    let cancelled = false;

    const fetchNamespaces = async () => {
      try {
        const ns = await api.getNamespaces();
        if (cancelled) return;
        setNamespaces(ns);

        // If current selection doesn't exist in the list, auto-select the first
        if (ns.length > 0 && !ns.find((n) => n.name === selected)) {
          setSelected(ns[0].name);
        }
      } catch {
        // Silently retry on next interval
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    fetchNamespaces();
    const interval = setInterval(fetchNamespaces, POLL_INTERVAL);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <NamespaceContext.Provider value={{ namespaces, selected, setSelected, loading }}>
      {children}
    </NamespaceContext.Provider>
  );
}

/**
 * Hook to access the current namespace selection.
 * Must be used within a <NamespaceProvider>.
 */
export function useNamespace() {
  const ctx = useContext(NamespaceContext);
  if (!ctx) throw new Error("useNamespace must be used within <NamespaceProvider>");
  return ctx;
}
