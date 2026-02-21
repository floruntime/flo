/**
 * useKVWatch — SSE hook for live KV key updates.
 *
 * Opens an EventSource to `/api/v1/kv/namespaces/:ns/keys/:key/watch`
 * and pushes update/delete events to the caller via callbacks.
 *
 * The connection is automatically opened when `namespace` and `key` are
 * both truthy, and closed when either becomes falsy or the component
 * unmounts.
 */

import { useEffect, useRef, useState } from "react";

const API_BASE = import.meta.env.VITE_API_URL || "/api/v1";

export interface KVWatchEvent {
  key: string;
  namespace: string;
  value?: string;
  version?: number;
  found: boolean;
}

export type KVWatchStatus = "connecting" | "open" | "closed";

interface UseKVWatchOptions {
  /** Called on every `event: update` frame */
  onUpdate?: (event: KVWatchEvent) => void;
  /** Called on every `event: delete` frame */
  onDelete?: (event: KVWatchEvent) => void;
  /** Set false to temporarily disable the connection (default: true) */
  enabled?: boolean;
}

export function useKVWatch(
  namespace: string | null | undefined,
  key: string | null | undefined,
  options: UseKVWatchOptions = {}
) {
  const { onUpdate, onDelete, enabled = true } = options;
  const [status, setStatus] = useState<KVWatchStatus>("closed");
  const [lastEvent, setLastEvent] = useState<KVWatchEvent | null>(null);

  // Keep stable refs for callbacks so we don't re-open SSE on every render.
  const onUpdateRef = useRef(onUpdate);
  const onDeleteRef = useRef(onDelete);
  onUpdateRef.current = onUpdate;
  onDeleteRef.current = onDelete;

  useEffect(() => {
    if (!namespace || !key || !enabled) {
      setStatus("closed");
      setLastEvent(null);
      return;
    }

    const url = `${API_BASE}/kv/namespaces/${encodeURIComponent(namespace)}/keys/${encodeURIComponent(key)}/watch`;
    const es = new EventSource(url);
    setStatus("connecting");

    es.onopen = () => {
      setStatus("connecting"); // Wait for "connected" event before marking open
    };

    es.addEventListener("connected", () => {
      setStatus("open");
    });

    es.addEventListener("update", (e: MessageEvent) => {
      try {
        const data: KVWatchEvent = JSON.parse(e.data);
        setLastEvent(data);
        onUpdateRef.current?.(data);
      } catch {
        // Malformed frame — ignore
      }
    });

    es.addEventListener("delete", (e: MessageEvent) => {
      try {
        const data: KVWatchEvent = JSON.parse(e.data);
        setLastEvent(data);
        onDeleteRef.current?.(data);
      } catch {
        // Malformed frame — ignore
      }
    });

    es.onerror = () => {
      // EventSource auto-reconnects. Update status so the UI can show
      // a subtle indicator if desired.
      setStatus("connecting");
    };

    return () => {
      es.close();
      setStatus("closed");
    };
  }, [namespace, key, enabled]);

  return { status, lastEvent };
}
