import { useState, useEffect, useCallback, useRef } from 'react';

interface UseApiResult<T> {
  data: T | null;
  loading: boolean;
  error: string | null;
  refetch: () => void;
}

/**
 * Generic hook for fetching data from the Flo API.
 * Handles loading states, errors, and periodic refresh.
 *
 * @param fetcher - Async function that returns data
 * @param deps - Dependencies array that triggers refetch
 * @param intervalMs - Optional auto-refresh interval in milliseconds (0 = no auto-refresh)
 */
export function useApi<T>(
  fetcher: () => Promise<T>,
  deps: unknown[] = [],
  intervalMs: number = 0
): UseApiResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  // Track whether this is the first fetch (show spinner) vs background poll (silent update)
  const isFirstFetch = useRef(true);

  const fetchData = useCallback(async (showSpinner = false) => {
    try {
      if (showSpinner) setLoading(true);
      setError(null);
      const result = await fetcher();
      setData(result);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error');
    } finally {
      setLoading(false);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  // Manual refetch always shows spinner for immediate feedback
  const refetch = useCallback(() => fetchData(true), [fetchData]);

  useEffect(() => {
    // First fetch after dep change: show spinner
    isFirstFetch.current = true;
    fetchData(true);

    if (intervalMs > 0) {
      const interval = setInterval(() => {
        // Background polls: silent update, no spinner
        fetchData(false);
      }, intervalMs);
      return () => clearInterval(interval);
    }
  }, [fetchData, intervalMs]);

  return { data, loading, error, refetch };
}

/**
 * Loading spinner component for API-powered pages
 */
export function LoadingState({ message = 'Loading...' }: { message?: string }) {
  return (
    <div className="flex items-center justify-center h-64">
      <div className="text-center space-y-3">
        <div className="w-8 h-8 border-2 border-primary border-t-transparent rounded-full animate-spin mx-auto" />
        <p className="text-sm text-text-secondary">{message}</p>
      </div>
    </div>
  );
}

/**
 * Error state component for failed API calls
 */
export function ErrorState({ message, onRetry }: { message: string; onRetry?: () => void }) {
  return (
    <div className="flex items-center justify-center h-64">
      <div className="text-center space-y-3">
        <div className="w-10 h-10 rounded-full bg-error/10 flex items-center justify-center mx-auto">
          <span className="text-error text-lg">!</span>
        </div>
        <p className="text-sm text-text-secondary">{message}</p>
        {onRetry && (
          <button
            onClick={onRetry}
            className="text-sm text-primary hover:underline"
          >
            Retry
          </button>
        )}
      </div>
    </div>
  );
}

/**
 * Empty state for when API returns no data
 */
export function EmptyState({ title, description }: { title: string; description?: string }) {
  return (
    <div className="flex items-center justify-center h-64">
      <div className="text-center space-y-2">
        <p className="text-text-primary font-medium">{title}</p>
        {description && <p className="text-sm text-text-secondary">{description}</p>}
      </div>
    </div>
  );
}
