import { useState, useCallback, useEffect } from 'react';
import type { SavedView } from '../lib/workflow-types';

const STORAGE_KEY = 'flo:workflow:custom-views';

/**
 * Seed data — written to localStorage on first visit so the sidebar isn't empty.
 * Users can delete these; they won't reappear once localStorage has been initialised.
 */
const SEED_VIEWS: SavedView[] = [
  {
    id: 'customer-enrichment',
    name: 'Customer Enrichment Runs',
    icon: 'bookmark',
    isSystem: false,
    filters: { workflow: 'customer-enrichment' },
  },
];

function loadViews(): SavedView[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (raw === null) {
      // First visit — seed and persist
      localStorage.setItem(STORAGE_KEY, JSON.stringify(SEED_VIEWS));
      return SEED_VIEWS;
    }
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) return parsed;
    return [];
  } catch {
    return [];
  }
}

function persistViews(views: SavedView[]) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(views));
  } catch {
    // localStorage full or unavailable — silent fail
  }
}

export function useCustomViews() {
  const [views, setViews] = useState<SavedView[]>(loadViews);

  // Sync to localStorage whenever views change
  useEffect(() => {
    persistViews(views);
  }, [views]);

  const addView = useCallback((view: Omit<SavedView, 'id' | 'isSystem'>) => {
    const id = `custom-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
    const newView: SavedView = { ...view, id, isSystem: false };
    setViews((prev) => [...prev, newView]);
    return newView;
  }, []);

  const removeView = useCallback((id: string) => {
    setViews((prev) => prev.filter((v) => v.id !== id));
  }, []);

  const updateView = useCallback((id: string, patch: Partial<Omit<SavedView, 'id' | 'isSystem'>>) => {
    setViews((prev) =>
      prev.map((v) => (v.id === id ? { ...v, ...patch } : v)),
    );
  }, []);

  return { customViews: views, addView, removeView, updateView };
}
