import { createContext, useContext, useState, useCallback, useEffect, type ReactNode } from "react";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type AuthRole = "admin" | "operator" | "viewer";

export interface AuthState {
  /** JWT session token issued by the server */
  token: string;
  /** Role associated with the authenticated key */
  role: AuthRole;
  /** Epoch-seconds when the token expires */
  expiresAt: number;
}

interface AuthContextValue {
  /** Current auth state, null if not logged in */
  auth: AuthState | null;
  /** Whether a login request is in flight */
  loggingIn: boolean;
  /** Last login error message */
  loginError: string | null;
  /** Exchange an API key for a session token */
  login: (apiKey: string) => Promise<boolean>;
  /** Clear session and redirect to login */
  logout: () => void;
  /** True if the user has a valid (non-expired) session */
  isAuthenticated: boolean;
  /** Whether the server requires authentication (false when auth not configured) */
  authRequired: boolean;
  /** Whether the auth status check has completed */
  authChecked: boolean;
}

// ---------------------------------------------------------------------------
// Storage keys
// ---------------------------------------------------------------------------

const STORAGE_KEY = "flo:auth";

function loadPersistedAuth(): AuthState | null {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as AuthState;
    // Reject if expired
    if (parsed.expiresAt * 1000 < Date.now()) {
      localStorage.removeItem(STORAGE_KEY);
      return null;
    }
    return parsed;
  } catch {
    localStorage.removeItem(STORAGE_KEY);
    return null;
  }
}

function persistAuth(state: AuthState) {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
}

function clearAuth() {
  localStorage.removeItem(STORAGE_KEY);
}

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

const AuthContext = createContext<AuthContextValue | null>(null);

const API_BASE = import.meta.env.VITE_API_URL || "/api/v1";

export function AuthProvider({ children }: { children: ReactNode }) {
  const [auth, setAuth] = useState<AuthState | null>(loadPersistedAuth);
  const [loggingIn, setLoggingIn] = useState(false);
  const [loginError, setLoginError] = useState<string | null>(null);
  const [authRequired, setAuthRequired] = useState(true);
  const [authChecked, setAuthChecked] = useState(false);

  const isAuthenticated = auth !== null && auth.expiresAt * 1000 > Date.now();

  // Check server auth status on mount
  useEffect(() => {
    fetch(`${API_BASE}/auth/status`)
      .then((res) => res.json())
      .then((data: { required: boolean }) => {
        setAuthRequired(data.required);
      })
      .catch(() => {
        // If status check fails, assume auth is required (safe default)
        setAuthRequired(true);
      })
      .finally(() => setAuthChecked(true));
  }, []);

  // Periodic expiry check — log out when token expires
  useEffect(() => {
    if (!auth) return;
    const remaining = auth.expiresAt * 1000 - Date.now();
    if (remaining <= 0) {
      setAuth(null);
      clearAuth();
      return;
    }
    const timer = setTimeout(() => {
      setAuth(null);
      clearAuth();
    }, remaining);
    return () => clearTimeout(timer);
  }, [auth]);

  const login = useCallback(async (apiKey: string): Promise<boolean> => {
    setLoggingIn(true);
    setLoginError(null);
    try {
      const res = await fetch(`${API_BASE}/auth/session`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Api-Key": apiKey,
        },
        body: JSON.stringify({ api_key: apiKey }),
      });

      if (!res.ok) {
        const body = await res.text();
        setLoginError(body || `HTTP ${res.status}`);
        return false;
      }

      const data: { token: string; role: AuthRole; expires_in: number } = await res.json();
      const state: AuthState = {
        token: data.token,
        role: data.role,
        expiresAt: Math.floor(Date.now() / 1000) + data.expires_in,
      };
      setAuth(state);
      persistAuth(state);
      return true;
    } catch (err) {
      setLoginError(err instanceof Error ? err.message : "Login failed");
      return false;
    } finally {
      setLoggingIn(false);
    }
  }, []);

  const logout = useCallback(() => {
    // Fire-and-forget server notification
    if (auth?.token) {
      fetch(`${API_BASE}/auth/session`, {
        method: "DELETE",
        headers: { Authorization: `Bearer ${auth.token}` },
      }).catch(() => {});
    }
    setAuth(null);
    clearAuth();
  }, [auth]);

  return (
    <AuthContext.Provider value={{ auth, loggingIn, loginError, login, logout, isAuthenticated, authRequired, authChecked }}>
      {children}
    </AuthContext.Provider>
  );
}

/**
 * Hook to access auth state and actions.
 * Must be used within an <AuthProvider>.
 */
export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within <AuthProvider>");
  return ctx;
}

/**
 * Returns the current Bearer token for use in API calls.
 * Returns null if not authenticated.
 */
export function getAuthToken(): string | null {
  const stored = loadPersistedAuth();
  return stored?.token ?? null;
}
