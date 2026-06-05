/* Typed fetch wrapper over the Flo dashboard REST API (`/api/v1`).
   Dev: Vite proxies `/api` → the dashboard server (:9002). Prod: same origin.
   Auth: when the server has auth enabled, a session token is stored in localStorage
   and sent as `Authorization: Bearer`; when disabled (no `flo server bootstrap`),
   requests go through unauthenticated. */

export const API_BASE = import.meta.env.VITE_API_URL || '/api/v1'

const TOKEN_KEY = 'flo.session'

export function getToken(): string | null {
  try {
    return localStorage.getItem(TOKEN_KEY)
  } catch {
    return null
  }
}
export function setToken(t: string | null) {
  try {
    if (t) localStorage.setItem(TOKEN_KEY, t)
    else localStorage.removeItem(TOKEN_KEY)
  } catch {
    /* ignore */
  }
}

export class ApiError extends Error {
  status: number
  constructor(message: string, status: number) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

async function request<T>(method: string, path: string, body?: unknown, rawText = false): Promise<T> {
  const headers: Record<string, string> = {}
  const token = getToken()
  if (token) headers['Authorization'] = `Bearer ${token}`
  if (body !== undefined) headers['Content-Type'] = rawText ? 'text/plain' : 'application/json'

  const res = await fetch(`${API_BASE}/${path.replace(/^\//, '')}`, {
    method,
    headers,
    // Raw-text bodies (e.g. a YAML pipeline definition) are sent verbatim; the
    // server treats the request body as opaque bytes. Everything else is JSON.
    body: body === undefined ? undefined : rawText ? (body as string) : JSON.stringify(body),
  })

  const text = await res.text()
  let data: unknown = null
  if (text) {
    try {
      data = JSON.parse(text)
    } catch {
      data = text
    }
  }

  const errField =
    data && typeof data === 'object' && 'error' in data
      ? String((data as { error: unknown }).error)
      : null

  if (!res.ok) {
    throw new ApiError(errField ?? `HTTP ${res.status}`, res.status)
  }
  // The API encodes some failures as `{ "error": ... }` with a 200 — surface those too.
  if (errField !== null && data && Object.keys(data as object).length === 1) {
    throw new ApiError(errField, res.status)
  }
  return data as T
}

export const api = {
  get: <T>(path: string) => request<T>('GET', path),
  post: <T>(path: string, body?: unknown) => request<T>('POST', path, body ?? {}),
  /** POST a raw string body verbatim (e.g. a YAML pipeline definition). */
  postText: <T>(path: string, text: string) => request<T>('POST', path, text, true),
  put: <T>(path: string, body?: unknown) => request<T>('PUT', path, body ?? {}),
  del: <T>(path: string) => request<T>('DELETE', path),
}
