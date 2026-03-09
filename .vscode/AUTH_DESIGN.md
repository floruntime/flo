# Flo — Auth Design

> **Status**: Designed, not yet implemented. Tracked as **M.8** in the roadmap.
>
> **Existing infrastructure** (already built, not yet wired):
> - `src/node/network/jwks.zig` — JWKS client, RS256 + ES256 verification
> - `src/node/network/jwt.zig` — HS256/RS256/ES256 parsing, `matchScope()`
> - `src/config/auth.zig` — auth config struct
> - `src/node/ws_handler.zig` — extracts Bearer token on WS upgrade (not verified yet)

---

## Principle

Flo is fast and nimble. The auth model should be too. Three access layers, three different trust models — minimum viable mechanism for each, no users, no passwords, no policy YAML.

```
┌─────────────────────────────────────────────────────┐
│  Layer 1: CLI & Dashboard  →  API Keys + Roles       │
│  Layer 2: WebSocket        →  JWT from external IdP  │
│  Layer 3: Binary Protocol  →  Open (trust-the-net)   │
└─────────────────────────────────────────────────────┘
```

---

## Layer 1 — CLI & Dashboard: API Keys + Roles

### Bootstrap (explicit, not auto-print)

`flo server start` starts silently. The operator bootstraps in a separate, intentional step:

```bash
flo server bootstrap                    # prints root key to stdout once
flo server bootstrap --out flo.key      # or captures to file (CI/Docker/k8s)
```

- Server enforces **one-time only** — bootstrap fails if already run
- Plaintext key is **never stored to disk by Flo** — operator is responsible for saving it
- Key hash is stored in `~/.flo/data/__internal` (single-node) or replicated via Raft (cluster)
- Bootstrap also generates the **internal HS256 signing secret** used for dashboard session tokens

Why explicit, not auto-print on first start?
- Auto-print breaks systemd, Docker, cloud-init — key gets buried or lost in logs
- `flo server bootstrap` is intentional and scriptable
- Same pattern as `rune admin bootstrap`, `kubeadm init`

---

### Login & Contexts

Contexts allow multiple Flo clusters (dev/staging/prod) from one machine, like `kubectl config use-context`.

```bash
flo auth login                                          # interactive
flo auth login --key flo_sk_admin_xxx --server localhost:4040  # non-interactive
flo use-context prod                                    # switch active context
flo whoami                                              # show current context + role
```

`~/.flo/config.yaml` (chmod 600):
```yaml
current-context: dev
contexts:
  dev:
    server: localhost:4040
    api_key: flo_sk_admin_xxx
    namespace: default
  prod:
    server: flo.myco.com:4040
    api_key: flo_sk_operator_yyy
    namespace: production
```

---

### Key Management (admin only)

```bash
flo auth create-key --name ci-bot --role operator
flo auth create-key --name alice --role viewer --out alice.key
flo auth list-keys
flo auth revoke-key flo_sk_operator_yyy
```

API keys use the prefix `flo_sk_<role>_<random>` for at-a-glance identification.

---

### Roles

Three roles, no more. Role maps directly to `matchScope()` patterns already in `jwt.zig`.

| Role | Scopes | Use case |
|------|--------|----------|
| `admin` | `*:*:*` | Full access — create/revoke keys, config changes, all data |
| `operator` | `read:*:*`, `write:kv:*`, `write:stream:*`, `write:queue:*`, `write:ts:*` | App deployments, CI/CD pipelines |
| `viewer` | `read:*:*` | Read-only — dashboards, monitoring, audit |

---

### Dashboard Session Tokens (EKS-style exchange)

Long-lived API keys must **never persist in the browser** — they are XSS-readable in `localStorage`/`sessionStorage`. The dashboard exchanges the key for a short-lived session token at login, then discards the raw key:

```
API key (entered once at login, discarded after exchange)
    ↓  POST /api/v1/auth/session   { "api_key": "flo_sk_admin_xxx" }
Session token (short-lived HS256 JWT, 8h TTL, signed by Flo's internal secret)
    ↓  stored in browser sessionStorage
All subsequent dashboard REST calls:  Authorization: Bearer <session_token>
```

**Two endpoints:**
```
POST   /api/v1/auth/session    { "api_key": "..." }  →  { "token": "...", "expires_at": 1234, "role": "admin" }
DELETE /api/v1/auth/session                          →  logout (invalidates session server-side)
```

**Session token claims** (Flo-issued HS256 JWT):
```json
{
  "sub": "flo_sk_admin_xxx",
  "role": "admin",
  "iat": 1741478400,
  "exp": 1741507200
}
```

Flo signs with its own internal secret (generated at bootstrap, stored in `__internal`). Revoking the parent API key invalidates all sessions derived from it.

**Credential matrix:**

| Client | Credential | Lifetime | Where stored |
|--------|-----------|----------|-------------|
| CLI | API key (`flo_sk_xxx`) | Long-lived | `~/.flo/config.yaml` (chmod 600) |
| Dashboard | Session token (exchanged at login) | 8h | `sessionStorage` |
| CI/CD | API key | Long-lived | Env var / secrets manager |

---

### Wire Protocol

- **CLI → server**: `X-Api-Key: flo_sk_xxx` header on every request
- **Dashboard → server**: `Authorization: Bearer <session_token>` after exchange
- Server validates key/token, resolves role, calls `matchScope()` before dispatch
- Replaces the current flat `admin_token` string comparison in `dashboard/http_server.zig`

---

### Implementation Checklist

- [ ] `src/auth/keys.zig` — `ApiKey` struct (id, hashed_secret, role, created_at, expires_at), key generation (`flo_sk_<role>_<random>`), constant-time hash compare
- [ ] `src/auth/session.zig` — issue + verify Flo-signed HS256 session JWTs
- [ ] `src/auth/store.zig` — key storage in `__internal` namespace (hashed, replicated)
- [ ] `flo server bootstrap` subcommand — generate root key + internal signing secret, one-time enforced
- [ ] `flo auth login / create-key / list-keys / revoke-key / whoami` CLI commands
- [ ] `flo use-context` + `~/.flo/config.yaml` context management
- [ ] `POST /api/v1/auth/session` + `DELETE /api/v1/auth/session` dashboard endpoints
- [ ] Replace `admin_token` check in `dashboard/http_server.zig` with key/token middleware
- [ ] Wire role → `matchScope()` in dispatcher for all protected operations

**Not in scope for V1**: users, passwords, OAuth2, policy YAML, multi-org, namespace-scoped keys.

---

## Layer 2 — WebSocket: JWT from External IdP

### Use Cases

Browser clients, mobile apps, real-time subscriptions — where the **end user** authenticates via an external IdP (Supabase, Auth0, Clerk, Firebase). Flo does not issue these tokens; it only verifies them.

Typical flow:
```
User → Supabase login → Supabase issues ES256 JWT
Browser → WebSocket upgrade to Flo with: Authorization: Bearer <supabase_jwt>
Flo → verifies via JWKS at supabase project's /.well-known/jwks.json
Flo → pins connection to flo_namespace claim, enforces flo_scopes
```

### Model

- JWT sent on WS upgrade: `Authorization: Bearer <token>` or `?token=<token>`
- Flo verifies via:
  - JWKS endpoint (RS256/ES256) — for Supabase, Auth0, Clerk, Azure AD
  - Shared secret (HS256) — for simple setups or legacy Supabase
- Algorithm auto-detected from JWT header `alg` field (already implemented in `jwt.zig`)
- **Rejected upgrade = 401.** No fallback, no anonymous WS connections (configurable)
- Connection pinned to `flo_namespace` JWT claim
- Operations scoped by `flo_scopes` JWT claim, enforced via `matchScope()`

### JWT Claims (Flo-specific, added to IdP token)

| Claim | Type | Description |
|-------|------|-------------|
| `sub` | string | User ID (standard) |
| `flo_namespace` | string | Namespace this connection is locked to |
| `flo_scopes` | string[] | e.g. `["read:kv:*", "write:stream:chat:*"]` |
| `exp` | number | Expiration (standard, always enforced) |

### Config (`flo.toml`)

```toml
[auth]
jwks_url        = "https://your-project.supabase.co/.well-known/jwks.json"
jwt_secret      = ""          # for HS256; leave blank when using JWKS
require_ws_auth = true        # false = unauthenticated WS allowed (dev mode only)
```

### Scope

Node-level: one JWKS URL per Flo cluster. All WebSocket connections across the cluster use the same IdP. Namespace-level per-tenant IdPs are a future option if multi-tenancy requires it.

### Token Refresh

Client reconnects with a new token when the current one expires. No `REAUTH` opcode — keep the protocol simple.

### Implementation Checklist

- [ ] `ws_handler.parseUpgrade()` → call `verifyAndParse*()` based on detected algorithm → return 101 or 401
- [ ] Set `conn.user_id` and `conn.namespace` from verified claims
- [ ] Dispatcher enforces `flo_scopes` via `matchScope()` for WS-sourced requests
- [ ] `require_ws_auth` config flag wired into `ws_handler`
- [ ] Unit tests: valid token accepted, expired token rejected, wrong namespace rejected, invalid sig rejected

---

## Layer 3 — Binary Protocol: Open (trust-the-network)

No auth on the hot path. Same default model as Redis, Kafka, and PostgreSQL.

**Rationale**: The binary protocol is a private server-to-server API. Callers are application backends and microservices that the operator controls. Adding per-request auth adds latency to every single operation — unacceptable for a performance-first store. Operators secure access at the network layer: VPC security groups, firewall rules, or a TLS-terminating sidecar proxy.

**Future option**: Redis-style `AUTH <key>` as the first command after connect — optional, zero runtime cost when not configured, low priority. Only implement if users explicitly request it.

**Current state**: Correct — no changes needed.

---

## Files Affected (M.8 Implementation)

| File | Change |
|------|--------|
| `src/auth/keys.zig` | New — API key struct + CSPRNG generation + constant-time compare |
| `src/auth/session.zig` | New — Flo-issued HS256 session JWT for dashboard |
| `src/auth/store.zig` | New — key storage in `__internal` namespace |
| `src/cli/commands/auth.zig` | New — `login`, `create-key`, `list-keys`, `revoke-key` |
| `src/cli/commands/server.zig` | Update — add `bootstrap` subcommand |
| `src/cli/config.zig` | Update — context model (`~/.flo/config.yaml`) |
| `src/node/dashboard/http_server.zig` | Update — replace `admin_token` with key/session middleware |
| `src/node/dashboard/api/auth.zig` | New — `POST/DELETE /api/v1/auth/session` endpoints |
| `src/node/ws_handler.zig` | Update — wire JWT verification into upgrade path |
| `src/node/dispatcher.zig` | Update — enforce `matchScope()` for protected operations |
| `src/config/auth.zig` | Update — add `require_ws_auth`, `bootstrap_done` flag |
