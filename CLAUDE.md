# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Tuwunel is a Matrix homeserver (spec v1.17) running entirely on Cloudflare Workers edge infrastructure. It uses D1 (SQLite), KV, R2, Durable Objects, and Workflows. The live instance runs at `m.easydemo.org`.

## Development Commands

```bash
npm run dev              # Local dev server (wrangler dev -c wrangler.local.jsonc)
npm run deploy           # Deploy to Cloudflare (-c wrangler.local.jsonc)
npm run typecheck        # TypeScript type checking (tsc --noEmit)
npm run lint             # ESLint on src/  (see caveat below)
npm run test             # Vitest         (see caveat below)
npm run db:migrate       # Apply migrations/schema.sql (remote) - NOT the numbered migrations
npm run db:migrate:local # Apply migrations/schema.sql (local)
```

**Migration gotcha:** `db:migrate` only executes `migrations/schema.sql`. The numbered migrations (`002_*`-`018_*`) are **not** applied by any npm script - run each manually: `npx wrangler d1 execute tuwunel-db [--local] --file=./migrations/0XX_name.sql`. See `DEPLOYMENT.md` for the full first-time setup sequence (resource creation, secrets, domain).

**Tooling caveats:** there are currently no `*.test.ts` files (so `npm run test` runs nothing) and no ESLint config or `eslint` dependency in `package.json` (so `npm run lint` will not run as-is). `npm run typecheck` is the reliable pre-commit check.

**Wrangler config:** `wrangler.jsonc` is a tracked template with placeholder resource IDs. The real deployment config lives in `wrangler.local.jsonc`, which is gitignored - `npm run dev` and `npm run deploy` pass `-c wrangler.local.jsonc`, and raw `npx wrangler ...` commands need that flag too.

## Architecture

**Framework:** Hono web framework with typed `AppEnv` bindings for Cloudflare resources.

**Entry point:** `src/index.ts` - creates the Hono app, applies global middleware (CORS -> Logger -> Analytics, then Rate Limit scoped to `/_matrix/*`), mounts all route modules, and exports Durable Objects + Workflows. A few endpoints (filters, public rooms, user directory search, capabilities, stubs) are defined inline here rather than in a route module.

**Layered structure:**
- `src/api/` - Route handlers (~38 modules). Each exports a Hono instance mounted in the main app. Largest: `federation.ts` (103KB), `sliding-sync.ts` (81KB), `admin.ts` (80KB), `rooms.ts` (73KB).
- `src/middleware/` - Auth (`requireAuth()`), rate limiting (DO-based sliding window), federation auth (Ed25519 X-Matrix), idempotency, analytics.
- `src/services/` - Business logic: `database.ts` (D1 queries, no ORM), `federation-keys.ts`, `federation-sender.ts` (outbound PDU delivery), `media-federation.ts` (multipart/mixed media), `sync-wait.ts` (hibernatable-WebSocket long poll), `server-discovery.ts`, `email.ts` (Cloudflare Email Service), `oidc.ts`, `oidc-keys.ts` (RS256 id_token signing key), `turn.ts`, `livekit.ts`, `cloudflare-calls.ts`, `room-cache.ts`, `transactions.ts`, `appservice.ts`, `push-rule-evaluator.ts`. **Matrix state engine:** `event-auth.ts` (auth rules), `room-versions.ts` (v1-v12 behavior), `state-resolution.ts` (state res v2) + `state-resolution-v1.ts`.
- `src/durable-objects/` - 8 DOs: Room (WebSocket coordination), Sync, Federation (queue), CallRoom (video), Admin, UserKeys (E2EE), Push, RateLimit.
- `src/workflows/` - durable multi-step ops. 5 classes exist & are exported from `index.ts`: `RoomJoinWorkflow` (federation handshake w/ retry), `PushNotificationWorkflow`, `FederationCatchupWorkflow`, `MediaCleanupWorkflow`, `StateCompactionWorkflow`. **Note:** only `room-join-workflow` and `push-notification-workflow` are bound in `wrangler.jsonc` - the other three are not yet wired up as deployable workflow bindings.
- `src/consumers/` - `federation-consumer.ts`: a Cloudflare Queues batch handler for outbound federation. No `queues` binding exists in `wrangler.jsonc` yet, so it is not currently invoked.
- `src/crypto/` - placeholder module for E2EE crypto exports (currently empty); active crypto lives in `src/utils/crypto.ts`.
- `src/types/` - `env.ts` (Cloudflare bindings), `matrix.ts` (PDU/event types).
- `src/utils/` - `crypto.ts` (hashing/signing/constant-time compare), `event-hashing.ts` (redaction, reference hashes, event IDs, PDU signing), `ids.ts` (Matrix ID generation), `errors.ts` (MatrixApiError + Errors factory), `url-validator.ts` (SSRF guard for federation/media URLs).
- `src/admin/dashboard.ts` - Embedded admin web UI at `/admin`.
- `migrations/` - D1 schema files (`schema.sql` + numbered migrations `002`-`018`).

**Storage bindings (defined in `wrangler.jsonc`):**
- D1 `tuwunel-db` - Relational data (users, rooms, events, memberships, etc.)
- KV namespaces: `SESSIONS`, `DEVICE_KEYS`, `ONE_TIME_KEYS`, `CROSS_SIGNING_KEYS`, `CACHE`, `ACCOUNT_DATA`
- R2 `MEDIA` - Media file storage

**Key patterns:**
- Auth: token from `Authorization: Bearer` or `?access_token=`, SHA-256 hashed, looked up in D1 `access_tokens`. Middleware sets `userId`/`deviceId` on context.
- Errors: Use `MatrixApiError` class and `Errors` factory for standardized Matrix JSON responses (`errcode`, `error`).
- Database: Direct D1 prepared statements - no ORM. All queries in `src/services/database.ts` or inline in route handlers.
- IDs follow Matrix format: `@user:domain`, `!room_id:domain`, `$event_id:domain`, `#alias:domain`.
- Federation: Ed25519 signing, X-Matrix header validation, server key caching in KV.
- Real-time: Hibernatable WebSockets via RoomDurableObject, long-polling `/sync`, Sliding Sync (MSC3575/MSC4186) for Element X.
- Passwords hashed with PBKDF2-SHA256 (100,000 iterations).
- Room versions: supports v1-v12 (default `10`, advertised in `/capabilities`). State resolution dispatches by room version through `services/state-resolution.ts`, falling back to v1 for legacy rooms.

**TypeScript config:** Strict mode, ES2022 target, `@/*` path alias maps to `src/*`, `@cloudflare/workers-types`.

## Git Commit Rules

- Never include Claude attribution (e.g., `Co-Authored-By`) in commit messages.
