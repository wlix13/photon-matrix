# photon-matrix

A Matrix homeserver ([spec v1.17](https://spec.matrix.org/v1.17/)) running entirely on Cloudflare's edge - Workers, D1, KV, R2, Durable Objects and Workflows. No origin server, no container, no VM.

> **This is a fork** of [nkuntz1934/matrix-workers](https://github.com/nkuntz1934/matrix-workers), a proof of concept showing that E2EE Matrix could work over Element X on Workers. This fork carries that further: federation now works end to end against matrix.org, and a long list of client-visible bugs are fixed. See [What this fork changes](#what-this-fork-changes).

## Status

Runs a real homeserver: local users chat with each other and with users on remote servers, over E2EE, from Element Web and Element X. It is a personal deployment rather than a product - there is no test suite, no migration tooling, and no upgrade path guarantee. Read [Limitations](#limitations) before pointing anything you care about at it.

## What this fork changes

**Federation actually federates.** The upstream code had the endpoints but not a working handshake. This fork signs and delivers outbound PDUs through a per-destination Durable Object retry queue, completes the `make_join` -> `send_join` handshake with a full state import, persists inbound invites and PDUs, co-signs and returns the full recursive auth chain from `send_join`, computes minimal per-event `auth_events`, derives event IDs from the reference hash of the redacted event per room version, and backfills the recent timeline on join so pre-join events arrive.

| Area | What changed |
|------|--------------|
| Federation | Signed PDU delivery + DO retry queue, remote join with state import, inbound invites, send_join co-signing, per-room-version event IDs, redaction-aware signing |
| E2EE over federation | Remote device-key query and one-time-key claim, to-device fan-out as `m.direct_to_device` EDUs |
| Media | Federated fetch and serve as `multipart/mixed`, edge-cached downloads to cut R2 reads |
| Cost | Long-polling `/sync` parks on a hibernatable WebSocket, so the Sync DO stops billing wall-clock while a client waits |
| Client correctness | Redactions propagate, read receipts clear unread counts, device-list and to-device stream positions tracked separately, idempotent sends per transaction ID, working `/relations` and `/threads` |
| Auth | Signed OIDC `id_token` + JWKS (Element Web/Desktop login), hardened SSO callback, constant-time secret comparison, password policy |
| Security | Application services confined to their exclusive namespace, upload size enforced on the actual body, `Content-Disposition` sanitized, unsafe media types never served inline |
| Deployment | `wrangler.jsonc` is a tracked template; real resource IDs live in a gitignored `wrangler.local.jsonc` |

## Quick start

```bash
git clone https://github.com/wlix13/photon-matrix photon-matrix
cd photon-matrix
npm install
```

Create the Cloudflare resources (save every ID the commands print):

```bash
npx wrangler d1 create photon-matrix-db
npx wrangler kv namespace create SESSIONS
npx wrangler kv namespace create DEVICE_KEYS
npx wrangler kv namespace create ONE_TIME_KEYS
npx wrangler kv namespace create CROSS_SIGNING_KEYS
npx wrangler kv namespace create CACHE
npx wrangler kv namespace create ACCOUNT_DATA
npx wrangler r2 bucket create photon-matrix-media
```

Copy the tracked template and fill in those IDs - the copy is gitignored, so your account details never reach the repository:

```bash
cp wrangler.jsonc wrangler.local.jsonc
```

`npm run dev` and `npm run deploy` both read `wrangler.local.jsonc`. Raw `npx wrangler ...` commands need `-c wrangler.local.jsonc` passed explicitly.

Apply the schema, then **every** numbered migration in order - no npm script does this for you:

```bash
npx wrangler d1 execute photon-matrix-db --remote --file=./migrations/schema.sql
npx wrangler d1 execute photon-matrix-db --remote --file=./migrations/002_phase1_e2ee.sql
# ... through 018_backfill_event_relations.sql
```

Then deploy and point a custom domain at the Worker. [DEPLOYMENT.md](./DEPLOYMENT.md) has the full sequence including secrets, domain routing and optional features.

### Server name vs API domain

If your Matrix IDs should read `@you:example.org` while the API is served from `m.example.org`, set both variables and add a route for the delegation path only:

```jsonc
"vars": { "SERVER_NAME": "example.org", "WORKER_DOMAIN": "m.example.org" },
"routes": [
  { "pattern": "m.example.org", "custom_domain": true },
  { "pattern": "example.org/.well-known/matrix/*", "zone_name": "example.org" }
]
```

`WORKER_DOMAIN` is optional and falls back to `SERVER_NAME`. Every client-facing URL (well-known, OAuth discovery, UIA, media) is built from it.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Cloudflare Edge Network                           │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │   Workers   │  │  Durable    │  │     D1      │  │         R2          │ │
│  │   (Hono)    │──│  Objects    │──│  (SQLite)   │  │   (Object Storage)  │ │
│  │             │  │             │  │             │  │                     │ │
│  │ - Routing   │  │ - Room DO   │  │ - users     │  │ - Media files       │ │
│  │ - Auth      │  │ - Sync DO   │  │ - rooms     │  │ - Thumbnails        │ │
│  │ - API       │  │ - Fed DO    │  │ - events    │  │ - Avatars           │ │
│  │ - Rate Lim  │  │ - Keys DO   │  │ - keys      │  │                     │ │
│  └─────────────┘  │ - Push DO   │  │ - tokens    │  └─────────────────────┘ │
│         │         │ - Admin DO  │  └─────────────┘            │             │
│         │         │ - Call DO   │                             │             │
│         │         │ - Rate DO   │                             │             │
│         │         └─────────────┘         │                   │             │
│  ┌──────┴─────────────────────────────────┴───────────────────┴───────────┐ │
│  │                          KV Namespaces                                 │ │
│  │  SESSIONS - DEVICE_KEYS - CACHE - ONE_TIME_KEYS - CROSS_SIGNING_KEYS   │ │
│  │  ACCOUNT_DATA                                                          │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                     Workflows (Durable Execution)                      │ │
│  │  RoomJoinWorkflow - PushNotificationWorkflow                           │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────┘
```

Three more workflow classes exist in `src/workflows/` (federation catch-up, media cleanup, state compaction) but are not bound in `wrangler.jsonc` and therefore never run.

## Spec compliance

| Spec section | Implementation |
|--------------|----------------|
| [Client-Server API](https://spec.matrix.org/v1.17/client-server-api/) | [`src/api/`](src/api/) - auth, sync, rooms, messaging, profiles |
| [Server-Server API](https://spec.matrix.org/v1.17/server-server-api/) | [`src/api/federation.ts`](src/api/federation.ts), [`src/services/federation-sender.ts`](src/services/federation-sender.ts) |
| [Room versions](https://spec.matrix.org/v1.17/rooms/) | [`room-versions.ts`](src/services/room-versions.ts), [`event-auth.ts`](src/services/event-auth.ts), [`state-resolution.ts`](src/services/state-resolution.ts) - v1-v12, default 10 |
| [Event hashing & signing](https://spec.matrix.org/v1.17/server-server-api/#signing-events) | [`src/utils/event-hashing.ts`](src/utils/event-hashing.ts) |
| [End-to-end encryption](https://spec.matrix.org/v1.17/client-server-api/#end-to-end-encryption) | [`keys.ts`](src/api/keys.ts), [`key-backups.ts`](src/api/key-backups.ts), [`to-device.ts`](src/api/to-device.ts) |
| [OAuth 2.0 API](https://spec.matrix.org/v1.17/client-server-api/#oauth-20-api) | [`oauth.ts`](src/api/oauth.ts), [`oidc-auth.ts`](src/api/oidc-auth.ts), [`oidc-keys.ts`](src/services/oidc-keys.ts) |
| [Content repository](https://spec.matrix.org/v1.17/client-server-api/#content-repository) | [`media.ts`](src/api/media.ts), [`media-federation.ts`](src/services/media-federation.ts) |
| [Push notifications](https://spec.matrix.org/v1.17/client-server-api/#push-notifications) | [`push.ts`](src/api/push.ts), [`push-rule-evaluator.ts`](src/services/push-rule-evaluator.ts) |
| [Receipts](https://spec.matrix.org/v1.17/client-server-api/#receipts) - [Presence](https://spec.matrix.org/v1.17/client-server-api/#presence) - [Typing](https://spec.matrix.org/v1.17/client-server-api/#typing-notifications) | [`receipts.ts`](src/api/receipts.ts), [`presence.ts`](src/api/presence.ts), [`typing.ts`](src/api/typing.ts) |
| [Spaces](https://spec.matrix.org/v1.17/client-server-api/#spaces) - [VoIP](https://spec.matrix.org/v1.17/client-server-api/#voice-over-ip) | [`spaces.ts`](src/api/spaces.ts), [`voip.ts`](src/api/voip.ts), [`calls.ts`](src/api/calls.ts) |
| [Discovery](https://spec.matrix.org/v1.17/client-server-api/#server-discovery) - [Account data](https://spec.matrix.org/v1.17/client-server-api/#client-config) - [3PID](https://spec.matrix.org/v1.17/client-server-api/#adding-account-administrative-contact-information) | [`versions.ts`](src/api/versions.ts), [`account-data.ts`](src/api/account-data.ts), [`account.ts`](src/api/account.ts) |

**Unstable features:** Sliding Sync ([MSC3575](https://github.com/matrix-org/matrix-spec-proposals/pull/3575), [MSC4186](https://github.com/matrix-org/matrix-spec-proposals/pull/4186)), authenticated media ([MSC3916](https://github.com/matrix-org/matrix-spec-proposals/pull/3916)), cross-signing reset ([MSC4312](https://github.com/matrix-org/matrix-spec-proposals/pull/4312)), QR login ([MSC4108](https://github.com/matrix-org/matrix-spec-proposals/pull/4108)).

## Federation

Implemented and exercised against matrix.org: server key publication and notary queries, `PUT /send/{txnId}` in both directions, `make_join`/`send_join` (v1 and v2), `make_leave`/`send_leave`, `make_knock`/`send_knock`, `invite` (v1 and v2), `state`/`state_ids`/`event`/`event_auth`/`backfill`/`get_missing_events`/`timestamp_to_event`, device-key query and one-time-key claim, media download and thumbnail as `multipart/mixed`, directory and profile queries, public room lists, space hierarchy, and OpenID userinfo.

All outbound traffic is signed with Ed25519 and an `X-Matrix` authorization header; inbound PDUs are rejected unless their signature verifies over the redacted event.

## Admin dashboard

Served at `/admin` on your own domain. Server stats and activity charts, user management (create, deactivate, purge, reset passwords), room and media management, content reports, federation status, identity provider configuration, and a registration toggle. `Cmd/Ctrl+K` opens a command palette; `g h` / `g u` / `g r` jump between sections; `?` lists shortcuts.

Synapse-compatible `/_synapse/admin/*` endpoints are available for existing Matrix admin tooling.

## Development

```bash
npm install
npm run dev          # wrangler dev against wrangler.local.jsonc
npm run typecheck    # tsc --noEmit - the real pre-commit gate
```

There are no `*.test.ts` files, so `npm run test` runs nothing, and no ESLint config is committed, so `npm run lint` will not run as-is. `npm run typecheck` is what to trust. [CLAUDE.md](./CLAUDE.md) documents the module layout.

## Cloudflare bindings

| Binding | Type | Purpose |
|---------|------|---------|
| `DB` | D1 | Users, rooms, events, memberships, keys, tokens |
| `SESSIONS` | KV | Access and refresh tokens, OAuth codes |
| `DEVICE_KEYS` - `ONE_TIME_KEYS` - `CROSS_SIGNING_KEYS` | KV | E2EE key caches |
| `CACHE` | KV | Presence, federation transactions, sync filters, OIDC signing key |
| `ACCOUNT_DATA` | KV | User account data |
| `MEDIA` | R2 | Media file storage |
| `ROOMS` - `SYNC` - `FEDERATION` | Durable Object | Room coordination, sync waits, federation retry queue |
| `USER_KEYS` - `PUSH` - `ADMIN` - `RATE_LIMIT` - `CALL_ROOMS` | Durable Object | E2EE keys, push queue, admin state, rate limiting, calls |
| `ROOM_JOIN_WORKFLOW` - `PUSH_NOTIFICATION_WORKFLOW` | Workflow | Durable join handshake and push delivery |
| `EMAIL` | Send Email | 3PID verification via Cloudflare Email Service |

## Security

Passwords are PBKDF2-SHA256 (100,000 iterations) compared in constant time, with an eight-character minimum advertised at `/_matrix/client/v3/password_policy`. Rate limiting is a per-IP and per-user sliding window in a Durable Object. Federation requests are Ed25519-signed and validated through the `X-Matrix` header, and inbound PDU signatures are verified over the redacted event. Application service tokens may only assert identities inside their own exclusive namespace. Media uploads are checked against the real body size, filenames are sanitized before they reach `Content-Disposition`, and types that could execute in a browser are always served as downloads.

## Compatibility

Tested with Element Web, Element X (iOS) and Element X (Android); federation tested against matrix.org with the [Matrix Federation Tester](https://federationtester.matrix.org/).

## Limitations

| Constraint | Limit | Notes |
|------------|-------|-------|
| Worker CPU | 30s | Long operations belong in a Workflow |
| Worker memory | 128MB | Stream large responses |
| D1 database | 10GB | Archive old events if you get close |
| R2 object | 5GB | Chunked upload supported |
| KV value | 25MB | Split large datasets |

Numbered migrations are applied by hand, there is no automated test coverage, and three workflow classes ship unbound. Element Call needs an external LiveKit deployment - the Cloudflare Realtime SFU is not LiveKit-compatible.

## License

MIT, as inherited from the upstream project.
