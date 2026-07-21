# Deployment Guide

How to deploy this fork of [nkuntz1934/matrix-workers](https://github.com/nkuntz1934/matrix-workers) as your own Matrix homeserver on Cloudflare.

Everything here is manual. There is no Deploy to Cloudflare button: the tracked `wrangler.jsonc` carries a fixed `account_id` and real resource IDs, so one-click provisioning would either fail or bind your Worker to somebody else's resources. Plan on 20-30 minutes.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Step 1: Clone and install](#step-1-clone-and-install)
- [Step 2: Create Cloudflare resources](#step-2-create-cloudflare-resources)
- [Step 3: Write wrangler.local.jsonc](#step-3-write-wranglerlocaljsonc)
- [Step 4: Run the database migrations](#step-4-run-the-database-migrations)
- [Step 5: Set secrets](#step-5-set-secrets)
- [Step 6: Deploy](#step-6-deploy)
- [Step 7: Point a domain at the Worker](#step-7-point-a-domain-at-the-worker)
- [Step 8: Verify](#step-8-verify)
- [Step 9: First user and admin access](#step-9-first-user-and-admin-access)
- [Optional features](#optional-features)
- [Updating](#updating)
- [Troubleshooting](#troubleshooting)
- [Reference](#reference)

---

## Prerequisites

1. **Cloudflare account on the Workers Paid plan** ($5/month). Durable Objects are not optional here - sync, rate limiting, federation retries and E2EE key storage all run on them.
2. **Node.js 18+** (`node --version`).
3. **Wrangler**, used through `npx` so the version pinned in `package.json` is the one that runs:
   ```bash
   npx wrangler --version
   npx wrangler login
   ```
4. **A domain on Cloudflare.** Federation needs a real domain with DNS managed by Cloudflare; a `*.workers.dev` hostname will not federate.

Decide your `SERVER_NAME` before you deploy. It is the domain inside every user ID (`@you:example.org`) and **cannot be changed after the first user registers** without invalidating every ID and room your server took part in.

---

## Step 1: Clone and install

```bash
git clone https://github.com/wlix13/photon-matrix photon-matrix
cd photon-matrix
npm install
```

---

## Step 2: Create Cloudflare resources

Save every ID these commands print.

```bash
npx wrangler whoami                              # note your account ID

npx wrangler d1 create photon-matrix-db          # note database_id

npx wrangler kv namespace create SESSIONS
npx wrangler kv namespace create DEVICE_KEYS
npx wrangler kv namespace create ONE_TIME_KEYS
npx wrangler kv namespace create CROSS_SIGNING_KEYS
npx wrangler kv namespace create CACHE
npx wrangler kv namespace create ACCOUNT_DATA

npx wrangler r2 bucket create photon-matrix-media
```

All six KV namespaces are required - the Worker fails to start if a binding is missing. Durable Objects and Workflows are created by `wrangler deploy` from the config; you do not provision them by hand.

---

## Step 3: Write wrangler.local.jsonc

`wrangler.jsonc` is a tracked template. Copy it to `wrangler.local.jsonc`, which is gitignored, and edit that copy - `npm run dev` and `npm run deploy` both pass `-c wrangler.local.jsonc`, and any raw `npx wrangler ...` command needs the same flag.

```bash
cp wrangler.jsonc wrangler.local.jsonc
```

Then replace, in the copy:

| Field | Value |
|-------|-------|
| `name` | your Worker name |
| `account_id` | from `npx wrangler whoami` |
| `d1_databases[0].database_name` / `database_id` | from `d1 create` |
| `kv_namespaces[*].id` | the six IDs from `kv namespace create` |
| `r2_buckets[0].bucket_name` | your bucket |
| `vars.SERVER_NAME` | your Matrix identity domain |
| `routes[0].pattern` | the hostname serving the API |

Leave `durable_objects`, `migrations` and `workflows` exactly as they are. The `migrations` tags `v1`-`v6` declare the Durable Object classes as SQLite-backed, which is mandatory for accounts created after the classic backend was retired; renumbering or dropping them breaks deployment.

Delete what you are not using, or the deploy will fail on resources you do not own:

- `vpc_services` and `vars.LIVEKIT_*` - only for an external LiveKit server (see [Optional features](#optional-features))
- `vars.TURN_KEY_ID` - only with a Cloudflare TURN key of your own
- `send_email` - only with Cloudflare Email Service enabled on your account

### Server name vs API domain

If Matrix IDs should read `@you:example.org` while the API lives on `m.example.org`, set both variables and route only the delegation path from the apex:

```jsonc
"vars": {
  "SERVER_NAME": "example.org",
  "WORKER_DOMAIN": "m.example.org",
  "SERVER_VERSION": "0.1.0"
},
"routes": [
  { "pattern": "m.example.org", "custom_domain": true },
  { "pattern": "example.org/.well-known/matrix/*", "zone_name": "example.org" }
]
```

`WORKER_DOMAIN` is optional and falls back to `SERVER_NAME`. Every client-facing URL - well-known, OAuth discovery and JWKS, UIA fallback, media - is built from it, so a mismatch here shows up as Element failing to log in rather than as an obvious error.

Only the `/.well-known/matrix/*` path is intercepted on the apex; the rest of that zone keeps serving whatever it served before.

### Optional variables

```jsonc
"vars": {
  "ADMIN_CONTACT_EMAIL": "admin@example.org",   // surfaced at /_matrix/client/v1/support
  "ADMIN_CONTACT_MXID": "@admin:example.org",
  "SUPPORT_PAGE_URL": "https://example.org/support"
}
```

---

## Step 4: Run the database migrations

No npm script applies these. `npm run db:migrate` only runs `schema.sql`, and against the hardcoded legacy database name - ignore it. Apply the schema and then **every** numbered migration, in order, against your own database:

```bash
DB=photon-matrix-db

npx wrangler d1 execute $DB --remote --file=./migrations/schema.sql
npx wrangler d1 execute $DB --remote --file=./migrations/002_phase1_e2ee.sql
npx wrangler d1 execute $DB --remote --file=./migrations/003_account_management.sql
npx wrangler d1 execute $DB --remote --file=./migrations/004_reports_and_notices.sql
npx wrangler d1 execute $DB --remote --file=./migrations/005_server_config.sql
npx wrangler d1 execute $DB --remote --file=./migrations/005_idp_providers.sql
npx wrangler d1 execute $DB --remote --file=./migrations/006_query_optimization.sql
npx wrangler d1 execute $DB --remote --file=./migrations/007_secure_server_keys.sql
npx wrangler d1 execute $DB --remote --file=./migrations/008_federation_transactions.sql
npx wrangler d1 execute $DB --remote --file=./migrations/009_reports_extended.sql
npx wrangler d1 execute $DB --remote --file=./migrations/010_fix_reports_schema.sql
npx wrangler d1 execute $DB --remote --file=./migrations/011_identity_service.sql
npx wrangler d1 execute $DB --remote --file=./migrations/012_fts_search.sql
npx wrangler d1 execute $DB --remote --file=./migrations/013_remote_device_lists.sql
npx wrangler d1 execute $DB --remote --file=./migrations/014_appservice.sql
npx wrangler d1 execute $DB --remote --file=./migrations/015_identity_associations.sql
npx wrangler d1 execute $DB --remote --file=./migrations/016_invite_state.sql
npx wrangler d1 execute $DB --remote --file=./migrations/017_redactions.sql
npx wrangler d1 execute $DB --remote --file=./migrations/018_backfill_event_relations.sql
```

Two migrations share the `005` prefix and both must run. Each command should report `"success": true`. Drop `--remote` to seed a local dev database instead.

There is no migration ledger, so nothing tracks which files you have applied. Keep your own note of the highest number you ran.

---

## Step 5: Set secrets

Nothing is strictly required to boot. The Ed25519 federation signing key is generated on the first request to `/_matrix/key/v2/server` and stored in D1 (`server_keys`, `key_version = 2`) - do not set `SIGNING_KEY`, it is a vestigial field the code never reads.

Set what the features you want need:

```bash
npx wrangler secret put OIDC_ENCRYPTION_KEY -c wrangler.local.jsonc   # openssl rand -base64 32
```

`OIDC_ENCRYPTION_KEY` encrypts upstream identity provider client secrets at rest. Set it before configuring any SSO provider in the admin dashboard.

The rest are per-feature and listed under [Optional features](#optional-features).

---

## Step 6: Deploy

```bash
npm run deploy
```

The output prints the `*.workers.dev` URL and any custom domains it bound.

---

## Step 7: Point a domain at the Worker

If you declared `routes` with `custom_domain: true` in Step 3, `wrangler deploy` already created the DNS record and certificate. Otherwise, add it in the dashboard: **Workers & Pages** -> your Worker -> **Settings** -> **Domains & Routes** -> **Add** -> **Custom Domain**.

The Worker serves both delegation documents itself, at `/.well-known/matrix/server` and `/.well-known/matrix/client`, so no separate static site is needed. If your `SERVER_NAME` is an apex domain, the delegation route from Step 3 is what makes them reachable at the right host.

---

## Step 8: Verify

```bash
export MATRIX_SERVER="https://m.example.org"
export SERVER_NAME="example.org"

curl -s "$MATRIX_SERVER/_matrix/client/versions" | jq .
curl -s "https://$SERVER_NAME/.well-known/matrix/server" | jq .
curl -s "https://$SERVER_NAME/.well-known/matrix/client" | jq .
curl -s "$MATRIX_SERVER/_matrix/key/v2/server" | jq .          # generates the signing key on first call
curl -s "$MATRIX_SERVER/_matrix/federation/v1/version" | jq .
```

Then run the federation tester and look for `"FederationOK": true`:

```
https://federationtester.matrix.org/api/report?server_name=example.org
```

Finally, log in from a client: [Element Web](https://app.element.io) -> **Sign in** -> **Edit** homeserver -> your server URL. Element X (iOS and Android) works against the same deployment through Sliding Sync.

---

## Step 9: First user and admin access

Registration is enabled by default. Registration is a single `m.login.dummy` UIA stage, and passwords must be at least eight characters:

```bash
curl -X POST "$MATRIX_SERVER/_matrix/client/v3/register" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "admin",
    "password": "your-secure-password",
    "auth": {"type": "m.login.dummy"}
  }'
```

Nothing promotes the first user automatically. Grant admin rights directly in D1:

```bash
npx wrangler d1 execute photon-matrix-db --remote \
  --command "UPDATE users SET admin = 1 WHERE user_id = '@admin:example.org'"
```

That account can then open the dashboard at `https://m.example.org/admin` - server stats, user and room management, media, reports, federation status, identity providers, and the registration toggle. The Synapse-compatible `/_synapse/admin/*` endpoints accept the same account's access token, so existing Matrix admin tooling works.

Turn public registration off from the dashboard once your accounts exist.

---

## Optional features

### TURN for voice and video

1. Cloudflare dashboard -> **Calls** -> **TURN** -> create a key.
2. Put the key ID in `vars.TURN_KEY_ID` in `wrangler.local.jsonc`.
3. `npx wrangler secret put TURN_API_TOKEN -c wrangler.local.jsonc`

This covers legacy 1:1 Matrix calls.

### LiveKit for Element Call (MatrixRTC)

Element Call needs a real LiveKit SFU that you host elsewhere. The Cloudflare Realtime SFU is **not** LiveKit-compatible and cannot substitute for it.

```jsonc
"vars": {
  "LIVEKIT_API_KEY": "your-api-key",
  "LIVEKIT_URL": "wss://livekit.example.org"
}
```

```bash
npx wrangler secret put LIVEKIT_API_SECRET -c wrangler.local.jsonc
```

The `vpc_services` binding in the template reaches a LiveKit instance on a private network through Workers VPC. Remove it unless you have that setup.

### Cloudflare Realtime SFU

For the native calling paths that do not go through LiveKit:

```bash
npx wrangler secret put CALLS_APP_ID -c wrangler.local.jsonc
npx wrangler secret put CALLS_APP_SECRET -c wrangler.local.jsonc
```

Create the app at **Cloudflare dashboard** -> **Calls** -> **Realtime**.

### Email verification (3PID)

Requires Cloudflare Email Service on your account. Keep the `send_email` binding and set the sender address:

```bash
npx wrangler secret put EMAIL_FROM -c wrangler.local.jsonc   # e.g. noreply@m.example.org
```

Without the `EMAIL` binding, email verification fails cleanly and the rest of the server is unaffected.

### APNs push (iOS, bypasses Sygnal)

```bash
npx wrangler secret put APNS_KEY_ID -c wrangler.local.jsonc
npx wrangler secret put APNS_TEAM_ID -c wrangler.local.jsonc
npx wrangler secret put APNS_PRIVATE_KEY -c wrangler.local.jsonc   # contents of the .p8 file
npx wrangler secret put APNS_ENVIRONMENT -c wrangler.local.jsonc   # production | sandbox
```

### SSO / upstream OIDC

Providers are configured at runtime in the admin dashboard, not in the config file. Set `OIDC_ENCRYPTION_KEY` first (Step 5). The server also acts as an OIDC provider for clients: it signs `id_token`s with an RS256 key kept in the `CACHE` namespace and publishes it at `/.well-known/jwks.json`.

### Bindings the template does not declare

`ANALYTICS` (Analytics Engine), `BROWSER` (Browser Rendering) and `AI` (Workers AI) are all optional and guarded in code - request analytics, URL previews of JS-rendered pages and AI helpers stay off until you add the bindings yourself.

---

## Updating

```bash
git pull
npm install
npm run typecheck
```

Apply any migration files that are newer than the highest number you have run, then:

```bash
npm run deploy
```

Migrations are additive and expected to run before the code that needs them.

---

## Troubleshooting

**"Workers Paid plan required"** - Durable Objects need the paid plan. Dashboard -> **Workers & Pages** -> **Plans**.

**`no such table: ...` at runtime** - a numbered migration was skipped. Re-run the full list from Step 4; the files are idempotent enough to re-apply (`CREATE TABLE IF NOT EXISTS`, `INSERT OR IGNORE`), except `ALTER TABLE ADD COLUMN`, which errors harmlessly if the column already exists.

**Deploy fails on a resource ID** - you are still on the template values. Confirm `-c wrangler.local.jsonc` is in play and that `account_id`, the D1 ID, the KV IDs and the bucket name are yours.

**Federation tester fails** - check, in order: `https://<SERVER_NAME>/.well-known/matrix/server` returns your API host; `/_matrix/key/v2/server` returns a key with a `valid_until_ts` in the future; the API host is reachable over HTTPS; `SERVER_NAME` matches the name you handed the tester.

**Clients log in but immediately break** - usually `WORKER_DOMAIN` is unset or wrong on an apex-delegated deployment, so discovery hands clients a host that does not serve the API.

**`M_LIMIT_EXCEEDED`** - the per-client sliding window in `RateLimitDurableObject`. Defaults per minute: login 10, register 5, create room 10, media upload 30, search 30, send message 60, general API 100, media download 200, E2EE key ops 500, federation 500. `/sync` is exempt because its own timeout paces it. Limits live in `src/middleware/rate-limit.ts`.

**Registration returns 403** - the admin toggle is off. Flip it in the dashboard, or via the `AdminDurableObject` config endpoint.

**Anything else** - stream logs:

```bash
npx wrangler tail -c wrangler.local.jsonc
```

Observability is enabled in the config, so requests are also queryable from the dashboard.

---

## Reference

### Bindings

| Binding | Type | Purpose |
|---------|------|---------|
| `DB` | D1 | Users, rooms, events, memberships, keys, tokens, server keys |
| `SESSIONS` | KV | Access and refresh tokens, OAuth codes |
| `DEVICE_KEYS`, `ONE_TIME_KEYS`, `CROSS_SIGNING_KEYS` | KV | E2EE key caches |
| `CACHE` | KV | Presence, federation transactions, filters, OIDC signing key |
| `ACCOUNT_DATA` | KV | User account data |
| `MEDIA` | R2 | Uploads, thumbnails, avatars |
| `ROOMS`, `SYNC`, `FEDERATION` | Durable Object | Room coordination, sync waits, federation retry queue |
| `USER_KEYS`, `PUSH`, `ADMIN`, `RATE_LIMIT`, `CALL_ROOMS` | Durable Object | E2EE keys, push queue, admin state, rate limiting, calls |
| `ROOM_JOIN_WORKFLOW`, `PUSH_NOTIFICATION_WORKFLOW` | Workflow | Durable join handshake, push delivery |
| `EMAIL` | Send Email | 3PID verification |

Three further workflow classes exist in `src/workflows/` (federation catch-up, media cleanup, state compaction) but are deliberately unbound and never run. `src/consumers/federation-consumer.ts` likewise waits on a Queues binding that the config does not declare.

### Limits

| Constraint | Limit | Notes |
|------------|-------|-------|
| Worker CPU | 30s | Long operations belong in a Workflow |
| Worker memory | 128MB | Stream large responses |
| Media upload | 50MB | Enforced on the actual body, not the header |
| D1 database | 10GB | Archive old events if you approach it |
| R2 object | 5GB | Chunked upload supported |
| KV value | 25MB | Split large datasets |

### Links

- [Matrix spec v1.17](https://spec.matrix.org/v1.17/)
- [Cloudflare Workers docs](https://developers.cloudflare.com/workers/)
- [Matrix Federation Tester](https://federationtester.matrix.org)
- [Upstream project](https://github.com/nkuntz1934/matrix-workers)
</content>
