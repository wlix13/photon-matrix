// Outbound federation sender: delivers one locally-created PDU per transaction
// (PUT /_matrix/federation/v1/send/{txnId}, spec cap <=50 PDUs) over the signed X-Matrix
// transport to every remote server with a joined member; failed sends go to the
// FederationDurableObject retry queue. Callers use c.executionCtx.waitUntil() so delivery
// never blocks the client response. The PDU is the output of finalizeEvent(): it carries
// `hashes` and `signatures` and, for v3+ rooms, no `event_id` - the receiver derives the ID
// from the reference hash, so re-adding it breaks verification.

import type { Env } from '../types';
import {
  federationGet,
  federationPut,
  getServerSigningKey,
  makeFederationRequest,
  type SigningKey,
} from './federation-keys';
import { finalizeEvent } from '../utils/event-hashing';
import { getRoomVersion } from './room-versions';
import { generateEventId } from '../utils/ids';

// Per-isolate counter so txnIds stay unique when two sends land in the same millisecond.
let txnCounter = 0;
function nextTxnId(): string {
  txnCounter = (txnCounter + 1) % 1_000_000;
  return `${Date.now()}_${txnCounter}`;
}

/** Distinct server-parts of a room's joined members, excluding `excludeServer`. */
export async function getRemoteServersInRoom(
  db: D1Database,
  roomId: string,
  excludeServer: string
): Promise<string[]> {
  const rows = await db
    .prepare(
      `SELECT DISTINCT substr(user_id, instr(user_id, ':') + 1) AS server
       FROM room_memberships
       WHERE room_id = ? AND membership = 'join'`
    )
    .bind(roomId)
    .all<{ server: string }>();

  return rows.results
    .map((r) => r.server)
    .filter((s) => !!s && s !== excludeServer);
}

/**
 * Delivers a finalized wire PDU to each destination, defaulting to the room's remote joined
 * members. A per-destination failure is enqueued for retry and never blocks the others.
 */
export async function sendPduToServers(
  env: Env,
  roomId: string,
  wirePdu: Record<string, unknown>,
  destinations?: string[]
): Promise<void> {
  const targets =
    destinations ?? (await getRemoteServersInRoom(env.DB, roomId, env.SERVER_NAME));
  if (targets.length === 0) return;

  const signingKey = await getServerSigningKey(env.DB);
  if (!signingKey) {
    // Nothing to sign the transport with; queue every destination until a key is configured.
    console.error('[federation-sender] no signing key; enqueuing all destinations for retry', {
      roomId,
      targets,
    });
    await Promise.all(targets.map((d) => enqueueRetry(env, d, roomId, wirePdu)));
    return;
  }

  await Promise.all(
    targets.map((destination) =>
      deliverPdu(env, destination, roomId, wirePdu, signingKey).catch((err) => {
        console.error('[federation-sender] delivery threw', {
          destination,
          roomId,
          error: err instanceof Error ? err.message : String(err),
        });
        return enqueueRetry(env, destination, roomId, wirePdu);
      })
    )
  );
}

// One direct signed delivery; any non-2xx or network error enqueues the PDU for retry.
async function deliverPdu(
  env: Env,
  destination: string,
  roomId: string,
  wirePdu: Record<string, unknown>,
  signingKey: SigningKey
): Promise<void> {
  const txnId = nextTxnId();
  const path = `/_matrix/federation/v1/send/${encodeURIComponent(txnId)}`;
  const body = {
    origin: env.SERVER_NAME,
    origin_server_ts: Date.now(),
    pdus: [wirePdu],
    edus: [],
  };

  try {
    const response = await makeFederationRequest(
      'PUT',
      destination,
      path,
      env.SERVER_NAME,
      signingKey,
      env.CACHE,
      body
    );
    if (!response.ok) {
      const text = await response.text().catch(() => '');
      console.warn('[federation-sender] non-ok, enqueuing retry', {
        destination,
        roomId,
        status: response.status,
        body: text.slice(0, 200),
      });
      await enqueueRetry(env, destination, roomId, wirePdu);
      return;
    }
    // Drain the body so the connection can be reused.
    await response.text().catch(() => {});
  } catch (err) {
    console.warn('[federation-sender] send error, enqueuing retry', {
      destination,
      roomId,
      error: err instanceof Error ? err.message : String(err),
    });
    await enqueueRetry(env, destination, roomId, wirePdu);
  }
}

// Server-part of a Matrix user/room ID (@u:server -> server). '' if malformed.
export function serverNameOf(id: string): string {
  const idx = id.indexOf(':');
  return idx === -1 ? '' : id.slice(idx + 1);
}

// Leaves a room we hold no state for - a rejected invite, or a knock we never completed.
// Without local state there are no auth_events to build a leave PDU from, so the resident
// server hands us a template (make_leave) that we sign and hand back (send_leave).
export async function leaveRemoteRoom(
  env: Env,
  roomId: string,
  userId: string,
  servers: string[]
): Promise<boolean> {
  const signingKey = await getServerSigningKey(env.DB);
  if (!signingKey) {
    console.error('[federation-sender] cannot leave remote room without a signing key', { roomId });
    return false;
  }

  for (const server of servers) {
    try {
      const template = await makeLeave(env, server, roomId, userId);
      if (!template) continue;

      const { event, eventId } = await finalizeLeaveEvent(env, template, roomId, userId, signingKey);
      if (await sendLeave(env, server, roomId, eventId, event, template.roomVersion)) {
        console.log('[federation-sender] left remote room', { roomId, userId, server });
        return true;
      }
    } catch (err) {
      console.warn('[federation-sender] leave via server failed', {
        roomId,
        server,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  return false;
}

async function makeLeave(
  env: Env,
  server: string,
  roomId: string,
  userId: string
): Promise<{ roomVersion: string; event: Record<string, any> } | null> {
  const path =
    `/_matrix/federation/v1/make_leave/${encodeURIComponent(roomId)}/${encodeURIComponent(userId)}`;
  const response = await federationGet(server, path, env.SERVER_NAME, env.DB, env.CACHE);

  if (!response.ok) {
    console.warn('[federation-sender] make_leave rejected', {
      roomId,
      server,
      status: response.status,
    });
    return null;
  }

  const body = (await response.json()) as { room_version?: string; event?: Record<string, any> };
  if (!body.event) return null;

  return { roomVersion: body.room_version ?? '1', event: body.event };
}

async function finalizeLeaveEvent(
  env: Env,
  template: { roomVersion: string; event: Record<string, any> },
  roomId: string,
  userId: string,
  signingKey: SigningKey
): Promise<{ event: Record<string, any>; eventId: string }> {
  const { roomVersion, event: templateEvent } = template;

  const unsignedEvent: Record<string, any> = {
    type: 'm.room.member',
    room_id: roomId,
    sender: userId,
    state_key: userId,
    content: { membership: 'leave' },
    origin_server_ts: Date.now(),
    depth: typeof templateEvent.depth === 'number' ? templateEvent.depth : 0,
    auth_events: Array.isArray(templateEvent.auth_events) ? templateEvent.auth_events : [],
    prev_events: Array.isArray(templateEvent.prev_events) ? templateEvent.prev_events : [],
  };

  // v1/v2 rooms carry a server-generated event ID and the deprecated `origin` field on the wire.
  const legacyIds = getRoomVersion(roomVersion)?.eventIdFormat === 'v1';
  if (legacyIds) {
    unsignedEvent.origin = env.SERVER_NAME;
    unsignedEvent.event_id =
      typeof templateEvent.event_id === 'string'
        ? templateEvent.event_id
        : await generateEventId(env.SERVER_NAME);
  }

  const { event, eventId } = await finalizeEvent(
    unsignedEvent,
    roomVersion,
    env.SERVER_NAME,
    signingKey.keyId,
    signingKey.privateKeyJwk
  );

  if (legacyIds) {
    event.event_id = eventId;
  }

  return { event, eventId };
}

async function sendLeave(
  env: Env,
  server: string,
  roomId: string,
  eventId: string,
  event: Record<string, any>,
  roomVersion: string
): Promise<boolean> {
  const suffix = `${encodeURIComponent(roomId)}/${encodeURIComponent(eventId)}`;

  const v2 = await federationPut(
    server, `/_matrix/federation/v2/send_leave/${suffix}`, event, env.SERVER_NAME, env.DB, env.CACHE
  );
  if (v2.ok) return true;

  const v2Text = await v2.text();
  if (v2.status !== 404 && !v2Text.includes('M_UNRECOGNIZED')) {
    console.warn('[federation-sender] send_leave (v2) rejected', {
      roomId,
      server,
      roomVersion,
      status: v2.status,
      body: v2Text.slice(0, 200),
    });
    return false;
  }

  const v1 = await federationPut(
    server, `/_matrix/federation/v1/send_leave/${suffix}`, event, env.SERVER_NAME, env.DB, env.CACHE
  );
  if (v1.ok) return true;

  console.warn('[federation-sender] send_leave (v1) rejected', {
    roomId,
    server,
    status: v1.status,
  });
  return false;
}

// Queues an EDU (m.direct_to_device, m.typing, m.receipt, ...) into the per-destination
// FederationDurableObject, which signs and delivers it with retry. Fire-and-forget.
export async function sendEduToServer(
  env: Env,
  destination: string,
  eduType: string,
  content: Record<string, unknown>
): Promise<void> {
  try {
    const fedDO = env.FEDERATION.get(env.FEDERATION.idFromName(destination));
    await fedDO.fetch(
      new Request('http://internal/send-edu', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ destination, edu_type: eduType, content }),
      })
    );
  } catch (err) {
    console.error('[federation-sender] failed to enqueue EDU', {
      destination,
      eduType,
      error: err instanceof Error ? err.message : String(err),
    });
  }
}

// Hands a failed PDU to the per-destination FederationDurableObject, which owns the backoff schedule.
async function enqueueRetry(
  env: Env,
  destination: string,
  roomId: string,
  wirePdu: Record<string, unknown>
): Promise<void> {
  try {
    const fedDO = env.FEDERATION.get(env.FEDERATION.idFromName(destination));
    await fedDO.fetch(
      new Request('http://internal/send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ destination, roomId, wirePdu }),
      })
    );
  } catch (err) {
    console.error('[federation-sender] failed to enqueue retry', {
      destination,
      roomId,
      error: err instanceof Error ? err.message : String(err),
    });
  }
}
