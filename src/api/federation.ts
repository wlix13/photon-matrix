// Matrix Server-Server (Federation) API endpoints

import { Hono } from 'hono';
import type { Context } from 'hono';
import type { AppEnv, PDU, Env, Membership } from '../types';
import { Errors } from '../utils/errors';
import { generateSigningKeyPair, signJson, sha256, verifySignature, verifyContentHash } from '../utils/crypto';
import { requireFederationAuth } from '../middleware/federation-auth';
import {
  getRemoteKeysWithNotarySignature,
  verifyRemoteSignature,
  type ServerKeyResponse,
} from '../services/federation-keys';
import { sendPduToServers, getRemoteServersInRoom } from '../services/federation-sender';
import { buildMultipartMediaResponse } from '../services/media-federation';
import { validateUrl } from '../utils/url-validator';
import { checkEventAuth } from '../services/event-auth';
import {
  getRoomState,
  storeEvent,
  updateMembership,
  notifyUsersOfEvent,
} from '../services/database';
import { resolveState } from '../services/state-resolution';
import { computeEventId, signEventForFederation, redactEvent } from '../utils/event-hashing';
import { getRoomVersion } from '../services/room-versions';
import { wakeSyncForUsers } from './to-device';

// Supported room versions (v1-v12 per Matrix Spec v1.17)
const SUPPORTED_ROOM_VERSIONS = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12'];

const VALID_MEMBERSHIPS: ReadonlySet<string> = new Set([
  'join', 'invite', 'leave', 'ban', 'knock',
]);

const app = new Hono<AppEnv>();

// Converts a DB event row to a federation wire event: v3+ rooms omit event_id
// (the receiver derives it from the reference hash) and carry hashes + signatures.
function eventRowToWire(row: any, legacyIds: boolean): Record<string, unknown> {
  const ev: Record<string, unknown> = {
    room_id: row.room_id,
    sender: row.sender,
    type: row.event_type,
    content: JSON.parse(row.content || '{}'),
    origin_server_ts: row.origin_server_ts,
    depth: row.depth,
    auth_events: JSON.parse(row.auth_events || '[]'),
    prev_events: JSON.parse(row.prev_events || '[]'),
  };
  if (row.state_key !== null && row.state_key !== undefined) {
    ev.state_key = row.state_key;
  }
  if (row.hashes) ev.hashes = JSON.parse(row.hashes);
  if (row.signatures) ev.signatures = JSON.parse(row.signatures);
  if (legacyIds) ev.event_id = row.event_id;
  return ev;
}

// Whether a room's version uses server-generated (v1/v2) event IDs that appear
// on the wire, vs computed reference-hash IDs (v3+) that must be omitted.
async function roomUsesLegacyIds(db: D1Database, roomId: string): Promise<boolean> {
  const room = await db.prepare(
    `SELECT room_version FROM rooms WHERE room_id = ?`
  ).bind(roomId).first<{ room_version: string }>();
  return getRoomVersion(room?.room_version ?? '10')?.eventIdFormat === 'v1';
}

// Atomically allocate the next 'to_device' stream position. Mirrors the helper
// in api/to-device.ts (which is not exported) so inbound m.direct_to_device
// EDUs share the same ordering stream local clients read from.
async function getNextToDeviceStreamPosition(db: D1Database): Promise<number> {
  const result = await db.prepare(`
    UPDATE stream_positions SET position = position + 1
    WHERE stream_name = 'to_device' RETURNING position
  `).first<{ position: number }>();
  if (result) return result.position;

  const upsert = await db.prepare(`
    INSERT INTO stream_positions (stream_name, position) VALUES ('to_device', 1)
    ON CONFLICT (stream_name) DO UPDATE SET position = position + 1
    RETURNING position
  `).first<{ position: number }>();
  return upsert?.position ?? 1;
}

// Persists an inbound m.direct_to_device EDU for local recipients, fanning '*'
// out to every device of a user and deduplicating by message_id.
async function storeInboundToDevice(
  env: Env,
  origin: string,
  content: any,
  recipientsToWake: Set<string>
): Promise<void> {
  const sender: string = content?.sender || origin;
  const messageType: string | undefined = content?.type;
  const messageId: string | undefined = content?.message_id;
  const messages = content?.messages;
  if (!messageType || !messageId || !messages || typeof messages !== 'object') {
    return;
  }

  for (const [recipientUserId, deviceMap] of Object.entries(messages)) {
    if (recipientUserId.split(':')[1] !== env.SERVER_NAME) continue;
    if (!deviceMap || typeof deviceMap !== 'object') continue;

    let delivered = false;
    for (const [deviceId, msgContent] of Object.entries(deviceMap as Record<string, any>)) {
      let targetDevices: string[];
      if (deviceId === '*') {
        const rows = await env.DB.prepare(
          `SELECT device_id FROM devices WHERE user_id = ?`
        ).bind(recipientUserId).all<{ device_id: string }>();
        targetDevices = rows.results.map((r) => r.device_id);
      } else {
        targetDevices = [deviceId];
      }

      for (const targetDeviceId of targetDevices) {
        const streamPosition = await getNextToDeviceStreamPosition(env.DB);
        await env.DB.prepare(`
          INSERT INTO to_device_messages (
            recipient_user_id, recipient_device_id, sender_user_id,
            event_type, content, message_id, stream_position
          ) VALUES (?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT (recipient_user_id, recipient_device_id, message_id) DO NOTHING
        `).bind(
          recipientUserId,
          targetDeviceId,
          sender,
          messageType,
          JSON.stringify(msgContent),
          messageId,
          streamPosition
        ).run();
        delivered = true;
      }
    }
    if (delivered) recipientsToWake.add(recipientUserId);
  }
}

// Persists an inbound invite as a membership row plus stripped state on
// room_memberships.invite_state, which the /sync invite builders read.
async function storeInboundInvite(
  db: D1Database,
  roomId: string,
  roomVersion: string,
  invitedUserId: string,
  eventId: string,
  inviteEvent: any,
  inviteRoomState: any[] | undefined
): Promise<void> {
  // room_memberships has a FOREIGN KEY to rooms(room_id), so the remote room
  // needs a stub row first. The join path inserts its rooms row with INSERT OR
  // IGNORE, so this stub's room_version survives and has to be correct.
  await db.prepare(
    `INSERT OR IGNORE INTO rooms (room_id, room_version, is_public) VALUES (?, ?, 0)`
  ).bind(roomId, roomVersion).run();

  // Build stripped state keyed by (type, state_key) so duplicates collapse and
  // the invite membership event always wins over any copy in invite_room_state.
  const stripped = new Map<string, any>();
  for (const e of inviteRoomState || []) {
    if (!e || typeof e.type !== 'string') continue;
    const stateKey = typeof e.state_key === 'string' ? e.state_key : '';
    stripped.set(`${e.type}|${stateKey}`, {
      type: e.type,
      state_key: stateKey,
      content: e.content ?? {},
      sender: e.sender,
    });
  }
  // Element needs the invitee's own m.room.member to render "you were invited".
  stripped.set(`m.room.member|${invitedUserId}`, {
    type: 'm.room.member',
    state_key: invitedUserId,
    content: inviteEvent?.content ?? { membership: 'invite' },
    sender: inviteEvent?.sender,
  });

  const inviteStateJson = JSON.stringify([...stripped.values()]);

  // updateMembership does INSERT OR REPLACE and does not carry invite_state, so
  // the UPDATE has to run after it.
  await updateMembership(db, roomId, invitedUserId, 'invite', eventId);
  await db.prepare(
    `UPDATE room_memberships SET invite_state = ? WHERE room_id = ? AND user_id = ?`
  ).bind(inviteStateJson, roomId, invitedUserId).run();
}

// GET /_matrix/federation/v1/version - Server version info, declared before the
// auth middleware below so it stays unauthenticated
app.get('/_matrix/federation/v1/version', async (c) => {
  return c.json({
    server: {
      name: 'matrix-worker',
      version: c.env.SERVER_VERSION || '0.1.0',
    },
  });
});

// /_matrix/key/* stays unauthenticated: it is how servers establish trust
app.use('/_matrix/federation/v1/*', requireFederationAuth());
// v2 endpoints (invite, send_join, send_leave) need the same authentication
app.use('/_matrix/federation/v2/*', requireFederationAuth());

// GET /_matrix/key/v2/server - Get server signing keys
app.get('/_matrix/key/v2/server', async (c) => {
  const serverName = c.env.SERVER_NAME;

  let keys = await c.env.DB.prepare(
    `SELECT key_id, public_key, private_key_jwk, key_version, valid_from, valid_until
     FROM server_keys WHERE is_current = 1 ORDER BY key_version DESC`
  ).all<{
    key_id: string;
    public_key: string;
    private_key_jwk: string | null;
    key_version: number | null;
    valid_from: number;
    valid_until: number | null;
  }>();

  const hasSecureKey = keys.results.some((k) => k.key_version === 2 && k.private_key_jwk);

  if (keys.results.length === 0 || !hasSecureKey) {
    const keyPair = await generateSigningKeyPair();
    const validFrom = Date.now();
    const validUntil = validFrom + 365 * 24 * 60 * 60 * 1000; // 1 year

    // INSERT OR IGNORE so that if two Workers race here only one key wins;
    // otherwise both could survive as is_current = 1.
    await c.env.DB.batch([
      c.env.DB.prepare(`UPDATE server_keys SET is_current = 0 WHERE key_version = 2`),
      c.env.DB.prepare(
        `INSERT OR IGNORE INTO server_keys (key_id, public_key, private_key, private_key_jwk, key_version, valid_from, valid_until, is_current)
         VALUES (?, ?, ?, ?, 2, ?, ?, 1)`
      ).bind(
        keyPair.keyId,
        keyPair.publicKey,
        JSON.stringify(keyPair.privateKeyJwk),
        JSON.stringify(keyPair.privateKeyJwk),
        validFrom,
        validUntil
      ),
    ]);

    keys = {
      results: [
        {
          key_id: keyPair.keyId,
          public_key: keyPair.publicKey,
          private_key_jwk: JSON.stringify(keyPair.privateKeyJwk),
          key_version: 2,
          valid_from: validFrom,
          valid_until: validUntil,
        },
      ],
      success: true,
      meta: {
        duration: 0,
        size_after: 0,
        rows_read: 0,
        rows_written: 0,
        last_row_id: 0,
        changed_db: false,
        changes: 0,
      },
    };
  }

  const verifyKeys: Record<string, { key: string }> = {};
  for (const key of keys.results) {
    verifyKeys[key.key_id] = { key: key.public_key };
  }

  const validUntilTs = keys.results[0]?.valid_until || Date.now() + 365 * 24 * 60 * 60 * 1000;

  const response = {
    server_name: serverName,
    valid_until_ts: validUntilTs,
    verify_keys: verifyKeys,
    old_verify_keys: {},
  };

  const currentKey = keys.results.find((k) => k.key_version === 2 && k.private_key_jwk);
  if (currentKey && currentKey.private_key_jwk) {
    const signed = await signJson(
      response,
      serverName,
      currentKey.key_id,
      JSON.parse(currentKey.private_key_jwk)
    );
    return c.json(signed);
  }

  return c.json(response);
});

// GET /_matrix/key/v2/server/:keyId - Get specific key
app.get('/_matrix/key/v2/server/:keyId', async (c) => {
  const keyId = c.req.param('keyId');
  const serverName = c.env.SERVER_NAME;

  const key = await c.env.DB.prepare(
    `SELECT key_id, public_key, valid_from, valid_until FROM server_keys WHERE key_id = ?`
  ).bind(keyId).first<{ key_id: string; public_key: string; valid_from: number; valid_until: number | null }>();

  if (!key) {
    return Errors.notFound('Key not found').toResponse();
  }

  const response: Record<string, unknown> = {
    server_name: serverName,
    valid_until_ts: key.valid_until || (Date.now() + 365 * 24 * 60 * 60 * 1000),
    verify_keys: {
      [key.key_id]: { key: key.public_key },
    },
    old_verify_keys: {},
  };

  // Signed per spec, like the main key endpoint
  const signingKey = await getNotarySigningKey(c.env.DB);
  if (signingKey) {
    const signed = await signJson(response, serverName, signingKey.keyId, signingKey.privateKeyJwk);
    return c.json(signed);
  }

  return c.json(response);
});

// This server's current notary signing key, or null if none is configured.
async function getNotarySigningKey(db: D1Database): Promise<{
  keyId: string;
  privateKeyJwk: JsonWebKey;
} | null> {
  const key = await db.prepare(
    `SELECT key_id, private_key_jwk FROM server_keys WHERE is_current = 1 AND key_version = 2`
  ).first<{ key_id: string; private_key_jwk: string | null }>();

  if (!key || !key.private_key_jwk) {
    return null;
  }

  return {
    keyId: key.key_id,
    privateKeyJwk: JSON.parse(key.private_key_jwk),
  };
}

// Whether a server name is a hostname[:port] we can safely send requests to.
function isValidServerName(serverName: string): boolean {
  if (!serverName || serverName.length > 255) {
    return false;
  }

  // validateUrl only takes URLs, so wrap the name in one to reuse its SSRF checks
  const testUrl = `https://${serverName}/`;
  const validation = validateUrl(testUrl);

  return validation.valid;
}

const MAX_BATCH_SERVERS = 100;

// POST /_matrix/key/v2/query - Batch query for server keys (notary endpoint)
app.post('/_matrix/key/v2/query', async (c) => {
  let body: {
    server_keys?: Record<string, Record<string, { minimum_valid_until_ts?: number }>>;
  };

  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const serverKeys = body.server_keys;
  if (!serverKeys || typeof serverKeys !== 'object') {
    return Errors.missingParam('server_keys').toResponse();
  }

  const serverCount = Object.keys(serverKeys).length;
  if (serverCount > MAX_BATCH_SERVERS) {
    return c.json(
      {
        errcode: 'M_LIMIT_EXCEEDED',
        error: `Too many servers in batch request (max ${MAX_BATCH_SERVERS})`,
      },
      400
    );
  }

  const notaryKey = await getNotarySigningKey(c.env.DB);
  if (!notaryKey) {
    return c.json(
      {
        errcode: 'M_UNKNOWN',
        error: 'Server signing key not configured',
      },
      500
    );
  }

  const results: ServerKeyResponse[] = [];

  for (const [serverName, keyRequests] of Object.entries(serverKeys)) {
    // Validate server name to prevent SSRF
    if (!isValidServerName(serverName)) {
      console.warn(`Invalid server name in key query: ${serverName}`);
      continue;
    }

    if (serverName === c.env.SERVER_NAME) {
      const ownKeys = await c.env.DB.prepare(
        `SELECT key_id, public_key, valid_until FROM server_keys WHERE is_current = 1`
      ).all<{ key_id: string; public_key: string; valid_until: number | null }>();

      if (ownKeys.results.length > 0) {
        const verifyKeys: Record<string, { key: string }> = {};
        let maxValidUntil = 0;

        for (const key of ownKeys.results) {
          verifyKeys[key.key_id] = { key: key.public_key };
          if (key.valid_until && key.valid_until > maxValidUntil) {
            maxValidUntil = key.valid_until;
          }
        }

        const ownResponse: ServerKeyResponse = {
          server_name: serverName,
          valid_until_ts: maxValidUntil || Date.now() + 365 * 24 * 60 * 60 * 1000,
          verify_keys: verifyKeys,
          old_verify_keys: {},
        };

        const signed = (await signJson(
          ownResponse,
          c.env.SERVER_NAME,
          notaryKey.keyId,
          notaryKey.privateKeyJwk
        )) as ServerKeyResponse;

        results.push(signed);
      }
      continue;
    }

    for (const [keyId, keyRequest] of Object.entries(keyRequests)) {
      const minimumValidUntilTs = keyRequest.minimum_valid_until_ts || 0;

      const keyResponses = await getRemoteKeysWithNotarySignature(
        serverName,
        keyId === '' ? null : keyId, // Empty key ID means all keys
        minimumValidUntilTs,
        c.env.DB,
        c.env.CACHE,
        c.env.SERVER_NAME,
        notaryKey.keyId,
        notaryKey.privateKeyJwk
      );

      results.push(...keyResponses);
    }
  }

  return c.json({ server_keys: results });
});

// GET /_matrix/key/v2/query/:serverName - Query all keys for a server
app.get('/_matrix/key/v2/query/:serverName', async (c) => {
  const serverName = c.req.param('serverName');
  const minimumValidUntilTs = parseInt(c.req.query('minimum_valid_until_ts') || '0', 10);

  // Validate server name to prevent SSRF
  if (!isValidServerName(serverName)) {
    return c.json(
      {
        errcode: 'M_INVALID_PARAM',
        error: 'Invalid server name',
      },
      400
    );
  }

  const notaryKey = await getNotarySigningKey(c.env.DB);
  if (!notaryKey) {
    return c.json(
      {
        errcode: 'M_UNKNOWN',
        error: 'Server signing key not configured',
      },
      500
    );
  }

  if (serverName === c.env.SERVER_NAME) {
    const ownKeys = await c.env.DB.prepare(
      `SELECT key_id, public_key, valid_until FROM server_keys WHERE is_current = 1`
    ).all<{ key_id: string; public_key: string; valid_until: number | null }>();

    if (ownKeys.results.length === 0) {
      return Errors.notFound('No keys found').toResponse();
    }

    const verifyKeys: Record<string, { key: string }> = {};
    let maxValidUntil = 0;

    for (const key of ownKeys.results) {
      verifyKeys[key.key_id] = { key: key.public_key };
      if (key.valid_until && key.valid_until > maxValidUntil) {
        maxValidUntil = key.valid_until;
      }
    }

    const ownResponse: ServerKeyResponse = {
      server_name: serverName,
      valid_until_ts: maxValidUntil || Date.now() + 365 * 24 * 60 * 60 * 1000,
      verify_keys: verifyKeys,
      old_verify_keys: {},
    };

    const signed = (await signJson(
      ownResponse,
      c.env.SERVER_NAME,
      notaryKey.keyId,
      notaryKey.privateKeyJwk
    )) as ServerKeyResponse;

    return c.json({ server_keys: [signed] });
  }

  const keyResponses = await getRemoteKeysWithNotarySignature(
    serverName,
    null, // All keys
    minimumValidUntilTs,
    c.env.DB,
    c.env.CACHE,
    c.env.SERVER_NAME,
    notaryKey.keyId,
    notaryKey.privateKeyJwk
  );

  if (keyResponses.length === 0) {
    return Errors.notFound('No keys found for server').toResponse();
  }

  return c.json({ server_keys: keyResponses });
});

// GET /_matrix/key/v2/query/:serverName/:keyId - Query specific key for a server
app.get('/_matrix/key/v2/query/:serverName/:keyId', async (c) => {
  const serverName = c.req.param('serverName');
  const keyId = c.req.param('keyId');
  const minimumValidUntilTs = parseInt(c.req.query('minimum_valid_until_ts') || '0', 10);

  // Validate server name to prevent SSRF
  if (!isValidServerName(serverName)) {
    return c.json(
      {
        errcode: 'M_INVALID_PARAM',
        error: 'Invalid server name',
      },
      400
    );
  }

  const notaryKey = await getNotarySigningKey(c.env.DB);
  if (!notaryKey) {
    return c.json(
      {
        errcode: 'M_UNKNOWN',
        error: 'Server signing key not configured',
      },
      500
    );
  }

  if (serverName === c.env.SERVER_NAME) {
    const ownKey = await c.env.DB.prepare(
      `SELECT key_id, public_key, valid_until FROM server_keys WHERE key_id = ?`
    ).bind(keyId).first<{ key_id: string; public_key: string; valid_until: number | null }>();

    if (!ownKey) {
      return Errors.notFound('Key not found').toResponse();
    }

    const ownResponse: ServerKeyResponse = {
      server_name: serverName,
      valid_until_ts: ownKey.valid_until || Date.now() + 365 * 24 * 60 * 60 * 1000,
      verify_keys: {
        [ownKey.key_id]: { key: ownKey.public_key },
      },
      old_verify_keys: {},
    };

    const signed = (await signJson(
      ownResponse,
      c.env.SERVER_NAME,
      notaryKey.keyId,
      notaryKey.privateKeyJwk
    )) as ServerKeyResponse;

    return c.json({ server_keys: [signed] });
  }

  const keyResponses = await getRemoteKeysWithNotarySignature(
    serverName,
    keyId,
    minimumValidUntilTs,
    c.env.DB,
    c.env.CACHE,
    c.env.SERVER_NAME,
    notaryKey.keyId,
    notaryKey.privateKeyJwk
  );

  if (keyResponses.length === 0) {
    return Errors.notFound('Key not found').toResponse();
  }

  return c.json({ server_keys: keyResponses });
});

// PUT /_matrix/federation/v1/send/:txnId - Receive events from remote server
app.put('/_matrix/federation/v1/send/:txnId', async (c) => {
  const txnId = c.req.param('txnId');

  const origin = c.get('federationOrigin' as any) as string | undefined;
  if (!origin) {
    return Errors.unauthorized('Federation authentication required').toResponse();
  }

  const existingTxn = await c.env.DB.prepare(
    `SELECT response FROM federation_transactions WHERE origin = ? AND txn_id = ?`
  ).bind(origin, txnId).first<{ response: string | null }>();

  if (existingTxn?.response) {
    return c.json(JSON.parse(existingTxn.response));
  }

  let body: any;
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const { pdus, edus } = body;
  const pduResults: Record<string, any> = {};

  // Process incoming PDUs (Persistent Data Units - events)
  for (const pdu of pdus || []) {
    try {
      const roomId = pdu.room_id;

      // event_id is deliberately not required: v3+ PDUs arrive without one since
      // it is the reference hash of the redacted event, computed below.
      if (!roomId || !pdu.sender || !pdu.type || pdu.content === undefined || pdu.content === null) {
        pduResults[pdu.event_id || 'unknown'] = {
          error: 'Invalid PDU structure',
        };
        continue;
      }

      const pduOrigin = (pdu.sender as string).split(':')[1];
      if (!pduOrigin) {
        pduResults[pdu.event_id || 'unknown'] = { error: 'Invalid sender format' };
        continue;
      }

      // The room version drives event_id computation for v3+ and the auth check.
      const room = await c.env.DB.prepare(
        `SELECT room_id, room_version FROM rooms WHERE room_id = ?`
      ).bind(roomId).first<{ room_id: string; room_version: string }>();

      // Unknown room: acknowledge without storing. Federation only sends events
      // for rooms we are in, so this is out-of-order delivery or a room we left.
      if (!room) {
        pduResults[pdu.event_id || 'unknown'] = {};
        continue;
      }

      // Canonical event_id: v1/v2 carry it on the wire, but for v3+ it is the
      // reference hash, so compute it here and reject a PDU claiming a different
      // id rather than trusting the sender.
      let eventId: string;
      const idFormat = getRoomVersion(room.room_version)?.eventIdFormat ?? 'v4';
      if (idFormat === 'v1') {
        if (!pdu.event_id) {
          pduResults['unknown'] = { error: 'Missing event_id for v1/v2 room' };
          continue;
        }
        eventId = pdu.event_id;
      } else {
        let computedId: string;
        try {
          computedId = await computeEventId(pdu, room.room_version);
        } catch (idErr: any) {
          pduResults[pdu.event_id || 'unknown'] = {
            error: `Failed to compute event id: ${idErr?.message || 'unknown error'}`,
          };
          continue;
        }
        if (pdu.event_id && pdu.event_id !== computedId) {
          pduResults[pdu.event_id] = { error: 'Event ID does not match computed reference hash' };
          await c.env.DB.prepare(
            `INSERT OR REPLACE INTO processed_pdus (event_id, origin, room_id, processed_at, accepted, rejection_reason)
             VALUES (?, ?, ?, ?, 0, ?)`
          ).bind(pdu.event_id, pduOrigin, roomId, Date.now(), 'Event ID mismatch').run();
          continue;
        }
        eventId = computedId;
      }

      const existingPdu = await c.env.DB.prepare(
        `SELECT accepted, rejection_reason FROM processed_pdus WHERE event_id = ?`
      ).bind(eventId).first<{ accepted: number; rejection_reason: string | null }>();

      if (existingPdu) {
        if (existingPdu.accepted) {
          pduResults[eventId] = {};
        } else {
          pduResults[eventId] = {
            error: existingPdu.rejection_reason || 'Previously rejected',
          };
        }
        continue;
      }

      // Signatures are computed over the redacted event; every PDU must carry one.
      let signatureValid = false;
      if (pdu.signatures) {
        const redactedForVerify = redactEvent(pdu, room.room_version);
        // v3+ wire events carry no event_id, so keep a locally attached one out
        // of the verified material.
        delete redactedForVerify.event_id;
        const signatories = Object.keys(pdu.signatures);

        for (const signatory of signatories) {
          const keyIds = Object.keys(pdu.signatures[signatory]);
          for (const keyId of keyIds) {
            try {
              const isValid = await verifyRemoteSignature(
                redactedForVerify,
                signatory,
                keyId,
                c.env.DB,
                c.env.CACHE
              );
              if (isValid) {
                signatureValid = true;
                break;
              }
            } catch (e) {
              console.warn(`Signature verification failed for ${signatory}:${keyId}:`, e);
            }
          }
          if (signatureValid) break;
        }
      }

      if (!signatureValid) {
        pduResults[eventId] = { error: 'Invalid signature' };
        await c.env.DB.prepare(
          `INSERT OR REPLACE INTO processed_pdus (event_id, origin, room_id, processed_at, accepted, rejection_reason)
           VALUES (?, ?, ?, ?, 0, ?)`
        ).bind(eventId, pduOrigin, roomId, Date.now(), 'Invalid signature').run();
        continue;
      }

      if (pdu.hashes?.sha256) {
        try {
          const hashValid = await verifyContentHash(pdu as Record<string, unknown>, pdu.hashes.sha256);
          if (!hashValid) {
            pduResults[eventId] = { error: 'Content hash mismatch' };
            await c.env.DB.prepare(
              `INSERT OR REPLACE INTO processed_pdus (event_id, origin, room_id, processed_at, accepted, rejection_reason)
               VALUES (?, ?, ?, ?, 0, ?)`
            ).bind(eventId, pduOrigin, roomId, Date.now(), 'Content hash mismatch').run();
            continue;
          }
        } catch (hashErr) {
          console.warn(`[federation] Content hash check failed for ${eventId}:`, hashErr);
        }
      }

      try {
        const roomState = await getRoomState(c.env.DB, roomId);
        const authResult = checkEventAuth(pdu as PDU, roomState, room.room_version);
        if (!authResult.allowed) {
          pduResults[eventId] = { error: authResult.error || 'Event authorization failed' };
          await c.env.DB.prepare(
            `INSERT OR REPLACE INTO processed_pdus (event_id, origin, room_id, processed_at, accepted, rejection_reason)
             VALUES (?, ?, ?, ?, 0, ?)`
          ).bind(eventId, pduOrigin, roomId, Date.now(), authResult.error || 'Auth failed').run();
          continue;
        }
      } catch (authErr) {
        console.warn(`[federation] Auth check failed for ${eventId}, accepting anyway:`, authErr);
      }

      pduResults[eventId] = {};

      await c.env.DB.prepare(
        `INSERT OR REPLACE INTO processed_pdus (event_id, origin, room_id, processed_at, accepted, rejection_reason)
         VALUES (?, ?, ?, ?, 1, NULL)`
      ).bind(eventId, pduOrigin, roomId, Date.now()).run();

      // storeEvent assigns the stream_ordering that incremental /sync needs, and
      // updates current room state for state events.
      try {
        const storedEvent: PDU = {
          event_id: eventId,
          room_id: roomId,
          sender: pdu.sender,
          type: pdu.type,
          state_key: pdu.state_key,
          content: pdu.content,
          origin_server_ts: pdu.origin_server_ts,
          unsigned: pdu.unsigned,
          depth: pdu.depth || 0,
          auth_events: pdu.auth_events || [],
          prev_events: pdu.prev_events || [],
          hashes: pdu.hashes,
          signatures: pdu.signatures,
        };
        await storeEvent(c.env.DB, storedEvent);

        if (pdu.state_key !== undefined) {
          // If the PDU forks history (multiple prev_events), run state resolution
          // to pick the authoritative event for each (type, state_key). storeEvent
          // has already applied a last-writer-wins update, which this overrides.
          const prevEvents = pdu.prev_events || [];
          if (prevEvents.length > 1) {
            try {
              const currentState = await getRoomState(c.env.DB, roomId);
              const resolved = resolveState(room.room_version, [currentState, [storedEvent]]);
              for (const stateEvent of resolved) {
                if (stateEvent.state_key !== undefined) {
                  await c.env.DB.prepare(
                    `INSERT OR REPLACE INTO room_state (room_id, event_type, state_key, event_id)
                     VALUES (?, ?, ?, ?)`
                  ).bind(roomId, stateEvent.type, stateEvent.state_key, stateEvent.event_id).run();
                }
              }
            } catch (resolveErr) {
              console.warn(`[federation] State resolution failed for ${eventId}, keeping last-writer state:`, resolveErr);
            }
          }

          // Mirror remote membership changes so local queries and sync see them.
          if (pdu.type === 'm.room.member' && typeof pdu.state_key === 'string') {
            const membership = pdu.content?.membership;
            if (typeof membership === 'string' && VALID_MEMBERSHIPS.has(membership)) {
              await updateMembership(
                c.env.DB,
                roomId,
                pdu.state_key,
                membership as Membership,
                eventId,
                pdu.content?.displayname,
                pdu.content?.avatar_url
              );
            }
          }
        }

        // Wake local /sync long-polls after responding: the transaction ack gates
        // how fast the origin sends the next one, so don't block it on DO fan-out.
        c.executionCtx.waitUntil(notifyUsersOfEvent(c.env, roomId, eventId, pdu.type));
      } catch (e) {
        console.error(`Failed to store event ${eventId}:`, e);
      }
    } catch (e: any) {
      const eventId = pdu?.event_id || 'unknown';
      pduResults[eventId] = {
        error: e.message || 'Unknown error',
      };

      if (pdu?.event_id && pdu?.room_id) {
        const pduOrigin = (pdu.sender as string)?.split(':')[1] || origin;
        await c.env.DB.prepare(
          `INSERT OR REPLACE INTO processed_pdus (event_id, origin, room_id, processed_at, accepted, rejection_reason)
           VALUES (?, ?, ?, ?, 0, ?)`
        ).bind(pdu.event_id, pduOrigin, pdu.room_id, Date.now(), e.message || 'Unknown error').run();
      }
    }
  }

  // Process EDUs (Ephemeral Data Units)
  const toDeviceRecipients = new Set<string>();
  for (const edu of edus || []) {
    try {
      const eduType = edu.edu_type;
      const content = edu.content;

      switch (eduType) {
        case 'm.typing':
          // Inbound typing notifications are not applied yet
          break;

        case 'm.presence': {
          const presencePush = content?.push as Array<{
            user_id: string;
            presence: string;
            status_msg?: string;
            last_active_ago?: number;
            currently_active?: boolean;
          }> | undefined;
          if (presencePush) {
            for (const update of presencePush) {
              if (update.user_id && update.presence) {
                const now = Date.now();
                const lastActive = update.last_active_ago
                  ? now - update.last_active_ago
                  : now;
                try {
                  await c.env.DB.prepare(`
                    INSERT INTO presence (user_id, presence, status_msg, last_active_ts, currently_active)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (user_id) DO UPDATE SET
                      presence = excluded.presence,
                      status_msg = excluded.status_msg,
                      last_active_ts = excluded.last_active_ts,
                      currently_active = excluded.currently_active
                  `).bind(
                    update.user_id,
                    update.presence,
                    update.status_msg || null,
                    lastActive,
                    update.currently_active ? 1 : 0
                  ).run();
                } catch (presErr) {
                  console.warn(`[federation] Failed to update presence for ${update.user_id}:`, presErr);
                }
              }
            }
          }
          break;
        }

        case 'm.device_list_update': {
          const deviceUserId = content?.user_id as string;
          const deviceId = content?.device_id as string;
          const streamId = (content?.stream_id as number) || 0;
          const deviceKeys = content?.keys;
          const deviceDisplayName = content?.device_display_name as string | undefined;
          const deleted = content?.deleted as boolean;

          if (deviceUserId && deviceId) {
            try {
              if (deleted) {
                await c.env.DB.prepare(
                  `DELETE FROM remote_device_lists WHERE user_id = ? AND device_id = ?`
                ).bind(deviceUserId, deviceId).run();
              } else {
                await c.env.DB.prepare(`
                  INSERT INTO remote_device_lists (user_id, device_id, device_display_name, keys, stream_id, updated_at)
                  VALUES (?, ?, ?, ?, ?, ?)
                  ON CONFLICT (user_id, device_id) DO UPDATE SET
                    device_display_name = excluded.device_display_name,
                    keys = excluded.keys,
                    stream_id = excluded.stream_id,
                    updated_at = excluded.updated_at
                `).bind(
                  deviceUserId,
                  deviceId,
                  deviceDisplayName || null,
                  deviceKeys ? JSON.stringify(deviceKeys) : null,
                  streamId,
                  Date.now()
                ).run();
              }

              await c.env.DB.prepare(`
                INSERT INTO remote_device_list_streams (user_id, stream_id, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT (user_id) DO UPDATE SET
                  stream_id = MAX(remote_device_list_streams.stream_id, excluded.stream_id),
                  updated_at = excluded.updated_at
              `).bind(deviceUserId, streamId, Date.now()).run();
            } catch (devErr) {
              console.warn(`[federation] Failed to update device list for ${deviceUserId}:`, devErr);
            }
          }
          break;
        }

        case 'm.receipt':
          // Inbound read receipts are not applied yet
          break;

        case 'm.direct_to_device':
          // Store to-device messages for local recipients so /sync delivers them.
          await storeInboundToDevice(c.env, origin, content, toDeviceRecipients);
          break;

        case 'm.signing_key_update':
          // Inbound cross-signing key updates are not applied yet
          break;

        default:
          console.log(`Received unknown EDU type: ${eduType}`);
      }

      // Store EDU for record keeping
      const eduId = await sha256(`${origin}:${eduType}:${Date.now()}:${Math.random()}`);
      await c.env.DB.prepare(
        `INSERT OR REPLACE INTO processed_edus (edu_id, edu_type, origin, processed_at, content)
         VALUES (?, ?, ?, ?, ?)`
      ).bind(eduId, eduType, origin, Date.now(), JSON.stringify(content)).run();
    } catch (e) {
      console.error(`Failed to process EDU:`, e);
    }
  }

  // Wake to-device recipients so their /sync returns before the poll timeout.
  if (toDeviceRecipients.size > 0) {
    c.executionCtx.waitUntil(wakeSyncForUsers(c.env, toDeviceRecipients));
  }

  const response = { pdus: pduResults };

  // Cache the response so a retried transaction returns the same result
  await c.env.DB.prepare(
    `INSERT OR REPLACE INTO federation_transactions (txn_id, origin, received_at, response)
     VALUES (?, ?, ?, ?)`
  ).bind(txnId, origin, Date.now(), JSON.stringify(response)).run();

  return c.json(response);
});

// GET /_matrix/federation/v1/event/:eventId - Get a single event
app.get('/_matrix/federation/v1/event/:eventId', async (c) => {
  const eventId = c.req.param('eventId');

  const event = await c.env.DB.prepare(
    `SELECT event_id, room_id, sender, event_type, state_key, content,
     origin_server_ts, depth, auth_events, prev_events, hashes, signatures
     FROM events WHERE event_id = ?`
  ).bind(eventId).first<{
    event_id: string;
    room_id: string;
    sender: string;
    event_type: string;
    state_key: string | null;
    content: string;
    origin_server_ts: number;
    depth: number;
    auth_events: string;
    prev_events: string;
    hashes: string | null;
    signatures: string | null;
  }>();

  if (!event) {
    return Errors.notFound('Event not found').toResponse();
  }

  const legacyIds = await roomUsesLegacyIds(c.env.DB, event.room_id);
  return c.json({
    origin: c.env.SERVER_NAME,
    origin_server_ts: Date.now(),
    pdus: [eventRowToWire(event, legacyIds)],
  });
});

// GET /_matrix/federation/v1/state/:roomId - Get room state
app.get('/_matrix/federation/v1/state/:roomId', async (c) => {
  const roomId = c.req.param('roomId');
  // State at a specific event is not implemented; current state is returned
  void c.req.query('event_id');

  const legacyIds = await roomUsesLegacyIds(c.env.DB, roomId);

  // Full rows so hashes/signatures make it into the wire events
  const stateEvents = await c.env.DB.prepare(
    `SELECT e.event_id, e.room_id, e.sender, e.event_type, e.state_key, e.content,
     e.origin_server_ts, e.depth, e.auth_events, e.prev_events, e.hashes, e.signatures
     FROM room_state rs
     JOIN events e ON rs.event_id = e.event_id
     WHERE rs.room_id = ?`
  ).bind(roomId).all();

  const pdus = (stateEvents.results as any[]).map(e => eventRowToWire(e, legacyIds));

  // Recursive auth chain closure over the state events' auth_events
  const seen = new Set<string>();
  const authChain: Record<string, unknown>[] = [];
  let frontier: string[] = [];
  for (const e of stateEvents.results as any[]) {
    frontier.push(...JSON.parse(e.auth_events || '[]'));
  }
  while (frontier.length > 0) {
    const next: string[] = [];
    for (const authId of frontier) {
      if (seen.has(authId)) continue;
      seen.add(authId);
      const authEvent = await c.env.DB.prepare(
        `SELECT event_id, room_id, sender, event_type, state_key, content,
         origin_server_ts, depth, auth_events, prev_events, hashes, signatures
         FROM events WHERE event_id = ?`
      ).bind(authId).first();
      if (authEvent) {
        authChain.push(eventRowToWire(authEvent, legacyIds));
        next.push(...JSON.parse((authEvent as any).auth_events || '[]'));
      }
    }
    frontier = next;
  }

  return c.json({
    origin: c.env.SERVER_NAME,
    origin_server_ts: Date.now(),
    pdus,
    auth_chain: authChain,
  });
});

// GET /_matrix/federation/v1/state_ids/:roomId - Get room state event IDs only
app.get('/_matrix/federation/v1/state_ids/:roomId', async (c) => {
  const roomId = c.req.param('roomId');
  const eventId = c.req.query('event_id');

  const room = await c.env.DB.prepare(
    `SELECT room_id FROM rooms WHERE room_id = ?`
  ).bind(roomId).first<{ room_id: string }>();

  if (!room) {
    return Errors.notFound('Room not found').toResponse();
  }

  let stateEventIds: string[];
  let authChainIds: string[];

  if (eventId) {
    // State snapshots are not tracked, so this falls back to current state
    const stateEvents = await c.env.DB.prepare(
      `SELECT e.event_id, e.auth_events
       FROM room_state rs
       JOIN events e ON rs.event_id = e.event_id
       WHERE rs.room_id = ?`
    ).bind(roomId).all<{ event_id: string; auth_events: string }>();

    stateEventIds = stateEvents.results.map(e => e.event_id);

    const authChainSet = new Set<string>();
    for (const event of stateEvents.results) {
      const authEvents = JSON.parse(event.auth_events) as string[];
      for (const authId of authEvents) {
        authChainSet.add(authId);
      }
    }
    authChainIds = Array.from(authChainSet);
  } else {
    const stateEvents = await c.env.DB.prepare(
      `SELECT e.event_id, e.auth_events
       FROM room_state rs
       JOIN events e ON rs.event_id = e.event_id
       WHERE rs.room_id = ?`
    ).bind(roomId).all<{ event_id: string; auth_events: string }>();

    stateEventIds = stateEvents.results.map(e => e.event_id);

    const authChainSet = new Set<string>();
    for (const event of stateEvents.results) {
      const authEvents = JSON.parse(event.auth_events) as string[];
      for (const authId of authEvents) {
        authChainSet.add(authId);
      }
    }
    authChainIds = Array.from(authChainSet);
  }

  return c.json({
    pdu_ids: stateEventIds,
    auth_chain_ids: authChainIds,
  });
});

// GET /_matrix/federation/v1/event_auth/:roomId/:eventId - Get auth chain for an event
app.get('/_matrix/federation/v1/event_auth/:roomId/:eventId', async (c) => {
  const roomId = c.req.param('roomId');
  const eventId = c.req.param('eventId');

  const room = await c.env.DB.prepare(
    `SELECT room_id FROM rooms WHERE room_id = ?`
  ).bind(roomId).first<{ room_id: string }>();

  if (!room) {
    return Errors.notFound('Room not found').toResponse();
  }

  const event = await c.env.DB.prepare(
    `SELECT event_id, auth_events FROM events WHERE event_id = ? AND room_id = ?`
  ).bind(eventId, roomId).first<{ event_id: string; auth_events: string }>();

  if (!event) {
    return Errors.notFound('Event not found').toResponse();
  }

  // Build auth chain by recursively collecting auth events (wire format)
  const legacyIds = await roomUsesLegacyIds(c.env.DB, roomId);
  const authChain: Record<string, unknown>[] = [];
  const visited = new Set<string>();
  const toProcess = JSON.parse(event.auth_events) as string[];

  while (toProcess.length > 0) {
    const authId = toProcess.shift()!;
    if (visited.has(authId)) continue;
    visited.add(authId);

    const authEvent = await c.env.DB.prepare(
      `SELECT event_id, room_id, sender, event_type, state_key, content,
       origin_server_ts, depth, auth_events, prev_events, hashes, signatures
       FROM events WHERE event_id = ?`
    ).bind(authId).first();

    if (authEvent) {
      authChain.push(eventRowToWire(authEvent, legacyIds));
      for (const id of JSON.parse((authEvent as any).auth_events || '[]') as string[]) {
        if (!visited.has(id)) {
          toProcess.push(id);
        }
      }
    }
  }

  return c.json({
    auth_chain: authChain,
  });
});

// GET /_matrix/federation/v1/backfill/:roomId - Fetch historical events
app.get('/_matrix/federation/v1/backfill/:roomId', async (c) => {
  const roomId = c.req.param('roomId');
  const limit = Math.min(parseInt(c.req.query('limit') || '100', 10), 1000);
  const vParam = c.req.query('v'); // Starting event IDs

  const room = await c.env.DB.prepare(
    `SELECT room_id FROM rooms WHERE room_id = ?`
  ).bind(roomId).first<{ room_id: string }>();

  if (!room) {
    return Errors.notFound('Room not found').toResponse();
  }

  const startEventIds = vParam ? vParam.split(',') : [];

  let events: any[];
  if (startEventIds.length > 0) {
    // Walk back from the shallowest of the requested events
    const startEvents = await c.env.DB.prepare(
      `SELECT MIN(depth) as min_depth FROM events WHERE event_id IN (${startEventIds.map(() => '?').join(',')})`
    ).bind(...startEventIds).first<{ min_depth: number }>();

    const maxDepth = startEvents?.min_depth || 0;

    events = (await c.env.DB.prepare(
      `SELECT event_id, room_id, sender, event_type, state_key, content,
       origin_server_ts, depth, auth_events, prev_events, hashes, signatures
       FROM events
       WHERE room_id = ? AND depth < ?
       ORDER BY depth DESC
       LIMIT ?`
    ).bind(roomId, maxDepth, limit).all()).results;
  } else {
    events = (await c.env.DB.prepare(
      `SELECT event_id, room_id, sender, event_type, state_key, content,
       origin_server_ts, depth, auth_events, prev_events, hashes, signatures
       FROM events
       WHERE room_id = ?
       ORDER BY depth DESC
       LIMIT ?`
    ).bind(roomId, limit).all()).results;
  }

  const backfillLegacyIds = await roomUsesLegacyIds(c.env.DB, roomId);
  const pdus = events.map((e: any) => eventRowToWire(e, backfillLegacyIds));

  return c.json({
    origin: c.env.SERVER_NAME,
    origin_server_ts: Date.now(),
    pdus,
  });
});

// POST /_matrix/federation/v1/get_missing_events/:roomId - Fill event gaps
app.post('/_matrix/federation/v1/get_missing_events/:roomId', async (c) => {
  const roomId = c.req.param('roomId');

  let body: {
    earliest_events?: string[];
    latest_events?: string[];
    limit?: number;
    min_depth?: number;
  };

  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const earliestEvents = body.earliest_events || [];
  const latestEvents = body.latest_events || [];
  const limit = Math.min(body.limit || 10, 100);
  const minDepth = body.min_depth || 0;

  const room = await c.env.DB.prepare(
    `SELECT room_id FROM rooms WHERE room_id = ?`
  ).bind(roomId).first<{ room_id: string }>();

  if (!room) {
    return Errors.notFound('Room not found').toResponse();
  }

  // Walk backwards from latest_events to earliest_events
  const missingLegacyIds = await roomUsesLegacyIds(c.env.DB, roomId);
  const events: any[] = [];
  const visited = new Set<string>(earliestEvents);
  const toProcess = [...latestEvents];

  while (toProcess.length > 0 && events.length < limit) {
    const eventId = toProcess.shift()!;
    if (visited.has(eventId)) continue;
    visited.add(eventId);

    const event = await c.env.DB.prepare(
      `SELECT event_id, room_id, sender, event_type, state_key, content,
       origin_server_ts, depth, auth_events, prev_events, hashes, signatures
       FROM events
       WHERE event_id = ? AND room_id = ? AND depth >= ?`
    ).bind(eventId, roomId, minDepth).first();

    if (event) {
      events.push(eventRowToWire(event, missingLegacyIds));

      const prevEvents = JSON.parse((event as any).prev_events || '[]') as string[];
      for (const prevId of prevEvents) {
        if (!visited.has(prevId)) {
          toProcess.push(prevId);
        }
      }
    }
  }

  return c.json({
    events,
  });
});

// GET /_matrix/federation/v1/make_join/:roomId/:userId - Prepare join request
app.get('/_matrix/federation/v1/make_join/:roomId/:userId', async (c) => {
  const roomId = c.req.param('roomId');
  const userId = c.req.param('userId');

  const room = await c.env.DB.prepare(
    `SELECT room_id, room_version FROM rooms WHERE room_id = ?`
  ).bind(roomId).first<{ room_id: string; room_version: string }>();

  if (!room) {
    return Errors.notFound('Room not found').toResponse();
  }

  // Auth events the joining server must reference
  const createEvent = await c.env.DB.prepare(
    `SELECT e.event_id FROM room_state rs
     JOIN events e ON rs.event_id = e.event_id
     WHERE rs.room_id = ? AND rs.event_type = 'm.room.create'`
  ).bind(roomId).first<{ event_id: string }>();

  const joinRulesEvent = await c.env.DB.prepare(
    `SELECT e.event_id FROM room_state rs
     JOIN events e ON rs.event_id = e.event_id
     WHERE rs.room_id = ? AND rs.event_type = 'm.room.join_rules'`
  ).bind(roomId).first<{ event_id: string }>();

  const powerLevelsEvent = await c.env.DB.prepare(
    `SELECT e.event_id FROM room_state rs
     JOIN events e ON rs.event_id = e.event_id
     WHERE rs.room_id = ? AND rs.event_type = 'm.room.power_levels'`
  ).bind(roomId).first<{ event_id: string }>();

  // The joining user's current membership (the invite) belongs in auth_events:
  // the join auth rule looks for a prior invite here, so without it a join into
  // an invite-only room cannot be authorized by the resident server.
  const memberEvent = await c.env.DB.prepare(
    `SELECT e.event_id FROM room_state rs
     JOIN events e ON rs.event_id = e.event_id
     WHERE rs.room_id = ? AND rs.event_type = 'm.room.member' AND rs.state_key = ?`
  ).bind(roomId, userId).first<{ event_id: string }>();

  const authEvents: string[] = [];
  if (createEvent) authEvents.push(createEvent.event_id);
  if (powerLevelsEvent) authEvents.push(powerLevelsEvent.event_id);
  if (joinRulesEvent) authEvents.push(joinRulesEvent.event_id);
  if (memberEvent) authEvents.push(memberEvent.event_id);

  // Get latest event for prev_events
  const latestEvent = await c.env.DB.prepare(
    `SELECT event_id, depth FROM events WHERE room_id = ? ORDER BY depth DESC LIMIT 1`
  ).bind(roomId).first<{ event_id: string; depth: number }>();

  const prevEvents = latestEvent ? [latestEvent.event_id] : [];
  const depth = (latestEvent?.depth || 0) + 1;

  // Create unsigned join event template
  const eventTemplate = {
    room_id: roomId,
    sender: userId,
    type: 'm.room.member',
    state_key: userId,
    content: {
      membership: 'join',
    },
    origin_server_ts: Date.now(),
    depth,
    auth_events: authEvents,
    prev_events: prevEvents,
  };

  return c.json({
    room_version: room.room_version,
    event: eventTemplate,
  });
});

// Shared send_join implementation (v1 + v2): validates the remote user's join
// event, persists it, fans it out to the other servers in the room, and returns
// current room state plus the full recursive auth chain in wire format.
async function processSendJoin(
  c: Context<AppEnv>,
  roomId: string,
  eventId: string
): Promise<Response | Record<string, unknown>> {
  let joinEvent: any;
  try {
    joinEvent = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const db = c.env.DB;
  const room = await db.prepare(
    `SELECT room_id, room_version FROM rooms WHERE room_id = ?`
  ).bind(roomId).first<{ room_id: string; room_version: string }>();
  if (!room) {
    return Errors.notFound('Room not found').toResponse();
  }

  const stateKey = joinEvent?.state_key;
  if (
    joinEvent?.type !== 'm.room.member' ||
    joinEvent?.content?.membership !== 'join' ||
    typeof stateKey !== 'string' ||
    stateKey !== joinEvent?.sender ||
    joinEvent?.room_id !== roomId
  ) {
    return c.json({ errcode: 'M_INVALID_PARAM', error: 'Not a valid join event for this room' }, 400);
  }

  const behavior = getRoomVersion(room.room_version);
  const legacyIds = behavior?.eventIdFormat === 'v1';

  // v3+ event IDs are derived from the redacted event, never from a claim
  if (legacyIds) {
    if (joinEvent.event_id !== eventId) {
      return c.json({ errcode: 'M_INVALID_PARAM', error: 'Event ID mismatch' }, 400);
    }
  } else {
    const computedId = await computeEventId(joinEvent, room.room_version);
    if (computedId !== eventId) {
      return c.json({ errcode: 'M_INVALID_PARAM', error: 'Event ID does not match reference hash' }, 400);
    }
  }

  // Verify the joining server's signature (over the redacted event, per spec)
  const senderServer = stateKey.split(':').slice(1).join(':');
  const redactedForVerify = redactEvent(joinEvent, room.room_version);
  delete redactedForVerify.event_id;
  let signatureValid = false;
  for (const keyId of Object.keys(joinEvent.signatures?.[senderServer] ?? {})) {
    try {
      if (await verifyRemoteSignature(redactedForVerify, senderServer, keyId, db, c.env.CACHE)) {
        signatureValid = true;
        break;
      }
    } catch (e) {
      console.warn('[send_join] signature check failed:', e);
    }
  }
  if (!signatureValid) {
    return c.json({ errcode: 'M_FORBIDDEN', error: 'Join event signature verification failed' }, 403);
  }

  // Authorization: public rooms, or an existing invite/join for the user
  // (restricted-join validation is not yet implemented - treated like invite)
  const joinRuleRow = await db.prepare(
    `SELECT e.content FROM room_state rs JOIN events e ON rs.event_id = e.event_id
     WHERE rs.room_id = ? AND rs.event_type = 'm.room.join_rules' AND rs.state_key = ''`
  ).bind(roomId).first<{ content: string }>();
  let joinRule = 'invite';
  try {
    joinRule = JSON.parse(joinRuleRow?.content ?? '{}').join_rule ?? 'invite';
  } catch {
    // Malformed join_rules content - keep the restrictive default
  }
  if (joinRule !== 'public') {
    const existing = await db.prepare(
      `SELECT membership FROM room_memberships WHERE room_id = ? AND user_id = ?`
    ).bind(roomId, stateKey).first<{ membership: string }>();
    if (!existing || (existing.membership !== 'invite' && existing.membership !== 'join')) {
      return c.json({ errcode: 'M_FORBIDDEN', error: 'User is not invited to this room' }, 403);
    }
  }

  // The joining server authors and signs this event; the resident server only
  // adds its own signature for restricted rooms (join_authorised_via_users_server).
  // `unsigned` is local metadata and does not belong on a federated PDU.
  const wireJoinEvent: Record<string, unknown> = { ...joinEvent };
  delete wireJoinEvent.unsigned;
  delete wireJoinEvent.event_id;

  // DB row -> wire-format event (v3+ wire events carry no event_id)
  const toWire = (row: any): Record<string, unknown> => {
    const ev: Record<string, unknown> = {
      room_id: row.room_id,
      sender: row.sender,
      type: row.event_type,
      content: JSON.parse(row.content || '{}'),
      origin_server_ts: row.origin_server_ts,
      depth: row.depth,
      auth_events: JSON.parse(row.auth_events || '[]'),
      prev_events: JSON.parse(row.prev_events || '[]'),
    };
    if (row.state_key !== null && row.state_key !== undefined) {
      ev.state_key = row.state_key;
    }
    if (row.hashes) ev.hashes = JSON.parse(row.hashes);
    if (row.signatures) ev.signatures = JSON.parse(row.signatures);
    if (legacyIds) ev.event_id = row.event_id;
    return ev;
  };

  // Current room state (prior to this join, per spec)
  const stateRows = await db.prepare(
    `SELECT e.* FROM room_state rs JOIN events e ON rs.event_id = e.event_id WHERE rs.room_id = ?`
  ).bind(roomId).all();
  const stateWire = (stateRows.results as any[]).map(toWire);

  // Auth chain: recursive closure over auth_events references
  const seen = new Set<string>();
  const authChainWire: Record<string, unknown>[] = [];
  let frontier: string[] = [];
  for (const r of stateRows.results as any[]) {
    frontier.push(...JSON.parse(r.auth_events || '[]'));
  }
  frontier.push(...((joinEvent.auth_events as string[] | undefined) ?? []));
  while (frontier.length > 0) {
    const next: string[] = [];
    for (const id of frontier) {
      if (seen.has(id)) continue;
      seen.add(id);
      const row = await db.prepare(`SELECT * FROM events WHERE event_id = ?`).bind(id).first();
      if (row) {
        authChainWire.push(toWire(row));
        next.push(...JSON.parse((row as any).auth_events || '[]'));
      }
    }
    frontier = next;
  }

  // Persist the join with the sender's signature, update state + membership
  const storedJoin: Record<string, unknown> = { ...joinEvent, event_id: eventId };
  delete storedJoin.unsigned;
  await storeEvent(db, storedJoin as unknown as PDU);
  await db.prepare(
    `INSERT OR REPLACE INTO room_state (room_id, event_type, state_key, event_id)
     VALUES (?, 'm.room.member', ?, ?)`
  ).bind(roomId, stateKey, eventId).run();
  await updateMembership(db, roomId, stateKey, 'join', eventId);
  await notifyUsersOfEvent(c.env, roomId, eventId, 'm.room.member');

  // For v1/v2 the wire event keeps event_id
  const wireJoin: Record<string, unknown> = { ...wireJoinEvent };
  if (legacyIds) wireJoin.event_id = eventId;

  // Fan the join out to the other remote servers in the room
  const remoteServers = await getRemoteServersInRoom(db, roomId, c.env.SERVER_NAME);
  const others = remoteServers.filter(s => s !== senderServer);
  if (others.length > 0) {
    c.executionCtx.waitUntil(sendPduToServers(c.env, roomId, wireJoin, others));
  }

  return {
    origin: c.env.SERVER_NAME,
    event: wireJoin,
    state: stateWire,
    auth_chain: authChainWire,
    servers_in_room: [...new Set([c.env.SERVER_NAME, senderServer, ...remoteServers])],
  };
}

// PUT /_matrix/federation/v1/send_join/:roomId/:eventId - Complete join
app.put('/_matrix/federation/v1/send_join/:roomId/:eventId', async (c) => {
  const result = await processSendJoin(c, c.req.param('roomId'), c.req.param('eventId'));
  if (result instanceof Response) {
    return result;
  }
  // v1 responds with the legacy [200, body] tuple
  return c.json([200, result]);
});

// PUT /_matrix/federation/v2/send_join/:roomId/:eventId - Complete join (v2)
// v2 wraps the response in an object instead of the legacy [200, body] tuple
app.put('/_matrix/federation/v2/send_join/:roomId/:eventId', async (c) => {
  const result = await processSendJoin(c, c.req.param('roomId'), c.req.param('eventId'));
  if (result instanceof Response) {
    return result;
  }
  // Always full state; omit_members is not implemented
  return c.json({ ...result, members_omitted: false });
});

// GET /_matrix/federation/v1/make_leave/:roomId/:userId - Prepare leave request
app.get('/_matrix/federation/v1/make_leave/:roomId/:userId', async (c) => {
  const roomId = c.req.param('roomId');
  const userId = c.req.param('userId');

  const room = await c.env.DB.prepare(
    `SELECT room_id, room_version FROM rooms WHERE room_id = ?`
  ).bind(roomId).first<{ room_id: string; room_version: string }>();

  if (!room) {
    return Errors.notFound('Room not found').toResponse();
  }

  const membership = await c.env.DB.prepare(
    `SELECT e.event_id FROM room_state rs
     JOIN events e ON rs.event_id = e.event_id
     WHERE rs.room_id = ? AND rs.event_type = 'm.room.member' AND rs.state_key = ?`
  ).bind(roomId, userId).first<{ event_id: string }>();

  if (!membership) {
    return c.json(
      { errcode: 'M_FORBIDDEN', error: 'User is not a member of the room' },
      403
    );
  }

  // Get auth events for leave
  const createEvent = await c.env.DB.prepare(
    `SELECT e.event_id FROM room_state rs
     JOIN events e ON rs.event_id = e.event_id
     WHERE rs.room_id = ? AND rs.event_type = 'm.room.create'`
  ).bind(roomId).first<{ event_id: string }>();

  const powerLevelsEvent = await c.env.DB.prepare(
    `SELECT e.event_id FROM room_state rs
     JOIN events e ON rs.event_id = e.event_id
     WHERE rs.room_id = ? AND rs.event_type = 'm.room.power_levels'`
  ).bind(roomId).first<{ event_id: string }>();

  const authEvents: string[] = [];
  if (createEvent) authEvents.push(createEvent.event_id);
  if (powerLevelsEvent) authEvents.push(powerLevelsEvent.event_id);
  if (membership) authEvents.push(membership.event_id);

  // Get latest event for prev_events
  const latestEvent = await c.env.DB.prepare(
    `SELECT event_id, depth FROM events WHERE room_id = ? ORDER BY depth DESC LIMIT 1`
  ).bind(roomId).first<{ event_id: string; depth: number }>();

  const prevEvents = latestEvent ? [latestEvent.event_id] : [];
  const depth = (latestEvent?.depth || 0) + 1;

  // Create unsigned leave event template
  const eventTemplate = {
    room_id: roomId,
    sender: userId,
    type: 'm.room.member',
    state_key: userId,
    content: {
      membership: 'leave',
    },
    origin_server_ts: Date.now(),
    depth,
    auth_events: authEvents,
    prev_events: prevEvents,
  };

  return c.json({
    room_version: room.room_version,
    event: eventTemplate,
  });
});

// PUT /_matrix/federation/v1/send_leave/:roomId/:eventId - Complete leave
app.put('/_matrix/federation/v1/send_leave/:roomId/:eventId', async (c) => {
  const roomId = c.req.param('roomId');
  const eventId = c.req.param('eventId');

  let body: any;
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  if (body.type !== 'm.room.member' || body.content?.membership !== 'leave') {
    return c.json(
      { errcode: 'M_INVALID_PARAM', error: 'Event is not a leave event' },
      400
    );
  }

  if (body.event_id && body.event_id !== eventId) {
    return c.json(
      { errcode: 'M_INVALID_PARAM', error: 'Event ID mismatch' },
      400
    );
  }

  const room = await c.env.DB.prepare(
    `SELECT room_id FROM rooms WHERE room_id = ?`
  ).bind(roomId).first<{ room_id: string }>();

  if (!room) {
    return Errors.notFound('Room not found').toResponse();
  }

  // v1 returns empty array on success [200, {}]
  return c.json([200, {}]);
});

// PUT /_matrix/federation/v2/send_leave/:roomId/:eventId - Complete leave (v2)
app.put('/_matrix/federation/v2/send_leave/:roomId/:eventId', async (c) => {
  const roomId = c.req.param('roomId');
  const eventId = c.req.param('eventId');

  let body: any;
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  if (body.type !== 'm.room.member' || body.content?.membership !== 'leave') {
    return c.json(
      { errcode: 'M_INVALID_PARAM', error: 'Event is not a leave event' },
      400
    );
  }

  if (body.event_id && body.event_id !== eventId) {
    return c.json(
      { errcode: 'M_INVALID_PARAM', error: 'Event ID mismatch' },
      400
    );
  }

  const room = await c.env.DB.prepare(
    `SELECT room_id FROM rooms WHERE room_id = ?`
  ).bind(roomId).first<{ room_id: string }>();

  if (!room) {
    return Errors.notFound('Room not found').toResponse();
  }

  // v2 returns empty object on success
  return c.json({});
});

// PUT /_matrix/federation/v1/invite/:roomId/:eventId - Receive invite (v1)
// Used when a remote server invites a local user to a room
app.put('/_matrix/federation/v1/invite/:roomId/:eventId', async (c) => {
  // The room is remote, so trust the room_id / event_id from the URL (v3+ invite
  // events carry no event_id in the body).
  const roomId = c.req.param('roomId');
  const eventId = c.req.param('eventId');

  let body: {
    room_version?: string;
    event?: any;
    invite_room_state?: any[];
  };

  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const inviteEvent = body.event || body;

  if (inviteEvent.type !== 'm.room.member' || inviteEvent.content?.membership !== 'invite') {
    return c.json(
      { errcode: 'M_INVALID_PARAM', error: 'Event is not an invite event' },
      400
    );
  }

  if (inviteEvent.event_id && inviteEvent.event_id !== eventId) {
    return c.json(
      { errcode: 'M_INVALID_PARAM', error: 'Event ID mismatch' },
      400
    );
  }

  const stateKey = inviteEvent.state_key;
  if (!stateKey || !stateKey.includes(':')) {
    return c.json(
      { errcode: 'M_INVALID_PARAM', error: 'Invalid state_key for invite' },
      400
    );
  }

  const invitedServer = stateKey.split(':')[1];
  if (invitedServer !== c.env.SERVER_NAME) {
    return c.json(
      { errcode: 'M_FORBIDDEN', error: 'User is not local to this server' },
      403
    );
  }

  const localUser = await c.env.DB.prepare(
    `SELECT user_id FROM users WHERE user_id = ?`
  ).bind(stateKey).first<{ user_id: string }>();

  if (!localUser) {
    return c.json(
      { errcode: 'M_NOT_FOUND', error: 'User not found' },
      404
    );
  }

  const key = await c.env.DB.prepare(
    `SELECT key_id, private_key_jwk FROM server_keys WHERE is_current = 1 AND key_version = 2`
  ).first<{ key_id: string; private_key_jwk: string | null }>();

  if (!key || !key.private_key_jwk) {
    return c.json(
      { errcode: 'M_UNKNOWN', error: 'Server signing key not configured' },
      500
    );
  }

  // Sign over the redacted event so the signature validates on the inviting
  // server. The v1 invite endpoint predates v3 rooms and carries no room_version,
  // so default to '1', which applies the pre-v11 redaction rules v1/v2 expect.
  const roomVersion = body.room_version || '1';
  const signedEvent = await signEventForFederation(
    inviteEvent,
    roomVersion,
    c.env.SERVER_NAME,
    key.key_id,
    JSON.parse(key.private_key_jwk)
  );

  // Persist the invite so the local user sees it in /sync, then wake their sync.
  const inviteRoomState = body.invite_room_state ?? inviteEvent.unsigned?.invite_room_state;
  await storeInboundInvite(c.env.DB, roomId, roomVersion, stateKey, eventId, inviteEvent, inviteRoomState);
  c.executionCtx.waitUntil(wakeSyncForUsers(c.env, [stateKey]));

  // v1 responds with [200, { event }] per the legacy federation spec.
  return c.json([200, { event: signedEvent }]);
});

// PUT /_matrix/federation/v2/invite/:roomId/:eventId - Receive invite (v2)
app.put('/_matrix/federation/v2/invite/:roomId/:eventId', async (c) => {
  // The room is remote, so trust the room_id / event_id from the URL (v3+ invite
  // events carry no event_id in the body).
  const roomId = c.req.param('roomId');
  const eventId = c.req.param('eventId');

  let body: {
    room_version: string;
    event: any;
    invite_room_state?: any[];
  };

  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const roomVersion = body.room_version;
  const inviteEvent = body.event;

  if (!roomVersion) {
    return Errors.missingParam('room_version').toResponse();
  }

  if (!inviteEvent) {
    return Errors.missingParam('event').toResponse();
  }

  if (!SUPPORTED_ROOM_VERSIONS.includes(roomVersion)) {
    return c.json(
      { errcode: 'M_INCOMPATIBLE_ROOM_VERSION', error: `Unsupported room version: ${roomVersion}` },
      400
    );
  }

  if (inviteEvent.type !== 'm.room.member' || inviteEvent.content?.membership !== 'invite') {
    return c.json(
      { errcode: 'M_INVALID_PARAM', error: 'Event is not an invite event' },
      400
    );
  }

  if (inviteEvent.event_id && inviteEvent.event_id !== eventId) {
    return c.json(
      { errcode: 'M_INVALID_PARAM', error: 'Event ID mismatch' },
      400
    );
  }

  const stateKey = inviteEvent.state_key;
  if (!stateKey || !stateKey.includes(':')) {
    return c.json(
      { errcode: 'M_INVALID_PARAM', error: 'Invalid state_key for invite' },
      400
    );
  }

  const invitedServer = stateKey.split(':')[1];
  if (invitedServer !== c.env.SERVER_NAME) {
    return c.json(
      { errcode: 'M_FORBIDDEN', error: 'User is not local to this server' },
      403
    );
  }

  const localUser = await c.env.DB.prepare(
    `SELECT user_id FROM users WHERE user_id = ?`
  ).bind(stateKey).first<{ user_id: string }>();

  if (!localUser) {
    return c.json(
      { errcode: 'M_NOT_FOUND', error: 'User not found' },
      404
    );
  }

  const key = await c.env.DB.prepare(
    `SELECT key_id, private_key_jwk FROM server_keys WHERE is_current = 1 AND key_version = 2`
  ).first<{ key_id: string; private_key_jwk: string | null }>();

  if (!key || !key.private_key_jwk) {
    return c.json(
      { errcode: 'M_UNKNOWN', error: 'Server signing key not configured' },
      500
    );
  }

  // Sign over the redacted event so the signature validates on the inviting server.
  const signedEvent = await signEventForFederation(
    inviteEvent,
    roomVersion,
    c.env.SERVER_NAME,
    key.key_id,
    JSON.parse(key.private_key_jwk)
  );

  // Persist the invite so the local user sees it in /sync, then wake their sync.
  const inviteRoomState = body.invite_room_state ?? inviteEvent.unsigned?.invite_room_state;
  await storeInboundInvite(c.env.DB, roomId, roomVersion, stateKey, eventId, inviteEvent, inviteRoomState);
  c.executionCtx.waitUntil(wakeSyncForUsers(c.env, [stateKey]));

  return c.json({ event: signedEvent });
});

// GET /_matrix/federation/v1/query/directory - Resolve room alias
app.get('/_matrix/federation/v1/query/directory', async (c) => {
  const alias = c.req.query('room_alias');

  if (!alias) {
    return Errors.missingParam('room_alias').toResponse();
  }

  const roomId = await c.env.DB.prepare(
    `SELECT room_id FROM room_aliases WHERE alias = ?`
  ).bind(alias).first<{ room_id: string }>();

  if (!roomId) {
    return Errors.notFound('Room alias not found').toResponse();
  }

  return c.json({
    room_id: roomId.room_id,
    servers: [c.env.SERVER_NAME],
  });
});

// GET /_matrix/federation/v1/query/profile - Query user profile
app.get('/_matrix/federation/v1/query/profile', async (c) => {
  const userId = c.req.query('user_id');
  const field = c.req.query('field');

  if (!userId) {
    return Errors.missingParam('user_id').toResponse();
  }

  const user = await c.env.DB.prepare(
    `SELECT display_name, avatar_url FROM users WHERE user_id = ?`
  ).bind(userId).first<{ display_name: string | null; avatar_url: string | null }>();

  if (!user) {
    return Errors.notFound('User not found').toResponse();
  }

  if (field === 'displayname') {
    return c.json({ displayname: user.display_name });
  } else if (field === 'avatar_url') {
    return c.json({ avatar_url: user.avatar_url });
  }

  return c.json({
    displayname: user.display_name,
    avatar_url: user.avatar_url,
  });
});

// Federation E2EE endpoints for cross-server encrypted messaging

// The UserKeys Durable Object stub for a user
function getUserKeysDO(env: typeof app extends Hono<infer E> ? E['Bindings'] : never, userId: string): DurableObjectStub {
  const id = env.USER_KEYS.idFromName(userId);
  return env.USER_KEYS.get(id);
}

// Device keys for a user, or for one device, from the UserKeys DO
async function getDeviceKeysFromDO(env: typeof app extends Hono<infer E> ? E['Bindings'] : never, userId: string, deviceId?: string): Promise<any> {
  const stub = getUserKeysDO(env, userId);
  const url = deviceId
    ? `http://internal/device-keys/get?device_id=${encodeURIComponent(deviceId)}`
    : 'http://internal/device-keys/get';
  const response = await stub.fetch(new Request(url));
  if (!response.ok) {
    return deviceId ? null : {};
  }
  return await response.json();
}

// Cross-signing keys for a user from the UserKeys DO
async function getCrossSigningKeysFromDO(env: typeof app extends Hono<infer E> ? E['Bindings'] : never, userId: string): Promise<{
  master?: any;
  self_signing?: any;
  user_signing?: any;
}> {
  const stub = getUserKeysDO(env, userId);
  const response = await stub.fetch(new Request('http://internal/cross-signing/get'));
  if (!response.ok) {
    return {};
  }
  return await response.json();
}

// POST /_matrix/federation/v1/user/keys/query - Query device keys for local users
// This endpoint is called by remote servers to get device keys for E2EE
app.post('/_matrix/federation/v1/user/keys/query', async (c) => {
  const serverName = c.env.SERVER_NAME;
  const db = c.env.DB;

  let body: {
    device_keys?: Record<string, string[]>;
  };

  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const requestedKeys = body.device_keys;
  if (!requestedKeys || typeof requestedKeys !== 'object') {
    return Errors.missingParam('device_keys').toResponse();
  }

  const deviceKeys: Record<string, Record<string, any>> = {};
  const masterKeys: Record<string, any> = {};
  const selfSigningKeys: Record<string, any> = {};

  // Merges cross-signing signatures stored in D1 into a device key
  async function mergeSignaturesForDevice(userId: string, deviceId: string, deviceKey: any): Promise<any> {
    const dbSignatures = await db.prepare(`
      SELECT signer_user_id, signer_key_id, signature
      FROM cross_signing_signatures
      WHERE user_id = ? AND key_id = ?
    `).bind(userId, deviceId).all<{
      signer_user_id: string;
      signer_key_id: string;
      signature: string;
    }>();

    if (dbSignatures.results.length > 0) {
      deviceKey.signatures = deviceKey.signatures || {};
      for (const sig of dbSignatures.results) {
        deviceKey.signatures[sig.signer_user_id] = deviceKey.signatures[sig.signer_user_id] || {};
        deviceKey.signatures[sig.signer_user_id][sig.signer_key_id] = sig.signature;
      }
    }

    return deviceKey;
  }

  for (const [userId, requestedDevices] of Object.entries(requestedKeys)) {
    const userServerName = userId.split(':')[1];
    if (userServerName !== serverName) {
      // Skip non-local users - federation should query their home server
      continue;
    }

    const user = await db.prepare(
      `SELECT user_id FROM users WHERE user_id = ?`
    ).bind(userId).first<{ user_id: string }>();

    if (!user) {
      continue;
    }

    deviceKeys[userId] = {};

    // Get device keys from Durable Object (strongly consistent)
    if (!requestedDevices || requestedDevices.length === 0) {
      const allDeviceKeys = await getDeviceKeysFromDO(c.env, userId);
      for (const [deviceId, keys] of Object.entries(allDeviceKeys)) {
        if (keys) {
          deviceKeys[userId][deviceId] = await mergeSignaturesForDevice(userId, deviceId, keys);
        }
      }
    } else {
      for (const deviceId of requestedDevices) {
        const keys = await getDeviceKeysFromDO(c.env, userId, deviceId);
        if (keys) {
          deviceKeys[userId][deviceId] = await mergeSignaturesForDevice(userId, deviceId, keys);
        }
      }
    }

    // Federation carries master + self_signing only; user_signing stays private per spec.
    const csKeys = await getCrossSigningKeysFromDO(c.env, userId);

    if (csKeys.master) {
      masterKeys[userId] = csKeys.master;
    }
    if (csKeys.self_signing) {
      selfSigningKeys[userId] = csKeys.self_signing;
    }
  }

  return c.json({
    device_keys: deviceKeys,
    master_keys: masterKeys,
    self_signing_keys: selfSigningKeys,
  });
});

// POST /_matrix/federation/v1/user/keys/claim - Claim one-time keys for E2EE session establishment
// Remote servers call this to get OTKs to establish encrypted sessions with local users
app.post('/_matrix/federation/v1/user/keys/claim', async (c) => {
  const serverName = c.env.SERVER_NAME;
  const db = c.env.DB;

  let body: {
    one_time_keys?: Record<string, Record<string, string>>;
  };

  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const requestedKeys = body.one_time_keys;
  if (!requestedKeys || typeof requestedKeys !== 'object') {
    return Errors.missingParam('one_time_keys').toResponse();
  }

  const oneTimeKeys: Record<string, Record<string, Record<string, any>>> = {};

  for (const [userId, devices] of Object.entries(requestedKeys)) {
    const userServerName = userId.split(':')[1];
    if (userServerName !== serverName) {
      continue;
    }

    oneTimeKeys[userId] = {};

    for (const [deviceId, algorithm] of Object.entries(devices)) {
      // Try to claim a one-time key from KV first (fast path)
      const existingKeys = await c.env.ONE_TIME_KEYS.get(
        `otk:${userId}:${deviceId}`,
        'json'
      ) as Record<string, { keyId: string; keyData: any; claimed: boolean }[]> | null;

      let foundKey = false;

      if (existingKeys && existingKeys[algorithm]) {
        const keyIndex = existingKeys[algorithm].findIndex(k => !k.claimed);
        if (keyIndex >= 0) {
          const key = existingKeys[algorithm][keyIndex];
          existingKeys[algorithm][keyIndex].claimed = true;

          await c.env.ONE_TIME_KEYS.put(
            `otk:${userId}:${deviceId}`,
            JSON.stringify(existingKeys)
          );

          await db.prepare(`
            UPDATE one_time_keys SET claimed = 1, claimed_at = ?
            WHERE user_id = ? AND device_id = ? AND key_id = ?
          `).bind(Date.now(), userId, deviceId, key.keyId).run();

          oneTimeKeys[userId][deviceId] = {
            [key.keyId]: key.keyData,
          };
          foundKey = true;
        }
      }

      if (!foundKey) {
        // Fallback to D1 for keys not in KV
        const otk = await db.prepare(`
          SELECT id, key_id, key_data FROM one_time_keys
          WHERE user_id = ? AND device_id = ? AND algorithm = ? AND claimed = 0
          LIMIT 1
        `).bind(userId, deviceId, algorithm).first<{
          id: number;
          key_id: string;
          key_data: string;
        }>();

        if (otk) {
          await db.prepare(`
            UPDATE one_time_keys SET claimed = 1, claimed_at = ? WHERE id = ?
          `).bind(Date.now(), otk.id).run();

          oneTimeKeys[userId][deviceId] = {
            [otk.key_id]: JSON.parse(otk.key_data),
          };
          foundKey = true;
        }
      }

      if (!foundKey) {
        // Try fallback key as last resort
        const fallback = await db.prepare(`
          SELECT key_id, key_data, used FROM fallback_keys
          WHERE user_id = ? AND device_id = ? AND algorithm = ?
        `).bind(userId, deviceId, algorithm).first<{
          key_id: string;
          key_data: string;
          used: number;
        }>();

        if (fallback) {
          await db.prepare(`
            UPDATE fallback_keys SET used = 1 WHERE user_id = ? AND device_id = ? AND algorithm = ?
          `).bind(userId, deviceId, algorithm).run();

          const keyData = JSON.parse(fallback.key_data);
          oneTimeKeys[userId][deviceId] = {
            [fallback.key_id]: {
              ...keyData,
              fallback: true,
            },
          };
        }
      }
    }
  }

  return c.json({
    one_time_keys: oneTimeKeys,
  });
});

// GET /_matrix/federation/v1/user/devices/:userId - Get device list for a local user
// Remote servers call this to get the list of devices for a user
app.get('/_matrix/federation/v1/user/devices/:userId', async (c) => {
  const serverName = c.env.SERVER_NAME;
  const userId = c.req.param('userId');
  const db = c.env.DB;

  const userServerName = userId.split(':')[1];
  if (userServerName !== serverName) {
    return c.json({
      errcode: 'M_FORBIDDEN',
      error: 'User is not local to this server',
    }, 403);
  }

  const user = await db.prepare(
    `SELECT user_id FROM users WHERE user_id = ?`
  ).bind(userId).first<{ user_id: string }>();

  if (!user) {
    return Errors.notFound('User not found').toResponse();
  }

  // Get all devices from D1 (for display names)
  const dbDevices = await db.prepare(
    `SELECT device_id, display_name FROM devices WHERE user_id = ?`
  ).bind(userId).all<{ device_id: string; display_name: string | null }>();

  // Get device keys from Durable Object (strongly consistent)
  const allDeviceKeys = await getDeviceKeysFromDO(c.env, userId);

  const streamPosition = await db.prepare(
    `SELECT MAX(stream_position) as stream_id FROM device_key_changes WHERE user_id = ?`
  ).bind(userId).first<{ stream_id: number | null }>();

  const devices: Array<{
    device_id: string;
    keys?: any;
    device_display_name?: string;
  }> = [];

  for (const dbDevice of dbDevices.results) {
    const deviceKeys = allDeviceKeys[dbDevice.device_id];
    devices.push({
      device_id: dbDevice.device_id,
      keys: deviceKeys || undefined,
      device_display_name: dbDevice.display_name || undefined,
    });
  }

  // Get cross-signing keys (master + self_signing only for federation)
  const csKeys = await getCrossSigningKeysFromDO(c.env, userId);

  const response: any = {
    user_id: userId,
    stream_id: streamPosition?.stream_id || 0,
    devices,
  };

  if (csKeys.master) {
    response.master_key = csKeys.master;
  }
  if (csKeys.self_signing) {
    response.self_signing_key = csKeys.self_signing;
  }

  return c.json(response);
});

// Knock protocol endpoints - users request to join a room

// GET /_matrix/federation/v1/make_knock/:roomId/:userId - Prepare knock request
// Remote servers call this to get a knock event template
app.get('/_matrix/federation/v1/make_knock/:roomId/:userId', async (c) => {
  const roomId = c.req.param('roomId');
  const userId = c.req.param('userId');
  const db = c.env.DB;

  const room = await db.prepare(
    `SELECT room_id, room_version FROM rooms WHERE room_id = ?`
  ).bind(roomId).first<{ room_id: string; room_version: string }>();

  if (!room) {
    return Errors.notFound('Room not found').toResponse();
  }

  // Get join_rules to verify room allows knocking
  const joinRulesEvent = await db.prepare(`
    SELECT e.content FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.join_rules'
  `).bind(roomId).first<{ content: string }>();

  if (joinRulesEvent) {
    const joinRules = JSON.parse(joinRulesEvent.content);
    if (joinRules.join_rule !== 'knock' && joinRules.join_rule !== 'knock_restricted') {
      return c.json({
        errcode: 'M_FORBIDDEN',
        error: 'Room does not allow knocking',
      }, 403);
    }
  } else {
    // Default join_rule is 'invite', knocking not allowed
    return c.json({
      errcode: 'M_FORBIDDEN',
      error: 'Room does not allow knocking',
    }, 403);
  }

  const membership = await db.prepare(
    `SELECT membership FROM room_memberships WHERE room_id = ? AND user_id = ?`
  ).bind(roomId, userId).first<{ membership: string }>();

  if (membership?.membership === 'ban') {
    return c.json({
      errcode: 'M_FORBIDDEN',
      error: 'User is banned from this room',
    }, 403);
  }

  if (membership?.membership === 'join') {
    return c.json({
      errcode: 'M_FORBIDDEN',
      error: 'User is already a member of this room',
    }, 403);
  }

  // Get auth events for knock
  const createEvent = await db.prepare(`
    SELECT e.event_id FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.create'
  `).bind(roomId).first<{ event_id: string }>();

  const joinRulesEventId = await db.prepare(`
    SELECT e.event_id FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.join_rules'
  `).bind(roomId).first<{ event_id: string }>();

  const powerLevelsEvent = await db.prepare(`
    SELECT e.event_id FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.power_levels'
  `).bind(roomId).first<{ event_id: string }>();

  const authEvents: string[] = [];
  if (createEvent) authEvents.push(createEvent.event_id);
  if (joinRulesEventId) authEvents.push(joinRulesEventId.event_id);
  if (powerLevelsEvent) authEvents.push(powerLevelsEvent.event_id);

  // Get latest event for prev_events
  const latestEvent = await db.prepare(
    `SELECT event_id, depth FROM events WHERE room_id = ? ORDER BY depth DESC LIMIT 1`
  ).bind(roomId).first<{ event_id: string; depth: number }>();

  const prevEvents = latestEvent ? [latestEvent.event_id] : [];
  const depth = (latestEvent?.depth || 0) + 1;

  // Create unsigned knock event template
  const eventTemplate = {
    room_id: roomId,
    sender: userId,
    type: 'm.room.member',
    state_key: userId,
    content: {
      membership: 'knock',
    },
    origin_server_ts: Date.now(),
    depth,
    auth_events: authEvents,
    prev_events: prevEvents,
  };

  return c.json({
    room_version: room.room_version,
    event: eventTemplate,
  });
});

// PUT /_matrix/federation/v1/send_knock/:roomId/:eventId - Complete knock
// Remote servers call this to finalize the knock with a signed event
app.put('/_matrix/federation/v1/send_knock/:roomId/:eventId', async (c) => {
  const roomId = c.req.param('roomId');
  const eventId = c.req.param('eventId');
  const db = c.env.DB;

  let body: any;
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  if (body.type !== 'm.room.member' || body.content?.membership !== 'knock') {
    return c.json({
      errcode: 'M_INVALID_PARAM',
      error: 'Event is not a knock event',
    }, 400);
  }

  if (body.event_id && body.event_id !== eventId) {
    return c.json({
      errcode: 'M_INVALID_PARAM',
      error: 'Event ID mismatch',
    }, 400);
  }

  const room = await db.prepare(
    `SELECT room_id FROM rooms WHERE room_id = ?`
  ).bind(roomId).first<{ room_id: string }>();

  if (!room) {
    return Errors.notFound('Room not found').toResponse();
  }

  // Verify room allows knocking
  const joinRulesEvent = await db.prepare(`
    SELECT e.content FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.join_rules'
  `).bind(roomId).first<{ content: string }>();

  if (joinRulesEvent) {
    const joinRules = JSON.parse(joinRulesEvent.content);
    if (joinRules.join_rule !== 'knock' && joinRules.join_rule !== 'knock_restricted') {
      return c.json({
        errcode: 'M_FORBIDDEN',
        error: 'Room does not allow knocking',
      }, 403);
    }
  } else {
    return c.json({
      errcode: 'M_FORBIDDEN',
      error: 'Room does not allow knocking',
    }, 403);
  }

  // Check if user is already banned or joined
  const userId = body.state_key;
  const membership = await db.prepare(
    `SELECT membership FROM room_memberships WHERE room_id = ? AND user_id = ?`
  ).bind(roomId, userId).first<{ membership: string }>();

  if (membership?.membership === 'ban') {
    return c.json({
      errcode: 'M_FORBIDDEN',
      error: 'User is banned from this room',
    }, 403);
  }

  if (membership?.membership === 'join') {
    return c.json({
      errcode: 'M_FORBIDDEN',
      error: 'User is already a member of this room',
    }, 403);
  }

  try {
    await db.prepare(`
      INSERT OR IGNORE INTO events
      (event_id, room_id, sender, event_type, state_key, content, origin_server_ts, depth, auth_events, prev_events, hashes, signatures)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(
      eventId,
      roomId,
      body.sender,
      body.type,
      body.state_key,
      JSON.stringify(body.content),
      body.origin_server_ts,
      body.depth || 0,
      JSON.stringify(body.auth_events || []),
      JSON.stringify(body.prev_events || []),
      body.hashes ? JSON.stringify(body.hashes) : null,
      body.signatures ? JSON.stringify(body.signatures) : null
    ).run();

    await db.prepare(`
      INSERT OR REPLACE INTO room_state (room_id, event_type, state_key, event_id)
      VALUES (?, ?, ?, ?)
    `).bind(roomId, body.type, body.state_key, eventId).run();

    await db.prepare(`
      INSERT OR REPLACE INTO room_memberships (room_id, user_id, membership, event_id)
      VALUES (?, ?, 'knock', ?)
    `).bind(roomId, userId, eventId).run();
  } catch (e) {
    console.error(`Failed to store knock event ${eventId}:`, e);
  }

  // Return stripped state events (room name, avatar, join_rules, canonical_alias)
  const strippedState: any[] = [];

  const nameEvent = await db.prepare(`
    SELECT e.event_type, e.state_key, e.content, e.sender, e.origin_server_ts
    FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.name'
  `).bind(roomId).first<{
    event_type: string;
    state_key: string;
    content: string;
    sender: string;
    origin_server_ts: number;
  }>();

  if (nameEvent) {
    strippedState.push({
      type: nameEvent.event_type,
      state_key: nameEvent.state_key,
      content: JSON.parse(nameEvent.content),
      sender: nameEvent.sender,
    });
  }

  const avatarEvent = await db.prepare(`
    SELECT e.event_type, e.state_key, e.content, e.sender
    FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.avatar'
  `).bind(roomId).first<{
    event_type: string;
    state_key: string;
    content: string;
    sender: string;
  }>();

  if (avatarEvent) {
    strippedState.push({
      type: avatarEvent.event_type,
      state_key: avatarEvent.state_key,
      content: JSON.parse(avatarEvent.content),
      sender: avatarEvent.sender,
    });
  }

  if (joinRulesEvent) {
    const joinRulesEventFull = await db.prepare(`
      SELECT e.event_type, e.state_key, e.content, e.sender
      FROM room_state rs
      JOIN events e ON rs.event_id = e.event_id
      WHERE rs.room_id = ? AND rs.event_type = 'm.room.join_rules'
    `).bind(roomId).first<{
      event_type: string;
      state_key: string;
      content: string;
      sender: string;
    }>();

    if (joinRulesEventFull) {
      strippedState.push({
        type: joinRulesEventFull.event_type,
        state_key: joinRulesEventFull.state_key,
        content: JSON.parse(joinRulesEventFull.content),
        sender: joinRulesEventFull.sender,
      });
    }
  }

  const aliasEvent = await db.prepare(`
    SELECT e.event_type, e.state_key, e.content, e.sender
    FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.canonical_alias'
  `).bind(roomId).first<{
    event_type: string;
    state_key: string;
    content: string;
    sender: string;
  }>();

  if (aliasEvent) {
    strippedState.push({
      type: aliasEvent.event_type,
      state_key: aliasEvent.state_key,
      content: JSON.parse(aliasEvent.content),
      sender: aliasEvent.sender,
    });
  }

  return c.json({
    knock_room_state: strippedState,
  });
});

// Federation media endpoints - serve local media to remote servers

// GET /_matrix/federation/v1/media/download/:mediaId - Download media via federation
// The response must be multipart/mixed: a JSON metadata part plus the media part.
app.get('/_matrix/federation/v1/media/download/:mediaId', async (c) => {
  const mediaId = c.req.param('mediaId');

  const object = await c.env.MEDIA.get(mediaId);
  if (!object) {
    return Errors.notFound('Media not found').toResponse();
  }

  const metadata = await c.env.DB.prepare(
    `SELECT content_type, filename FROM media WHERE media_id = ?`
  ).bind(mediaId).first<{ content_type: string; filename: string | null }>();

  const data = new Uint8Array(await object.arrayBuffer());
  return buildMultipartMediaResponse(
    data,
    metadata?.content_type || 'application/octet-stream',
    metadata?.filename || undefined
  );
});

// GET /_matrix/federation/v1/media/thumbnail/:mediaId - Get thumbnail via federation
app.get('/_matrix/federation/v1/media/thumbnail/:mediaId', async (c) => {
  const mediaId = c.req.param('mediaId');
  const width = Math.min(parseInt(c.req.query('width') || '96'), 1920);
  const height = Math.min(parseInt(c.req.query('height') || '96'), 1920);
  const method = c.req.query('method') || 'scale';

  const metadata = await c.env.DB.prepare(
    `SELECT content_type FROM media WHERE media_id = ?`
  ).bind(mediaId).first<{ content_type: string }>();

  if (!metadata) {
    return Errors.notFound('Media not found').toResponse();
  }

  const isImage = metadata.content_type.startsWith('image/');

  const thumbnailKey = `thumb_${mediaId}_${width}x${height}_${method}`;
  const existingThumb = await c.env.MEDIA.get(thumbnailKey);

  if (existingThumb) {
    const headers = new Headers();
    headers.set('Content-Type', 'image/jpeg');
    headers.set('Cache-Control', 'public, max-age=31536000, immutable');
    return new Response(existingThumb.body, { headers });
  }

  const object = await c.env.MEDIA.get(mediaId);
  if (!object) {
    return Errors.notFound('Media not found').toResponse();
  }

  // Thumbnails are not generated here: the original bytes go back as the media
  // part of the required multipart/mixed response and the requester scales them.
  void isImage;
  const data = new Uint8Array(await object.arrayBuffer());
  return buildMultipartMediaResponse(data, metadata.content_type);
});

// Federation public rooms directory

// GET /_matrix/federation/v1/publicRooms - Get public rooms
app.get('/_matrix/federation/v1/publicRooms', async (c) => {
  const db = c.env.DB;
  const serverName = c.env.SERVER_NAME;

  const limit = Math.min(parseInt(c.req.query('limit') || '100'), 500);
  const since = c.req.query('since');
  // include_all_networks is not implemented
  void c.req.query('include_all_networks');

  // Parse since token for pagination (format: "offset_N")
  let offset = 0;
  if (since && since.startsWith('offset_')) {
    offset = parseInt(since.substring(7), 10) || 0;
  }

  const rooms = await db.prepare(`
    SELECT r.room_id
    FROM rooms r
    WHERE r.is_public = 1
    ORDER BY r.created_at DESC
    LIMIT ? OFFSET ?
  `).bind(limit + 1, offset).all<{ room_id: string }>();

  const hasMore = rooms.results.length > limit;
  const roomResults = rooms.results.slice(0, limit);

  const chunks: any[] = [];

  for (const room of roomResults) {
    const roomInfo = await getRoomPublicInfo(db, room.room_id, serverName);
    if (roomInfo) {
      chunks.push(roomInfo);
    }
  }

  const totalCount = await db.prepare(`
    SELECT COUNT(*) as count FROM rooms WHERE is_public = 1
  `).first<{ count: number }>();

  const response: any = {
    chunk: chunks,
    total_room_count_estimate: totalCount?.count || 0,
  };

  if (hasMore) {
    response.next_batch = `offset_${offset + limit}`;
  }
  if (offset > 0) {
    response.prev_batch = `offset_${Math.max(0, offset - limit)}`;
  }

  return c.json(response);
});

// POST /_matrix/federation/v1/publicRooms - Search public rooms
app.post('/_matrix/federation/v1/publicRooms', async (c) => {
  const db = c.env.DB;
  const serverName = c.env.SERVER_NAME;

  let body: {
    limit?: number;
    since?: string;
    filter?: { generic_search_term?: string };
    include_all_networks?: boolean;
  };

  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const limit = Math.min(body.limit || 100, 500);
  const since = body.since;
  const searchTerm = body.filter?.generic_search_term?.toLowerCase();

  let offset = 0;
  if (since && since.startsWith('offset_')) {
    offset = parseInt(since.substring(7), 10) || 0;
  }

  let rooms;
  if (searchTerm) {
    rooms = await db.prepare(`
      SELECT DISTINCT r.room_id
      FROM rooms r
      LEFT JOIN room_state rs_name ON rs_name.room_id = r.room_id AND rs_name.event_type = 'm.room.name'
      LEFT JOIN events e_name ON rs_name.event_id = e_name.event_id
      LEFT JOIN room_state rs_topic ON rs_topic.room_id = r.room_id AND rs_topic.event_type = 'm.room.topic'
      LEFT JOIN events e_topic ON rs_topic.event_id = e_topic.event_id
      LEFT JOIN room_aliases ra ON ra.room_id = r.room_id
      WHERE r.is_public = 1
        AND (
          LOWER(e_name.content) LIKE ?
          OR LOWER(e_topic.content) LIKE ?
          OR LOWER(ra.alias) LIKE ?
        )
      ORDER BY r.created_at DESC
      LIMIT ? OFFSET ?
    `).bind(`%${searchTerm}%`, `%${searchTerm}%`, `%${searchTerm}%`, limit + 1, offset).all<{ room_id: string }>();
  } else {
    rooms = await db.prepare(`
      SELECT r.room_id
      FROM rooms r
      WHERE r.is_public = 1
      ORDER BY r.created_at DESC
      LIMIT ? OFFSET ?
    `).bind(limit + 1, offset).all<{ room_id: string }>();
  }

  const hasMore = rooms.results.length > limit;
  const roomResults = rooms.results.slice(0, limit);

  const chunks: any[] = [];

  for (const room of roomResults) {
    const roomInfo = await getRoomPublicInfo(db, room.room_id, serverName);
    if (roomInfo) {
      chunks.push(roomInfo);
    }
  }

  const totalCount = await db.prepare(`
    SELECT COUNT(*) as count FROM rooms WHERE is_public = 1
  `).first<{ count: number }>();

  const response: any = {
    chunk: chunks,
    total_room_count_estimate: totalCount?.count || 0,
  };

  if (hasMore) {
    response.next_batch = `offset_${offset + limit}`;
  }
  if (offset > 0) {
    response.prev_batch = `offset_${Math.max(0, offset - limit)}`;
  }

  return c.json(response);
});

// Builds a public rooms directory entry for a room
async function getRoomPublicInfo(db: D1Database, roomId: string, _serverName: string): Promise<any> {
  const nameEvent = await db.prepare(`
    SELECT e.content FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.name'
  `).bind(roomId).first<{ content: string }>();

  const topicEvent = await db.prepare(`
    SELECT e.content FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.topic'
  `).bind(roomId).first<{ content: string }>();

  const aliasEvent = await db.prepare(`
    SELECT e.content FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.canonical_alias'
  `).bind(roomId).first<{ content: string }>();

  const avatarEvent = await db.prepare(`
    SELECT e.content FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.avatar'
  `).bind(roomId).first<{ content: string }>();

  const joinRuleEvent = await db.prepare(`
    SELECT e.content FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.join_rules'
  `).bind(roomId).first<{ content: string }>();

  const historyEvent = await db.prepare(`
    SELECT e.content FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.history_visibility'
  `).bind(roomId).first<{ content: string }>();

  const guestEvent = await db.prepare(`
    SELECT e.content FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.guest_access'
  `).bind(roomId).first<{ content: string }>();

  const memberCount = await db.prepare(`
    SELECT COUNT(*) as count FROM room_memberships WHERE room_id = ? AND membership = 'join'
  `).bind(roomId).first<{ count: number }>();

  const createEvent = await db.prepare(`
    SELECT e.content FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.create'
  `).bind(roomId).first<{ content: string }>();

  let roomType: string | undefined;
  if (createEvent) {
    try {
      const content = JSON.parse(createEvent.content);
      roomType = content.type;
    } catch {}
  }

  let historyVisibility = 'shared';
  if (historyEvent) {
    try {
      historyVisibility = JSON.parse(historyEvent.content).history_visibility;
    } catch {}
  }

  let guestAccess = false;
  if (guestEvent) {
    try {
      guestAccess = JSON.parse(guestEvent.content).guest_access === 'can_join';
    } catch {}
  }

  return {
    room_id: roomId,
    name: nameEvent ? JSON.parse(nameEvent.content).name : undefined,
    topic: topicEvent ? JSON.parse(topicEvent.content).topic : undefined,
    canonical_alias: aliasEvent ? JSON.parse(aliasEvent.content).alias : undefined,
    avatar_url: avatarEvent ? JSON.parse(avatarEvent.content).url : undefined,
    join_rule: joinRuleEvent ? JSON.parse(joinRuleEvent.content).join_rule : 'invite',
    num_joined_members: memberCount?.count || 0,
    world_readable: historyVisibility === 'world_readable',
    guest_can_join: guestAccess,
    room_type: roomType,
  };
}

// Federation space hierarchy

// GET /_matrix/federation/v1/hierarchy/:roomId - Get space hierarchy
app.get('/_matrix/federation/v1/hierarchy/:roomId', async (c) => {
  const roomId = c.req.param('roomId');
  const db = c.env.DB;
  const serverName = c.env.SERVER_NAME;

  const suggestedOnly = c.req.query('suggested_only') === 'true';
  const limit = Math.min(parseInt(c.req.query('limit') || '50'), 100);
  const from = c.req.query('from');

  const room = await db.prepare(`
    SELECT room_id FROM rooms WHERE room_id = ?
  `).bind(roomId).first();

  if (!room) {
    return Errors.notFound('Room not found').toResponse();
  }

  let offset = 0;
  if (from && from.startsWith('offset_')) {
    offset = parseInt(from.substring(7), 10) || 0;
  }

  // Get space children from m.space.child state events
  const childEvents = await db.prepare(`
    SELECT rs.state_key, e.content
    FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.space.child'
    LIMIT ? OFFSET ?
  `).bind(roomId, limit + 1, offset).all<{ state_key: string; content: string }>();

  const hasMore = childEvents.results.length > limit;
  const childResults = childEvents.results.slice(0, limit);

  const rooms: any[] = [];

  // Add the space itself first (only on first page)
  if (offset === 0) {
    const spaceInfo = await getRoomPublicInfo(db, roomId, serverName);
    if (spaceInfo) {
      const spaceChildState = await db.prepare(`
        SELECT rs.state_key, e.content
        FROM room_state rs
        JOIN events e ON rs.event_id = e.event_id
        WHERE rs.room_id = ? AND rs.event_type = 'm.space.child'
      `).bind(roomId).all<{ state_key: string; content: string }>();

      rooms.push({
        ...spaceInfo,
        children_state: spaceChildState.results.map(ce => {
          try {
            return {
              type: 'm.space.child',
              state_key: ce.state_key,
              content: JSON.parse(ce.content),
            };
          } catch {
            return null;
          }
        }).filter(Boolean),
      });
    }
  }

  for (const child of childResults) {
    const childRoomId = child.state_key;

    try {
      const content = JSON.parse(child.content);

      if (suggestedOnly && !content.suggested) {
        continue;
      }

      // Skip if content has empty via array (deleted child)
      if (!content.via || content.via.length === 0) {
        continue;
      }

      const childInfo = await getRoomPublicInfo(db, childRoomId, serverName);
      if (childInfo) {
        rooms.push({
          ...childInfo,
          children_state: [], // Grandchildren not included in federation response
        });
      }
    } catch {
      // Skip invalid child entries
    }
  }

  const response: any = {
    room: rooms[0] || null,
    children: rooms.slice(1),
    inaccessible_children: [],
  };

  if (hasMore) {
    response.next_batch = `offset_${offset + limit}`;
  }

  return c.json(response);
});

// Miscellaneous federation endpoints

// GET /_matrix/federation/v1/timestamp_to_event/:roomId - Find event closest to timestamp
app.get('/_matrix/federation/v1/timestamp_to_event/:roomId', async (c) => {
  const roomId = c.req.param('roomId');
  const ts = parseInt(c.req.query('ts') || '0', 10);
  const dir = c.req.query('dir') || 'f'; // 'f' = forward, 'b' = backward
  const db = c.env.DB;

  if (!ts || ts <= 0) {
    return Errors.missingParam('ts').toResponse();
  }

  const room = await db.prepare(
    `SELECT room_id FROM rooms WHERE room_id = ?`
  ).bind(roomId).first<{ room_id: string }>();

  if (!room) {
    return Errors.notFound('Room not found').toResponse();
  }

  let event;
  if (dir === 'b') {
    // Find closest event at or before timestamp
    event = await db.prepare(`
      SELECT event_id, origin_server_ts
      FROM events
      WHERE room_id = ? AND origin_server_ts <= ?
      ORDER BY origin_server_ts DESC
      LIMIT 1
    `).bind(roomId, ts).first<{ event_id: string; origin_server_ts: number }>();
  } else {
    // Find closest event at or after timestamp
    event = await db.prepare(`
      SELECT event_id, origin_server_ts
      FROM events
      WHERE room_id = ? AND origin_server_ts >= ?
      ORDER BY origin_server_ts ASC
      LIMIT 1
    `).bind(roomId, ts).first<{ event_id: string; origin_server_ts: number }>();
  }

  if (!event) {
    return Errors.notFound('No event found near timestamp').toResponse();
  }

  return c.json({
    event_id: event.event_id,
    origin_server_ts: event.origin_server_ts,
  });
});

// GET /_matrix/federation/v1/openid/userinfo - Validate OpenID token and return user info
app.get('/_matrix/federation/v1/openid/userinfo', async (c) => {
  const accessToken = c.req.query('access_token');

  if (!accessToken) {
    return Errors.missingParam('access_token').toResponse();
  }

  const tokenData = await c.env.SESSIONS.get(`openid:${accessToken}`, 'json') as {
    user_id: string;
    expires_at: number;
  } | null;

  if (!tokenData) {
    return c.json({
      errcode: 'M_UNKNOWN_TOKEN',
      error: 'Invalid or expired OpenID token',
    }, 401);
  }

  if (Date.now() > tokenData.expires_at) {
    await c.env.SESSIONS.delete(`openid:${accessToken}`);
    return c.json({
      errcode: 'M_UNKNOWN_TOKEN',
      error: 'OpenID token has expired',
    }, 401);
  }

  return c.json({
    sub: tokenData.user_id,
  });
});

// PUT /_matrix/federation/v1/exchange_third_party_invite/:roomId - Exchange 3PID invite
// Handles third-party invites when a user accepts an invite via their verified email/phone
app.put('/_matrix/federation/v1/exchange_third_party_invite/:roomId', async (c) => {
  const roomId = c.req.param('roomId');
  const db = c.env.DB;
  const serverName = c.env.SERVER_NAME;

  let body: {
    type: string;
    room_id: string;
    sender: string;
    state_key: string;
    content: {
      membership: string;
      third_party_invite?: {
        display_name?: string;
        signed: {
          mxid: string;
          token: string;
          signatures: Record<string, Record<string, string>>;
        };
      };
    };
    origin_server_ts?: number;
    depth?: number;
    auth_events?: string[];
    prev_events?: string[];
    event_id?: string;
    signatures?: Record<string, Record<string, string>>;
  };

  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  if (body.type !== 'm.room.member' || body.content?.membership !== 'invite') {
    return c.json({
      errcode: 'M_INVALID_PARAM',
      error: 'Event must be a membership invite',
    }, 400);
  }

  if (body.room_id !== roomId) {
    return c.json({
      errcode: 'M_INVALID_PARAM',
      error: 'Room ID mismatch',
    }, 400);
  }

  const thirdPartyInvite = body.content.third_party_invite;
  if (!thirdPartyInvite || !thirdPartyInvite.signed) {
    return c.json({
      errcode: 'M_INVALID_PARAM',
      error: 'Missing third_party_invite or signed data',
    }, 400);
  }

  const { mxid, token, signatures } = thirdPartyInvite.signed;
  if (!mxid || !token || !signatures) {
    return c.json({
      errcode: 'M_INVALID_PARAM',
      error: 'Incomplete signed data in third_party_invite',
    }, 400);
  }

  const room = await db.prepare(`
    SELECT room_id, room_version FROM rooms WHERE room_id = ?
  `).bind(roomId).first<{ room_id: string; room_version: string }>();

  if (!room) {
    return Errors.notFound('Room not found').toResponse();
  }

  // Find the matching m.room.third_party_invite state event
  const thirdPartyInviteEvent = await db.prepare(`
    SELECT e.event_id, e.content, e.sender, e.state_key
    FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.third_party_invite' AND rs.state_key = ?
  `).bind(roomId, token).first<{
    event_id: string;
    content: string;
    sender: string;
    state_key: string;
  }>();

  if (!thirdPartyInviteEvent) {
    return c.json({
      errcode: 'M_FORBIDDEN',
      error: 'No third party invite found with matching token',
    }, 403);
  }

  // Parse the third-party invite content to get the public keys
  let inviteContent: {
    display_name?: string;
    key_validity_url?: string;
    public_key?: string;
    public_keys?: Array<{ public_key: string; key_validity_url?: string }>;
  };

  try {
    inviteContent = JSON.parse(thirdPartyInviteEvent.content);
  } catch {
    return c.json({
      errcode: 'M_INVALID_PARAM',
      error: 'Invalid third party invite content',
    }, 400);
  }

  // The signed object is { mxid, sender, token, signatures }, where sender comes
  // from the original third_party_invite event rather than the request body.
  const signedDataForVerification: Record<string, unknown> = {
    mxid,
    sender: thirdPartyInviteEvent.sender,
    token,
    signatures,
  };

  let signatureValid = false;
  const publicKeys = inviteContent.public_keys || [];
  if (inviteContent.public_key) {
    publicKeys.push({ public_key: inviteContent.public_key });
  }

  for (const keyInfo of publicKeys) {
    const publicKey = keyInfo.public_key;
    if (!publicKey) continue;

    // Signatures are keyed by server name, typically the identity server
    for (const [signingServer, keySignatures] of Object.entries(signatures)) {
      for (const [keyId, signature] of Object.entries(keySignatures)) {
        if (!signature) continue;

        try {
          const isValid = await verifySignature(
            signedDataForVerification,
            signingServer,
            keyId,
            publicKey
          );

          if (isValid) {
            signatureValid = true;
            break;
          }
        } catch (e) {
          console.warn(`Failed to verify signature from ${signingServer}:${keyId}:`, e);
        }
      }
      if (signatureValid) break;
    }
    if (signatureValid) break;
  }

  if (!signatureValid) {
    return c.json({
      errcode: 'M_FORBIDDEN',
      error: 'Could not verify third party invite signature',
    }, 403);
  }

  if (mxid !== body.state_key) {
    return c.json({
      errcode: 'M_INVALID_PARAM',
      error: 'mxid does not match state_key',
    }, 400);
  }

  const key = await db.prepare(
    `SELECT key_id, private_key_jwk FROM server_keys WHERE is_current = 1 AND key_version = 2`
  ).first<{ key_id: string; private_key_jwk: string | null }>();

  if (!key || !key.private_key_jwk) {
    return c.json({
      errcode: 'M_UNKNOWN',
      error: 'Server signing key not configured',
    }, 500);
  }

  // Get auth events for the invite
  const createEvent = await db.prepare(`
    SELECT e.event_id FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.create'
  `).bind(roomId).first<{ event_id: string }>();

  const joinRulesEvent = await db.prepare(`
    SELECT e.event_id FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.join_rules'
  `).bind(roomId).first<{ event_id: string }>();

  const powerLevelsEvent = await db.prepare(`
    SELECT e.event_id FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.power_levels'
  `).bind(roomId).first<{ event_id: string }>();

  const senderMembershipEvent = await db.prepare(`
    SELECT e.event_id FROM room_state rs
    JOIN events e ON rs.event_id = e.event_id
    WHERE rs.room_id = ? AND rs.event_type = 'm.room.member' AND rs.state_key = ?
  `).bind(roomId, body.sender).first<{ event_id: string }>();

  const authEvents: string[] = [];
  if (createEvent) authEvents.push(createEvent.event_id);
  if (joinRulesEvent) authEvents.push(joinRulesEvent.event_id);
  if (powerLevelsEvent) authEvents.push(powerLevelsEvent.event_id);
  if (senderMembershipEvent) authEvents.push(senderMembershipEvent.event_id);
  authEvents.push(thirdPartyInviteEvent.event_id);

  // Get latest event for prev_events
  const latestEvent = await db.prepare(
    `SELECT event_id, depth FROM events WHERE room_id = ? ORDER BY depth DESC LIMIT 1`
  ).bind(roomId).first<{ event_id: string; depth: number }>();

  const prevEvents = latestEvent ? [latestEvent.event_id] : [];
  const depth = (latestEvent?.depth || 0) + 1;
  const originServerTs = Date.now();

  const inviteEvent = {
    room_id: roomId,
    sender: body.sender,
    type: 'm.room.member',
    state_key: mxid,
    content: {
      membership: 'invite',
      third_party_invite: {
        display_name: inviteContent.display_name || thirdPartyInvite.display_name,
        signed: thirdPartyInvite.signed,
      },
    },
    origin_server_ts: originServerTs,
    depth,
    auth_events: authEvents,
    prev_events: prevEvents,
  };

  // With no event_id in the request, fall back to a local hash of the event -
  // this is not the room-version reference hash.
  const eventIdHash = await sha256(JSON.stringify({
    ...inviteEvent,
    origin: serverName,
  }));
  const eventId = body.event_id || `$${eventIdHash}`;

  const signedEvent = await signJson(
    { ...inviteEvent, event_id: eventId },
    serverName,
    key.key_id,
    JSON.parse(key.private_key_jwk)
  );

  try {
    await db.prepare(`
      INSERT OR IGNORE INTO events
      (event_id, room_id, sender, event_type, state_key, content, origin_server_ts, depth, auth_events, prev_events, signatures)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).bind(
      eventId,
      roomId,
      body.sender,
      'm.room.member',
      mxid,
      JSON.stringify(inviteEvent.content),
      originServerTs,
      depth,
      JSON.stringify(authEvents),
      JSON.stringify(prevEvents),
      JSON.stringify((signedEvent as any).signatures)
    ).run();

    await db.prepare(`
      INSERT OR REPLACE INTO room_state (room_id, event_type, state_key, event_id)
      VALUES (?, 'm.room.member', ?, ?)
    `).bind(roomId, mxid, eventId).run();

    await db.prepare(`
      INSERT OR REPLACE INTO room_memberships (room_id, user_id, membership, event_id, display_name)
      VALUES (?, ?, 'invite', ?, ?)
    `).bind(roomId, mxid, eventId, inviteContent.display_name || null).run();

    // Delete the third party invite state event (it's been consumed)
    await db.prepare(`
      DELETE FROM room_state
      WHERE room_id = ? AND event_type = 'm.room.third_party_invite' AND state_key = ?
    `).bind(roomId, token).run();
  } catch (e) {
    console.error('Failed to store third party invite exchange event:', e);
    return c.json({
      errcode: 'M_UNKNOWN',
      error: 'Failed to store invite event',
    }, 500);
  }

  return c.json({});
});

export default app;
