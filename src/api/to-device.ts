// To-device messaging: E2EE key exchange (m.room_key), device verification
// (m.key.verification.*) and other direct device-to-device events, delivered to the
// recipient through /sync and the sliding sync to_device extension.
// Spec: https://spec.matrix.org/v1.12/client-server-api/#send-to-device-messaging

import { Hono } from 'hono';
import type { AppEnv, Env } from '../types';
import { Errors } from '../utils/errors';
import { requireAuth } from '../middleware/auth';

const app = new Hono<AppEnv>();

interface ToDeviceRequest {
  messages: Record<string, Record<string, any>>;
  // messages[user_id][device_id] = content
  // device_id can be "*" to send to all devices
}

async function getNextStreamPosition(db: D1Database, streamName: string): Promise<number> {
  // Atomic UPDATE with RETURNING - no race condition
  const result = await db.prepare(`
    UPDATE stream_positions
    SET position = position + 1
    WHERE stream_name = ?
    RETURNING position
  `).bind(streamName).first<{ position: number }>();

  if (result) {
    return result.position;
  }

  // Row doesn't exist - atomic upsert (edge case, should be created by migration)
  const upsertResult = await db.prepare(`
    INSERT INTO stream_positions (stream_name, position)
    VALUES (?, 1)
    ON CONFLICT (stream_name) DO UPDATE SET position = position + 1
    RETURNING position
  `).bind(streamName).first<{ position: number }>();

  return upsertResult?.position ?? 1;
}

// Wake each recipient's SyncDurableObject so long-polling /sync and sliding sync
// return the new messages immediately rather than after the ~25s poll timeout.
export async function wakeSyncForUsers(env: Env, userIds: Iterable<string>): Promise<void> {
  const wakes = [...new Set(userIds)].map(async (recipientUserId) => {
    try {
      const syncDO = env.SYNC.get(env.SYNC.idFromName(recipientUserId));
      await syncDO.fetch(new Request('http://internal/wake', { method: 'POST' }));
    } catch (error) {
      // Delivery still happens on the next poll cycle - don't fail the send
      console.error('[to-device] Failed to wake sync for', recipientUserId, error);
    }
  });
  await Promise.all(wakes);
}

async function getUserDevices(db: D1Database, userId: string): Promise<string[]> {
  const devices = await db.prepare(`
    SELECT device_id FROM devices WHERE user_id = ?
  `).bind(userId).all<{ device_id: string }>();

  return devices.results.map(d => d.device_id);
}

// PUT /sendToDevice/:eventType/:txnId - Send to-device messages
app.put('/_matrix/client/v3/sendToDevice/:eventType/:txnId', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const eventType = c.req.param('eventType');
  const txnId = c.req.param('txnId');
  const db = c.env.DB;

  // Check for duplicate transaction
  const existingTxn = await db.prepare(`
    SELECT response FROM transaction_ids WHERE user_id = ? AND txn_id = ?
  `).bind(userId, txnId).first<{ response: string }>();

  if (existingTxn) {
    return c.json(JSON.parse(existingTxn.response || '{}'));
  }

  let body: ToDeviceRequest;
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  if (!body.messages) {
    return Errors.missingParam('messages').toResponse();
  }

  const localServer = c.env.SERVER_NAME;
  const localRecipients: string[] = [];
  // Remote deliveries grouped by destination server, preserving the original
  // per-user/per-device structure for the m.direct_to_device EDU.
  const remoteByServer: Record<string, Record<string, Record<string, any>>> = {};

  for (const [recipientUserId, deviceMessages] of Object.entries(body.messages)) {
    const recipientServer = recipientUserId.slice(recipientUserId.indexOf(':') + 1);

    // Remote recipient: batch into an m.direct_to_device EDU per destination.
    // Their devices live on their own server - we do not resolve '*' here.
    if (recipientServer && recipientServer !== localServer) {
      if (!remoteByServer[recipientServer]) remoteByServer[recipientServer] = {};
      remoteByServer[recipientServer][recipientUserId] = deviceMessages;
      continue;
    }

    // Local recipient: store per-device for delivery via the recipient's /sync
    localRecipients.push(recipientUserId);
    for (const [deviceId, content] of Object.entries(deviceMessages)) {
      const targetDevices = deviceId === '*'
        ? await getUserDevices(db, recipientUserId)
        : [deviceId];

      for (const targetDeviceId of targetDevices) {
        const streamPosition = await getNextStreamPosition(db, 'to_device');
        const messageId = `${userId}_${txnId}_${recipientUserId}_${targetDeviceId}_${Date.now()}`;

        await db.prepare(`
          INSERT INTO to_device_messages (
            recipient_user_id, recipient_device_id, sender_user_id,
            event_type, content, message_id, stream_position
          ) VALUES (?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT (recipient_user_id, recipient_device_id, message_id) DO NOTHING
        `).bind(
          recipientUserId,
          targetDeviceId,
          userId,
          eventType,
          JSON.stringify(content),
          messageId,
          streamPosition
        ).run();
      }
    }
  }

  // Store transaction for idempotency
  await db.prepare(`
    INSERT INTO transaction_ids (user_id, txn_id, response)
    VALUES (?, ?, '{}')
    ON CONFLICT (user_id, txn_id) DO NOTHING
  `).bind(userId, txnId).run();

  // Wake local recipients' long-polling sync (messages are committed to D1)
  if (localRecipients.length > 0) {
    c.executionCtx.waitUntil(wakeSyncForUsers(c.env, localRecipients));
  }

  // Fan out to remote servers as m.direct_to_device EDUs (E2EE key sharing).
  // Each destination gets only its own users' messages.
  const remoteServers = Object.keys(remoteByServer);
  if (remoteServers.length > 0) {
    c.executionCtx.waitUntil((async () => {
      const { sendEduToServer } = await import('../services/federation-sender');
      await Promise.all(remoteServers.map(server =>
        sendEduToServer(c.env, server, 'm.direct_to_device', {
          sender: userId,
          type: eventType,
          message_id: `${userId}_${txnId}`,
          messages: remoteByServer[server],
        })
      ));
    })());
  }

  return c.json({});
});

// Pending to-device messages for one device, plus the to-device token to hand back.
export async function getToDeviceMessages(
  db: D1Database,
  userId: string,
  deviceId: string,
  since?: string,
  limit: number = 100
): Promise<{ events: any[]; nextBatch: string }> {
  // The since token is a to-device stream position: those stay well below 1e9, while a
  // client that sends epoch milliseconds instead is above 1e12.
  let sincePos = 0;
  if (since) {
    const parsed = parseInt(since);
    if (!isNaN(parsed) && parsed > 0 && parsed < 1000000000) {
      sincePos = parsed;
    } else if (parsed >= 1000000000) {
      // Timestamp-shaped token: ignore it and treat this as a first sync
      console.log('[to-device] Ignoring invalid since token (looks like timestamp):', since);
    }
  }

  // Read the pending messages before acknowledging anything, so a message can never be
  // marked delivered without having been returned to the client.
  const messages = await db.prepare(`
    SELECT id, sender_user_id, event_type, content, stream_position
    FROM to_device_messages
    WHERE recipient_user_id = ?
      AND recipient_device_id = ?
      AND delivered = 0
      AND stream_position > ?
    ORDER BY stream_position ASC
    LIMIT ?
  `).bind(userId, deviceId, sincePos, limit).all<{
    id: number;
    sender_user_id: string;
    event_type: string;
    content: string;
    stream_position: number;
  }>();

  // By echoing sincePos the client confirms receipt of everything up to it.
  if (sincePos > 0) {
    const ackResult = await db.prepare(`
      UPDATE to_device_messages
      SET delivered = 1
      WHERE recipient_user_id = ?
        AND recipient_device_id = ?
        AND stream_position <= ?
        AND delivered = 0
    `).bind(userId, deviceId, sincePos).run();

    if (ackResult.meta.changes > 0) {
      console.log('[to-device] Acknowledged', ackResult.meta.changes, 'messages up to position', sincePos, 'for', userId, 'device:', deviceId);
    }
  }

  if (messages.results.length > 0) {
    console.log('[to-device] Returning', messages.results.length, 'messages to', userId, 'device:', deviceId);
    for (const msg of messages.results) {
      console.log('[to-device]   -', msg.event_type, 'from', msg.sender_user_id, 'pos:', msg.stream_position);
    }
  }

  const events = messages.results.map(msg => ({
    sender: msg.sender_user_id,
    type: msg.event_type,
    content: JSON.parse(msg.content),
  }));

  // Advance only to the highest position actually returned, and leave the token alone on
  // an empty response: a token past messages the client never got would acknowledge them.
  let nextBatch: string;
  if (messages.results.length > 0) {
    const maxReturnedPos = Math.max(...messages.results.map(m => m.stream_position));
    nextBatch = String(maxReturnedPos);
  } else {
    nextBatch = String(sincePos);
  }

  return { events, nextBatch };
}

// Drop delivered messages older than maxAgeMs; called periodically.
export async function cleanupOldToDeviceMessages(db: D1Database, maxAgeMs: number = 7 * 24 * 60 * 60 * 1000): Promise<number> {
  const cutoff = Date.now() - maxAgeMs;

  const result = await db.prepare(`
    DELETE FROM to_device_messages WHERE created_at < ? AND delivered = 1
  `).bind(cutoff).run();

  return result.meta.changes || 0;
}

export default app;
