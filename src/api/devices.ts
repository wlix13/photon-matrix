// Device management: listing, renaming and deleting a user's devices/sessions.
// Spec: https://spec.matrix.org/v1.12/client-server-api/#device-management

import { Hono } from 'hono';
import type { AppEnv, Env } from '../types';
import { Errors } from '../utils/errors';
import { requireAuth } from '../middleware/auth';
import { verifyPassword } from '../utils/crypto';
import { recordKeyChange } from './keys';
import { wakeSyncForUsers } from './to-device';

const app = new Hono<AppEnv>();

// Remove every trace of a device: D1 rows plus the E2EE key stores. Device keys live in
// the UserKeys Durable Object (primary) and the DEVICE_KEYS/ONE_TIME_KEYS KV namespaces
// (cache), not in a D1 table.
export async function purgeDevice(env: Env, db: D1Database, userId: string, deviceId: string): Promise<void> {
  await db.prepare(`
    DELETE FROM access_tokens WHERE user_id = ? AND device_id = ?
  `).bind(userId, deviceId).run();
  await db.prepare(`
    DELETE FROM one_time_keys WHERE user_id = ? AND device_id = ?
  `).bind(userId, deviceId).run();
  await db.prepare(`
    DELETE FROM fallback_keys WHERE user_id = ? AND device_id = ?
  `).bind(userId, deviceId).run();
  await db.prepare(`
    DELETE FROM to_device_messages WHERE recipient_user_id = ? AND recipient_device_id = ?
  `).bind(userId, deviceId).run();
  await db.prepare(`
    DELETE FROM devices WHERE user_id = ? AND device_id = ?
  `).bind(userId, deviceId).run();

  const stub = env.USER_KEYS.get(env.USER_KEYS.idFromName(userId));
  await stub.fetch(new Request('http://internal/device-keys/delete', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ device_id: deviceId }),
  }));
  await env.DEVICE_KEYS.delete(`device:${userId}:${deviceId}`);
  await env.ONE_TIME_KEYS.delete(`otk:${userId}:${deviceId}`);

  // Record on the device-list stream so other sessions see the device vanish
  await recordKeyChange(db, userId, deviceId, 'delete');
}

// Validate the UIA auth dict for device deletion. Returns a Response to send
// back on failure, or null when authentication succeeded.
async function checkDeleteAuth(
  db: D1Database,
  userId: string,
  auth: { type?: string; password?: string; session?: string },
): Promise<Response | null> {
  // Only the password stage is offered, so any other type is unrecognized and rejected
  // rather than treated as authenticated.
  if (auth.type !== 'm.login.password') {
    return Response.json({
      errcode: 'M_UNRECOGNIZED',
      error: `Unrecognized authentication type: ${auth.type}`,
    }, { status: 400 });
  }

  const user = await db.prepare(`
    SELECT password_hash FROM users WHERE user_id = ?
  `).bind(userId).first<{ password_hash: string | null }>();

  if (!user?.password_hash || !auth.password) {
    return Errors.forbidden('Invalid password').toResponse();
  }

  const valid = await verifyPassword(auth.password, user.password_hash);
  if (!valid) {
    return Errors.forbidden('Invalid password').toResponse();
  }

  return null;
}

// GET /_matrix/client/v3/devices - List all devices
app.get('/_matrix/client/v3/devices', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const db = c.env.DB;

  const devices = await db.prepare(`
    SELECT device_id, display_name, last_seen_ts, last_seen_ip
    FROM devices
    WHERE user_id = ?
  `).bind(userId).all<{
    device_id: string;
    display_name: string | null;
    last_seen_ts: number | null;
    last_seen_ip: string | null;
  }>();

  return c.json({
    devices: devices.results.map(d => ({
      device_id: d.device_id,
      display_name: d.display_name || undefined,
      last_seen_ts: d.last_seen_ts || undefined,
      last_seen_ip: d.last_seen_ip || undefined,
    })),
  });
});

// GET /_matrix/client/v3/devices/:deviceId - Get a specific device
app.get('/_matrix/client/v3/devices/:deviceId', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const deviceId = c.req.param('deviceId');
  const db = c.env.DB;

  const device = await db.prepare(`
    SELECT device_id, display_name, last_seen_ts, last_seen_ip
    FROM devices
    WHERE user_id = ? AND device_id = ?
  `).bind(userId, deviceId).first<{
    device_id: string;
    display_name: string | null;
    last_seen_ts: number | null;
    last_seen_ip: string | null;
  }>();

  if (!device) {
    return Errors.notFound('Device not found').toResponse();
  }

  return c.json({
    device_id: device.device_id,
    display_name: device.display_name || undefined,
    last_seen_ts: device.last_seen_ts || undefined,
    last_seen_ip: device.last_seen_ip || undefined,
  });
});

// PUT /_matrix/client/v3/devices/:deviceId - Update device info
app.put('/_matrix/client/v3/devices/:deviceId', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const deviceId = c.req.param('deviceId');
  const db = c.env.DB;

  let body: { display_name?: string };
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  // Check device exists and belongs to user
  const device = await db.prepare(`
    SELECT device_id FROM devices WHERE user_id = ? AND device_id = ?
  `).bind(userId, deviceId).first();

  if (!device) {
    return Errors.notFound('Device not found').toResponse();
  }

  if (body.display_name !== undefined) {
    await db.prepare(`
      UPDATE devices SET display_name = ? WHERE user_id = ? AND device_id = ?
    `).bind(body.display_name, userId, deviceId).run();
  }

  return c.json({});
});

// DELETE /_matrix/client/v3/devices/:deviceId - Delete a device
app.delete('/_matrix/client/v3/devices/:deviceId', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const deviceId = c.req.param('deviceId');
  // The requesting device is not treated specially - a session may delete itself
  void c.get('deviceId');
  const db = c.env.DB;

  // Check device exists and belongs to user
  const device = await db.prepare(`
    SELECT device_id FROM devices WHERE user_id = ? AND device_id = ?
  `).bind(userId, deviceId).first();

  if (!device) {
    return Errors.notFound('Device not found').toResponse();
  }

  // Try to get auth from body for UIA
  let auth: { type?: string; password?: string; session?: string } | undefined;
  try {
    const body = await c.req.json();
    auth = body.auth;
  } catch {
    // No auth provided
  }

  // If no auth provided, return UIA response
  if (!auth) {
    const sessionId = crypto.randomUUID();
    return c.json({
      flows: [{ stages: ['m.login.password'] }],
      params: {},
      session: sessionId,
    }, 401);
  }

  // Verify the auth dict (rejects unrecognized types and bad passwords)
  const authError = await checkDeleteAuth(db, userId, auth);
  if (authError) {
    return authError;
  }

  await purgeDevice(c.env, db, userId, deviceId);

  // Wake the user's other sessions so the device list updates promptly
  c.executionCtx.waitUntil(wakeSyncForUsers(c.env, [userId]));

  return c.json({});
});

// POST /_matrix/client/v3/delete_devices - Delete multiple devices
app.post('/_matrix/client/v3/delete_devices', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const db = c.env.DB;

  let body: { devices: string[]; auth?: { type?: string; password?: string; session?: string } };
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  if (!body.devices || !Array.isArray(body.devices)) {
    return Errors.missingParam('devices').toResponse();
  }

  // If no auth provided, return UIA response
  if (!body.auth) {
    const sessionId = crypto.randomUUID();
    return c.json({
      flows: [{ stages: ['m.login.password'] }],
      params: {},
      session: sessionId,
    }, 401);
  }

  // Verify the auth dict (rejects unrecognized types and bad passwords)
  const authError = await checkDeleteAuth(db, userId, body.auth);
  if (authError) {
    return authError;
  }

  // Delete each device, but only ones that belong to this user
  for (const deviceId of body.devices) {
    const device = await db.prepare(`
      SELECT device_id FROM devices WHERE user_id = ? AND device_id = ?
    `).bind(userId, deviceId).first();
    if (device) {
      await purgeDevice(c.env, db, userId, deviceId);
    }
  }

  // Wake the user's other sessions so the device list updates promptly
  c.executionCtx.waitUntil(wakeSyncForUsers(c.env, [userId]));

  return c.json({});
});

export default app;
