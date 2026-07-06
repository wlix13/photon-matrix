// E2EE key management: device keys, one-time keys, cross-signing keys and key changes.
// Spec: https://spec.matrix.org/v1.12/client-server-api/#end-to-end-encryption
//
// Device and cross-signing keys are stored in the UserKeys Durable Object rather than D1:
// E2EE bootstrap uploads keys and immediately queries them back, which D1's eventually
// consistent read replicas cannot serve reliably. D1 and KV hold backup/cache copies.

import { Hono } from 'hono';
import type { AppEnv, Env } from '../types';
import { Errors } from '../utils/errors';
import { requireAuth } from '../middleware/auth';
import { verifyPassword } from '../utils/crypto';
import { generateOpaqueId } from '../utils/ids';
import { getPasswordHash } from '../services/database';
import { parseSyncToken } from './sync';

const app = new Hono<AppEnv>();

// Helper to get the UserKeys Durable Object stub for a user
function getUserKeysDO(env: Env, userId: string): DurableObjectStub {
  const id = env.USER_KEYS.idFromName(userId);
  return env.USER_KEYS.get(id);
}

// Fetch cross-signing keys from Durable Object (strongly consistent)
async function getCrossSigningKeysFromDO(env: Env, userId: string): Promise<{
  master?: any;
  self_signing?: any;
  user_signing?: any;
}> {
  const stub = getUserKeysDO(env, userId);
  const response = await stub.fetch(new Request('http://internal/cross-signing/get'));

  if (!response.ok) {
    const errorText = await response.text().catch(() => 'unknown error');
    console.error('[keys] DO cross-signing get failed:', response.status, errorText);
    throw new Error(`DO cross-signing get failed: ${response.status} - ${errorText}`);
  }

  return await response.json();
}

// Store cross-signing keys in Durable Object (strongly consistent)
async function putCrossSigningKeysToDO(env: Env, userId: string, keys: {
  master?: any;
  self_signing?: any;
  user_signing?: any;
}): Promise<void> {
  const stub = getUserKeysDO(env, userId);
  const response = await stub.fetch(new Request('http://internal/cross-signing/put', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(keys),
  }));

  if (!response.ok) {
    const errorText = await response.text().catch(() => 'unknown error');
    console.error('[keys] DO cross-signing put failed:', response.status, errorText);
    throw new Error(`DO cross-signing put failed: ${response.status} - ${errorText}`);
  }
}

// Fetch device keys from Durable Object (strongly consistent)
async function getDeviceKeysFromDO(env: Env, userId: string, deviceId?: string): Promise<any> {
  const stub = getUserKeysDO(env, userId);
  const url = deviceId
    ? `http://internal/device-keys/get?device_id=${encodeURIComponent(deviceId)}`
    : 'http://internal/device-keys/get';
  const response = await stub.fetch(new Request(url));

  if (!response.ok) {
    const errorText = await response.text().catch(() => 'unknown error');
    console.error('[keys] DO device-keys get failed:', response.status, errorText, 'deviceId:', deviceId);
    throw new Error(`DO device-keys get failed: ${response.status} - ${errorText}`);
  }

  return await response.json();
}

// Store device keys in Durable Object (strongly consistent)
async function putDeviceKeysToDO(env: Env, userId: string, deviceId: string, keys: any): Promise<void> {
  const stub = getUserKeysDO(env, userId);
  const response = await stub.fetch(new Request('http://internal/device-keys/put', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ device_id: deviceId, keys }),
  }));

  if (!response.ok) {
    const errorText = await response.text().catch(() => 'unknown error');
    console.error('[keys] DO device-keys put failed:', response.status, errorText, 'deviceId:', deviceId);
    throw new Error(`DO device-keys put failed: ${response.status} - ${errorText}`);
  }
}


async function getNextStreamPosition(db: D1Database, streamName: string): Promise<number> {
  await db.prepare(`
    UPDATE stream_positions SET position = position + 1 WHERE stream_name = ?
  `).bind(streamName).run();

  const result = await db.prepare(`
    SELECT position FROM stream_positions WHERE stream_name = ?
  `).bind(streamName).first<{ position: number }>();

  return result?.position || 1;
}

export async function recordKeyChange(db: D1Database, userId: string, deviceId: string | null, changeType: string): Promise<void> {
  const streamPosition = await getNextStreamPosition(db, 'device_keys');

  await db.prepare(`
    INSERT INTO device_key_changes (user_id, device_id, change_type, stream_position)
    VALUES (?, ?, ?, ?)
  `).bind(userId, deviceId, changeType, streamPosition).run();
}

// POST /_matrix/client/v3/keys/upload - Upload device keys and one-time keys
app.post('/_matrix/client/v3/keys/upload', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const deviceId = c.get('deviceId');
  const db = c.env.DB;

  let body: any;
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const { device_keys, one_time_keys, fallback_keys } = body;

  // Store device keys with strong consistency
  if (device_keys) {
    console.log('[keys/upload] Validating device_keys:', {
      authUserId: userId,
      authDeviceId: deviceId,
      bodyUserId: device_keys.user_id,
      bodyDeviceId: device_keys.device_id,
      userMatch: device_keys.user_id === userId,
      deviceMatch: device_keys.device_id === deviceId,
    });
    
    if (device_keys.user_id !== userId || device_keys.device_id !== deviceId) {
      console.log('[keys/upload] MISMATCH - returning 400');
      return c.json({
        errcode: 'M_INVALID_PARAM',
        error: `device_keys.user_id and device_keys.device_id must match authenticated user. Got user_id=${device_keys.user_id} (expected ${userId}), device_id=${device_keys.device_id} (expected ${deviceId})`,
      }, 400);
    }

    // Durable Object is the primary store (strongly consistent)
    await putDeviceKeysToDO(c.env, userId, deviceId!, device_keys);

    // Also write to KV as backup/cache
    await c.env.DEVICE_KEYS.put(
      `device:${userId}:${deviceId}`,
      JSON.stringify(device_keys)
    );

    // Record key change for /keys/changes
    await recordKeyChange(db, userId, deviceId, 'update');

    // Queue outbound m.device_list_update EDUs to federated servers
    try {
      const { getServersInRoomsWithUser } = await import('../services/database');
      const remoteServers = await getServersInRoomsWithUser(db, userId);
      const localServer = c.env.SERVER_NAME;
      const uniqueServers = [...new Set(remoteServers)].filter(s => s !== localServer);

      for (const server of uniqueServers) {
        const fedDO = c.env.FEDERATION.get(c.env.FEDERATION.idFromName(server));
        await fedDO.fetch(new Request('http://internal/send-edu', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            destination: server,
            edu_type: 'm.device_list_update',
            content: {
              user_id: userId,
              device_id: deviceId,
              device_display_name: device_keys.unsigned?.device_display_name,
              stream_id: Date.now(),
              keys: device_keys,
              deleted: false,
            },
          }),
        }));
      }
    } catch (fedErr) {
      console.warn('[keys] Failed to queue device list EDUs:', fedErr);
    }
  }

  // Store one-time keys in KV for fast access
  const oneTimeKeyCounts: Record<string, number> = {};

  if (one_time_keys) {
    const existingKeys = await c.env.ONE_TIME_KEYS.get(
      `otk:${userId}:${deviceId}`,
      'json'
    ) as Record<string, { keyId: string; keyData: any; claimed: boolean }[]> | null || {};

    for (const [keyId, keyData] of Object.entries(one_time_keys)) {
      const [algorithm] = keyId.split(':');

      if (!existingKeys[algorithm]) {
        existingKeys[algorithm] = [];
      }

      const existingIndex = existingKeys[algorithm].findIndex(k => k.keyId === keyId);
      if (existingIndex >= 0) {
        existingKeys[algorithm][existingIndex] = { keyId, keyData, claimed: false };
      } else {
        existingKeys[algorithm].push({ keyId, keyData, claimed: false });
      }

      // Also write to D1 as backup
      await db.prepare(`
        INSERT INTO one_time_keys (user_id, device_id, algorithm, key_id, key_data)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (user_id, device_id, algorithm, key_id) DO UPDATE SET
          key_data = excluded.key_data
      `).bind(
        userId,
        deviceId,
        algorithm,
        keyId,
        JSON.stringify(keyData)
      ).run();
    }

    await c.env.ONE_TIME_KEYS.put(
      `otk:${userId}:${deviceId}`,
      JSON.stringify(existingKeys)
    );

    for (const [algorithm, keys] of Object.entries(existingKeys)) {
      oneTimeKeyCounts[algorithm] = keys.filter(k => !k.claimed).length;
    }
  } else {
    const existingKeys = await c.env.ONE_TIME_KEYS.get(
      `otk:${userId}:${deviceId}`,
      'json'
    ) as Record<string, { keyId: string; keyData: any; claimed: boolean }[]> | null;

    if (existingKeys) {
      for (const [algorithm, keys] of Object.entries(existingKeys)) {
        oneTimeKeyCounts[algorithm] = keys.filter(k => !k.claimed).length;
      }
    }
  }

  if (fallback_keys) {
    for (const [keyId, keyData] of Object.entries(fallback_keys)) {
      const [algorithm] = keyId.split(':');

      await db.prepare(`
        INSERT INTO fallback_keys (user_id, device_id, algorithm, key_id, key_data)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (user_id, device_id, algorithm) DO UPDATE SET
          key_id = excluded.key_id,
          key_data = excluded.key_data,
          used = 0
      `).bind(userId, deviceId, algorithm, keyId, JSON.stringify(keyData)).run();
    }
  }

  return c.json({
    one_time_key_counts: oneTimeKeyCounts,
  });
});

// POST /_matrix/client/v3/keys/query - Query device keys for users
app.post('/_matrix/client/v3/keys/query', requireAuth(), async (c) => {
  const db = c.env.DB;

  let body: any;
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const { device_keys: requestedKeys } = body;

  const deviceKeys: Record<string, Record<string, any>> = {};
  const masterKeys: Record<string, any> = {};
  const selfSigningKeys: Record<string, any> = {};
  const userSigningKeys: Record<string, any> = {};
  const failures: Record<string, any> = {};

  // Fold cross-signing signatures stored in D1 into a device key object
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

  if (requestedKeys) {
    for (const [userId, devices] of Object.entries(requestedKeys)) {
      deviceKeys[userId] = {};

      // An empty device list means "all of this user's devices"
      const requestedDevices = Array.isArray(devices) && devices.length > 0 ? devices : null;

      if (requestedDevices === null || requestedDevices.length === 0) {
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

      const requestingUserId = c.get('userId');

      const csKeys = await getCrossSigningKeysFromDO(c.env, userId);

      if (csKeys.master) {
        masterKeys[userId] = csKeys.master;
      }
      if (csKeys.self_signing) {
        selfSigningKeys[userId] = csKeys.self_signing;
      }
      // Only return user_signing key if querying own keys
      if (csKeys.user_signing && userId === requestingUserId) {
        userSigningKeys[userId] = csKeys.user_signing;
      }
    }
  }

  // Counts only: a device that comes back with no keys is what clients report as
  // "doesn't support encryption", so this line is the first thing to check.
  const querySummary = Object.entries(deviceKeys).map(([uid, devs]) =>
    `${uid}=${Object.keys(devs).length}dev${masterKeys[uid] ? '+msk' : ''}${selfSigningKeys[uid] ? '+ssk' : ''}`
  ).join(' ');
  console.log('[keys/query] by', c.get('userId'), '->', querySummary || '(empty)');

  return c.json({
    device_keys: deviceKeys,
    master_keys: masterKeys,
    self_signing_keys: selfSigningKeys,
    user_signing_keys: userSigningKeys,
    failures,
  });
});

// POST /_matrix/client/v3/keys/claim - Claim one-time keys for establishing sessions
app.post('/_matrix/client/v3/keys/claim', requireAuth(), async (c) => {
  const db = c.env.DB;

  let body: any;
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const { one_time_keys: requestedKeys } = body;

  const oneTimeKeys: Record<string, Record<string, Record<string, any>>> = {};
  const failures: Record<string, any> = {};

  if (requestedKeys) {
    for (const [userId, devices] of Object.entries(requestedKeys)) {
      oneTimeKeys[userId] = {};

      for (const [deviceId, algorithm] of Object.entries(devices as Record<string, string>)) {
        // Try to claim a one-time key from KV first
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

            // Also mark in D1
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
          // Fallback to D1 for legacy keys
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
          // Try fallback key
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
  }

  return c.json({
    one_time_keys: oneTimeKeys,
    failures,
  });
});

// GET /_matrix/client/v3/keys/changes - Get users whose keys have changed
app.get('/_matrix/client/v3/keys/changes', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const from = c.req.query('from');
  const to = c.req.query('to');
  const db = c.env.DB;

  if (!from || !to) {
    return Errors.missingParam('from and to required').toResponse();
  }

  // from/to are sync tokens ("s{events}_td{toDevice}_dl{deviceLists}") - the
  // device-list component is the one that matches device_key_changes positions
  const fromPosition = parseSyncToken(from).deviceLists;
  const parsedTo = parseSyncToken(to).deviceLists;
  const toPosition = parsedTo > 0 ? parsedTo : Number.MAX_SAFE_INTEGER;

  // Only users sharing a joined room with the requester are reported
  const changes = await db.prepare(`
    SELECT DISTINCT dkc.user_id, dkc.change_type
    FROM device_key_changes dkc
    WHERE dkc.stream_position > ? AND dkc.stream_position <= ?
      AND dkc.user_id IN (
        SELECT DISTINCT rm2.user_id
        FROM room_memberships rm1
        JOIN room_memberships rm2 ON rm1.room_id = rm2.room_id
        WHERE rm1.user_id = ? AND rm1.membership = 'join' AND rm2.membership = 'join'
      )
  `).bind(fromPosition, toPosition, userId).all<{
    user_id: string;
    change_type: string;
  }>();

  // Per spec a deleted device still counts as "changed"; "left" is for users the client
  // no longer shares an encrypted room with, which is not tracked yet.
  const changed = [...new Set(changes.results.map(change => change.user_id))];

  return c.json({
    changed,
    left: [],
  });
});

// Helper: Check if user has OIDC/SSO link (logged in via external IdP)
async function isOIDCUser(db: D1Database, userId: string): Promise<boolean> {
  const result = await db.prepare(`
    SELECT COUNT(*) as count FROM idp_user_links WHERE user_id = ?
  `).bind(userId).first<{ count: number }>();
  return (result?.count || 0) > 0;
}

// Helper: Check if user has password set
async function hasPassword(db: D1Database, userId: string): Promise<boolean> {
  const hash = await getPasswordHash(db, userId);
  return hash !== null && hash.length > 0;
}

// POST /_matrix/client/v3/keys/device_signing/upload - Upload cross-signing keys
// Spec: https://spec.matrix.org/v1.12/client-server-api/#post_matrixclientv3keysdevice_signingupload
// Replacing existing keys needs UIA: OIDC users get the MSC4312 OAuth reset flow (or the
// legacy m.login.sso / m.login.token stages), password users get m.login.password.
app.post('/_matrix/client/v3/keys/device_signing/upload', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const db = c.env.DB;

  let body: any;
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const { master_key, self_signing_key, user_signing_key, auth } = body;

  console.log('[keys] Cross-signing upload for user:', userId);
  console.log('[keys] Auth provided:', auth ? JSON.stringify(auth) : 'none');
  if (master_key) console.log('[keys] Master key:', JSON.stringify(master_key));
  if (self_signing_key) console.log('[keys] Self-signing key:', JSON.stringify(self_signing_key));
  if (user_signing_key) console.log('[keys] User-signing key:', JSON.stringify(user_signing_key));

  const existingKeys = await db.prepare(`
    SELECT COUNT(*) as count FROM cross_signing_keys WHERE user_id = ?
  `).bind(userId).first<{ count: number }>();

  const hasExistingKeys = (existingKeys?.count || 0) > 0;
  console.log('[keys] User has existing keys:', hasExistingKeys);

  const userIsOIDC = await isOIDCUser(db, userId);
  const userHasPassword = await hasPassword(db, userId);
  // Log auth method type without exposing sensitive details
  console.log('[keys] User auth method: OIDC=' + (userIsOIDC ? 'yes' : 'no') + ', password=' + (userHasPassword ? 'yes' : 'no'));

  // MSC3967: the first upload of cross-signing keys skips UIA; later uploads replace
  // existing keys and must be authenticated.
  if (!hasExistingKeys) {
    console.log('[keys] First-time cross-signing setup - skipping UIA per MSC3967');
  } else if (!auth) {
    const sessionId = await generateOpaqueId(16);

    const flows: Array<{ stages: string[] }> = [];
    // UIA approval/OAuth URLs must use the worker API domain, not the apex
    const apiDomain = c.env.WORKER_DOMAIN || c.env.SERVER_NAME;
    const baseUrl = `https://${apiDomain}`;
    const params: Record<string, any> = {};
    
    if (userIsOIDC) {
      // MSC4312 asks servers to offer both the stable (m.oauth) and unstable
      // (org.matrix.cross_signing_reset) stages, since clients implement either one.
      const unstableStage = 'org.matrix.cross_signing_reset';
      const stableStage = 'm.oauth';
      
      flows.push({ stages: [unstableStage] });
      flows.push({ stages: [stableStage] });
      
      // Page where the user approves the cross-signing reset
      const approvalUrl = `${baseUrl}/oauth/authorize/uia?session=${sessionId}&action=org.matrix.cross_signing_reset`;
      
      params[unstableStage] = { url: approvalUrl };
      params[stableStage] = { url: approvalUrl };
    }
    
    if (userHasPassword) {
      flows.push({ stages: ['m.login.password'] });
    }
    
    // Neither OIDC nor a password: fall back to the password flow
    if (flows.length === 0) {
      flows.push({ stages: ['m.login.password'] });
    }

    // Store session in KV for validation
    await c.env.CACHE.put(
      `uia_session:${sessionId}`,
      JSON.stringify({
        user_id: userId,
        created_at: Date.now(),
        type: 'device_signing_upload',
        completed_stages: [],
        is_oidc_user: userIsOIDC,
        has_password: userHasPassword,
      }),
      { expirationTtl: 300 } // 5 minute session
    );

    console.log('[keys] UIA required (existing keys), returning challenge with session:', sessionId, 'flows:', flows);

    return c.json({
      flows,
      params,
      session: sessionId,
    }, 401);
  } else {
    // Auth provided for key replacement - validate it
    console.log('[keys] Auth type:', auth.type);

    if (auth.type === 'm.login.password') {
      const storedHash = await getPasswordHash(db, userId);
      if (!storedHash) {
        console.log('[keys] No password hash found for user');
        return Errors.forbidden('No password set for user').toResponse();
      }

      if (!auth.password) {
        console.log('[keys] No password in auth object');
        return Errors.missingParam('auth.password').toResponse();
      }

      const valid = await verifyPassword(auth.password, storedHash);
      if (!valid) {
        console.log('[keys] Invalid password');
        return Errors.forbidden('Invalid password').toResponse();
      }

      console.log('[keys] Password validated successfully');
    } else if (auth.type === 'org.matrix.cross_signing_reset' || auth.type === 'm.oauth' ||
               auth.type === 'm.login.oauth' || auth.type === 'm.login.sso' || auth.type === 'm.login.token' ||
               !auth.type) {
      // MSC4312 cross-signing reset for OIDC users: accepts the stable (m.oauth) and
      // unstable stage names, the legacy SSO/token types, and a bare session with no type.
      
      const sessionId = auth.session;
      if (!sessionId) {
        console.log('[keys] No session ID in OAuth/cross-signing auth');
        return Errors.missingParam('auth.session').toResponse();
      }

      const sessionJson = await c.env.CACHE.get(`uia_session:${sessionId}`);
      if (!sessionJson) {
        console.log('[keys] UIA session not found or expired');
        return c.json({
          errcode: 'M_UNKNOWN',
          error: 'UIA session not found or expired',
        }, 401);
      }

      const session = JSON.parse(sessionJson);
      
      if (session.user_id !== userId) {
        console.log('[keys] UIA session user mismatch');
        return Errors.forbidden('Session user mismatch').toResponse();
      }

      // Approved if any of the accepted stage names has completed
      const completedStages = session.completed_stages || [];
      const hasOAuthApproval = completedStages.includes('org.matrix.cross_signing_reset') ||
                              completedStages.includes('m.oauth') ||
                              completedStages.includes('m.login.oauth') ||
                              completedStages.includes('m.login.sso') ||
                              completedStages.includes('m.login.token');
      
      if (!hasOAuthApproval) {
        console.log('[keys] Cross-signing reset not approved for this session');
        return c.json({
          errcode: 'M_UNAUTHORIZED',
          error: 'Cross-signing reset not approved. Please approve the request at the provided URL.',
        }, 401);
      }

      console.log('[keys] Cross-signing reset approved via OAuth flow');
      
      await c.env.CACHE.delete(`uia_session:${sessionId}`);
    } else {
      console.log('[keys] Unknown auth type:', auth.type);
      return c.json({
        errcode: 'M_UNRECOGNIZED',
        error: `Unrecognized auth type: ${auth.type}`,
      }, 400);
    }
  }

  // SSSS state is only logged: uploads are allowed without it, because clients set up
  // secret storage right after uploading cross-signing keys during bootstrap.
  const ssssDefault = await c.env.ACCOUNT_DATA.get(
    `global:${userId}:m.secret_storage.default_key`,
    'json'
  ) as { key?: string } | null;

  let hasValidSSS = !!(ssssDefault && ssssDefault.key);
  if (!hasValidSSS) {
    // Also check D1 as fallback
    const d1Ssss = await db.prepare(`
      SELECT content FROM account_data
      WHERE user_id = ? AND event_type = 'm.secret_storage.default_key' AND room_id = ''
    `).bind(userId).first<{ content: string }>();

    if (d1Ssss) {
      try {
        const parsed = JSON.parse(d1Ssss.content);
        hasValidSSS = !!parsed.key;
      } catch {
        hasValidSSS = false;
      }
    }
  }

  if (!hasValidSSS) {
    console.log('[keys] SSSS not configured for user', userId, '- allowing cross-signing upload (client will prompt for recovery setup)');
  } else {
    console.log('[keys] SSSS is configured, proceeding to store cross-signing keys');
  }

  // Get existing keys from Durable Object (strongly consistent)
  const existingCSKeys = await getCrossSigningKeysFromDO(c.env, userId);

  // Merge new keys with existing
  const csKeys = { ...existingCSKeys };
  if (master_key) csKeys.master = master_key;
  if (self_signing_key) csKeys.self_signing = self_signing_key;
  if (user_signing_key) csKeys.user_signing = user_signing_key;

  // Durable Object is the primary store (strongly consistent)
  await putCrossSigningKeysToDO(c.env, userId, csKeys);
  console.log('[keys] Cross-signing keys stored in Durable Object for user:', userId);

  // D1 holds the eventually-consistent backup copy
  if (master_key) {
    const keyId = Object.keys(master_key.keys || {})[0] || '';
    await db.prepare(`
      INSERT INTO cross_signing_keys (user_id, key_type, key_id, key_data)
      VALUES (?, 'master', ?, ?)
      ON CONFLICT (user_id, key_type) DO UPDATE SET
        key_id = excluded.key_id,
        key_data = excluded.key_data
    `).bind(userId, keyId, JSON.stringify(master_key)).run();
    await recordKeyChange(db, userId, null, 'update');
  }

  if (self_signing_key) {
    const keyId = Object.keys(self_signing_key.keys || {})[0] || '';
    await db.prepare(`
      INSERT INTO cross_signing_keys (user_id, key_type, key_id, key_data)
      VALUES (?, 'self_signing', ?, ?)
      ON CONFLICT (user_id, key_type) DO UPDATE SET
        key_id = excluded.key_id,
        key_data = excluded.key_data
    `).bind(userId, keyId, JSON.stringify(self_signing_key)).run();
  }

  if (user_signing_key) {
    const keyId = Object.keys(user_signing_key.keys || {})[0] || '';
    await db.prepare(`
      INSERT INTO cross_signing_keys (user_id, key_type, key_id, key_data)
      VALUES (?, 'user_signing', ?, ?)
      ON CONFLICT (user_id, key_type) DO UPDATE SET
        key_id = excluded.key_id,
        key_data = excluded.key_data
    `).bind(userId, keyId, JSON.stringify(user_signing_key)).run();
  }

  // Write to KV as cache (eventually consistent, for performance)
  await c.env.CROSS_SIGNING_KEYS.put(`user:${userId}`, JSON.stringify(csKeys));

  return c.json({});
});

// POST /_matrix/client/v3/keys/signatures/upload - Upload signatures for keys
// Spec: https://spec.matrix.org/v1.12/client-server-api/#post_matrixclientv3keyssignaturesupload
// Body is { user_id: { key_id: signed_key_object } }, where key_id is the device_id for
// device keys and the base64 public key for cross-signing keys.
app.post('/_matrix/client/v3/keys/signatures/upload', requireAuth(), async (c) => {
  const signerUserId = c.get('userId');
  const db = c.env.DB;

  let body: any;
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  console.log('[signatures/upload] Request from:', signerUserId);
  console.log('[signatures/upload] Body:', JSON.stringify(body));

  const failures: Record<string, Record<string, { errcode: string; error: string }>> = {};

  for (const [userId, keys] of Object.entries(body)) {
    for (const [keyId, signedKey] of Object.entries(keys as Record<string, any>)) {
      try {
        const signedKeyObj = signedKey as any;

        const signatures = signedKeyObj.signatures?.[signerUserId] || {};

        console.log('[signatures/upload] Processing:', { userId, keyId, hasDeviceId: !!signedKeyObj.device_id });
        console.log('[signatures/upload] Signatures to store:', JSON.stringify(signatures));

        for (const [signerKeyId, signature] of Object.entries(signatures)) {
          // Use the device_id as key_id for device keys, otherwise use the provided keyId
          const effectiveKeyId = signedKeyObj.device_id || keyId;

          await db.prepare(`
            INSERT INTO cross_signing_signatures (
              user_id, key_id, signer_user_id, signer_key_id, signature
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (user_id, key_id, signer_user_id, signer_key_id) DO UPDATE SET
              signature = excluded.signature
          `).bind(userId, effectiveKeyId, signerUserId, signerKeyId, signature as string).run();

          console.log('[signatures/upload] Stored signature:', {
            userId,
            effectiveKeyId,
            signerUserId,
            signerKeyId
          });
        }

        // Device keys carry their signatures inline, so the stored copy needs updating too
        if (signedKeyObj.device_id) {
          const deviceId = signedKeyObj.device_id;
          console.log('[signatures/upload] Updating device key for device:', deviceId);

          // Read from Durable Object (strongly consistent)
          const existingKey = await getDeviceKeysFromDO(c.env, userId, deviceId);

          if (existingKey) {
            existingKey.signatures = existingKey.signatures || {};
            existingKey.signatures[signerUserId] = {
              ...existingKey.signatures[signerUserId],
              ...signatures,
            };

            // Write to Durable Object (primary - strongly consistent)
            await putDeviceKeysToDO(c.env, userId, deviceId, existingKey);

            // Also update KV as backup/cache
            await c.env.DEVICE_KEYS.put(
              `device:${userId}:${deviceId}`,
              JSON.stringify(existingKey)
            );

            console.log('[signatures/upload] Updated device key signatures:', {
              deviceId,
              newSignatures: Object.keys(signatures)
            });
          } else {
            console.log('[signatures/upload] Device key not found:', deviceId);
          }
        }

        // Record key change for sync notifications
        await recordKeyChange(db, userId, signedKeyObj.device_id || null, 'update');
      } catch (err) {
        console.error('[signatures/upload] Error processing signature:', err);
        if (!failures[userId]) failures[userId] = {};
        failures[userId][keyId] = {
          errcode: 'M_UNKNOWN',
          error: 'Failed to store signature',
        };
      }
    }
  }

  console.log('[signatures/upload] Completed, failures:', Object.keys(failures).length > 0 ? failures : 'none');
  return c.json({ failures });
});

// GET /_matrix/client/v3/auth/m.login.sso/redirect - Start SSO authentication for a UIA session
// Spec: https://spec.matrix.org/v1.12/client-server-api/#get_matrixclientv3authmlloginssofallbackweb
app.get('/_matrix/client/v3/auth/m.login.sso/redirect', async (c) => {
  const sessionId = c.req.query('session');
  const redirectUrl = c.req.query('redirectUrl');

  if (!sessionId) {
    return c.json({
      errcode: 'M_MISSING_PARAM',
      error: 'Missing session parameter',
    }, 400);
  }

  const sessionJson = await c.env.CACHE.get(`uia_session:${sessionId}`);
  if (!sessionJson) {
    return c.json({
      errcode: 'M_UNKNOWN',
      error: 'UIA session not found or expired',
    }, 404);
  }

  const session = JSON.parse(sessionJson);
  // OAuth endpoints live on the worker API domain, not the apex
  const apiDomain = c.env.WORKER_DOMAIN || c.env.SERVER_NAME;
  const baseUrl = `https://${apiDomain}`;

  // Store the redirect URL for after SSO completes
  session.redirect_url = redirectUrl || `${baseUrl}/_matrix/client/v3/auth/m.login.sso/callback`;
  await c.env.CACHE.put(`uia_session:${sessionId}`, JSON.stringify(session), { expirationTtl: 300 });

  // The user re-authenticates with their IdP to complete the UIA flow
  const authorizeUrl = new URL(`${baseUrl}/oauth/authorize`);
  authorizeUrl.searchParams.set('response_type', 'code');
  authorizeUrl.searchParams.set('client_id', 'matrix-uia');
  authorizeUrl.searchParams.set('redirect_uri', `${baseUrl}/_matrix/client/v3/auth/m.login.sso/callback`);
  authorizeUrl.searchParams.set('scope', 'openid');
  authorizeUrl.searchParams.set('state', sessionId);

  console.log('[keys/sso] Redirecting to SSO for UIA session:', sessionId);
  return c.redirect(authorizeUrl.toString());
});

// GET /_matrix/client/v3/auth/m.login.sso/callback - SSO callback for UIA
app.get('/_matrix/client/v3/auth/m.login.sso/callback', async (c) => {
  const code = c.req.query('code');
  const state = c.req.query('state'); // This is the UIA session ID
  const error = c.req.query('error');
  const errorDescription = c.req.query('error_description');

  if (error) {
    console.log('[keys/sso] SSO error:', error, errorDescription);
    return c.html(generateSSOErrorPage('SSO Authentication Failed', errorDescription || error));
  }

  if (!state) {
    return c.html(generateSSOErrorPage('Invalid Request', 'Missing state parameter'));
  }

  const sessionJson = await c.env.CACHE.get(`uia_session:${state}`);
  if (!sessionJson) {
    return c.html(generateSSOErrorPage('Session Expired', 'The UIA session has expired. Please try again.'));
  }

  const session = JSON.parse(sessionJson);

  if (!code) {
    return c.html(generateSSOErrorPage('Authentication Failed', 'No authorization code received'));
  }

  // The code must be one our own /oauth/authorize issued (stored as oauth_code:<code>),
  // so knowing the session ID alone is not enough to complete this flow.
  const authCodeJson = await c.env.SESSIONS.get(`oauth_code:${code}`);
  if (!authCodeJson) {
    console.warn('[keys/sso] Invalid or already-used authorization code for UIA session:', state);
    return c.html(generateSSOErrorPage('Authentication Failed', 'Invalid or expired authorization code'));
  }

  let authCode: { user_id: string; client_id: string; redirect_uri: string };
  try {
    authCode = JSON.parse(authCodeJson);
  } catch {
    return c.html(generateSSOErrorPage('Authentication Failed', 'Malformed authorization code'));
  }

  // Verify the code was issued for the same user as the UIA session
  if (authCode.user_id !== session.user_id) {
    console.warn('[keys/sso] Authorization code user mismatch for UIA session:', state);
    return c.html(generateSSOErrorPage('Authentication Failed', 'Authorization code does not match session user'));
  }

  await c.env.SESSIONS.delete(`oauth_code:${code}`);

  session.completed_stages = session.completed_stages || [];
  if (!session.completed_stages.includes('m.login.sso')) {
    session.completed_stages.push('m.login.sso');
  }
  session.sso_completed_at = Date.now();

  await c.env.CACHE.put(`uia_session:${state}`, JSON.stringify(session), { expirationTtl: 300 });

  console.log('[keys/sso] SSO completed for UIA session:', state);

  return c.html(generateSSOSuccessPage(state, session.redirect_url));
});

// POST /_matrix/client/v3/auth/m.login.token/submit - Submit token for UIA
// Alternative flow for OIDC users who have a valid token
app.post('/_matrix/client/v3/auth/m.login.token/submit', requireAuth(), async (c) => {
  const userId = c.get('userId');

  let body: any;
  try {
    body = await c.req.json();
  } catch {
    return Errors.badJson().toResponse();
  }

  const { session } = body;

  if (!session) {
    return Errors.missingParam('session').toResponse();
  }

  const sessionJson = await c.env.CACHE.get(`uia_session:${session}`);
  if (!sessionJson) {
    return c.json({
      errcode: 'M_UNKNOWN',
      error: 'UIA session not found or expired',
    }, 404);
  }

  const sessionData = JSON.parse(sessionJson);

  if (sessionData.user_id !== userId) {
    return Errors.forbidden('Session user mismatch').toResponse();
  }

  // requireAuth() already proved the caller holds a valid access token, which is all the
  // m.login.token stage asks for.
  sessionData.completed_stages = sessionData.completed_stages || [];
  if (!sessionData.completed_stages.includes('m.login.token')) {
    sessionData.completed_stages.push('m.login.token');
  }
  sessionData.token_completed_at = Date.now();

  await c.env.CACHE.put(`uia_session:${session}`, JSON.stringify(sessionData), { expirationTtl: 300 });

  console.log('[keys/token] Token UIA completed for session:', session);

  return c.json({
    completed: ['m.login.token'],
    session,
  });
});

// Helper: Generate SSO error page
function generateSSOErrorPage(title: string, message: string): string {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>${title}</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #f1f5f9; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; }
    .container { background: #1e293b; padding: 40px; border-radius: 12px; max-width: 400px; text-align: center; border: 1px solid #334155; }
    h1 { color: #ef4444; margin-bottom: 16px; }
    p { color: #94a3b8; margin-bottom: 24px; }
  </style>
</head>
<body>
  <div class="container">
    <h1>${title}</h1>
    <p>${message}</p>
    <p>You can close this window and try again.</p>
  </div>
</body>
</html>`;
}

// Helper: Generate SSO success page
function generateSSOSuccessPage(sessionId: string, _redirectUrl?: string): string {
  // _redirectUrl is reserved for custom redirects, which are not supported yet
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Authentication Successful</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f172a; color: #f1f5f9; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; }
    .container { background: #1e293b; padding: 40px; border-radius: 12px; max-width: 400px; text-align: center; border: 1px solid #334155; }
    h1 { color: #22c55e; margin-bottom: 16px; }
    p { color: #94a3b8; margin-bottom: 24px; }
    .session { font-family: monospace; background: #0f172a; padding: 8px 16px; border-radius: 8px; font-size: 12px; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Authentication Successful</h1>
    <p>Your identity has been verified.</p>
    <p>You can now return to your Matrix client and complete the operation.</p>
    <p class="session">Session: ${sessionId}</p>
    <script>
      // Notify the opener only if it shares our origin
      if (window.opener) {
        window.opener.postMessage({ type: 'uia_complete', session: '${sessionId}' }, window.location.origin);
        setTimeout(() => window.close(), 2000);
      }
    </script>
  </div>
</body>
</html>`;
}

export default app;
