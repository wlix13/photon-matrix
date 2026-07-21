// Matrix homeserver entry point: middleware, route mounting and error handling

import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { logger } from 'hono/logger';
import type { AppEnv } from './types';

// Import API routes
import versions from './api/versions';
import login from './api/login';
import rooms from './api/rooms';
import sync from './api/sync';
import slidingSync from './api/sliding-sync';
import profile from './api/profile';
import media from './api/media';
import voip from './api/voip';
import keys from './api/keys';
import federation from './api/federation';
import admin from './api/admin';
import keyBackups from './api/key-backups';
import toDevice from './api/to-device';
import push from './api/push';
import accountData from './api/account-data';
import typing from './api/typing';
import receipts from './api/receipts';
import tags from './api/tags';
import devices from './api/devices';
import presence from './api/presence';
import aliases from './api/aliases';
import relations from './api/relations';
import spaces from './api/spaces';
import account from './api/account';
import search from './api/search';
import serverNotices from './api/server-notices';
import report from './api/report';
import calls from './api/calls';
import rtc from './api/rtc';
import appservice from './api/appservice';
import identity from './api/identity';
// import qrLogin from './api/qr-login'; // QR feature commented out - requires MSC4108/OIDC for Element X
import oidcAuth from './api/oidc-auth';
import oauth from './api/oauth';
import { adminDashboardHtml } from './admin/dashboard';
import { rateLimitMiddleware } from './middleware/rate-limit';
import { requireAuth } from './middleware/auth';
import { analyticsMiddleware } from './middleware/analytics';

// Durable Object classes, re-exported for the Workers runtime
export { RoomDurableObject, SyncDurableObject, FederationDurableObject, CallRoomDurableObject, AdminDurableObject, UserKeysDurableObject, PushDurableObject, RateLimitDurableObject } from './durable-objects';

// Workflow classes, re-exported for the Workers runtime
export { RoomJoinWorkflow, PushNotificationWorkflow, FederationCatchupWorkflow, MediaCleanupWorkflow, StateCompactionWorkflow } from './workflows';

const app = new Hono<AppEnv>();

// CORS runs first so headers are present even on error responses from later
// middleware such as the rate limiter.
app.use('*', cors({
  origin: '*',
  allowMethods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowHeaders: ['Content-Type', 'Authorization', 'X-Matrix-Origin'],
  exposeHeaders: ['Content-Type', 'Content-Length'],
  maxAge: 86400,
}));

// Global middleware
app.use('*', logger());
app.use('*', analyticsMiddleware());

// Rate limiting for Matrix API endpoints
app.use('/_matrix/*', rateLimitMiddleware);

// Health check
app.get('/health', (c) => c.json({ status: 'ok', server: 'matrix-worker' }));

// Admin dashboard - serve HTML with security headers
app.get('/admin', (c) => {
  const html = adminDashboardHtml(c.env.SERVER_NAME);
  return c.html(html, 200, {
    // 'unsafe-inline' is required by the dashboard's inline scripts and styles
    'Content-Security-Policy':
      "default-src 'self'; script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; style-src 'self' 'unsafe-inline'; img-src 'self' data: https:; connect-src 'self'; frame-ancestors 'none'",
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY',
    'Referrer-Policy': 'strict-origin-when-cross-origin',
  });
});

app.get('/admin/', (c) => {
  const html = adminDashboardHtml(c.env.SERVER_NAME);
  return c.html(html, 200, {
    'Content-Security-Policy':
      "default-src 'self'; script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; style-src 'self' 'unsafe-inline'; img-src 'self' data: https:; connect-src 'self'; frame-ancestors 'none'",
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY',
    'Referrer-Policy': 'strict-origin-when-cross-origin',
  });
});

// Admin API routes
app.route('/', admin);

// QR code login landing page - commented out, requires MSC4108/OIDC for Element X
// app.route('/', qrLogin);

// OIDC/SSO authentication
app.route('/', oidcAuth);

// OAuth 2.0 provider endpoints
app.route('/', oauth);

// Matrix version discovery
app.route('/', versions);

// Client-Server API
app.route('/', login);
app.route('/', rooms);
app.route('/', sync);
app.route('/', slidingSync);
app.route('/', profile);
app.route('/', media);
app.route('/', voip);
app.route('/', keys);
app.route('/', keyBackups);
app.route('/', toDevice);
app.route('/', push);
app.route('/', accountData);
app.route('/', typing);
app.route('/', receipts);
app.route('/', tags);
app.route('/', devices);
app.route('/', presence);
app.route('/', aliases);
app.route('/', relations);
app.route('/', spaces);
app.route('/', account);
app.route('/', serverNotices);
app.route('/', report);

// Cloudflare Calls-based video calling API
app.route('/', calls);

// MatrixRTC (LiveKit) JWT service for Element X calls
app.route('/', rtc);

// Application Service API
app.route('/', appservice);

// Identity Service API
app.route('/', identity);

// Server-Server (Federation) API
app.route('/', federation);

// Capabilities endpoint
app.get('/_matrix/client/v3/capabilities', (c) => {
  return c.json({
    capabilities: {
      'm.change_password': {
        enabled: true,
      },
      'm.room_versions': {
        default: '10',
        available: {
          '1': 'stable',
          '2': 'stable',
          '3': 'stable',
          '4': 'stable',
          '5': 'stable',
          '6': 'stable',
          '7': 'stable',
          '8': 'stable',
          '9': 'stable',
          '10': 'stable',
          '11': 'stable',
          '12': 'stable',
        },
      },
      'm.set_displayname': {
        enabled: true,
      },
      'm.set_avatar_url': {
        enabled: true,
      },
      'm.3pid_changes': {
        enabled: true,
      },
    },
  });
});

// Filter endpoints - persist filters in KV for sync optimization
app.post('/_matrix/client/v3/user/:userId/filter', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const requestedUserId = c.req.param('userId');

  // Users can only create filters for themselves
  if (userId !== requestedUserId) {
    return c.json({ errcode: 'M_FORBIDDEN', error: 'Cannot create filters for other users' }, 403);
  }

  let filter: Record<string, unknown>;
  try {
    filter = await c.req.json();
  } catch {
    return c.json({ errcode: 'M_BAD_JSON', error: 'Invalid JSON' }, 400);
  }

  // Generate filter ID and store in KV
  const filterId = crypto.randomUUID().split('-')[0];
  await c.env.CACHE.put(
    `filter:${userId}:${filterId}`,
    JSON.stringify(filter),
    { expirationTtl: 30 * 24 * 60 * 60 } // 30 days TTL
  );

  return c.json({ filter_id: filterId });
});

app.get('/_matrix/client/v3/user/:userId/filter/:filterId', requireAuth(), async (c) => {
  const userId = c.get('userId');
  const requestedUserId = c.req.param('userId');
  const filterId = c.req.param('filterId');

  // Users can only read their own filters
  if (userId !== requestedUserId) {
    return c.json({ errcode: 'M_FORBIDDEN', error: 'Cannot read filters for other users' }, 403);
  }

  const filterJson = await c.env.CACHE.get(`filter:${userId}:${filterId}`);
  if (!filterJson) {
    // Return empty filter if not found (per spec, unknown filter IDs should return empty)
    return c.json({});
  }

  try {
    const filter = JSON.parse(filterJson);
    return c.json(filter);
  } catch {
    return c.json({});
  }
});

app.route('/', search);

// Public rooms directory
app.get('/_matrix/client/v3/publicRooms', async (c) => {
  const db = c.env.DB;

  const rooms = await db.prepare(
    `SELECT r.room_id, r.room_version
     FROM rooms r
     WHERE r.is_public = 1
     LIMIT 100`
  ).all<{ room_id: string; room_version: string }>();

  const publicRooms: any[] = [];

  for (const room of rooms.results) {
    // Get room name and topic from state
    const nameEvent = await db.prepare(
      `SELECT e.content FROM room_state rs
       JOIN events e ON rs.event_id = e.event_id
       WHERE rs.room_id = ? AND rs.event_type = 'm.room.name'`
    ).bind(room.room_id).first<{ content: string }>();

    const topicEvent = await db.prepare(
      `SELECT e.content FROM room_state rs
       JOIN events e ON rs.event_id = e.event_id
       WHERE rs.room_id = ? AND rs.event_type = 'm.room.topic'`
    ).bind(room.room_id).first<{ content: string }>();

    // Get member count
    const memberCount = await db.prepare(
      `SELECT COUNT(*) as count FROM room_memberships WHERE room_id = ? AND membership = 'join'`
    ).bind(room.room_id).first<{ count: number }>();

    publicRooms.push({
      room_id: room.room_id,
      name: nameEvent ? JSON.parse(nameEvent.content).name : undefined,
      topic: topicEvent ? JSON.parse(topicEvent.content).topic : undefined,
      num_joined_members: memberCount?.count || 0,
      world_readable: false,
      guest_can_join: false,
    });
  }

  return c.json({
    chunk: publicRooms,
    total_room_count_estimate: publicRooms.length,
  });
});

app.post('/_matrix/client/v3/publicRooms', async (c) => {
  // POST variant with filtering: not implemented, returns an empty chunk
  return c.json({
    chunk: [],
    total_room_count_estimate: 0,
  });
});

// User directory search (requires authentication per Matrix spec)
type DirectoryUser = { user_id: string; display_name: string | null; avatar_url: string | null };

// users_fts comes from a manually applied migration; D1 only reports its absence in the error text
function isMissingUsersFts(err: unknown): boolean {
  const message = err instanceof Error
    ? `${err.message} ${err.cause instanceof Error ? err.cause.message : ''}`
    : String(err);
  return message.includes('no such table') && message.includes('users_fts');
}

app.post('/_matrix/client/v3/user_directory/search', requireAuth(), async (c) => {
  const db = c.env.DB;
  const requestingUserId = c.get('userId');

  let body: { search_term: string; limit?: number };
  try {
    body = await c.req.json();
  } catch {
    return c.json({ errcode: 'M_BAD_JSON', error: 'Invalid JSON' }, 400);
  }

  const searchTerm = body.search_term || '';
  const limit = Math.min(body.limit || 10, 50);

  console.log('[user_directory] Search request:', {
    requestingUserId,
    searchTerm,
    limit,
    userAgent: c.req.header('User-Agent'),
  });

  if (!searchTerm) {
    return c.json({ results: [], limited: false });
  }

  // FTS tokenisation drops the punctuation of "@alice:example.org", so resolve exact IDs directly
  let exactUserId: string | null = null;
  if (/^@[^:\s]+:\S+$/.test(searchTerm)) {
    exactUserId = searchTerm;
  } else if (/^@?[^@:\s]+$/.test(searchTerm)) {
    exactUserId = `@${searchTerm.replace(/^@/, '')}:${c.env.SERVER_NAME}`;
  }

  const exactMatch = exactUserId
    ? await db.prepare(`
        SELECT user_id, display_name, avatar_url
        FROM users
        WHERE user_id = ?
          AND is_deactivated = 0
          AND is_guest = 0
          AND user_id != ?
      `).bind(exactUserId, requestingUserId).first<DirectoryUser>()
    : null;

  // Quote each token: anything outside letters/digits is an FTS5 operator
  const ftsSearchTerm = (searchTerm.match(/[\p{L}\p{N}_]+/gu) ?? []).map(t => `"${t}"`).join(' ');
  let ranked: DirectoryUser[] = [];

  if (ftsSearchTerm) {
    // Search for users using FTS5 for ranked full-text search
    try {
      const results = await db.prepare(`
        SELECT u.user_id, u.display_name, u.avatar_url
        FROM users_fts fts
        JOIN users u ON fts.user_id = u.user_id
        WHERE users_fts MATCH ?
          AND u.is_deactivated = 0
          AND u.is_guest = 0
          AND u.user_id != ?
        ORDER BY bm25(users_fts)
        LIMIT ?
      `).bind(ftsSearchTerm, requestingUserId, limit + 1).all<DirectoryUser>();
      ranked = results.results;
    } catch (err) {
      if (!isMissingUsersFts(err)) throw err;
      console.log('[user_directory] users_fts missing, using LIKE fallback:', { searchTerm });
      const likeTerm = `%${searchTerm}%`;
      const results = await db.prepare(`
        SELECT user_id, display_name, avatar_url
        FROM users
        WHERE (user_id LIKE ? OR display_name LIKE ?)
          AND is_deactivated = 0
          AND is_guest = 0
          AND user_id != ?
        ORDER BY user_id
        LIMIT ?
      `).bind(likeTerm, likeTerm, requestingUserId, limit + 1).all<DirectoryUser>();
      ranked = results.results;
    }
  }

  const matches = exactMatch
    ? [exactMatch, ...ranked.filter(u => u.user_id !== exactMatch.user_id)]
    : ranked;

  const limited = matches.length > limit;
  // Return explicit null values (not undefined/omitted) so Element X knows user exists
  const users = matches.slice(0, limit).map(u => ({
    user_id: u.user_id,
    display_name: u.display_name || null,
    avatar_url: u.avatar_url || null,
  }));

  console.log('[user_directory] Search results:', {
    searchTerm,
    resultCount: users.length,
    limited,
    firstResult: users[0],
  });

  return c.json({ results: users, limited });
});

// Third-party protocols (stub - no bridges configured)
app.get('/_matrix/client/v3/thirdparty/protocols', async (c) => {
  return c.json({});
});

// Dehydrated device (MSC3814) is not implemented. The errcode is M_UNRECOGNIZED
// because matrix-js-sdk reads M_NOT_FOUND as "supported, none stored yet" and
// then aborts secret-storage setup on the follow-up PUT.
app.get('/_matrix/client/unstable/org.matrix.msc3814.v1/dehydrated_device', async (c) => {
  return c.json({
    errcode: 'M_UNRECOGNIZED',
    error: 'Device dehydration is not supported by this server',
  }, 404);
});

// Legacy unstable auth-metadata endpoints (MSC2965), redirected to the stable route
app.get('/_matrix/client/unstable/org.matrix.msc2965/auth_issuer', async (c) => {
  return c.redirect('/_matrix/client/v1/auth_metadata', 307);
});

app.get('/_matrix/client/unstable/org.matrix.msc2965/auth_metadata', async (c) => {
  return c.redirect('/_matrix/client/v1/auth_metadata', 307);
});

// Fallback for unknown endpoints
app.all('/_matrix/*', (c) => {
  return c.json({
    errcode: 'M_UNRECOGNIZED',
    error: 'Unrecognized request',
  }, 404);
});

// 404 handler
app.notFound((c) => {
  return c.json({ error: 'Not found' }, 404);
});

// Error handler
app.onError((err, c) => {
  console.error('Unhandled error:', err);
  return c.json({
    errcode: 'M_UNKNOWN',
    error: 'An internal error occurred',
  }, 500);
});

export default app;
