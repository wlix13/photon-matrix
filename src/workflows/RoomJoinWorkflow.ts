// Durable execution of the Matrix federation join handshake (spec v1.17) for a local
// user joining a remote room:
//   1. make_join   GET /_matrix/federation/v1/make_join/{roomId}/{userId}?ver= -> template
//   2. finalize    hash and sign the template into a join event, derive its ID
//   3. send_join   PUT /_matrix/federation/v2/send_join/{roomId}/{eventId} (v1 on 404),
//                  then import the returned state and auth_chain
// Outbound HTTP uses the signed federation transport (federationGet / federationPut);
// a raw fetch() gets 401. Steps are idempotent so Workflow retries are safe.

import { WorkflowEntrypoint, WorkflowEvent, WorkflowStep } from 'cloudflare:workers';
import type { Env } from '../types';
import { generateEventId } from '../utils/ids';
import { finalizeEvent, computeEventId } from '../utils/event-hashing';
import { getServerSigningKey, federationGet, federationPut } from '../services/federation-keys';
import { getSupportedRoomVersions, getRoomVersion } from '../services/room-versions';
import { notifyUsersOfEvent } from '../services/database';

export interface JoinParams {
  roomId: string;
  userId: string;
  displayName?: string;
  avatarUrl?: string;
  isRemote: boolean;
  remoteServer?: string;
  // Servers to try in order (from ?via= / ?server_name=); takes precedence over `remoteServer`.
  candidateServers?: string[];
  reason?: string;
}

export interface JoinResult {
  eventId: string;
  roomId: string;
  success: boolean;
  error?: string;
}

// A federation event as it appears on the wire; v3+ events carry no event_id.
type WireEvent = Record<string, any>;

interface MakeJoinResult {
  roomVersion: string;
  template: WireEvent;
  server: string;
}

interface FinalizeResult {
  event: WireEvent;
  eventId: string;
  roomVersion: string;
  server: string;
}

interface SendJoinResponse {
  event?: WireEvent;
  state?: WireEvent[];
  auth_chain?: WireEvent[];
  origin?: string;
  servers_in_room?: string[];
}

export class RoomJoinWorkflow extends WorkflowEntrypoint<Env, JoinParams> {
  async run(event: WorkflowEvent<JoinParams>, step: WorkflowStep): Promise<JoinResult> {
    const { roomId, userId, isRemote, remoteServer, candidateServers, displayName, avatarUrl, reason } =
      event.payload;

    console.log('[RoomJoinWorkflow] Starting join', { roomId, userId, isRemote });

    const servers = this.resolveCandidateServers(roomId, remoteServer, candidateServers);

    if (!isRemote || servers.length === 0) {
      const message = !isRemote
        ? 'RoomJoinWorkflow only handles remote joins'
        : 'No remote servers available to join via';
      console.error('[RoomJoinWorkflow]', message, { roomId, userId });
      return { eventId: '', roomId, success: false, error: message };
    }

    try {
      const makeJoin = (await step.do(
        'make-join',
        { retries: { limit: 3, delay: 5000, backoff: 'exponential' }, timeout: 30000 },
        async () => this.makeJoin(servers, roomId, userId)
      )) as MakeJoinResult;

      const finalized = (await step.do('finalize-event', async () =>
        this.finalizeJoinEvent(makeJoin, { roomId, userId, displayName, avatarUrl, reason })
      )) as FinalizeResult;

      // One step so the large send_join response is consumed here, never persisted as step state.
      await step.do(
        'send-join-and-import',
        { retries: { limit: 3, delay: 5000, backoff: 'exponential' }, timeout: 60000 },
        async () => this.sendJoinAndImport(finalized, roomId, userId)
      );

      // Recovers events sent between the invite and the join; a backfill failure must not fail the join.
      await step.do('backfill-recent', async () => {
        try {
          await this.backfillRecent(finalized.server, roomId, finalized.roomVersion, finalized.eventId);
        } catch (err) {
          console.warn('[RoomJoinWorkflow] backfill failed (non-fatal)', {
            roomId,
            error: err instanceof Error ? err.message : String(err),
          });
        }
      });

      await step.do('notify', async () => {
        await notifyUsersOfEvent(this.env, roomId, finalized.eventId, 'm.room.member');
      });

      console.log('[RoomJoinWorkflow] Join completed', { roomId, userId, eventId: finalized.eventId });
      return { eventId: finalized.eventId, roomId, success: true };
    } catch (error) {
      console.error('[RoomJoinWorkflow] Join failed', { roomId, userId, error });
      return {
        eventId: '',
        roomId,
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      };
    }
  }

  // Ordered, de-duplicated servers to try: candidates, then `remoteServer`, then the room's domain.
  private resolveCandidateServers(
    roomId: string,
    remoteServer: string | undefined,
    candidateServers: string[] | undefined
  ): string[] {
    const ordered: string[] = [];
    if (candidateServers) ordered.push(...candidateServers);
    if (remoteServer) ordered.push(remoteServer);
    const colon = roomId.indexOf(':');
    if (colon !== -1) ordered.push(roomId.slice(colon + 1));

    const seen = new Set<string>();
    const result: string[] = [];
    for (const s of ordered) {
      if (!s || s === this.env.SERVER_NAME || seen.has(s)) continue;
      seen.add(s);
      result.push(s);
    }
    return result;
  }

  // Tries each candidate server until one returns a join template.
  private async makeJoin(servers: string[], roomId: string, userId: string): Promise<MakeJoinResult> {
    const vers = Object.keys(getSupportedRoomVersions());
    const query = vers.map((v) => `ver=${encodeURIComponent(v)}`).join('&');
    const path =
      `/_matrix/federation/v1/make_join/${encodeURIComponent(roomId)}/${encodeURIComponent(userId)}` +
      (query ? `?${query}` : '');

    let lastError = '';
    for (const server of servers) {
      try {
        const response = await federationGet(
          server,
          path,
          this.env.SERVER_NAME,
          this.env.DB,
          this.env.CACHE
        );
        if (!response.ok) {
          lastError = `make_join via ${server} -> ${response.status} ${await response.text()}`;
          console.warn('[RoomJoinWorkflow]', lastError);
          continue;
        }
        const body = (await response.json()) as { room_version?: string; event?: WireEvent };
        if (!body.event) {
          lastError = `make_join via ${server} returned no event template`;
          console.warn('[RoomJoinWorkflow]', lastError);
          continue;
        }
        console.log('[RoomJoinWorkflow] make_join ok', { server, roomVersion: body.room_version });
        return {
          roomVersion: body.room_version ?? '1',
          template: body.event,
          server,
        };
      } catch (err) {
        lastError = `make_join via ${server} error: ${err instanceof Error ? err.message : String(err)}`;
        console.warn('[RoomJoinWorkflow]', lastError);
      }
    }
    throw new Error(lastError || 'make_join failed on all candidate servers');
  }

  // Builds the join event from the template and finalizes it (content hash, signature, event ID).
  private async finalizeJoinEvent(
    makeJoin: MakeJoinResult,
    params: { roomId: string; userId: string; displayName?: string; avatarUrl?: string; reason?: string }
  ): Promise<FinalizeResult> {
    const { roomVersion, template, server } = makeJoin;
    const { roomId, userId, displayName, avatarUrl, reason } = params;

    const signingKey = await getServerSigningKey(this.env.DB);
    if (!signingKey) {
      throw new Error('Server signing key not configured');
    }

    // Template content carries join_authorised_via_users_server for restricted rooms; keep it.
    const content: Record<string, any> = {
      ...(template.content && typeof template.content === 'object' ? template.content : {}),
      membership: 'join',
    };
    if (displayName) content.displayname = displayName;
    if (avatarUrl) content.avatar_url = avatarUrl;
    if (reason) content.reason = reason;

    const unsignedEvent: Record<string, any> = {
      type: 'm.room.member',
      room_id: roomId,
      sender: userId,
      state_key: userId,
      content,
      origin_server_ts: Date.now(),
      depth: typeof template.depth === 'number' ? template.depth : 0,
      auth_events: Array.isArray(template.auth_events) ? template.auth_events : [],
      prev_events: Array.isArray(template.prev_events) ? template.prev_events : [],
    };

    // Room versions v1/v2 use server-generated event IDs and require the deprecated `origin` field.
    const behavior = getRoomVersion(roomVersion);
    const legacyIds = behavior?.eventIdFormat === 'v1';
    if (legacyIds) {
      unsignedEvent.origin = this.env.SERVER_NAME;
      unsignedEvent.event_id =
        typeof template.event_id === 'string'
          ? template.event_id
          : await generateEventId(this.env.SERVER_NAME);
    }

    const { event: signed, eventId } = await finalizeEvent(
      unsignedEvent,
      roomVersion,
      this.env.SERVER_NAME,
      signingKey.keyId,
      signingKey.privateKeyJwk
    );

    // For v1/v2 the wire format keeps event_id; v3+ omits it.
    if (legacyIds) {
      signed.event_id = eventId;
    }

    console.log('[RoomJoinWorkflow] finalized join event', { eventId, roomVersion });
    return { event: signed, eventId, roomVersion, server };
  }

  // send_join (v2, falling back to v1) then import the returned state.
  private async sendJoinAndImport(
    finalized: FinalizeResult,
    roomId: string,
    userId: string
  ): Promise<void> {
    const { event: joinEvent, eventId, roomVersion, server } = finalized;
    const data = await this.sendJoin(server, roomId, eventId, joinEvent);
    await this.importJoinState(roomId, roomVersion, data, joinEvent, eventId);
    console.log('[RoomJoinWorkflow] imported room state', {
      roomId,
      userId,
      state: data.state?.length ?? 0,
      authChain: data.auth_chain?.length ?? 0,
    });
  }

  // PUT send_join, preferring v2 and falling back to v1 for older servers.
  private async sendJoin(
    server: string,
    roomId: string,
    eventId: string,
    joinEvent: WireEvent
  ): Promise<SendJoinResponse> {
    // finalizeJoinEvent already omits event_id for v3+ (and sets it for v1/v2).
    const body: WireEvent = { ...joinEvent };

    const v2Path = `/_matrix/federation/v2/send_join/${encodeURIComponent(roomId)}/${encodeURIComponent(eventId)}`;
    const v2 = await federationPut(server, v2Path, body, this.env.SERVER_NAME, this.env.DB, this.env.CACHE);
    if (v2.ok) {
      return (await v2.json()) as SendJoinResponse;
    }

    const v2Text = await v2.text();
    const unrecognized = v2.status === 404 || v2Text.includes('M_UNRECOGNIZED');
    if (!unrecognized) {
      throw new Error(`send_join (v2) via ${server} -> ${v2.status} ${v2Text}`);
    }

    // v1 response is a [200, {...}] tuple; v2 is a bare object.
    const v1Path = `/_matrix/federation/v1/send_join/${encodeURIComponent(roomId)}/${encodeURIComponent(eventId)}`;
    const v1 = await federationPut(server, v1Path, body, this.env.SERVER_NAME, this.env.DB, this.env.CACHE);
    if (!v1.ok) {
      throw new Error(`send_join (v1) via ${server} -> ${v1.status} ${await v1.text()}`);
    }
    const v1Body = (await v1.json()) as [number, SendJoinResponse] | SendJoinResponse;
    return Array.isArray(v1Body) ? v1Body[1] : v1Body;
  }

  // Stores recent timeline events as history (no state changes); send_join's snapshot omits them.
  private async backfillRecent(
    server: string,
    roomId: string,
    roomVersion: string,
    joinEventId: string
  ): Promise<void> {
    const path = `/_matrix/federation/v1/backfill/${encodeURIComponent(roomId)}?v=${encodeURIComponent(joinEventId)}&limit=50`;
    const resp = await federationGet(server, path, this.env.SERVER_NAME, this.env.DB, this.env.CACHE);
    if (!resp.ok) {
      throw new Error(`backfill via ${server} -> ${resp.status}`);
    }
    const data = (await resp.json()) as { pdus?: WireEvent[] };
    const pdus = Array.isArray(data.pdus) ? data.pdus : [];
    if (pdus.length === 0) return;

    const toStore: Array<{ id: string; event: WireEvent; setState: boolean }> = [];
    for (const ev of pdus) {
      const id = await this.tryResolveEventId(ev, roomVersion);
      if (id) toStore.push({ id, event: ev, setState: false });
    }
    if (toStore.length > 0) {
      await this.batchImportEvents(roomId, toStore);
    }
    console.log('[RoomJoinWorkflow] backfilled', toStore.length, 'events for', roomId);
  }

  // Persists the rooms row, events, current room_state and memberships; idempotent for retries.
  private async importJoinState(
    roomId: string,
    roomVersion: string,
    data: SendJoinResponse,
    joinEvent: WireEvent,
    joinEventId: string
  ): Promise<void> {
    const stateEvents = Array.isArray(data.state) ? data.state : [];
    const authChain = Array.isArray(data.auth_chain) ? data.auth_chain : [];

    // Only `state` plus the join event define room_state / memberships; auth_chain is history only.
    const toStore: Array<{ id: string; event: WireEvent; setState: boolean }> = [];
    for (const ev of authChain) {
      const id = await this.tryResolveEventId(ev, roomVersion);
      if (id) toStore.push({ id, event: ev, setState: false });
    }
    for (const ev of stateEvents) {
      const id = await this.tryResolveEventId(ev, roomVersion);
      if (id) toStore.push({ id, event: ev, setState: true });
    }

    // The resident server may have added its signature (required for restricted joins), so prefer
    // its copy; appended last so it wins the room_state / membership upsert for this user.
    const ourJoin = data.event && typeof data.event === 'object' ? data.event : joinEvent;
    toStore.push({ id: joinEventId, event: ourJoin, setState: true });

    // The rooms row must exist first: events and room_state FK-reference it.
    await this.ensureRoomRow(roomId, roomVersion, stateEvents);

    await this.batchImportEvents(roomId, toStore);

    await this.batchImportMemberships(roomId, toStore);
  }

  private async tryResolveEventId(event: WireEvent, roomVersion: string): Promise<string | null> {
    try {
      const behavior = getRoomVersion(roomVersion);
      if (behavior?.eventIdFormat === 'v1') {
        return typeof event.event_id === 'string' ? event.event_id : null;
      }
      return await computeEventId(event, roomVersion);
    } catch (err) {
      console.warn('[RoomJoinWorkflow] could not resolve event ID, skipping', {
        type: event?.type,
        error: err instanceof Error ? err.message : String(err),
      });
      return null;
    }
  }

  private async ensureRoomRow(
    roomId: string,
    roomVersion: string,
    stateEvents: WireEvent[]
  ): Promise<void> {
    const createEvent = stateEvents.find((e) => e.type === 'm.room.create');
    const joinRules = stateEvents.find((e) => e.type === 'm.room.join_rules');
    const creatorId =
      (typeof createEvent?.content?.creator === 'string' ? createEvent.content.creator : undefined) ??
      (typeof createEvent?.sender === 'string' ? createEvent.sender : null);
    const isPublic = joinRules?.content?.join_rule === 'public' ? 1 : 0;

    await this.env.DB.prepare(
      `INSERT OR IGNORE INTO rooms (room_id, room_version, is_public, creator_id, created_at)
       VALUES (?, ?, ?, ?, ?)`
    )
      .bind(roomId, roomVersion, isPublic, creatorId, Date.now())
      .run();
  }

  // Stores events (INSERT OR IGNORE) and upserts room_state for state-defining events, batched.
  private async batchImportEvents(
    roomId: string,
    toStore: Array<{ id: string; event: WireEvent; setState: boolean }>
  ): Promise<void> {
    const db = this.env.DB;
    const last = await db
      .prepare(`SELECT MAX(stream_ordering) as m FROM events`)
      .first<{ m: number | null }>();
    let ordering = last?.m ?? 0;

    const statements: D1PreparedStatement[] = [];
    for (const { id, event, setState } of toStore) {
      ordering += 1;
      const stateKey = typeof event.state_key === 'string' ? event.state_key : null;
      statements.push(
        db
          .prepare(
            `INSERT OR IGNORE INTO events (event_id, room_id, sender, event_type, state_key, content,
             origin_server_ts, unsigned, depth, auth_events, prev_events, hashes, signatures, stream_ordering)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
          )
          .bind(
            id,
            roomId,
            typeof event.sender === 'string' ? event.sender : '',
            typeof event.type === 'string' ? event.type : '',
            stateKey,
            JSON.stringify(event.content ?? {}),
            typeof event.origin_server_ts === 'number' ? event.origin_server_ts : Date.now(),
            event.unsigned ? JSON.stringify(event.unsigned) : null,
            typeof event.depth === 'number' ? event.depth : 0,
            JSON.stringify(Array.isArray(event.auth_events) ? event.auth_events : []),
            JSON.stringify(Array.isArray(event.prev_events) ? event.prev_events : []),
            event.hashes ? JSON.stringify(event.hashes) : null,
            event.signatures ? JSON.stringify(event.signatures) : null,
            ordering
          )
      );
      if (setState && stateKey !== null) {
        statements.push(
          db
            .prepare(
              `INSERT OR REPLACE INTO room_state (room_id, event_type, state_key, event_id)
               VALUES (?, ?, ?, ?)`
            )
            .bind(roomId, typeof event.type === 'string' ? event.type : '', stateKey, id)
        );
      }
    }

    await this.runBatched(statements, 50);
  }

  private async batchImportMemberships(
    roomId: string,
    toStore: Array<{ id: string; event: WireEvent; setState: boolean }>
  ): Promise<void> {
    const db = this.env.DB;
    const statements: D1PreparedStatement[] = [];
    for (const { id, event, setState } of toStore) {
      if (!setState || event.type !== 'm.room.member') continue;
      const stateKey = typeof event.state_key === 'string' ? event.state_key : null;
      if (!stateKey) continue;
      const membership =
        typeof event.content?.membership === 'string' ? event.content.membership : null;
      if (!membership) continue;
      statements.push(
        db
          .prepare(
            `INSERT OR REPLACE INTO room_memberships (room_id, user_id, membership, event_id, display_name, avatar_url)
             VALUES (?, ?, ?, ?, ?, ?)`
          )
          .bind(
            roomId,
            stateKey,
            membership,
            id,
            typeof event.content?.displayname === 'string' ? event.content.displayname : null,
            typeof event.content?.avatar_url === 'string' ? event.content.avatar_url : null
          )
      );
    }
    await this.runBatched(statements, 50);
  }

  private async runBatched(statements: D1PreparedStatement[], chunkSize: number): Promise<void> {
    for (let i = 0; i < statements.length; i += chunkSize) {
      await this.env.DB.batch(statements.slice(i, i + chunkSize));
    }
  }
}
