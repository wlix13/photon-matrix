// Matrix event redaction, hashing, event-ID computation, and PDU signing.
// Spec: https://spec.matrix.org/v1.17/rooms/ (per-version redaction rules).
// Room v3+ event IDs are the SHA-256 reference hash of the redacted event, so
// every server derives the same ID from the wire JSON; signatures cover that
// same redacted event. Hashes and signatures use unpadded standard base64,
// v4+ event IDs unpadded URL-safe base64.

import { canonicalJson, signJson, calculateContentHash } from './crypto';
import { getRoomVersion } from '../services/room-versions';

async function sha256Bytes(data: string): Promise<Uint8Array> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(data));
  return new Uint8Array(digest);
}

function encodeUnpaddedBase64(bytes: Uint8Array, urlSafe: boolean): string {
  let b64 = btoa(String.fromCharCode(...bytes)).replace(/=+$/, '');
  if (urlSafe) {
    b64 = b64.replace(/\+/g, '-').replace(/\//g, '_');
  }
  return b64;
}

function versionAtLeast(roomVersion: string, n: number): boolean {
  const num = parseInt(roomVersion, 10);
  // Unknown/non-numeric versions are treated as newest
  return isNaN(num) ? true : num >= n;
}

// Redact an event per the room version's redaction algorithm.
export function redactEvent(
  event: Record<string, any>,
  roomVersion: string
): Record<string, any> {
  const behavior = getRoomVersion(roomVersion);
  const v11Rules = behavior?.updatedRedactionRules ?? false;

  // Top-level keys preserved by redaction
  const allowedTopLevel = v11Rules
    ? [
        'event_id', 'type', 'room_id', 'sender', 'state_key', 'hashes',
        'signatures', 'depth', 'prev_events', 'auth_events', 'origin_server_ts',
      ]
    : [
        'event_id', 'type', 'room_id', 'sender', 'state_key', 'hashes',
        'signatures', 'depth', 'prev_events', 'prev_state', 'auth_events',
        'origin', 'origin_server_ts', 'membership',
      ];

  const redacted: Record<string, any> = {};
  for (const key of allowedTopLevel) {
    if (key in event) {
      redacted[key] = event[key];
    }
  }

  const content: Record<string, any> =
    event.content && typeof event.content === 'object' ? event.content : {};
  const newContent: Record<string, any> = {};
  const keep = (keys: string[]) => {
    for (const key of keys) {
      if (key in content) {
        newContent[key] = content[key];
      }
    }
  };

  switch (event.type) {
    case 'm.room.member':
      keep(['membership']);
      if (versionAtLeast(roomVersion, 9)) {
        keep(['join_authorised_via_users_server']);
      }
      // v11+: preserve only the `signed` key of third_party_invite
      if (v11Rules && content.third_party_invite?.signed !== undefined) {
        newContent.third_party_invite = { signed: content.third_party_invite.signed };
      }
      break;
    case 'm.room.create':
      if (v11Rules) {
        // v11+ preserves the entire content
        Object.assign(newContent, content);
      } else {
        keep(['creator']);
      }
      break;
    case 'm.room.join_rules':
      keep(['join_rule']);
      if (behavior?.restrictedJoinsSupported) {
        keep(['allow']);
      }
      break;
    case 'm.room.power_levels':
      keep([
        'ban', 'events', 'events_default', 'kick', 'redact',
        'state_default', 'users', 'users_default',
      ]);
      if (v11Rules) {
        keep(['invite']);
      }
      break;
    case 'm.room.history_visibility':
      keep(['history_visibility']);
      break;
    case 'm.room.aliases':
      // Preserved only in room versions 1-5
      if (!versionAtLeast(roomVersion, 6)) {
        keep(['aliases']);
      }
      break;
    case 'm.room.redaction':
      if (v11Rules) {
        keep(['redacts']);
      }
      break;
  }

  redacted.content = newContent;
  return redacted;
}

// Reference hash: SHA-256 over the canonical JSON of the redacted event with
// signatures, unsigned, and event_id removed.
export async function calculateReferenceHashBytes(
  event: Record<string, any>,
  roomVersion: string
): Promise<Uint8Array> {
  const redacted = redactEvent(event, roomVersion);
  delete redacted.signatures;
  delete redacted.unsigned;
  // v3+ wire events carry no event_id; a locally attached one would change the hash
  delete redacted.event_id;
  return sha256Bytes(canonicalJson(redacted));
}

// Compute the event ID for a v3+ room event: '$' + the reference hash, standard
// base64 for v3 and URL-safe for v4+. Room v1/v2 IDs are server-generated.
export async function computeEventId(
  event: Record<string, any>,
  roomVersion: string
): Promise<string> {
  const behavior = getRoomVersion(roomVersion);
  const format = behavior?.eventIdFormat ?? 'v4';
  if (format === 'v1') {
    throw new Error(
      `Event IDs for room version ${roomVersion} are server-generated, not computed`
    );
  }
  const hash = await calculateReferenceHashBytes(event, roomVersion);
  return '$' + encodeUnpaddedBase64(hash, format === 'v4');
}

// Sign an event for federation: the signature covers the redacted event, which
// must already carry `hashes`, and is then merged onto the full event.
export async function signEventForFederation<T extends Record<string, any>>(
  event: T,
  roomVersion: string,
  serverName: string,
  keyId: string,
  privateKeyJwk: JsonWebKey | string
): Promise<T> {
  const redacted = redactEvent(event, roomVersion);
  delete redacted.event_id; // not part of the signed wire format for v3+
  const signedRedacted = await signJson(redacted, serverName, keyId, privateKeyJwk);
  return {
    ...event,
    signatures: signedRedacted.signatures,
  };
}

// Finalize an outbound event: set the content hash, sign over the redacted
// event, and compute the reference-hash event ID (v3+). The returned event
// carries hashes and signatures but no event_id, which stays off the wire.
export async function finalizeEvent<T extends Record<string, any>>(
  event: T,
  roomVersion: string,
  serverName: string,
  keyId: string,
  privateKeyJwk: JsonWebKey | string
): Promise<{ event: T; eventId: string }> {
  const working: Record<string, any> = { ...event };
  delete working.event_id;
  delete working.unsigned;

  working.hashes = { sha256: await calculateContentHash(working) };
  const signed = await signEventForFederation(
    working as T, roomVersion, serverName, keyId, privateKeyJwk
  );

  const behavior = getRoomVersion(roomVersion);
  const eventId = behavior?.eventIdFormat === 'v1'
    ? (event.event_id as string) // v1/v2: keep the server-generated ID
    : await computeEventId(signed, roomVersion);

  return { event: signed, eventId };
}
