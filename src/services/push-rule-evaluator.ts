// Notification and highlight counting driven by push rules, shared by /sync and sliding sync.

import { evaluatePushRules } from '../api/push';

export { evaluatePushRules };

interface UnreadEvent {
  event_id: string;
  event_type: string;
  content: string;
  sender: string;
  room_id: string;
  state_key?: string;
}

/** Count notifications and highlights for a room's unread events using the user's push rules. */
export async function countNotificationsWithRules(
  db: D1Database,
  userId: string,
  roomId: string,
  sinceStreamOrdering?: number,
): Promise<{ notification_count: number; highlight_count: number }> {
  // The badge clears on the m.read receipt (sent automatically when a message is viewed), not on
  // the m.fully_read marker (explicit "mark as read"), so take whichever position is furthest along.
  let readStreamOrdering = sinceStreamOrdering;

  if (readStreamOrdering === undefined) {
    const readEventIds: string[] = [];

    // m.read / m.read.private receipts (main timeline) for this user
    const receipts = await db.prepare(`
      SELECT event_id FROM receipts
      WHERE user_id = ? AND room_id = ? AND thread_id = ''
        AND receipt_type IN ('m.read', 'm.read.private')
    `).bind(userId, roomId).all<{ event_id: string }>();
    for (const r of receipts.results) readEventIds.push(r.event_id);

    // m.fully_read marker (from account data)
    const fullyReadMarker = await db.prepare(`
      SELECT content FROM account_data
      WHERE user_id = ? AND room_id = ? AND event_type = 'm.fully_read'
    `).bind(userId, roomId).first<{ content: string }>();
    if (fullyReadMarker) {
      try {
        const markerContent = JSON.parse(fullyReadMarker.content);
        if (markerContent.event_id) readEventIds.push(markerContent.event_id);
      } catch { /* ignore */ }
    }

    // Resolve to the furthest-along stream_ordering across all read markers
    for (const eventId of readEventIds) {
      const ev = await db.prepare(`
        SELECT stream_ordering FROM events WHERE event_id = ?
      `).bind(eventId).first<{ stream_ordering: number }>();
      if (ev?.stream_ordering !== undefined && ev.stream_ordering !== null) {
        if (readStreamOrdering === undefined || ev.stream_ordering > readStreamOrdering) {
          readStreamOrdering = ev.stream_ordering;
        }
      }
    }
  }

  // Get unread events (messages and encrypted events from others)
  let unreadEvents: UnreadEvent[];
  if (readStreamOrdering) {
    const results = await db.prepare(`
      SELECT event_id, event_type as type, content, sender, room_id, state_key
      FROM events
      WHERE room_id = ? AND stream_ordering > ? AND sender != ?
        AND event_type IN ('m.room.message', 'm.room.encrypted')
      ORDER BY stream_ordering ASC
      LIMIT 500
    `).bind(roomId, readStreamOrdering, userId).all<UnreadEvent>();
    unreadEvents = results.results;
  } else {
    const results = await db.prepare(`
      SELECT event_id, event_type as type, content, sender, room_id, state_key
      FROM events
      WHERE room_id = ? AND sender != ?
        AND event_type IN ('m.room.message', 'm.room.encrypted')
      ORDER BY stream_ordering DESC
      LIMIT 500
    `).bind(roomId, userId).all<UnreadEvent>();
    unreadEvents = results.results;
  }

  if (unreadEvents.length === 0) {
    return { notification_count: 0, highlight_count: 0 };
  }

  // Get room member count for push rule evaluation
  const memberCount = await db.prepare(`
    SELECT COUNT(*) as count FROM room_memberships
    WHERE room_id = ? AND membership = 'join'
  `).bind(roomId).first<{ count: number }>();

  // Get user's display name for mention detection
  const user = await db.prepare(`
    SELECT display_name FROM users WHERE user_id = ?
  `).bind(userId).first<{ display_name: string | null }>();

  let notificationCount = 0;
  let highlightCount = 0;

  for (const event of unreadEvents) {
    let parsedContent: Record<string, unknown>;
    try {
      parsedContent = typeof event.content === 'string' ? JSON.parse(event.content) : event.content;
    } catch {
      parsedContent = {};
    }

    const result = await evaluatePushRules(
      db,
      userId,
      {
        type: event.event_type,
        content: parsedContent,
        sender: event.sender,
        room_id: event.room_id,
        state_key: event.state_key,
      },
      memberCount?.count || 1,
      user?.display_name || undefined,
    );

    if (result.notify) {
      notificationCount++;
    }
    if (result.highlight) {
      highlightCount++;
    }
  }

  return { notification_count: notificationCount, highlight_count: highlightCount };
}
