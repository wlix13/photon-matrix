-- Backfill event_relations from the m.relates_to block already stored in events.content,
-- covering both rel_type relations (m.thread, m.annotation, m.reference, m.replace) and replies.
INSERT OR IGNORE INTO event_relations (event_id, relates_to_id, relation_type, aggregation_key)
SELECT
    e.event_id,
    json_extract(e.content, '$."m.relates_to".event_id'),
    json_extract(e.content, '$."m.relates_to".rel_type'),
    CASE WHEN json_type(e.content, '$."m.relates_to".key') = 'text'
         THEN json_extract(e.content, '$."m.relates_to".key') END
FROM events e
WHERE json_valid(e.content)
  AND json_type(e.content, '$."m.relates_to".rel_type') = 'text'
  AND json_type(e.content, '$."m.relates_to".event_id') = 'text';

-- A thread's reply fallback points at the root but is not itself a reply.
INSERT OR IGNORE INTO event_relations (event_id, relates_to_id, relation_type, aggregation_key)
SELECT
    e.event_id,
    json_extract(e.content, '$."m.relates_to"."m.in_reply_to".event_id'),
    'm.in_reply_to',
    NULL
FROM events e
WHERE json_valid(e.content)
  AND json_type(e.content, '$."m.relates_to"."m.in_reply_to".event_id') = 'text'
  AND COALESCE(json_extract(e.content, '$."m.relates_to".is_falling_back'), 0) <> 1;
