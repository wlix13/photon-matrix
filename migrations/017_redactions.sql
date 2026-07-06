-- Redaction support: events.redacts holds the target of a redaction event, and
-- events.redacted_because points a redacted event at the redaction that hid it.
ALTER TABLE events ADD COLUMN redacts TEXT;
ALTER TABLE events ADD COLUMN redacted_because TEXT;
CREATE INDEX IF NOT EXISTS idx_events_redacts ON events(redacts);
