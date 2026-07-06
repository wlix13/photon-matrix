-- Adds room_memberships.invite_state: stripped state (invite_room_state plus the
-- invite event) for invites received over federation, served to clients as
-- invite_state.events. Local invites leave it NULL. Numbered migrations are applied
-- by hand; run this one exactly once, as SQLite has no ADD COLUMN IF NOT EXISTS.
ALTER TABLE room_memberships ADD COLUMN invite_state TEXT;
