-- Migration 002: index to speed up cursor pagination for large accounts.
-- MySQL doesn't support INCLUDE columns (Postgres-only), so this is a
-- plain composite index instead of a covering index.
CREATE INDEX idx_changes_user_seq ON changes (user_id, server_seq);
