-- Migration 002: index to speed up cursor pagination for large accounts.
CREATE INDEX IF NOT EXISTS idx_changes_user_seq_covering
  ON changes (user_id, server_seq) INCLUDE (id, table_name, row_id, op);
