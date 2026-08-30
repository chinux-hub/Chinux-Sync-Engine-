CREATE TABLE IF NOT EXISTS changes (
  id          TEXT PRIMARY KEY,
  table_name  TEXT NOT NULL,
  row_id      TEXT NOT NULL,
  op          TEXT NOT NULL CHECK (op IN ('insert','update','delete')),
  fields      JSONB,
  field_hlcs  JSONB,
  hlc         TEXT NOT NULL,
  device_id   TEXT NOT NULL,
  user_id     TEXT NOT NULL,
  server_seq  BIGSERIAL,
  created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_changes_user_seq ON changes (user_id, server_seq);
CREATE INDEX IF NOT EXISTS idx_changes_created_at ON changes (created_at) WHERE op = 'delete';

CREATE TABLE IF NOT EXISTS entity_state (
  table_name  TEXT NOT NULL,
  row_id      TEXT NOT NULL,
  data        JSONB NOT NULL DEFAULT '{}',
  field_hlcs  JSONB NOT NULL DEFAULT '{}',
  deleted     BOOLEAN NOT NULL DEFAULT false,
  PRIMARY KEY (table_name, row_id)
);
