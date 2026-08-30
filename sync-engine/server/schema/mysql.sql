CREATE TABLE IF NOT EXISTS changes (
  id          VARCHAR(64) PRIMARY KEY,
  table_name  VARCHAR(128) NOT NULL,
  row_id      VARCHAR(128) NOT NULL,
  op          VARCHAR(16) NOT NULL,
  fields      JSON,
  field_hlcs  JSON,
  hlc         VARCHAR(64) NOT NULL,
  device_id   VARCHAR(128) NOT NULL,
  user_id     VARCHAR(128) NOT NULL,
  server_seq  BIGINT AUTO_INCREMENT UNIQUE,
  created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_user_seq (user_id, server_seq)
);

CREATE TABLE IF NOT EXISTS entity_state (
  table_name  VARCHAR(128) NOT NULL,
  row_id      VARCHAR(128) NOT NULL,
  data        JSON NOT NULL,
  field_hlcs  JSON NOT NULL,
  deleted     BOOLEAN NOT NULL DEFAULT false,
  PRIMARY KEY (table_name, row_id)
);
