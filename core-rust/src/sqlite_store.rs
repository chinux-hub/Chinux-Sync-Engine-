//! Local SQLite storage. The engine keeps its own bookkeeping tables
//! (prefixed `__sync_`) alongside a materialized `__sync_entities` cache —
//! a document-style store (table, row_id) -> JSON, the same shape as the
//! server's `entity_state`. This keeps client and server conflict logic
//! byte-for-byte identical.
//!
//! Apps that want normalized relational SQLite tables for fast local
//! queries (joins, indices, etc.) register a "projector" callback that the
//! engine invokes after each applied change, to upsert the same data into
//! app-owned tables. See docs/ARCHITECTURE.md.

use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use std::collections::HashMap;

pub struct Store {
    pub conn: Connection,
}

impl Store {
    /// Opens (creating if needed) an **encrypted** local SQLite database via
    /// SQLCipher. `encryption_key` must be a stable, per-device secret the
    /// native wrapper retrieves from platform-secure storage (Android
    /// Keystore / EncryptedSharedPreferences, iOS Keychain, or the OS
    /// keychain on desktop via e.g. the `keyring` crate) — never hardcoded,
    /// never stored alongside the database file itself. Losing the key means
    /// losing access to the local cache (the server remains the durable
    /// source of truth, so a fresh local DB just re-pulls everything).
    pub fn open(path: &str, encryption_key: &str) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        // PRAGMA key must be the very first statement on a fresh connection,
        // before any other read/write, or SQLCipher silently treats the DB
        // as unencrypted (and later statements would fail on an existing
        // encrypted file). Escape single quotes defensively.
        let escaped_key = encryption_key.replace('\'', "''");
        conn.execute_batch(&format!("PRAGMA key = '{}';", escaped_key))?;
        // Verify the key actually works against an existing encrypted file
        // (a wrong key doesn't error until first real access) — cheap probe.
        // A wrong key surfaces here as SQLITE_NOTADB, propagated as-is.
        conn.query_row("SELECT count(*) FROM sqlite_master", [], |_| Ok(()))?;

        conn.execute_batch(
            r#"
            PRAGMA journal_mode=WAL;
            PRAGMA foreign_keys=ON;

            CREATE TABLE IF NOT EXISTS __sync_meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            );

            -- Outbound queue: local changes not yet acknowledged by the server.
            CREATE TABLE IF NOT EXISTS __sync_outbox (
                id          TEXT PRIMARY KEY,
                table_name  TEXT NOT NULL,
                row_id      TEXT NOT NULL,
                op          TEXT NOT NULL,
                fields      TEXT,       -- JSON
                field_hlcs  TEXT,       -- JSON
                hlc         TEXT NOT NULL,
                created_at  INTEGER NOT NULL,
                pushed      INTEGER NOT NULL DEFAULT 0
            );

            -- Materialized local state: current merged view of every synced row.
            CREATE TABLE IF NOT EXISTS __sync_entities (
                table_name  TEXT NOT NULL,
                row_id      TEXT NOT NULL,
                data        TEXT NOT NULL DEFAULT '{}',   -- JSON
                field_hlcs  TEXT NOT NULL DEFAULT '{}',   -- JSON
                deleted     INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (table_name, row_id)
            );
            "#,
        )?;
        let store = Self { conn };
        store.run_migrations()?;
        Ok(store)
    }

    /// Ordered list of local schema migrations, applied once each on the
    /// `__sync_*` bookkeeping tables. App-owned "projector" tables (see
    /// module docs) manage their own migrations separately, the same way
    /// they manage their own schema.
    ///
    /// To add a migration: append a new `(version, sql)` tuple — never edit
    /// or reorder existing entries, since already-upgraded devices have
    /// already applied them and version numbers must stay stable.
    const MIGRATIONS: &'static [(i32, &'static str)] = &[
        // v1 is the CREATE TABLE IF NOT EXISTS statements in `open()` above,
        // implicitly "version 1" for any database that predates this
        // migration system. Real migrations start at v2:
        (2, "CREATE INDEX IF NOT EXISTS idx_outbox_pushed ON __sync_outbox(pushed);"),
    ];

    fn run_migrations(&self) -> rusqlite::Result<()> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS __sync_schema_version (version INTEGER NOT NULL);",
        )?;
        let current: i32 = self
            .conn
            .query_row("SELECT COALESCE(MAX(version), 1) FROM __sync_schema_version", [], |r| r.get(0))
            .unwrap_or(1);

        for (version, sql) in Self::MIGRATIONS {
            if *version <= current {
                continue;
            }
            let tx = self.conn.unchecked_transaction()?;
            tx.execute_batch(sql)?;
            tx.execute("INSERT INTO __sync_schema_version (version) VALUES (?1)", params![version])?;
            tx.commit()?;
        }
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> rusqlite::Result<Option<String>> {
        self.conn
            .query_row("SELECT value FROM __sync_meta WHERE key = ?1", params![key], |r| r.get(0))
            .optional()
    }

    pub fn set_meta(&self, key: &str, value: &str) -> rusqlite::Result<()> {
        self.conn.execute(
            "INSERT INTO __sync_meta (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    /// Record a locally-originated change in the outbox and apply it to the
    /// local materialized cache immediately (optimistic local write).
    pub fn record_local_change(
        &self,
        change_id: &str,
        table: &str,
        row_id: &str,
        op: &str,
        fields: &HashMap<String, Value>,
        field_hlcs: &HashMap<String, String>,
        hlc: &str,
        now_ms: i64,
    ) -> rusqlite::Result<()> {
        let fields_json = serde_json::to_string(fields).unwrap_or_else(|_| "{}".into());
        let hlcs_json = serde_json::to_string(field_hlcs).unwrap_or_else(|_| "{}".into());

        self.conn.execute(
            "INSERT INTO __sync_outbox (id, table_name, row_id, op, fields, field_hlcs, hlc, created_at, pushed)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,0)",
            params![change_id, table, row_id, op, fields_json, hlcs_json, hlc, now_ms],
        )?;

        self.apply_to_cache(table, row_id, op, fields, field_hlcs)
    }

    /// Apply an incoming (remote) change using field-level HLC comparison.
    /// Returns true if it changed local state (caller should notify the
    /// app / re-run projections), false if superseded.
    pub fn apply_remote_change(
        &self,
        table: &str,
        row_id: &str,
        op: &str,
        fields: &HashMap<String, Value>,
        field_hlcs: &HashMap<String, String>,
    ) -> rusqlite::Result<bool> {
        if op == "delete" {
            self.conn.execute(
                "INSERT INTO __sync_entities (table_name, row_id, data, field_hlcs, deleted)
                 VALUES (?1, ?2, '{}', '{}', 1)
                 ON CONFLICT(table_name, row_id) DO UPDATE SET deleted = 1",
                params![table, row_id],
            )?;
            return Ok(true);
        }

        let existing = self.get_entity_raw(table, row_id)?;
        let mut data: HashMap<String, Value> = existing
            .as_ref()
            .map(|(d, _)| serde_json::from_str(d).unwrap_or_default())
            .unwrap_or_default();
        let mut current_hlcs: HashMap<String, String> = existing
            .as_ref()
            .map(|(_, h)| serde_json::from_str(h).unwrap_or_default())
            .unwrap_or_default();

        let mut any_applied = false;
        for (col, val) in fields.iter() {
            let incoming_hlc = field_hlcs.get(col).cloned().unwrap_or_default();
            let should_apply = match current_hlcs.get(col) {
                Some(existing_hlc) => incoming_hlc.as_str() > existing_hlc.as_str(),
                None => true,
            };
            if should_apply {
                data.insert(col.clone(), val.clone());
                current_hlcs.insert(col.clone(), incoming_hlc);
                any_applied = true;
            }
        }

        if any_applied {
            self.apply_to_cache(table, row_id, "update", &data, &current_hlcs)?;
        }
        Ok(any_applied)
    }

    fn apply_to_cache(
        &self,
        table: &str,
        row_id: &str,
        op: &str,
        fields: &HashMap<String, Value>,
        field_hlcs: &HashMap<String, String>,
    ) -> rusqlite::Result<()> {
        if op == "delete" {
            self.conn.execute(
                "INSERT INTO __sync_entities (table_name, row_id, data, field_hlcs, deleted)
                 VALUES (?1, ?2, '{}', '{}', 1)
                 ON CONFLICT(table_name, row_id) DO UPDATE SET deleted = 1",
                params![table, row_id],
            )?;
            return Ok(());
        }

        let existing = self.get_entity_raw(table, row_id)?;
        let mut data: HashMap<String, Value> = existing
            .as_ref()
            .map(|(d, _)| serde_json::from_str(d).unwrap_or_default())
            .unwrap_or_default();
        let mut hlcs: HashMap<String, String> = existing
            .as_ref()
            .map(|(_, h)| serde_json::from_str(h).unwrap_or_default())
            .unwrap_or_default();

        for (k, v) in fields.iter() {
            data.insert(k.clone(), v.clone());
        }
        for (k, v) in field_hlcs.iter() {
            hlcs.insert(k.clone(), v.clone());
        }

        let data_json = serde_json::to_string(&data).unwrap();
        let hlcs_json = serde_json::to_string(&hlcs).unwrap();

        self.conn.execute(
            "INSERT INTO __sync_entities (table_name, row_id, data, field_hlcs, deleted)
             VALUES (?1, ?2, ?3, ?4, 0)
             ON CONFLICT(table_name, row_id) DO UPDATE SET data = ?3, field_hlcs = ?4, deleted = 0",
            params![table, row_id, data_json, hlcs_json],
        )?;
        Ok(())
    }

    fn get_entity_raw(&self, table: &str, row_id: &str) -> rusqlite::Result<Option<(String, String)>> {
        self.conn
            .query_row(
                "SELECT data, field_hlcs FROM __sync_entities WHERE table_name = ?1 AND row_id = ?2",
                params![table, row_id],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
            )
            .optional()
    }

    pub fn get_entity(&self, table: &str, row_id: &str) -> rusqlite::Result<Option<Value>> {
        let deleted: Option<i64> = self
            .conn
            .query_row(
                "SELECT deleted FROM __sync_entities WHERE table_name = ?1 AND row_id = ?2",
                params![table, row_id],
                |r| r.get(0),
            )
            .optional()?;
        if deleted == Some(1) || deleted.is_none() {
            return Ok(None);
        }
        Ok(self
            .get_entity_raw(table, row_id)?
            .map(|(d, _)| serde_json::from_str(&d).unwrap_or(Value::Null)))
    }

    /// Pending (not-yet-pushed) outbox rows, oldest first.
    pub fn get_pending_outbox(&self, limit: i64) -> rusqlite::Result<Vec<OutboxRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, table_name, row_id, op, fields, field_hlcs, hlc
             FROM __sync_outbox WHERE pushed = 0 ORDER BY created_at ASC LIMIT ?1",
        )?;
        let rows = stmt
            .query_map(params![limit], |r| {
                Ok(OutboxRow {
                    id: r.get(0)?,
                    table: r.get(1)?,
                    row_id: r.get(2)?,
                    op: r.get(3)?,
                    fields: r.get(4)?,
                    field_hlcs: r.get(5)?,
                    hlc: r.get(6)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    pub fn mark_pushed(&self, ids: &[String]) -> rusqlite::Result<()> {
        for id in ids {
            self.conn
                .execute("UPDATE __sync_outbox SET pushed = 1 WHERE id = ?1", params![id])?;
        }
        // Trim acknowledged rows periodically rather than on every push,
        // keeping a short retention window for debugging.
        self.conn.execute(
            "DELETE FROM __sync_outbox WHERE pushed = 1 AND created_at < ?1",
            params![chrono_now_minus_days(7)],
        )?;
        Ok(())
    }
}

pub struct OutboxRow {
    pub id: String,
    pub table: String,
    pub row_id: String,
    pub op: String,
    pub fields: Option<String>,
    pub field_hlcs: Option<String>,
    pub hlc: String,
}

fn chrono_now_minus_days(days: i64) -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    now - days * 24 * 60 * 60 * 1000
}
