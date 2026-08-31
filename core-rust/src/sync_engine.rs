use crate::backoff::Backoff;
use crate::hlc::HlcClock;
use crate::live_sync::{self, LiveSyncHandle, LiveSyncListener};
use crate::sqlite_store::Store;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("local storage error: {0}")]
    Storage(#[from] rusqlite::Error),
    #[error("network error: {0}")]
    Network(String),
    #[error("server rejected request: {0}")]
    Server(String),
}

#[derive(Serialize)]
struct WireChange<'a> {
    id: &'a str,
    table: &'a str,
    #[serde(rename = "rowId")]
    row_id: &'a str,
    op: &'a str,
    fields: Option<Value>,
    #[serde(rename = "fieldHlcs")]
    field_hlcs: Option<Value>,
    hlc: &'a str,
    #[serde(rename = "deviceId")]
    device_id: &'a str,
    #[serde(rename = "userId")]
    user_id: &'a str,
}

#[derive(Deserialize)]
struct PushResponse {
    accepted: u32,
    cursor: String,
}

#[derive(Deserialize, Clone)]
pub struct IncomingChange {
    pub id: String,
    pub table: String,
    #[serde(rename = "rowId")]
    pub row_id: String,
    pub op: String,
    pub fields: Option<HashMap<String, Value>>,
    #[serde(rename = "fieldHlcs")]
    pub field_hlcs: Option<HashMap<String, String>>,
    pub hlc: String,
}

#[derive(Deserialize)]
struct PullResponse {
    changes: Vec<IncomingChange>,
    cursor: String,
    #[serde(rename = "hasMore")]
    has_more: bool,
}

pub struct SyncEngine {
    store: Arc<Mutex<Store>>,
    clock: Arc<Mutex<HlcClock>>,
    server_url: String,
    auth_token: String,
    device_id: String,
    user_id: String,
    http: reqwest::blocking::Client,
    live_handle: Mutex<Option<LiveSyncHandle>>,
}

pub struct EngineConfig {
    pub sqlite_path: String,
    pub db_encryption_key: String,
    pub server_url: String,
    pub auth_token: String,
    pub device_id: String,
    pub user_id: String,
}

/// Max attempts for a single push/pull call before giving up and returning
/// a Network error to the caller (who should retry later — e.g. on next
/// foreground, next `online` event, or next scheduled background sync).
/// This bounds a single call's latency; it does NOT replace the caller's
/// own periodic retry — it just stops one call from retrying forever.
const MAX_ATTEMPTS: u32 = 5;

impl SyncEngine {
    pub fn new(config: EngineConfig) -> Result<Self, SyncError> {
        let store = Store::open(&config.sqlite_path, &config.db_encryption_key)?;
        let (phys, cnt) = restore_clock_state(&store)?;
        let clock = HlcClock::new(config.device_id.clone(), phys, cnt);
        Ok(Self {
            store: Arc::new(Mutex::new(store)),
            clock: Arc::new(Mutex::new(clock)),
            server_url: config.server_url,
            auth_token: config.auth_token,
            device_id: config.device_id,
            user_id: config.user_id,
            http: reqwest::blocking::Client::new(),
            live_handle: Mutex::new(None),
        })
    }

    /// Write a local change: applied to the local cache immediately
    /// (optimistic UI), queued for push on next sync. `fields` maps
    /// changed column names to their new JSON-encodable values.
    pub fn apply_local_change(
        &self,
        table: &str,
        row_id: &str,
        op: &str, // "insert" | "update" | "delete"
        fields: HashMap<String, Value>,
    ) -> Result<(), SyncError> {
        let hlc = {
            let mut clock = self.clock.lock().unwrap();
            let h = clock.tick();
            let (p, c) = clock.persisted_state();
            self.store.lock().unwrap().set_meta("hlc_physical", &p.to_string())?;
            self.store.lock().unwrap().set_meta("hlc_counter", &c.to_string())?;
            h
        };

        let field_hlcs: HashMap<String, String> =
            fields.keys().map(|k| (k.clone(), hlc.clone())).collect();

        let change_id = Uuid::new_v4().to_string();
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        self.store.lock().unwrap().record_local_change(
            &change_id, table, row_id, op, &fields, &field_hlcs, &hlc, now_ms,
        )?;
        Ok(())
    }

    /// Read the current merged local state of a row (or None if absent/deleted).
    pub fn get_row(&self, table: &str, row_id: &str) -> Result<Option<Value>, SyncError> {
        Ok(self.store.lock().unwrap().get_entity(table, row_id)?)
    }

    /// Push queued local changes to the server, retrying transient network
    /// failures with exponential backoff + jitter (bounded by
    /// `MAX_ATTEMPTS`, so this always returns rather than hanging forever).
    pub fn push(&self, batch_size: i64) -> Result<u32, SyncError> {
        let pending = self.store.lock().unwrap().get_pending_outbox(batch_size)?;
        if pending.is_empty() {
            return Ok(0);
        }

        let wire: Vec<WireChange> = pending
            .iter()
            .map(|r| WireChange {
                id: &r.id,
                table: &r.table,
                row_id: &r.row_id,
                op: &r.op,
                fields: r.fields.as_deref().and_then(|s| serde_json::from_str(s).ok()),
                field_hlcs: r.field_hlcs.as_deref().and_then(|s| serde_json::from_str(s).ok()),
                hlc: &r.hlc,
                device_id: &self.device_id,
                user_id: &self.user_id,
            })
            .collect();

        let body = serde_json::json!({
            "deviceId": self.device_id,
            "userId": self.user_id,
            "changes": wire,
        });

        let mut backoff = Backoff::new(300, 8_000);
        let mut last_err = None;
        for attempt in 0..MAX_ATTEMPTS {
            if attempt > 0 {
                std::thread::sleep(backoff.next_delay());
            }
            match self.try_push(&body) {
                Ok(parsed) => {
                    let ids: Vec<String> = pending.iter().map(|r| r.id.clone()).collect();
                    let store = self.store.lock().unwrap();
                    store.mark_pushed(&ids)?;
                    store.set_meta("last_push_cursor", &parsed.cursor)?;
                    return Ok(parsed.accepted);
                }
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or(SyncError::Network("push failed after retries".into())))
    }

    fn try_push(&self, body: &Value) -> Result<PushResponse, SyncError> {
        let resp = self
            .http
            .post(format!("{}/sync/push", self.server_url))
            .bearer_auth(&self.auth_token)
            .json(body)
            .send()
            .map_err(|e| SyncError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            // 4xx = the server rejected the request itself (bad auth, bad
            // payload) — retrying identically won't help, fail fast.
            if resp.status().is_client_error() {
                return Err(SyncError::Server(format!("push rejected: {}", resp.status())));
            }
            return Err(SyncError::Network(format!("push failed: {}", resp.status())));
        }
        resp.json().map_err(|e| SyncError::Network(e.to_string()))
    }

    /// Pull remote changes since the last cursor and apply them locally
    /// using field-level HLC merge. Retries transient failures the same way
    /// as `push`. Loops internally while `hasMore` is true.
    pub fn pull(&self) -> Result<u32, SyncError> {
        let mut cursor = self.store.lock().unwrap().get_meta("pull_cursor")?;
        let mut applied_total = 0u32;

        loop {
            let body = serde_json::json!({
                "deviceId": self.device_id,
                "userId": self.user_id,
                "sinceCursor": cursor,
                "limit": 500,
            });

            let mut backoff = Backoff::new(300, 8_000);
            let mut parsed: Option<PullResponse> = None;
            let mut last_err = None;
            for attempt in 0..MAX_ATTEMPTS {
                if attempt > 0 {
                    std::thread::sleep(backoff.next_delay());
                }
                match self.try_pull(&body) {
                    Ok(p) => { parsed = Some(p); break; }
                    Err(e) => last_err = Some(e),
                }
            }
            let parsed = match parsed {
                Some(p) => p,
                None => return Err(last_err.unwrap_or(SyncError::Network("pull failed after retries".into()))),
            };

            {
                let store = self.store.lock().unwrap();
                for change in &parsed.changes {
                    let applied = store.apply_remote_change(
                        &change.table,
                        &change.row_id,
                        &change.op,
                        &change.fields.clone().unwrap_or_default(),
                        &change.field_hlcs.clone().unwrap_or_default(),
                    )?;
                    if applied {
                        applied_total += 1;
                        self.clock.lock().unwrap().receive(&change.hlc);
                    }
                }
                store.set_meta("pull_cursor", &parsed.cursor)?;
            }
            cursor = Some(parsed.cursor.clone());

            if !parsed.has_more {
                break;
            }
        }

        Ok(applied_total)
    }

    fn try_pull(&self, body: &Value) -> Result<PullResponse, SyncError> {
        let resp = self
            .http
            .post(format!("{}/sync/pull", self.server_url))
            .bearer_auth(&self.auth_token)
            .json(body)
            .send()
            .map_err(|e| SyncError::Network(e.to_string()))?;

        if !resp.status().is_success() {
            if resp.status().is_client_error() {
                return Err(SyncError::Server(format!("pull rejected: {}", resp.status())));
            }
            return Err(SyncError::Network(format!("pull failed: {}", resp.status())));
        }
        resp.json().map_err(|e| SyncError::Network(e.to_string()))
    }

    /// Convenience: push then pull, the typical "sync now" operation.
    pub fn sync_now(&self) -> Result<(u32, u32), SyncError> {
        let pushed = self.push(500)?;
        let pulled = self.pull()?;
        Ok((pushed, pulled))
    }

    /// Starts the background WebSocket connection for near-real-time sync
    /// (see `live_sync.rs`). Safe to call multiple times — a prior
    /// connection is stopped first. Call on app foreground; call
    /// `stop_live_sync()` on background/backgrounding to free the socket.
    pub fn start_live_sync(&self, listener: Option<Arc<dyn LiveSyncListener>>) {
        self.stop_live_sync();
        let handle = live_sync::start(
            self.store.clone(),
            self.clock.clone(),
            self.server_url.clone(),
            self.auth_token.clone(),
            self.device_id.clone(),
            listener,
        );
        *self.live_handle.lock().unwrap() = Some(handle);
    }

    pub fn stop_live_sync(&self) {
        if let Some(handle) = self.live_handle.lock().unwrap().take() {
            handle.stop();
        }
    }
}

fn restore_clock_state(store: &Store) -> Result<(u64, u32), SyncError> {
    let phys = store
        .get_meta("hlc_physical")?
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let cnt = store
        .get_meta("hlc_counter")?
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(0);
    Ok((phys, cnt))
}
