mod backoff;
mod hlc;
mod live_sync;
mod sqlite_store;
mod sync_engine;

use live_sync::LiveSyncListener;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use sync_engine::{EngineConfig, SyncEngine as InnerEngine};

uniffi::setup_scaffolding!();

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum EngineError {
    #[error("{message}")]
    Failed { message: String },
}

impl From<sync_engine::SyncError> for EngineError {
    fn from(e: sync_engine::SyncError) -> Self {
        EngineError::Failed { message: e.to_string() }
    }
}

/// Public field value type exposed across the FFI boundary. UniFFI can't
/// directly marshal `serde_json::Value`, so client languages pass a small
/// tagged enum instead; the core converts to/from JSON internally.
#[derive(uniffi::Enum, Clone)]
pub enum FieldValue {
    Text { value: String },
    Number { value: f64 },
    Bool { value: bool },
    Null,
}

impl From<FieldValue> for Value {
    fn from(v: FieldValue) -> Self {
        match v {
            FieldValue::Text { value } => Value::String(value),
            FieldValue::Number { value } => serde_json::Number::from_f64(value)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            FieldValue::Bool { value } => Value::Bool(value),
            FieldValue::Null => Value::Null,
        }
    }
}

#[derive(uniffi::Record)]
pub struct EngineConfigFfi {
    pub sqlite_path: String,
    /// Per-device secret used to encrypt the local SQLite database via
    /// SQLCipher. Retrieve this from platform-secure storage (Android
    /// Keystore, iOS Keychain, desktop OS keychain) in the native wrapper —
    /// never hardcode it, never store it next to the .db file.
    pub db_encryption_key: String,
    pub server_url: String,
    pub auth_token: String,
    pub device_id: String,
    pub user_id: String,
}

#[derive(uniffi::Object)]
pub struct SyncEngineHandle {
    inner: InnerEngine,
}

#[uniffi::export]
impl SyncEngineHandle {
    #[uniffi::constructor]
    pub fn new(config: EngineConfigFfi) -> Result<Arc<Self>, EngineError> {
        let inner = InnerEngine::new(EngineConfig {
            sqlite_path: config.sqlite_path,
            db_encryption_key: config.db_encryption_key,
            server_url: config.server_url,
            auth_token: config.auth_token,
            device_id: config.device_id,
            user_id: config.user_id,
        })?;
        Ok(Arc::new(Self { inner }))
    }

    /// op: "insert" | "update" | "delete". `fields` unused for delete.
    pub fn apply_local_change(
        &self,
        table: String,
        row_id: String,
        op: String,
        fields: HashMap<String, FieldValue>,
    ) -> Result<(), EngineError> {
        let converted: HashMap<String, Value> =
            fields.into_iter().map(|(k, v)| (k, v.into())).collect();
        self.inner.apply_local_change(&table, &row_id, &op, converted)?;
        Ok(())
    }

    /// Returns the merged row as a JSON string, or None if absent/deleted.
    /// (JSON string, not a typed object, so this stays stable regardless of
    /// app schema — decode it with your platform's JSON library.)
    pub fn get_row_json(&self, table: String, row_id: String) -> Result<Option<String>, EngineError> {
        let val = self.inner.get_row(&table, &row_id)?;
        Ok(val.map(|v| v.to_string()))
    }

    pub fn push(&self) -> Result<u32, EngineError> {
        Ok(self.inner.push(500)?)
    }

    pub fn pull(&self) -> Result<u32, EngineError> {
        Ok(self.inner.pull()?)
    }

    /// Push then pull. Returns (pushed_count, pulled_count).
    pub fn sync_now(&self) -> Result<SyncResult, EngineError> {
        let (pushed, pulled) = self.inner.sync_now()?;
        Ok(SyncResult { pushed, pulled })
    }

    /// Starts near-real-time sync over WebSocket. `listener` is optional —
    /// implement `LiveSyncListener` in Kotlin/Swift/TS to get a callback
    /// when a live change lands (e.g. to refresh the UI immediately). Call
    /// on app foreground/resume.
    pub fn start_live_sync(&self, listener: Option<Arc<dyn LiveSyncListener>>) {
        self.inner.start_live_sync(listener);
    }

    /// Stops the live socket. Call on app background — see each platform's
    /// background-scheduling notes in clients/*/README for why holding a
    /// socket open in the background doesn't work reliably anyway (the OS
    /// suspends it), so this should pair with a scheduled background pull
    /// instead (WorkManager / BGTaskScheduler — see client wrappers).
    pub fn stop_live_sync(&self) {
        self.inner.stop_live_sync();
    }
}

#[derive(uniffi::Record)]
pub struct SyncResult {
    pub pushed: u32,
    pub pulled: u32,
}
