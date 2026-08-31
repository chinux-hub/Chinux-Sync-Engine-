// src-tauri/src/sync_commands.rs
//
// Desktop apps built with Tauri embed the Rust core directly (no FFI layer
// needed — it's already Rust) and expose it to the JS/TS frontend via
// #[tauri::command]. Register these in your `tauri::Builder` invoke_handler.
//
// Add to src-tauri/Cargo.toml:
//   sync_core = { path = "../../core-rust" }
//   keyring = "3"     // OS keychain access: macOS Keychain, Windows
//                      // Credential Manager, Linux Secret Service
//   rand = "0.8"

use serde_json::Value;
use std::collections::HashMap;
use std::sync::OnceLock;
use sync_core::{EngineConfigFfi, FieldValue, LiveSyncListener, SyncEngineHandle};

static ENGINE: OnceLock<std::sync::Arc<SyncEngineHandle>> = OnceLock::new();

const KEYRING_SERVICE: &str = "com.yourapp.sync";
const KEYRING_ACCOUNT: &str = "local-db-key";

/// Retrieves this device's local-DB encryption key from the OS keychain
/// (macOS Keychain, Windows Credential Manager, or Linux Secret Service via
/// the `keyring` crate), generating one on first run. Never sent to the
/// server — losing it just means a fresh local cache re-pulls from the
/// server on next sync.
fn get_or_create_encryption_key() -> Result<String, String> {
    let entry = keyring::Entry::new(KEYRING_SERVICE, KEYRING_ACCOUNT).map_err(|e| e.to_string())?;
    if let Ok(existing) = entry.get_password() {
        return Ok(existing);
    }
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    let key = base64_encode(&bytes);
    entry.set_password(&key).map_err(|e| e.to_string())?;
    Ok(key)
}

fn base64_encode(bytes: &[u8]) -> String {
    // Minimal, dependency-free base64 (the crate already used elsewhere in
    // core-rust would work too — shown standalone here so this file has no
    // new deps beyond `keyring`/`rand`).
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = *chunk.get(1).unwrap_or(&0) as u32;
        let b2 = *chunk.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(CHARS[((n >> 18) & 63) as usize] as char);
        out.push(CHARS[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 { CHARS[((n >> 6) & 63) as usize] as char } else { '=' });
        out.push(if chunk.len() > 2 { CHARS[(n & 63) as usize] as char } else { '=' });
    }
    out
}

#[tauri::command]
pub fn sync_init(
    app_handle: tauri::AppHandle,
    server_url: String,
    auth_token: String,
    device_id: String,
    user_id: String,
) -> Result<(), String> {
    let dir = app_handle
        .path_resolver()
        .app_data_dir()
        .ok_or("no app data dir")?;
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let sqlite_path = dir.join("sync.db").to_string_lossy().to_string();
    let db_encryption_key = get_or_create_encryption_key()?;

    let handle = SyncEngineHandle::new(EngineConfigFfi {
        sqlite_path,
        db_encryption_key,
        server_url,
        auth_token,
        device_id,
        user_id,
    })
    .map_err(|e| e.to_string())?;

    ENGINE.set(handle).map_err(|_| "engine already initialized".to_string())
}

fn json_to_field_value(v: &Value) -> FieldValue {
    match v {
        Value::String(s) => FieldValue::Text { value: s.clone() },
        Value::Bool(b) => FieldValue::Bool { value: *b },
        Value::Number(n) => FieldValue::Number { value: n.as_f64().unwrap_or(0.0) },
        _ => FieldValue::Null,
    }
}

#[tauri::command]
pub fn sync_upsert(table: String, row_id: String, op: String, fields: HashMap<String, Value>) -> Result<(), String> {
    let engine = ENGINE.get().ok_or("engine not initialized")?;
    let converted: HashMap<String, FieldValue> =
        fields.iter().map(|(k, v)| (k.clone(), json_to_field_value(v))).collect();
    engine.apply_local_change(table, row_id, op, converted).map_err(|e| e.to_string())
}

#[tauri::command]
pub fn sync_get_row(table: String, row_id: String) -> Result<Option<Value>, String> {
    let engine = ENGINE.get().ok_or("engine not initialized")?;
    let json = engine.get_row_json(table, row_id).map_err(|e| e.to_string())?;
    Ok(json.and_then(|s| serde_json::from_str(&s).ok()))
}

#[tauri::command]
pub fn sync_now() -> Result<(u32, u32), String> {
    let engine = ENGINE.get().ok_or("engine not initialized")?;
    let result = engine.sync_now().map_err(|e| e.to_string())?;
    Ok((result.pushed, result.pulled))
}

/// Forwards live-sync events to the frontend as Tauri events
/// ("sync://changes", "sync://connection") — listen for these in
/// syncEngine.ts with `@tauri-apps/api/event`'s `listen()`.
struct TauriLiveSyncListener {
    app_handle: tauri::AppHandle,
}
impl LiveSyncListener for TauriLiveSyncListener {
    fn on_changes_applied(&self, count: u32) {
        let _ = self.app_handle.emit_all("sync://changes", count);
    }
    fn on_connection_state(&self, connected: bool) {
        let _ = self.app_handle.emit_all("sync://connection", connected);
    }
}

/// Desktop apps generally stay resident (no OS-imposed background suspend
/// the way iOS/Android have), so unlike the mobile wrappers, desktop can
/// reasonably keep live-sync running continuously rather than needing a
/// separate background-scheduling mechanism — call this once after
/// sync_init, no corresponding "stop on background" needed.
#[tauri::command]
pub fn sync_start_live(app_handle: tauri::AppHandle) -> Result<(), String> {
    let engine = ENGINE.get().ok_or("engine not initialized")?;
    engine.start_live_sync(Some(std::sync::Arc::new(TauriLiveSyncListener { app_handle })));
    Ok(())
}

#[tauri::command]
pub fn sync_stop_live() -> Result<(), String> {
    let engine = ENGINE.get().ok_or("engine not initialized")?;
    engine.stop_live_sync();
    Ok(())
}

// Register in main.rs:
// tauri::Builder::default()
//   .invoke_handler(tauri::generate_handler![
//       sync_commands::sync_init, sync_commands::sync_upsert,
//       sync_commands::sync_get_row, sync_commands::sync_now,
//       sync_commands::sync_start_live, sync_commands::sync_stop_live
//   ])
