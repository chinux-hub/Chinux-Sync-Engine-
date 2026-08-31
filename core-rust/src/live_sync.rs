//! Client-side WebSocket connection to the server's `/sync/live` endpoint
//! (see `server/src/sync/wsHub.ts`). The server side already broadcasts
//! changes to other connected devices in real time — this module is what
//! was missing: something that actually connects and listens.
//!
//! Runs on a background thread, reconnects with exponential backoff+jitter
//! on any drop, and always falls back gracefully: if the socket is down,
//! changes still arrive on the next `pull()` (foreground, on-interval, or
//! background-scheduled — see the native wrapper docs). This is a latency
//! optimization, never a correctness requirement.

use crate::backoff::Backoff;
use crate::sqlite_store::Store;
use crate::hlc::HlcClock;
use serde::Deserialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tungstenite::{connect, Message};
use url::Url;

#[derive(Deserialize)]
struct IncomingChange {
    table: String,
    #[serde(rename = "rowId")]
    row_id: String,
    op: String,
    fields: Option<std::collections::HashMap<String, serde_json::Value>>,
    #[serde(rename = "fieldHlcs")]
    field_hlcs: Option<std::collections::HashMap<String, String>>,
    hlc: String,
}

#[derive(Deserialize)]
struct LiveMessage {
    #[serde(rename = "type")]
    kind: String,
    changes: Option<Vec<IncomingChange>>,
}

/// Implement this in the native wrapper (Kotlin/Swift/TS) to get notified
/// when a live change lands, so the UI can refresh without waiting for the
/// next poll. Optional — apps that only care about eventual consistency
/// via `pull()` don't need to implement this at all.
#[uniffi::export(with_foreign)]
pub trait LiveSyncListener: Send + Sync {
    fn on_changes_applied(&self, count: u32);
    fn on_connection_state(&self, connected: bool);
}

pub struct LiveSyncHandle {
    stop_flag: Arc<AtomicBool>,
}

impl LiveSyncHandle {
    pub fn stop(&self) {
        self.stop_flag.store(true, Ordering::SeqCst);
    }
}

/// Spawns the background reconnect-loop thread. Call once per app session
/// (e.g. on app foreground); call `.stop()` on the returned handle when
/// backgrounding, to avoid holding a socket open the OS will kill anyway.
pub fn start(
    store: Arc<Mutex<Store>>,
    clock: Arc<Mutex<HlcClock>>,
    server_url: String,
    auth_token: String,
    device_id: String,
    listener: Option<Arc<dyn LiveSyncListener>>,
) -> LiveSyncHandle {
    let stop_flag = Arc::new(AtomicBool::new(false));
    let stop_flag_thread = stop_flag.clone();

    thread::spawn(move || {
        let mut backoff = Backoff::new(500, 30_000); // 0.5s base, cap 30s

        while !stop_flag_thread.load(Ordering::SeqCst) {
            let ws_url = to_ws_url(&server_url, &auth_token, &device_id);
            match connect(&ws_url) {
                Ok((mut socket, _response)) => {
                    backoff.reset();
                    if let Some(l) = &listener {
                        l.on_connection_state(true);
                    }

                    loop {
                        if stop_flag_thread.load(Ordering::SeqCst) {
                            let _ = socket.close(None);
                            return;
                        }
                        match socket.read() {
                            Ok(Message::Text(text)) => {
                                if let Ok(msg) = serde_json::from_str::<LiveMessage>(&text) {
                                    if msg.kind == "changes" {
                                        if let Some(changes) = msg.changes {
                                            apply_incoming(&store, &clock, changes, &listener);
                                        }
                                    }
                                }
                            }
                            Ok(Message::Close(_)) | Err(_) => break, // fall through to reconnect
                            _ => {}
                        }
                    }
                }
                Err(_) => { /* connect failed — fall through to backoff + retry */ }
            }

            if let Some(l) = &listener {
                l.on_connection_state(false);
            }
            if stop_flag_thread.load(Ordering::SeqCst) {
                return;
            }
            thread::sleep(backoff.next_delay());
        }
    });

    LiveSyncHandle { stop_flag }
}

fn apply_incoming(
    store: &Arc<Mutex<Store>>,
    clock: &Arc<Mutex<HlcClock>>,
    changes: Vec<IncomingChange>,
    listener: &Option<Arc<dyn LiveSyncListener>>,
) {
    let store = store.lock().unwrap();
    let mut applied_count = 0u32;
    for change in &changes {
        let applied = store
            .apply_remote_change(
                &change.table,
                &change.row_id,
                &change.op,
                &change.fields.clone().unwrap_or_default(),
                &change.field_hlcs.clone().unwrap_or_default(),
            )
            .unwrap_or(false);
        if applied {
            applied_count += 1;
            clock.lock().unwrap().receive(&change.hlc);
        }
    }
    if applied_count > 0 {
        if let Some(l) = listener {
            l.on_changes_applied(applied_count);
        }
    }
}

fn to_ws_url(server_url: &str, token: &str, device_id: &str) -> String {
    let mut url = Url::parse(server_url).unwrap_or_else(|_| Url::parse("http://localhost").unwrap());
    let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
    let _ = url.set_scheme(scheme);
    url.set_path("/sync/live");
    url.query_pairs_mut()
        .append_pair("token", token)
        .append_pair("deviceId", device_id);
    url.to_string()
}
