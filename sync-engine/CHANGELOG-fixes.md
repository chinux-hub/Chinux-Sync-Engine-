# Sync Engine — Fixes Applied

Five gaps were identified in review (see conversation history) and are now fixed. None required new external infrastructure — everything works within the existing SQLite-on-device / Postgres-or-MySQL-or-MongoDB-on-server design.

---

## 1. Encryption at rest

**Was:** `Store::open()` used plain `rusqlite::Connection::open()` — the local SQLite file was fully readable by anyone with device/file access.

**Now:**
- `core-rust/Cargo.toml`: `rusqlite` feature switched from `bundled` to `bundled-sqlcipher`.
- `core-rust/src/sqlite_store.rs`: `Store::open(path, encryption_key)` now takes a key, runs `PRAGMA key = '...'` as the very first statement, and probes the connection to fail fast on a wrong key rather than failing confusingly later.
- `core-rust/src/sync_engine.rs` / `lib.rs`: `EngineConfig` / `EngineConfigFfi` gained a `db_encryption_key` field, threaded through to `Store::open`.
- **Android** (`clients/android/SyncEngine.kt`): key generated once via `SecureRandom`, stored in `EncryptedSharedPreferences` (backed by Android Keystore).
- **iOS** (`clients/ios/SyncEngine.swift`): key generated once via `SecRandomCopyBytes`, stored in the iOS Keychain (`kSecAttrAccessibleAfterFirstUnlockThisDeviceOnly`, excluded from backups).
- **Desktop** (`clients/desktop/tauri_commands.rs`): key generated once, stored via the `keyring` crate (macOS Keychain / Windows Credential Manager / Linux Secret Service).

**You need to:** add `androidx.security:security-crypto` to the Android Gradle file, and `keyring` to the Tauri `Cargo.toml`, as noted in the respective files.

---

## 2. Schema migrations

**Was:** no migration system anywhere — client or server. A future schema change would have no defined upgrade path.

**Now:**
- **Client**: `core-rust/src/sqlite_store.rs` adds a `__sync_schema_version` table and a `run_migrations()` method, called automatically inside `Store::open()`. Add new migrations by appending to the `MIGRATIONS` const array — never edit existing entries.
- **Server**: `server/src/db/adapter.ts` adds `runMigrations()` to the `DBAdapter` interface. `server/src/db/migrationRunner.ts` is a shared runner for the two SQL adapters; `server/migrations/postgres/` and `server/migrations/mysql/` hold numbered `.sql` files. MongoDB migrations are scripted (not DDL) — see `server/migrations/mongodb/002_add_index.ts` and the adapter's `runMigrations()`.
- `server/src/index.ts` now calls `db.runMigrations()` on every boot — idempotent, safe to run repeatedly.

**You need to:** add real migrations to the relevant folder as your schema evolves; the one included (`002_add_outbox_index` / `002_add_index`) is an example index-creation migration to establish the pattern.

---

## 3. Live WebSocket sync (client side was missing)

**Was:** `server/src/sync/wsHub.ts` broadcasts changes over WebSocket — but no client (Android, iOS, or Desktop) ever connected to it. "Live sync" was actually just whatever polling interval each client happened to run.

**Now:**
- `core-rust/src/live_sync.rs` (new): connects to `{server}/sync/live`, listens for `{type:"changes", changes:[...]}` messages, applies them via the same field-level HLC merge logic as `pull()`, and reconnects on drop using the new backoff module.
- `LiveSyncListener` trait (exported via UniFFI as a callback interface) lets native code get notified when a live change lands, to refresh UI immediately rather than waiting for the next poll.
- Wired into `SyncEngine::start_live_sync()` / `stop_live_sync()`, exposed through `lib.rs`, and called from all three native wrappers (`SyncEngine.kt`, `SyncEngine.swift`, `tauri_commands.rs` + `syncEngine.ts`).

**Important:** this is a latency optimization, never a correctness requirement — if the socket is down, changes still arrive on the next `pull()` (foreground, periodic, or background-scheduled). Mobile wrappers stop the socket on background (since the OS would kill it anyway) and rely on scheduled background sync instead (see #5).

---

## 4. Retry / backoff

**Was:** a failed push/pull just returned an error and got blindly retried on a fixed timer (e.g. `setInterval(30_000)` on desktop) — no exponential backoff, no jitter, so a server outage would get hit identically by every client every 30 seconds.

**Now:**
- `core-rust/src/backoff.rs` (new): AWS-style full-jitter exponential backoff (`random(0, min(cap, base * 2^attempt))`), reset on success.
- `sync_engine.rs`: `push()` and `pull()` retry up to `MAX_ATTEMPTS` (5) using this backoff before surfacing a `Network` error to the caller. Client (4xx) errors fail fast without retrying, since retrying an identical rejected request never helps.
- `live_sync.rs`: the reconnect loop uses the same backoff module (0.5s base, 30s cap).
- `clients/desktop/syncEngine.ts`: `startPeriodicSync()` replaces the old naive `setInterval` — backs off on repeated failure, resets to the fast interval on success, and resyncs immediately on the browser's `online` event rather than waiting out a stale backoff delay.

---

## 5. Background sync scheduling

**Was:** Android and iOS wrappers only synced while the app was in the foreground — no WorkManager, no BGTaskScheduler. An offline device wouldn't catch up until next opened.

**Now:**
- **Android**: `BackgroundSyncWorker` (a `CoroutineWorker`) + `BackgroundSync.schedule()` / `.cancel()` using `WorkManager`, with a `NetworkType.CONNECTED` constraint and exponential backoff policy. Minimum 15-minute interval (a WorkManager platform limit, not a choice made here).
- **iOS**: `BackgroundSync.register()` / `.scheduleNext()` using `BGTaskScheduler` / `BGAppRefreshTaskRequest`. Requires adding the task identifier to `Info.plist` under `BGTaskSchedulerPermittedIdentifiers` and enabling the Background Fetch capability (noted in the file).
- **Desktop**: no equivalent needed — desktop apps aren't OS-suspended the way mobile apps are, so the existing `startPeriodicSync()` (now with backoff, per #4) already covers this case while the app is running.

**You need to:** call `BackgroundSync.schedule(context)` once (e.g. in `Application.onCreate()`) on Android, and `BackgroundSync.register(...)` + `.scheduleNext()` at the appropriate lifecycle points on iOS, as shown in each file's usage example at the bottom.

---

## What to do next

1. Since none of this can be compiled in this environment (no `cargo`/Xcode/Android Studio here), run `cargo check` in `core-rust/` as your first step to catch anything a manual review missed.
2. Regenerate the UniFFI bindings (`cargo run --bin uniffi-bindgen generate ...`) since `EngineConfigFfi` and the new `LiveSyncListener` callback interface changed the FFI surface.
3. Add the new Gradle/Cargo dependencies noted above (`androidx.security:security-crypto`, `androidx.work:work-runtime-ktx`, `keyring`, `tungstenite`, `rand`, `url` — the last three are already added to `core-rust/Cargo.toml`).
