# Offline-first Multi-Device Sync Engine

Self-hosted, no BaaS dependency. One Rust sync core shared by Android, iOS,
and Desktop; a Node/TypeScript server with pluggable Postgres, MySQL, or
MongoDB adapters.

Read `docs/ARCHITECTURE.md` first — it explains the HLC clock, field-level
conflict resolution, and change-log design that everything else depends on.

## Repo layout
```
core-rust/    Shared sync engine (Rust) — HLC, SQLite storage, push/pull.
              Compiles to Android (.so via JNI/UniFFI), iOS (.a / XCFramework
              via C FFI/UniFFI), and links directly into Desktop (Tauri).
server/       Sync server (Node/TypeScript). DB-agnostic sync logic in
              src/sync/*, swappable adapters in src/db/*.
clients/
  android/    Kotlin wrapper (SyncEngine.kt) over generated UniFFI bindings.
  ios/        Swift wrapper (SyncEngine.swift) over generated UniFFI bindings.
  desktop/    Tauri commands (Rust) + TS wrapper for a webview frontend.
docker-compose.dev.yml   Spins up Postgres/MySQL/MongoDB for local testing.
```

## Getting the server running

```bash
cd server
cp .env.example .env        # edit DB_BACKEND + connection string
npm install
docker compose -f ../docker-compose.dev.yml up -d postgres   # or mysql/mongodb
npm run dev
```

`POST /sync/push` and `POST /sync/pull` (JWT bearer auth) implement the
protocol in `src/sync/protocol.ts`. A `/sync/live` WebSocket pushes changes
to a user's other connected devices in real time as an optimization; pull
is still the source of truth for catching up after being offline.

## Building the Rust core for each platform

```bash
cd core-rust

# Android (all common ABIs)
cargo install cargo-ndk
cargo ndk -t arm64-v8a -t armeabi-v7a -t x86_64 build --release
cargo run --bin uniffi-bindgen generate --library target/aarch64-linux-android/release/libsync_core.so \
  --language kotlin --out-dir ../clients/android/generated

# iOS
rustup target add aarch64-apple-ios aarch64-apple-ios-sim
cargo build --target aarch64-apple-ios --release
cargo build --target aarch64-apple-ios-sim --release
cargo run --bin uniffi-bindgen generate --library target/aarch64-apple-ios/release/libsync_core.a \
  --language swift --out-dir ../clients/ios/generated
xcodebuild -create-xcframework \
  -library target/aarch64-apple-ios/release/libsync_core.a \
  -library target/aarch64-apple-ios-sim/release/libsync_core.a \
  -output ../clients/ios/SyncCore.xcframework

# Desktop (Tauri) — no bindgen needed, it's linked as a normal Rust crate
# dependency of src-tauri; see clients/desktop/tauri_commands.rs
```

## Using it in app code
All three platform wrappers expose the same shape:
```
upsert(table, rowId, fields, isInsert)
delete(table, rowId)
getRow(table, rowId) -> merged row or null
syncNow() -> { pushed, pulled }
```
Call `syncNow()` on app foreground, on network reconnect, and on a periodic
timer/background job. Local writes apply instantly (optimistic, offline-safe)
and are queued for the next successful push automatically — nothing is lost
if the device is offline for weeks.

## What's a reference implementation vs. what you still need to add
Built and complete:
- HLC clock (identical algorithm client + server), field-level LWW merge
- Change log + tombstones + cursor-based delta pull
- All three DB adapters (Postgres, MySQL, MongoDB) implementing one interface
- Rust core with SQLite local storage, outbox queue, push/pull
- Kotlin, Swift, and TS/Tauri wrappers with matching APIs

Before shipping, you'll still want to add:
- Your actual auth/login flow issuing the JWTs `authMiddleware` expects
- Schema/table allowlisting + per-row authorization (who can write which rows)
- Push notification wake-up for background sync on iOS/Android
- Payload compression and chunking for very large initial syncs
- Monitoring/alerting on the tombstone GC and adapter error rates
