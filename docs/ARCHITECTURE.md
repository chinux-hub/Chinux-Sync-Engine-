# Sync Engine — Architecture

## Fixes applied (see CHANGELOG-fixes.md for full detail)

As of this revision, five previously-identified gaps have been closed:

1. **Encryption at rest** — local SQLite is now opened via SQLCipher with a per-device key from platform-secure storage (Android Keystore, iOS Keychain, OS keychain on desktop).
2. **Schema migrations** — both client (`__sync_schema_version` table, Rust migration runner) and server (`schema_migrations` table, per-adapter migration runner) now have a real versioned migration path.
3. **Live WebSocket sync** — the server's existing broadcast hub now has an actual client: `core-rust/src/live_sync.rs` connects, listens, applies changes, and reconnects with backoff. Previously the server broadcast to nobody.
4. **Retry/backoff** — push/pull and the live-sync reconnect loop use exponential backoff with full jitter (`core-rust/src/backoff.rs`), replacing blind fixed-interval retries.
5. **Background sync scheduling** — Android uses WorkManager, iOS uses BGTaskScheduler, both respecting OS-imposed constraints rather than only syncing in the foreground.

## Goals
- Offline-first: clients read/write local SQLite or IndexedDB instantly, sync opportunistically.
- Multi-platform: one sync algorithm across Android, iOS, Desktop, and Web (browser/PWA).
- Server DB agnostic: Postgres, MySQL, MongoDB via a common adapter interface.
- No vendor lock-in (no Supabase / BaaS dependency).

## High-level flow

```
 ┌─────────────┐        ┌─────────────┐        ┌──────────────┐        ┌──────────────┐
 │  Android    │        │  iOS        │        │  Desktop     │        │  Web / PWA   │
 │  (Kotlin)   │        │  (Swift)    │        │  (Tauri/Rust)│        │  (TS / Web)  │
 │  ┌────────┐ │        │  ┌────────┐ │        │  ┌────────┐  │        │  ┌─────────┐ │
 │  │ Rust   │ │        │  │ Rust   │ │        │  │ Rust   │  │        │  │ HLC/Sync│ │
 │  │ core.so│ │        │  │ core.a │ │        │  │ core   │  │        │  │ Engine  │ │
 │  │ (JNI)  │ │        │  │ (FFI)  │ │        │  │(native)│  │        │  │ (TS)    │ │
 │  └───┬────┘ │        │  └───┬────┘ │        │  └───┬────┘  │        │  └───┬─────┘ │
 │  SQLite     │        │  SQLite     │        │  SQLite      │        │  IndexedDB   │
 └──────┼──────┘        └──────┼──────┘        └──────┼───────┘        └──────┼───────┘
        │  HTTPS + WebSocket (delta sync protocol)                            │
        └──────────────────────────────────────┬──────────────────────────────┘
                                               │
                                      ┌────────▼─────────┐
                                      │   Sync Server     │
                                      │  (Node/TS)        │
                                      │  DB Adapter layer │
                                      └────────┬──────────┘
                                   ┌───────────┼───────────┐
                               ┌────▼───┐  ┌────▼───┐  ┌────▼────┐
                               │Postgres│  │ MySQL  │  │ MongoDB │
                               └────────┘  └────────┘  └─────────┘
```

## Core concepts

### 1. Change log, not snapshots
Every mutation (insert/update/delete) is captured as a **change record**:
```
{ id, table, row_id, op, fields: {col: value}, hlc, device_id }
```
Clients never sync full tables — only the delta of changes since their last
sync cursor. This is what makes offline sync efficient and conflict-resolvable.

### 2. Hybrid Logical Clock (HLC)
Each device keeps an HLC = `(physical_time_ms, logical_counter, device_id)`.
- Monotonic even if device clocks drift or go offline for weeks.
- Total ordering across devices without a central sequencer.
- Used both as the "when did this happen" stamp and the tie-breaker for
  conflicting writes.

### 3. Conflict resolution: field-level LWW
Conflicts are resolved **per column**, not per row. If Device A changes
`title` and Device B changes `price` on the same row while both offline,
both edits survive — they don't overwrite each other. If both changed the
same column, the write with the higher HLC wins deterministically on every
device (no central authority needed to agree, since HLC comparison is
commutative and available locally).

This is a deliberate middle ground:
- Simpler and cheaper than full CRDTs (no CRDT data structures per field type).
- Rarely loses data, unlike whole-row LWW, since most concurrent edits touch
  different fields.
- Deterministic: every replica converges to the same state without central
  coordination (this is the actual "CRDT" property — commutative,
  associative, idempotent merge — just implemented at field granularity
  instead of with generalized CRDT types).

Tradeoff: same-field concurrent edits still pick a winner and silently drop
the loser's value for that field. Where that's unacceptable (e.g. financial
counters), the schema should model the field as an explicit operation log
(e.g. `+5`, `-3`) rather than a scalar — the protocol supports per-column
merge strategies, and the default LWW can be overridden per column.

### 4. Sync protocol (push/pull, transport-agnostic)
- **Push**: client sends `changes[]` since its last acked cursor.
- **Pull**: client sends `since_cursor`, server returns `changes[]` +
  new `cursor`.
- Works over plain HTTPS (polling on reconnect) or WebSocket
  (server pushes live changes to connected clients). Same protocol either way.
- Server applies each incoming change via the row's stored HLC — if the
  incoming HLC is older than the field's current HLC, the change is a no-op
  (already superseded); otherwise it's applied and re-broadcast to other
  devices.

### 5. Tombstones
Deletes are recorded as change records with `op: "delete"`, not removed
outright, so other devices syncing later know to delete their local copy
too. Tombstones are garbage-collected after a retention window (default 30
days) once all known devices have acknowledged them.

### 6. Server DB adapters
`DBAdapter` interface (see `server/src/db/adapter.ts`) exposes exactly the
operations the sync protocol needs: `appendChanges`, `getChangesSince`,
`upsertRow`, `getRow`. Postgres/MySQL adapters use a `changes` table +
per-entity tables; the MongoDB adapter uses a `changes` collection +
per-entity collections. Swapping databases means swapping the adapter, not
touching sync logic.

## Why a Rust core instead of per-platform reimplementation
The sync/merge/HLC logic is the part that must be *bit-for-bit consistent*
across devices — a bug in one platform's merge logic causes silent,
hard-to-debug divergence. Writing it once in Rust and compiling it to:
- a `.so`/`.aar` for Android via JNI (using `uniffi-rs` bindgen)
- a static `.a`/`.xcframework` for iOS via C FFI
- a native module for Desktop (Tauri apps embed Rust directly; Electron
  can load it as a native Node addon via `napi-rs`)

...means one audited implementation instead of three. Platform code becomes
a thin wrapper: local SQLite access, UI bindings, and calling into the core
for `apply_local_change()`, `sync_push()`, `sync_pull()`.

## Repo layout
```
core-rust/    — sync engine: HLC, change tracking, merge logic, SQLite I/O
server/       — Node/TS sync server + DB adapters (Postgres/MySQL/MongoDB)
clients/android/  — Kotlin wrapper over the JNI bindings
clients/ios/      — Swift wrapper over the C FFI bindings
clients/desktop/  — TS wrapper (Tauri/Node) over the native addon
```
