# Web Client for Sync Engine (`@chinux/sync-engine-web`)

Offline-first sync engine client for modern Web Apps and PWAs (React, Vue, Svelte, Angular, vanilla JS, Next.js).

Shares the exact same wire protocol, Hybrid Logical Clock (HLC) format, field-level Last-Write-Wins (LWW) conflict resolution, and WebSocket live-sync mechanisms as the Android, iOS, and Desktop clients.

---

## Features

- **Offline-First Storage**: Uses browser-native **IndexedDB** (`__sync_meta`, `__sync_outbox`, `__sync_entities`). No WASM download, no multi-megabyte runtime overhead, and no special COOP/COEP HTTP headers required.
- **Identical HLC Algorithm**: Guarantees total ordering across Web, Mobile, Desktop, and Server.
- **Field-Level LWW Conflict Resolution**: Concurrent edits on different columns merge cleanly without data loss.
- **Delta Sync**: Efficient push/pull delta sync protocol with cursor tracking.
- **Real-Time Live Sync**: Browser `WebSocket` connection with automatic reconnection, exponential backoff, and full jitter.
- **Network & Tab Lifecycle Aware**:
  - Automatically reacts to `window.online` events to immediately flush outbox changes and catch up.
  - Automatically reacts to `document.visibilitychange` when the user switches back to the tab.
- **Reactive Subscriptions**: Subscribe to table or row mutations to power reactive UI state.

---

## Installation

```bash
# From within the repository or after publishing:
npm install clients/web
# Or import directly from your source tree in monorepos:
import { SyncEngine } from "./path/to/clients/web/src";
```

---

## Quickstart

```typescript
import { SyncEngine } from "@chinux/sync-engine-web";

// 1. Initialize with server URL, auth token, and user ID
await SyncEngine.init({
  serverUrl: "https://sync.example.com", // or http://localhost:4000
  authToken: "your-jwt-bearer-token",
  userId: "user-123",
  // Optional: deviceId (auto-generated & stored if omitted), dbName: "my_app_sync"
});

// 2. Perform optimistic local writes (works offline!)
const taskId = crypto.randomUUID();
await SyncEngine.upsert("tasks", taskId, {
  title: "Finish report",
  completed: false,
}, true); // isInsert = true

// 3. Read rows from local IndexedDB cache
const task = await SyncEngine.getRow("tasks", taskId);
const allTasks = await SyncEngine.getAllRows("tasks");

// 4. Manual sync
const { pushed, pulled } = await SyncEngine.syncNow();
console.log(`Pushed: ${pushed}, Pulled: ${pulled}`);

// 5. Start background periodic sync and live WebSocket sync
SyncEngine.startPeriodicSync();
SyncEngine.startLiveSync(
  (count) => console.log(`${count} live change(s) applied`),
  (connected) => console.log("WebSocket connected:", connected)
);
```

---

## API Reference

### `SyncEngine.init(config: WebEngineConfig): Promise<void>`
Initializes the engine, creates or opens the IndexedDB database, restores the HLC clock state, and sets up browser network event listeners (`online`, `visibilitychange`).

| Config Option | Type | Description |
|---|---|---|
| `serverUrl` | `string` | Base URL of the sync server (e.g., `https://api.myapp.com` or `http://localhost:4000`) |
| `authToken` | `string` | JWT bearer token sent in HTTP `Authorization` and WebSocket query params |
| `userId` | `string` | Current authenticated user ID |
| `deviceId` | `string` (optional) | Stable device identifier. If omitted, auto-generated and persisted in `localStorage`/IndexedDB |
| `dbName` | `string` (optional) | IndexedDB database name. Defaults to `"chinux_sync_engine"` |

---

### `SyncEngine.upsert(table, rowId, fields, isInsert): Promise<void>`
Writes an insert or update locally. Changes are applied immediately to the local cache (optimistic UI) and queued in the outbox to be pushed to the server.

```typescript
await SyncEngine.upsert("notes", noteId, { title: "Meeting notes", content: "..." }, false);
```

---

### `SyncEngine.delete(table, rowId): Promise<void>`
Records a local delete and queues a tombstone change for synchronization with other devices.

```typescript
await SyncEngine.delete("notes", noteId);
```

---

### `SyncEngine.getRow<T>(table, rowId): Promise<T | null>`
Reads the current merged local state of a row from IndexedDB. Returns `null` if the row does not exist or has been deleted.

---

### `SyncEngine.getAllRows<T>(table): Promise<T[]>`
Reads all non-deleted rows for a given table from IndexedDB.

---

### `SyncEngine.syncNow(): Promise<{ pushed: number; pulled: number }>`
Flushes pending outbox records to the server (`POST /sync/push`) and fetches new changes since the last pull cursor (`POST /sync/pull`).

---

### `SyncEngine.startPeriodicSync(baseIntervalMs = 30_000): void`
Runs sync on a self-adjusting timer with exponential backoff and full jitter on network errors. Resets back to `baseIntervalMs` on success. Automatically triggers on `window.online`.

---

### `SyncEngine.stopPeriodicSync(): void`
Stops the periodic sync loop.

---

### `SyncEngine.startLiveSync(onChanges?, onConnectionChange?): void`
Opens a real-time WebSocket connection to `/sync/live`. Incoming changes are merged via field-level LWW into the IndexedDB cache and subscribers are notified. Automatically reconnects with exponential backoff on dropouts.

---

### `SyncEngine.stopLiveSync(): void`
Closes the WebSocket connection and cancels reconnection timers.

---

### `SyncEngine.subscribe(table, callback): () => void`
Subscribes to mutations for a specific table (or `"*"` for all tables). Returns an unsubscribe function.

```typescript
const unsubscribe = SyncEngine.subscribe("tasks", (event) => {
  console.log(`Table ${event.table} row ${event.rowId} mutated via ${event.source}:`, event.data);
});
// When component unmounts:
unsubscribe();
```

---

## React Integration Example

```tsx
import React, { useEffect, useState } from "react";
import { SyncEngine } from "@chinux/sync-engine-web";

export function useSyncTable<T = any>(table: string) {
  const [rows, setRows] = useState<T[]>([]);

  useEffect(() => {
    let mounted = true;

    const refresh = async () => {
      const data = await SyncEngine.getAllRows<T>(table);
      if (mounted) setRows(data);
    };

    refresh();
    const unsubscribe = SyncEngine.subscribe(table, () => {
      refresh();
    });

    return () => {
      mounted = false;
      unsubscribe();
    };
  }, [table]);

  return rows;
}

export function TodoList() {
  const tasks = useSyncTable<{ id: string; title: string; completed: boolean }>("tasks");

  const addTask = async (title: string) => {
    const id = crypto.randomUUID();
    await SyncEngine.upsert("tasks", id, { id, title, completed: false }, true);
    // Optimistic UI updates automatically via subscription!
  };

  return (
    <ul>
      {tasks.map((task) => (
        <li key={task.id}>{task.title}</li>
      ))}
    </ul>
  );
}
```

---

## Offline PWA & Service Worker Support

- The web client works seamlessly inside **Progressive Web Apps (PWAs)** and is completely independent of network availability.
- When the user goes offline, writes succeed immediately into IndexedDB.
- When the user reconnects, the engine triggers an instant catch-up sync.
- Can be paired with a Service Worker for offline asset caching (HTML/CSS/JS) to make your web app 100% offline-capable.
