import { Backoff } from "./backoff";
import { HlcClock } from "./hlc";
import { IdbStore } from "./idbStore";
import { LiveSyncManager } from "./liveSync";
import {
  ChangeOp,
  ChangeRecord,
  OutboxRecord,
  PullRequest,
  PullResponse,
  PushRequest,
  PushResponse,
  SyncChangeCallback,
  SyncChangeEvent,
  SyncResult,
  WebEngineConfig,
} from "./types";

function generateUUID(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

export class SyncEngine {
  private static store: IdbStore | null = null;
  private static clock: HlcClock | null = null;
  private static config: WebEngineConfig | null = null;
  private static periodicBackoff = new Backoff(1000, 30_000);
  private static periodicTimer: ReturnType<typeof setTimeout> | null = null;
  private static liveManager: LiveSyncManager | null = null;
  private static subscribers = new Map<string, Set<SyncChangeCallback>>();
  private static listenersAttached = false;

  /**
   * Initializes the web sync engine.
   * Opens IndexedDB, restores the HLC clock state, and prepares background network hooks.
   */
  static async init(config: WebEngineConfig): Promise<void> {
    const store = new IdbStore(config.dbName ?? "chinux_sync_engine");
    await store.open();

    // Determine or generate a stable device ID
    let deviceId = config.deviceId;
    if (!deviceId) {
      if (typeof localStorage !== "undefined") {
        deviceId = localStorage.getItem("__sync_engine_device_id") || undefined;
      }
      if (!deviceId) {
        deviceId = (await store.getMeta("device_id")) || undefined;
      }
      if (!deviceId) {
        deviceId = `web-${generateUUID()}`;
        if (typeof localStorage !== "undefined") {
          localStorage.setItem("__sync_engine_device_id", deviceId);
        }
        await store.setMeta("device_id", deviceId);
      }
    }

    const effectiveConfig: WebEngineConfig = {
      ...config,
      deviceId,
    };

    // Restore persisted HLC state
    const savedPhys = await store.getMeta("hlc_physical");
    const savedCnt = await store.getMeta("hlc_counter");
    const physical = savedPhys ? parseInt(savedPhys, 10) : 0;
    const counter = savedCnt ? parseInt(savedCnt, 10) : 0;

    const clock = new HlcClock(deviceId, physical, counter);

    this.store = store;
    this.clock = clock;
    this.config = effectiveConfig;

    if (!this.listenersAttached && typeof window !== "undefined") {
      this.attachWindowListeners();
      this.listenersAttached = true;
    }
  }

  private static getStore(): IdbStore {
    if (!this.store) throw new Error("SyncEngine is not initialized. Call SyncEngine.init() first.");
    return this.store;
  }

  private static getClock(): HlcClock {
    if (!this.clock) throw new Error("SyncEngine is not initialized. Call SyncEngine.init() first.");
    return this.clock;
  }

  private static getConfig(): WebEngineConfig {
    if (!this.config) throw new Error("SyncEngine is not initialized. Call SyncEngine.init() first.");
    return this.config;
  }

  /**
   * Apply an insert or update locally.
   * Write is immediate and optimistic in IndexedDB, and queued for server push.
   */
  static async upsert(
    table: string,
    rowId: string,
    fields: Record<string, unknown>,
    isInsert = true
  ): Promise<void> {
    const store = this.getStore();
    const clock = this.getClock();
    const config = this.getConfig();

    const hlc = clock.tick();
    const { physical, counter } = clock.getPersistedState();
    await store.setMeta("hlc_physical", physical.toString());
    await store.setMeta("hlc_counter", counter.toString());

    const fieldHlcs: Record<string, string> = {};
    for (const key of Object.keys(fields)) {
      fieldHlcs[key] = hlc;
    }

    const changeId = generateUUID();
    const outboxRecord: OutboxRecord = {
      id: changeId,
      table,
      rowId,
      op: isInsert ? "insert" : "update",
      fields,
      fieldHlcs,
      hlc,
      createdAt: Date.now(),
      pushed: 0,
    };

    await store.recordLocalChange(outboxRecord, fields, fieldHlcs);

    // Notify listeners
    const mergedData = await store.getEntity(table, rowId);
    this.emitChange({
      table,
      rowId,
      op: outboxRecord.op,
      source: "local",
      data: mergedData,
    });
  }

  /**
   * Delete a row locally and queue a tombstone change for sync.
   */
  static async delete(table: string, rowId: string): Promise<void> {
    const store = this.getStore();
    const clock = this.getClock();
    const config = this.getConfig();

    const hlc = clock.tick();
    const { physical, counter } = clock.getPersistedState();
    await store.setMeta("hlc_physical", physical.toString());
    await store.setMeta("hlc_counter", counter.toString());

    const changeId = generateUUID();
    const outboxRecord: OutboxRecord = {
      id: changeId,
      table,
      rowId,
      op: "delete",
      fields: {},
      fieldHlcs: {},
      hlc,
      createdAt: Date.now(),
      pushed: 0,
    };

    await store.recordLocalChange(outboxRecord, {}, {});

    this.emitChange({
      table,
      rowId,
      op: "delete",
      source: "local",
      data: null,
    });
  }

  /**
   * Retrieves the current merged local state of a row from IndexedDB.
   */
  static async getRow<T = Record<string, unknown>>(table: string, rowId: string): Promise<T | null> {
    const store = this.getStore();
    const row = await store.getEntity(table, rowId);
    return (row as T) ?? null;
  }

  /**
   * Retrieves all non-deleted rows for a table from the local IndexedDB cache.
   */
  static async getAllRows<T = Record<string, unknown>>(table: string): Promise<T[]> {
    const store = this.getStore();
    const rows = await store.getAllEntities(table);
    return rows as T[];
  }

  /**
   * Push queued local changes to the sync server.
   */
  static async push(batchSize = 500): Promise<number> {
    const store = this.getStore();
    const config = this.getConfig();

    const pending = await store.getPendingOutbox(batchSize);
    if (pending.length === 0) return 0;

    const wireChanges: ChangeRecord[] = pending.map((r) => ({
      id: r.id,
      table: r.table,
      rowId: r.rowId,
      op: r.op,
      fields: r.op === "delete" ? undefined : r.fields,
      fieldHlcs: r.op === "delete" ? undefined : r.fieldHlcs,
      hlc: r.hlc,
      deviceId: config.deviceId!,
      userId: config.userId,
    }));

    const reqBody: PushRequest = {
      deviceId: config.deviceId!,
      userId: config.userId,
      changes: wireChanges,
    };

    const resp = await fetch(`${config.serverUrl}/sync/push`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${config.authToken}`,
      },
      body: JSON.stringify(reqBody),
    });

    if (!resp.ok) {
      throw new Error(`Sync push failed with status ${resp.status}: ${await resp.text()}`);
    }

    const result: PushResponse = await resp.json();
    const ids = pending.map((p) => p.id);
    await store.markPushed(ids);
    if (result.cursor) {
      await store.setMeta("last_push_cursor", result.cursor);
    }

    return result.accepted;
  }

  /**
   * Pull delta changes from the sync server since the last sync cursor.
   * Merges changes column-by-column using field-level LWW.
   */
  static async pull(): Promise<number> {
    const store = this.getStore();
    const clock = this.getClock();
    const config = this.getConfig();

    let cursor = await store.getMeta("pull_cursor");
    let totalApplied = 0;

    while (true) {
      const reqBody: PullRequest = {
        deviceId: config.deviceId!,
        userId: config.userId,
        sinceCursor: cursor,
        limit: 500,
      };

      const resp = await fetch(`${config.serverUrl}/sync/pull`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${config.authToken}`,
        },
        body: JSON.stringify(reqBody),
      });

      if (!resp.ok) {
        throw new Error(`Sync pull failed with status ${resp.status}: ${await resp.text()}`);
      }

      const parsed: PullResponse = await resp.json();

      for (const change of parsed.changes) {
        const applied = await store.applyRemoteChange(
          change.table,
          change.rowId,
          change.op,
          change.fields ?? {},
          change.fieldHlcs ?? {}
        );

        if (applied) {
          totalApplied++;
          clock.receive(change.hlc);

          const entityData = await store.getEntity(change.table, change.rowId);
          this.emitChange({
            table: change.table,
            rowId: change.rowId,
            op: change.op,
            source: "remote",
            data: entityData,
          });
        }
      }

      cursor = parsed.cursor;
      await store.setMeta("pull_cursor", cursor);

      // Persist updated clock state
      const { physical, counter } = clock.getPersistedState();
      await store.setMeta("hlc_physical", physical.toString());
      await store.setMeta("hlc_counter", counter.toString());

      if (!parsed.hasMore) {
        break;
      }
    }

    return totalApplied;
  }

  /**
   * Push local changes then pull remote changes.
   * Primary method called on app focus, manual "Sync Now" button, or reconnect.
   */
  static async syncNow(): Promise<SyncResult> {
    const pushed = await this.push();
    const pulled = await this.pull();
    return { pushed, pulled };
  }

  /**
   * Starts periodic sync with exponential backoff and full jitter on network failures.
   */
  static startPeriodicSync(baseIntervalMs = 30_000): void {
    this.stopPeriodicSync();
    const tick = async () => {
      try {
        await this.syncNow();
        this.periodicBackoff.reset();
        this.periodicTimer = setTimeout(tick, baseIntervalMs);
      } catch (err) {
        console.warn("[SyncEngine Web] Periodic sync error, backing off:", err);
        this.periodicTimer = setTimeout(tick, this.periodicBackoff.nextDelay());
      }
    };
    this.periodicTimer = setTimeout(tick, 0);
  }

  static stopPeriodicSync(): void {
    if (this.periodicTimer) {
      clearTimeout(this.periodicTimer);
      this.periodicTimer = null;
    }
  }

  /**
   * Opens near-real-time live sync over WebSockets.
   */
  static startLiveSync(
    onChanges?: (count: number) => void,
    onConnectionChange?: (connected: boolean) => void
  ): void {
    this.stopLiveSync();

    const store = this.getStore();
    const clock = this.getClock();
    const config = this.getConfig();

    this.liveManager = new LiveSyncManager({
      serverUrl: config.serverUrl,
      authToken: config.authToken,
      deviceId: config.deviceId!,
      store,
      clock,
      onChanges,
      onConnectionChange,
      onEntityChange: (event) => this.emitChange(event),
    });

    this.liveManager.start();
  }

  static stopLiveSync(): void {
    if (this.liveManager) {
      this.liveManager.stop();
      this.liveManager = null;
    }
  }

  /**
   * Subscribes to changes on a specific table (or "*" for all tables).
   * Returns an unsubscribe function.
   */
  static subscribe(table: string, callback: SyncChangeCallback): () => void {
    let set = this.subscribers.get(table);
    if (!set) {
      set = new Set();
      this.subscribers.set(table, set);
    }
    set.add(callback);

    return () => {
      set?.delete(callback);
      if (set?.size === 0) {
        this.subscribers.delete(table);
      }
    };
  }

  private static emitChange(event: SyncChangeEvent): void {
    const tableSubscribers = this.subscribers.get(event.table);
    if (tableSubscribers) {
      for (const cb of tableSubscribers) {
        try {
          cb(event);
        } catch (e) {
          console.error("[SyncEngine Web] Subscriber callback threw:", e);
        }
      }
    }

    const wildcardSubscribers = this.subscribers.get("*");
    if (wildcardSubscribers) {
      for (const cb of wildcardSubscribers) {
        try {
          cb(event);
        } catch (e) {
          console.error("[SyncEngine Web] Wildcard subscriber threw:", e);
        }
      }
    }
  }

  private static attachWindowListeners(): void {
    window.addEventListener("online", () => {
      // Reconnected — immediately attempt sync and reset backoff
      this.periodicBackoff.reset();
      this.syncNow().catch((e) => console.warn("[SyncEngine Web] Online reconnect sync failed:", e));
    });

    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "visible") {
        // Tab brought to foreground — catch up
        this.syncNow().catch((e) => console.warn("[SyncEngine Web] Visibility catch-up sync failed:", e));
      }
    });
  }
}
