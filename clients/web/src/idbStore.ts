import { ChangeOp, EntityRecord, OutboxRecord } from "./types";

const DB_VERSION = 1;
const STORE_META = "__sync_meta";
const STORE_OUTBOX = "__sync_outbox";
const STORE_ENTITIES = "__sync_entities";

export class IdbStore {
  private db: IDBDatabase | null = null;
  private readonly dbName: string;

  constructor(dbName = "chinux_sync_engine") {
    this.dbName = dbName;
  }

  async open(): Promise<void> {
    if (this.db) return;

    return new Promise((resolve, reject) => {
      const request = indexedDB.open(this.dbName, DB_VERSION);

      request.onupgradeneeded = (event) => {
        const db = (event.target as IDBOpenDBRequest).result;

        // Metadata table (__sync_meta)
        if (!db.objectStoreNames.contains(STORE_META)) {
          db.createObjectStore(STORE_META, { keyPath: "key" });
        }

        // Outbox queue (__sync_outbox)
        if (!db.objectStoreNames.contains(STORE_OUTBOX)) {
          const outboxStore = db.createObjectStore(STORE_OUTBOX, { keyPath: "id" });
          outboxStore.createIndex("pushed", "pushed", { unique: false });
          outboxStore.createIndex("createdAt", "createdAt", { unique: false });
        }

        // Materialized entity cache (__sync_entities)
        if (!db.objectStoreNames.contains(STORE_ENTITIES)) {
          const entityStore = db.createObjectStore(STORE_ENTITIES, {
            keyPath: ["table", "rowId"],
          });
          entityStore.createIndex("table", "table", { unique: false });
        }
      };

      request.onsuccess = () => {
        this.db = request.result;
        resolve();
      };

      request.onerror = () => {
        reject(new Error(`Failed to open IndexedDB '${this.dbName}': ${request.error?.message}`));
      };
    });
  }

  private getDB(): IDBDatabase {
    if (!this.db) {
      throw new Error("IdbStore has not been opened. Call open() first.");
    }
    return this.db;
  }

  async getMeta(key: string): Promise<string | null> {
    const db = this.getDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_META, "readonly");
      const store = tx.objectStore(STORE_META);
      const req = store.get(key);

      req.onsuccess = () => {
        resolve(req.result ? req.result.value : null);
      };
      req.onerror = () => reject(req.error);
    });
  }

  async setMeta(key: string, value: string): Promise<void> {
    const db = this.getDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_META, "readwrite");
      const store = tx.objectStore(STORE_META);
      const req = store.put({ key, value });

      req.onsuccess = () => resolve();
      req.onerror = () => reject(req.error);
    });
  }

  /**
   * Optimistically records a locally-created change in the outbox queue
   * and immediately applies it to the materialized entities cache.
   * Both operations run atomically in a single IndexedDB transaction.
   */
  async recordLocalChange(
    outboxRecord: OutboxRecord,
    updatedFields: Record<string, unknown>,
    fieldHlcs: Record<string, string>
  ): Promise<void> {
    const db = this.getDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction([STORE_OUTBOX, STORE_ENTITIES], "readwrite");
      const outboxStore = tx.objectStore(STORE_OUTBOX);
      const entityStore = tx.objectStore(STORE_ENTITIES);

      // 1. Put into outbox
      outboxStore.put(outboxRecord);

      // 2. Fetch and update materialized cache
      const getReq = entityStore.get([outboxRecord.table, outboxRecord.rowId]);
      getReq.onsuccess = () => {
        const existing: EntityRecord | undefined = getReq.result;
        let data: Record<string, unknown> = {};
        let hlcs: Record<string, string> = {};

        if (existing && !existing.deleted) {
          data = { ...existing.data };
          hlcs = { ...existing.fieldHlcs };
        }

        if (outboxRecord.op === "delete") {
          entityStore.put({
            table: outboxRecord.table,
            rowId: outboxRecord.rowId,
            data: {},
            fieldHlcs: {},
            deleted: true,
            updatedAt: Date.now(),
          });
        } else {
          // Merge in fields
          for (const [k, v] of Object.entries(updatedFields)) {
            data[k] = v;
          }
          for (const [k, v] of Object.entries(fieldHlcs)) {
            hlcs[k] = v;
          }
          entityStore.put({
            table: outboxRecord.table,
            rowId: outboxRecord.rowId,
            data,
            fieldHlcs: hlcs,
            deleted: false,
            updatedAt: Date.now(),
          });
        }
      };

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async getPendingOutbox(limit: number): Promise<OutboxRecord[]> {
    const db = this.getDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_OUTBOX, "readonly");
      const store = tx.objectStore(STORE_OUTBOX);
      const index = store.index("pushed");
      const req = index.openCursor(IDBKeyRange.only(0));
      const results: OutboxRecord[] = [];

      req.onsuccess = () => {
        const cursor = req.result;
        if (cursor && results.length < limit) {
          results.push(cursor.value as OutboxRecord);
          cursor.continue();
        } else {
          resolve(results);
        }
      };
      req.onerror = () => reject(req.error);
    });
  }

  async markPushed(ids: string[]): Promise<void> {
    if (ids.length === 0) return;
    const db = this.getDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_OUTBOX, "readwrite");
      const store = tx.objectStore(STORE_OUTBOX);

      for (const id of ids) {
        const getReq = store.get(id);
        getReq.onsuccess = () => {
          if (getReq.result) {
            const item: OutboxRecord = getReq.result;
            item.pushed = 1;
            store.put(item);
          }
        };
      }

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async getEntity(table: string, rowId: string): Promise<Record<string, unknown> | null> {
    const db = this.getDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_ENTITIES, "readonly");
      const store = tx.objectStore(STORE_ENTITIES);
      const req = store.get([table, rowId]);

      req.onsuccess = () => {
        const record = req.result as EntityRecord | undefined;
        if (!record || record.deleted) {
          resolve(null);
        } else {
          resolve(record.data);
        }
      };
      req.onerror = () => reject(req.error);
    });
  }

  async getAllEntities(table: string): Promise<Record<string, unknown>[]> {
    const db = this.getDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_ENTITIES, "readonly");
      const store = tx.objectStore(STORE_ENTITIES);
      const index = store.index("table");
      const req = index.getAll(table);

      req.onsuccess = () => {
        const records = (req.result as EntityRecord[]) || [];
        const nonDeleted = records
          .filter((r) => !r.deleted)
          .map((r) => r.data);
        resolve(nonDeleted);
      };
      req.onerror = () => reject(req.error);
    });
  }

  /**
   * Applies an incoming remote change using field-level LWW conflict resolution.
   * Returns true if local materialized state was updated, false if superseded.
   */
  async applyRemoteChange(
    table: string,
    rowId: string,
    op: ChangeOp,
    fields: Record<string, unknown> = {},
    fieldHlcs: Record<string, string> = {}
  ): Promise<boolean> {
    const db = this.getDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(STORE_ENTITIES, "readwrite");
      const store = tx.objectStore(STORE_ENTITIES);
      const req = store.get([table, rowId]);

      let modified = false;

      req.onsuccess = () => {
        const existing = req.result as EntityRecord | undefined;

        if (op === "delete") {
          store.put({
            table,
            rowId,
            data: {},
            fieldHlcs: {},
            deleted: true,
            updatedAt: Date.now(),
          });
          modified = true;
          return;
        }

        let currentData: Record<string, unknown> = {};
        let currentHlcs: Record<string, string> = {};

        if (existing && !existing.deleted) {
          currentData = { ...existing.data };
          currentHlcs = { ...existing.fieldHlcs };
        }

        let anyApplied = false;
        for (const [col, val] of Object.entries(fields)) {
          const incomingHlc = fieldHlcs[col] || "";
          const existingHlc = currentHlcs[col];

          const shouldApply = !existingHlc || incomingHlc > existingHlc;
          if (shouldApply) {
            currentData[col] = val;
            if (incomingHlc) {
              currentHlcs[col] = incomingHlc;
            }
            anyApplied = true;
          }
        }

        if (anyApplied || (existing && existing.deleted)) {
          store.put({
            table,
            rowId,
            data: currentData,
            fieldHlcs: currentHlcs,
            deleted: false,
            updatedAt: Date.now(),
          });
          modified = true;
        }
      };

      tx.oncomplete = () => resolve(modified);
      tx.onerror = () => reject(tx.error);
    });
  }
}
