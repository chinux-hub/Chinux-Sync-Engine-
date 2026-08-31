import { MongoClient, Db, Collection } from "mongodb";
import { DBAdapter } from "./adapter";
import { ChangeRecord } from "../sync/protocol";

/**
 * Collections:
 *
 * changes: { _id (change id), table, rowId, op, fields, fieldHlcs, hlc,
 *            deviceId, userId, seq (monotonic, from counters collection), createdAt }
 *   indexes: { userId: 1, seq: 1 }
 *
 * entity_state: { _id: `${table}:${rowId}`, table, rowId, data, fieldHlcs, deleted }
 *
 * counters: { _id: "changes_seq", value: <number> }  -- used to generate a
 *   monotonic seq since Mongo has no native autoincrement.
 */
export class MongoDBAdapter implements DBAdapter {
  private client: MongoClient;
  private db!: Db;
  private changes!: Collection<any>;
  private entities!: Collection<any>;
  private counters!: Collection<any>;

  constructor(private uri: string, private dbName: string) {
    this.client = new MongoClient(uri);
  }

  async connect(): Promise<void> {
    await this.client.connect();
    this.db = this.client.db(this.dbName);
    this.changes = this.db.collection("changes");
    this.entities = this.db.collection("entity_state");
    this.counters = this.db.collection("counters");
    await this.changes.createIndex({ userId: 1, seq: 1 });
    await this.entities.createIndex({ table: 1, rowId: 1 }, { unique: true });
  }

  async disconnect(): Promise<void> {
    await this.client.close();
  }

  private async nextSeq(): Promise<number> {
    const res = await this.counters.findOneAndUpdate(
      { _id: "changes_seq" as any },
      { $inc: { value: 1 } },
      { upsert: true, returnDocument: "after" }
    );
    return res?.value ?? res?.value === 0 ? res!.value : 1;
  }

  async appendChanges(changes: ChangeRecord[]): Promise<string[]> {
    const inserted: string[] = [];
    for (const c of changes) {
      const exists = await this.changes.findOne({ _id: c.id as any });
      if (exists) continue;
      const seq = await this.nextSeq();
      await this.changes.insertOne({
        _id: c.id as any,
        table: c.table,
        rowId: c.rowId,
        op: c.op,
        fields: c.fields ?? null,
        fieldHlcs: c.fieldHlcs ?? null,
        hlc: c.hlc,
        deviceId: c.deviceId,
        userId: c.userId,
        seq,
        createdAt: new Date(),
      });
      inserted.push(c.id);
    }
    return inserted;
  }

  async getChangesSince(userId: string, sinceCursor: string | null, limit: number) {
    const seq = sinceCursor ? Number(sinceCursor) : 0;
    const docs = await this.changes
      .find({ userId, seq: { $gt: seq } })
      .sort({ seq: 1 })
      .limit(limit + 1)
      .toArray();
    const hasMore = docs.length > limit;
    const page = hasMore ? docs.slice(0, limit) : docs;
    const changesOut: ChangeRecord[] = page.map((r) => ({
      id: r._id,
      table: r.table,
      rowId: r.rowId,
      op: r.op,
      fields: r.fields ?? undefined,
      fieldHlcs: r.fieldHlcs ?? undefined,
      hlc: r.hlc,
      deviceId: r.deviceId,
      userId: r.userId,
    }));
    const nextCursor = page.length ? String(page[page.length - 1].seq) : sinceCursor ?? "0";
    return { changes: changesOut, nextCursor, hasMore };
  }

  async applyChangeToEntity(change: ChangeRecord): Promise<boolean> {
    const key = `${change.table}:${change.rowId}`;

    if (change.op === "delete") {
      await this.entities.updateOne(
        { _id: key as any },
        { $set: { table: change.table, rowId: change.rowId, deleted: true } },
        { upsert: true }
      );
      return true;
    }

    // Mongo has no row-level SELECT ... FOR UPDATE across drivers the same way
    // Postgres/MySQL do; use findOneAndUpdate with a retry loop for the
    // compare-and-swap on field_hlcs to stay safe under concurrent writers.
    for (let attempt = 0; attempt < 5; attempt++) {
      const existing = await this.entities.findOne({ _id: key as any });
      const data: Record<string, unknown> = existing?.data ?? {};
      const fieldHlcs: Record<string, string> = existing?.fieldHlcs ?? {};
      let anyApplied = false;

      for (const [col, val] of Object.entries(change.fields ?? {})) {
        const incomingHlc = change.fieldHlcs?.[col] ?? change.hlc;
        const currentHlc = fieldHlcs[col];
        if (!currentHlc || incomingHlc > currentHlc) {
          data[col] = val;
          fieldHlcs[col] = incomingHlc;
          anyApplied = true;
        }
      }
      if (!anyApplied) return false;

      const res = await this.entities.updateOne(
        { _id: key as any, ...(existing ? { fieldHlcs: existing.fieldHlcs ?? {} } : { fieldHlcs: { $exists: false } }) },
        { $set: { table: change.table, rowId: change.rowId, data, fieldHlcs, deleted: false } },
        { upsert: true }
      );
      if (res.matchedCount > 0 || res.upsertedCount > 0) return true;
      // else: lost race, retry
    }
    return false;
  }

  async getRow(table: string, rowId: string) {
    const doc = await this.entities.findOne({ _id: `${table}:${rowId}` as any });
    if (!doc || doc.deleted) return null;
    return doc.data;
  }

  async garbageCollectTombstones(olderThanMs: number): Promise<number> {
    const cutoff = new Date(Date.now() - olderThanMs);
    const res = await this.changes.deleteMany({ op: "delete", createdAt: { $lt: cutoff } });
    return res.deletedCount ?? 0;
  }

  async runMigrations(): Promise<{ applied: number[] }> {
    const migrationsCol = this.db.collection("schema_migrations");
    const appliedDocs = await migrationsCol.find({}).toArray();
    const appliedVersions = new Set(appliedDocs.map((d: any) => d.version));

    // Statically imported (not a directory scan, since these are TS
    // modules, not flat SQL files) — add new migrations to this list.
    const migrations = [(await import("../../../migrations/mongodb/002_add_index")).default];

    const applied: number[] = [];
    for (const m of migrations.sort((a, b) => a.version - b.version)) {
      if (appliedVersions.has(m.version)) continue;
      await m.run(this.db);
      await migrationsCol.insertOne({ version: m.version, name: m.name, appliedAt: new Date() } as any);
      applied.push(m.version);
    }
    return { applied };
  }
}
