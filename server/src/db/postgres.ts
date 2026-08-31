import { Pool } from "pg";
import path from "path";
import { DBAdapter } from "./adapter";
import { ChangeRecord } from "../sync/protocol";
import { runSqlMigrations } from "./migrationRunner";

/**
 * Schema (run once, see server/schema/postgres.sql):
 *
 * CREATE TABLE changes (
 *   id            TEXT PRIMARY KEY,
 *   table_name    TEXT NOT NULL,
 *   row_id        TEXT NOT NULL,
 *   op            TEXT NOT NULL,
 *   fields        JSONB,
 *   field_hlcs    JSONB,
 *   hlc           TEXT NOT NULL,
 *   device_id     TEXT NOT NULL,
 *   user_id       TEXT NOT NULL,
 *   server_seq    BIGSERIAL,       -- monotonic cursor, assigned on insert
 *   created_at    TIMESTAMPTZ DEFAULT now()
 * );
 * CREATE INDEX idx_changes_user_seq ON changes (user_id, server_seq);
 *
 * -- One materialized table per entity type, e.g.:
 * CREATE TABLE entity_state (
 *   table_name TEXT NOT NULL,
 *   row_id     TEXT NOT NULL,
 *   data       JSONB NOT NULL DEFAULT '{}',
 *   field_hlcs JSONB NOT NULL DEFAULT '{}',
 *   deleted    BOOLEAN NOT NULL DEFAULT false,
 *   PRIMARY KEY (table_name, row_id)
 * );
 */
export class PostgresAdapter implements DBAdapter {
  private pool: Pool;

  constructor(connectionString: string) {
    this.pool = new Pool({ connectionString });
  }

  async connect(): Promise<void> {
    await this.pool.query("SELECT 1");
  }

  async disconnect(): Promise<void> {
    await this.pool.end();
  }

  async appendChanges(changes: ChangeRecord[]): Promise<string[]> {
    if (changes.length === 0) return [];
    const client = await this.pool.connect();
    const inserted: string[] = [];
    try {
      await client.query("BEGIN");
      for (const c of changes) {
        const res = await client.query(
          `INSERT INTO changes (id, table_name, row_id, op, fields, field_hlcs, hlc, device_id, user_id)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
           ON CONFLICT (id) DO NOTHING
           RETURNING id`,
          [c.id, c.table, c.rowId, c.op, c.fields ?? null, c.fieldHlcs ?? null, c.hlc, c.deviceId, c.userId]
        );
        if (res.rowCount) inserted.push(c.id);
      }
      await client.query("COMMIT");
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
    return inserted;
  }

  async getChangesSince(userId: string, sinceCursor: string | null, limit: number) {
    const seq = sinceCursor ? BigInt(sinceCursor) : BigInt(0);
    const res = await this.pool.query(
      `SELECT id, table_name, row_id, op, fields, field_hlcs, hlc, device_id, user_id, server_seq
       FROM changes
       WHERE user_id = $1 AND server_seq > $2
       ORDER BY server_seq ASC
       LIMIT $3`,
      [userId, seq.toString(), limit + 1]
    );
    const rows = res.rows;
    const hasMore = rows.length > limit;
    const page = hasMore ? rows.slice(0, limit) : rows;
    const changes: ChangeRecord[] = page.map((r) => ({
      id: r.id,
      table: r.table_name,
      rowId: r.row_id,
      op: r.op,
      fields: r.fields ?? undefined,
      fieldHlcs: r.field_hlcs ?? undefined,
      hlc: r.hlc,
      deviceId: r.device_id,
      userId: r.user_id,
    }));
    const nextCursor = page.length ? String(page[page.length - 1].server_seq) : sinceCursor ?? "0";
    return { changes, nextCursor, hasMore };
  }

  async applyChangeToEntity(change: ChangeRecord): Promise<boolean> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const existing = await client.query(
        `SELECT data, field_hlcs FROM entity_state WHERE table_name=$1 AND row_id=$2 FOR UPDATE`,
        [change.table, change.rowId]
      );

      if (change.op === "delete") {
        await client.query(
          `INSERT INTO entity_state (table_name, row_id, data, field_hlcs, deleted)
           VALUES ($1,$2,'{}','{}', true)
           ON CONFLICT (table_name, row_id) DO UPDATE SET deleted = true`,
          [change.table, change.rowId]
        );
        await client.query("COMMIT");
        return true;
      }

      let data: Record<string, unknown> = existing.rows[0]?.data ?? {};
      let fieldHlcs: Record<string, string> = existing.rows[0]?.field_hlcs ?? {};
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

      if (anyApplied) {
        await client.query(
          `INSERT INTO entity_state (table_name, row_id, data, field_hlcs, deleted)
           VALUES ($1,$2,$3,$4,false)
           ON CONFLICT (table_name, row_id)
           DO UPDATE SET data = $3, field_hlcs = $4, deleted = false`,
          [change.table, change.rowId, JSON.stringify(data), JSON.stringify(fieldHlcs)]
        );
      }
      await client.query("COMMIT");
      return anyApplied;
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  }

  async getRow(table: string, rowId: string) {
    const res = await this.pool.query(
      `SELECT data, deleted FROM entity_state WHERE table_name=$1 AND row_id=$2`,
      [table, rowId]
    );
    if (!res.rows[0] || res.rows[0].deleted) return null;
    return res.rows[0].data;
  }

  async garbageCollectTombstones(olderThanMs: number): Promise<number> {
    const cutoff = new Date(Date.now() - olderThanMs);
    const res = await this.pool.query(
      `DELETE FROM changes WHERE op='delete' AND created_at < $1`,
      [cutoff]
    );
    return res.rowCount ?? 0;
  }

  async runMigrations(): Promise<{ applied: number[] }> {
    const dir = path.join(__dirname, "../../migrations/postgres");
    return runSqlMigrations(
      dir,
      async (sql, params) => this.pool.query(sql, params as any[]),
      `CREATE TABLE IF NOT EXISTS schema_migrations (
         version    INTEGER PRIMARY KEY,
         name       TEXT NOT NULL,
         applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
       )`,
      "dollar"
    );
  }
}
