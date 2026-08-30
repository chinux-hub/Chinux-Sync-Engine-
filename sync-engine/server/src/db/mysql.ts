import mysql, { Pool, PoolConnection } from "mysql2/promise";
import path from "path";
import { DBAdapter } from "./adapter";
import { ChangeRecord } from "../sync/protocol";
import { runSqlMigrations } from "./migrationRunner";

/**
 * Schema (see server/schema/mysql.sql):
 *
 * CREATE TABLE changes (
 *   id          VARCHAR(64) PRIMARY KEY,
 *   table_name  VARCHAR(128) NOT NULL,
 *   row_id      VARCHAR(128) NOT NULL,
 *   op          VARCHAR(16) NOT NULL,
 *   fields      JSON,
 *   field_hlcs  JSON,
 *   hlc         VARCHAR(64) NOT NULL,
 *   device_id   VARCHAR(128) NOT NULL,
 *   user_id     VARCHAR(128) NOT NULL,
 *   server_seq  BIGINT AUTO_INCREMENT UNIQUE,
 *   created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
 *   INDEX idx_user_seq (user_id, server_seq)
 * );
 *
 * CREATE TABLE entity_state (
 *   table_name VARCHAR(128) NOT NULL,
 *   row_id     VARCHAR(128) NOT NULL,
 *   data       JSON NOT NULL,
 *   field_hlcs JSON NOT NULL,
 *   deleted    BOOLEAN NOT NULL DEFAULT false,
 *   PRIMARY KEY (table_name, row_id)
 * );
 */
export class MySQLAdapter implements DBAdapter {
  private pool: Pool;

  constructor(config: { uri: string }) {
    this.pool = mysql.createPool(config.uri);
  }

  async connect(): Promise<void> {
    await this.pool.query("SELECT 1");
  }

  async disconnect(): Promise<void> {
    await this.pool.end();
  }

  async appendChanges(changes: ChangeRecord[]): Promise<string[]> {
    if (changes.length === 0) return [];
    const conn: PoolConnection = await this.pool.getConnection();
    const inserted: string[] = [];
    try {
      await conn.beginTransaction();
      for (const c of changes) {
        const [res]: any = await conn.query(
          `INSERT IGNORE INTO changes (id, table_name, row_id, op, fields, field_hlcs, hlc, device_id, user_id)
           VALUES (?,?,?,?,?,?,?,?,?)`,
          [
            c.id,
            c.table,
            c.rowId,
            c.op,
            c.fields ? JSON.stringify(c.fields) : null,
            c.fieldHlcs ? JSON.stringify(c.fieldHlcs) : null,
            c.hlc,
            c.deviceId,
            c.userId,
          ]
        );
        if (res.affectedRows > 0) inserted.push(c.id);
      }
      await conn.commit();
    } catch (e) {
      await conn.rollback();
      throw e;
    } finally {
      conn.release();
    }
    return inserted;
  }

  async getChangesSince(userId: string, sinceCursor: string | null, limit: number) {
    const seq = sinceCursor ? Number(sinceCursor) : 0;
    const [rows]: any = await this.pool.query(
      `SELECT id, table_name, row_id, op, fields, field_hlcs, hlc, device_id, user_id, server_seq
       FROM changes
       WHERE user_id = ? AND server_seq > ?
       ORDER BY server_seq ASC
       LIMIT ?`,
      [userId, seq, limit + 1]
    );
    const hasMore = rows.length > limit;
    const page = hasMore ? rows.slice(0, limit) : rows;
    const changes: ChangeRecord[] = page.map((r: any) => ({
      id: r.id,
      table: r.table_name,
      rowId: r.row_id,
      op: r.op,
      fields: r.fields ? JSON.parse(r.fields) : undefined,
      fieldHlcs: r.field_hlcs ? JSON.parse(r.field_hlcs) : undefined,
      hlc: r.hlc,
      deviceId: r.device_id,
      userId: r.user_id,
    }));
    const nextCursor = page.length ? String(page[page.length - 1].server_seq) : sinceCursor ?? "0";
    return { changes, nextCursor, hasMore };
  }

  async applyChangeToEntity(change: ChangeRecord): Promise<boolean> {
    const conn = await this.pool.getConnection();
    try {
      await conn.beginTransaction();
      const [rows]: any = await conn.query(
        `SELECT data, field_hlcs FROM entity_state WHERE table_name=? AND row_id=? FOR UPDATE`,
        [change.table, change.rowId]
      );

      if (change.op === "delete") {
        await conn.query(
          `INSERT INTO entity_state (table_name, row_id, data, field_hlcs, deleted)
           VALUES (?,?,'{}','{}',true)
           ON DUPLICATE KEY UPDATE deleted = true`,
          [change.table, change.rowId]
        );
        await conn.commit();
        return true;
      }

      let data: Record<string, unknown> = rows[0]?.data ? JSON.parse(rows[0].data) : {};
      let fieldHlcs: Record<string, string> = rows[0]?.field_hlcs ? JSON.parse(rows[0].field_hlcs) : {};
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
        await conn.query(
          `INSERT INTO entity_state (table_name, row_id, data, field_hlcs, deleted)
           VALUES (?,?,?,?,false)
           ON DUPLICATE KEY UPDATE data = ?, field_hlcs = ?, deleted = false`,
          [change.table, change.rowId, JSON.stringify(data), JSON.stringify(fieldHlcs), JSON.stringify(data), JSON.stringify(fieldHlcs)]
        );
      }
      await conn.commit();
      return anyApplied;
    } catch (e) {
      await conn.rollback();
      throw e;
    } finally {
      conn.release();
    }
  }

  async getRow(table: string, rowId: string) {
    const [rows]: any = await this.pool.query(
      `SELECT data, deleted FROM entity_state WHERE table_name=? AND row_id=?`,
      [table, rowId]
    );
    if (!rows[0] || rows[0].deleted) return null;
    return JSON.parse(rows[0].data);
  }

  async garbageCollectTombstones(olderThanMs: number): Promise<number> {
    const cutoff = new Date(Date.now() - olderThanMs);
    const [res]: any = await this.pool.query(`DELETE FROM changes WHERE op='delete' AND created_at < ?`, [cutoff]);
    return res.affectedRows ?? 0;
  }

  async runMigrations(): Promise<{ applied: number[] }> {
    const dir = path.join(__dirname, "../../migrations/mysql");
    return runSqlMigrations(
      dir,
      // mysql2's pool.query returns a [rows, fields] tuple, not {rows} —
      // adapt its shape to what the shared runner expects.
      async (sql, params) => {
        const [rows] = await this.pool.query(sql, params as any[]);
        return { rows: Array.isArray(rows) ? (rows as unknown[]) : [] };
      },
      `CREATE TABLE IF NOT EXISTS schema_migrations (
         version    INT PRIMARY KEY,
         name       VARCHAR(255) NOT NULL,
         applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
       )`,
      "question"
    );
  }
}
