import { ChangeRecord } from "../sync/protocol";

/**
 * Everything the sync protocol needs from a database. Implement this once
 * per backend (Postgres, MySQL, MongoDB, ...) and the sync logic in
 * src/sync/* never has to change.
 */
export interface DBAdapter {
  connect(): Promise<void>;
  disconnect(): Promise<void>;

  /** Persist a batch of incoming changes to the append-only change log,
   * skipping any whose id already exists (idempotency). Returns which
   * change ids were newly inserted. */
  appendChanges(changes: ChangeRecord[]): Promise<string[]>;

  /** Fetch changes with cursor > sinceCursor, ordered by cursor ascending,
   * scoped to the given user, up to `limit` records. */
  getChangesSince(
    userId: string,
    sinceCursor: string | null,
    limit: number
  ): Promise<{ changes: ChangeRecord[]; nextCursor: string; hasMore: boolean }>;

  /** Apply a change to the materialized entity table/collection (not just
   * the log) using field-level HLC comparison: a field is only overwritten
   * if the incoming field HLC is greater than the field's currently stored
   * HLC. Returns true if applied, false if superseded (no-op). */
  applyChangeToEntity(change: ChangeRecord): Promise<boolean>;

  /** Read current materialized state of a row, mainly for debugging/admin. */
  getRow(table: string, rowId: string): Promise<Record<string, unknown> | null>;

  /** Remove tombstoned delete records older than `olderThanMs`, once all
   * known devices have acked past that cursor. */
  garbageCollectTombstones(olderThanMs: number): Promise<number>;

  /** Run any migrations not yet applied to this database, tracked in a
   * `schema_migrations` table (see server/migrations/*). Idempotent — safe
   * to call on every server start. Migrations run in ascending numeric
   * order; each runs inside its own transaction where the underlying DB
   * supports DDL transactions (Postgres, MySQL) — MongoDB migrations are
   * scripted operations, not DDL, and must be individually idempotent. */
  runMigrations(): Promise<{ applied: number[] }>;
}
