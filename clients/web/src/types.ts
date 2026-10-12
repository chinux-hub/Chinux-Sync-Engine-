/**
 * Core wire and engine types for the web sync engine.
 * Fully compatible with the server protocol and mobile/desktop clients.
 */

export type ChangeOp = "insert" | "update" | "delete";

export type FieldValue = string | number | boolean | null | Record<string, unknown> | unknown[];

/** A single field-level change to one row. */
export interface ChangeRecord {
  id: string; // unique change id (uuid), for idempotency/dedup
  table: string;
  rowId: string;
  op: ChangeOp;
  fields?: Record<string, unknown>; // column -> new value (only changed columns)
  fieldHlcs?: Record<string, string>; // column -> HLC of that field's write
  hlc: string; // HLC of this change record overall
  deviceId: string;
  userId: string;
}

export interface PushRequest {
  deviceId: string;
  userId: string;
  changes: ChangeRecord[];
}

export interface PushResponse {
  accepted: number;
  rejected: { changeId: string; reason: string }[];
  cursor: string;
}

export interface PullRequest {
  deviceId: string;
  userId: string;
  sinceCursor: string | null;
  limit?: number;
}

export interface PullResponse {
  changes: ChangeRecord[];
  cursor: string;
  hasMore: boolean;
}

export interface SyncResult {
  pushed: number;
  pulled: number;
}

export interface WebEngineConfig {
  /** Full URL of the sync server, e.g. "https://api.example.com" or "http://localhost:4000" */
  serverUrl: string;
  /** JWT bearer authentication token */
  authToken: string;
  /** Current authenticated user ID */
  userId: string;
  /** Stable unique device ID. If omitted, will be auto-generated and stored in IndexedDB/localStorage */
  deviceId?: string;
  /** IndexedDB database name. Defaults to "chinux_sync_engine" */
  dbName?: string;
}

/** Internal schema for materialized entities stored in IndexedDB */
export interface EntityRecord {
  table: string;
  rowId: string;
  data: Record<string, unknown>;
  fieldHlcs: Record<string, string>;
  deleted: boolean;
  updatedAt: number;
}

/** Internal schema for queued outbox records stored in IndexedDB */
export interface OutboxRecord {
  id: string;
  table: string;
  rowId: string;
  op: ChangeOp;
  fields: Record<string, unknown>;
  fieldHlcs: Record<string, string>;
  hlc: string;
  createdAt: number;
  pushed: number; // 0 = false, 1 = true
}

export interface SyncChangeEvent {
  table: string;
  rowId: string;
  op: ChangeOp;
  source: "local" | "remote";
  data: Record<string, unknown> | null;
}

export type SyncChangeCallback = (event: SyncChangeEvent) => void;
