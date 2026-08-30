/** Core wire types for the sync protocol. Transport-agnostic: the same
 * shapes are used whether delivered over HTTPS polling or WebSocket push. */

export type ChangeOp = "insert" | "update" | "delete";

/** A single field-level change to one row. `fields` is omitted for deletes. */
export interface ChangeRecord {
  id: string; // unique change id (uuid), for idempotency/dedup
  table: string;
  rowId: string;
  op: ChangeOp;
  fields?: Record<string, unknown>; // column -> new value (only changed columns)
  fieldHlcs?: Record<string, string>; // column -> HLC of that field's write
  hlc: string; // HLC of this change record overall (used for delete/row-level ordering)
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
  cursor: string; // server cursor after applying this push, client should store it
}

export interface PullRequest {
  deviceId: string;
  userId: string;
  sinceCursor: string | null; // null = full initial sync
  limit?: number;
}

export interface PullResponse {
  changes: ChangeRecord[];
  cursor: string; // pass this back as sinceCursor next time
  hasMore: boolean;
}
