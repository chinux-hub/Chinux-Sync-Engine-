import { Db } from "mongodb";

/**
 * Mongo migrations are scripted operations rather than SQL DDL — each
 * exports `{ version, name, run(db) }`. Must be idempotent: `run` may
 * legitimately execute again if the tracking write in mongoRunner fails
 * right after a successful `run`, so operations here use Mongo's own
 * idempotent forms (createIndex is naturally a no-op if the index already
 * exists with the same spec).
 */
export default {
  version: 2,
  name: "add_index",
  async run(db: Db): Promise<void> {
    await db.collection("changes").createIndex({ userId: 1, seq: 1 });
  },
};
