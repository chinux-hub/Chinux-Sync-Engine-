import { Router, Request, Response } from "express";
import { DBAdapter } from "../db/adapter";
import { PushRequest, PushResponse, PullRequest, PullResponse } from "./protocol";
import { WSHub } from "./wsHub";

export function createSyncRouter(db: DBAdapter, wsHub: WSHub): Router {
  const router = Router();

  // POST /sync/push  { deviceId, userId, changes[] }
  router.post("/push", async (req: Request, res: Response) => {
    const body = req.body as PushRequest;
    if (!body?.changes || !Array.isArray(body.changes)) {
      return res.status(400).json({ error: "changes[] required" });
    }
    // authenticatedUserId comes from auth middleware (req.userId); we cross-check
    // against the body to prevent a client claiming another user's data.
    const authedUserId = (req as any).userId;
    if (authedUserId && authedUserId !== body.userId) {
      return res.status(403).json({ error: "userId mismatch" });
    }

    const insertedIds = await db.appendChanges(body.changes);
    const rejected: PushResponse["rejected"] = [];

    for (const change of body.changes) {
      if (!insertedIds.includes(change.id)) continue; // duplicate, already applied previously
      try {
        await db.applyChangeToEntity(change);
      } catch (e: any) {
        rejected.push({ changeId: change.id, reason: e?.message ?? "apply failed" });
      }
    }

    // Broadcast to this user's other connected devices for live sync.
    wsHub.broadcastToUser(body.userId, body.deviceId, body.changes);

    const { nextCursor } = await db.getChangesSince(body.userId, null, 1);
    const response: PushResponse = {
      accepted: insertedIds.length,
      rejected,
      cursor: nextCursor,
    };
    res.json(response);
  });

  // POST /sync/pull  { deviceId, userId, sinceCursor, limit? }
  router.post("/pull", async (req: Request, res: Response) => {
    const body = req.body as PullRequest;
    const authedUserId = (req as any).userId;
    if (authedUserId && authedUserId !== body.userId) {
      return res.status(403).json({ error: "userId mismatch" });
    }

    const limit = body.limit ?? 500;
    const { changes, nextCursor, hasMore } = await db.getChangesSince(
      body.userId,
      body.sinceCursor,
      limit
    );
    // Don't echo a device's own changes back to it.
    const filtered = changes.filter((c) => c.deviceId !== body.deviceId);

    const response: PullResponse = { changes: filtered, cursor: nextCursor, hasMore };
    res.json(response);
  });

  return router;
}
