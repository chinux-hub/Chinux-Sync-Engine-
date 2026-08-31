import { WebSocketServer, WebSocket } from "ws";
import { Server } from "http";
import { ChangeRecord } from "./protocol";
import jwt from "jsonwebtoken";

interface Connection {
  ws: WebSocket;
  userId: string;
  deviceId: string;
}

/**
 * Optional real-time layer: when a device is online and connected, changes
 * from other devices are pushed immediately instead of waiting for the next
 * poll. Purely an optimization — clients must still be able to catch up via
 * POST /sync/pull after reconnecting from offline, since WS delivery isn't
 * guaranteed (that's what the cursor-based pull is for).
 */
export class WSHub {
  private connections: Connection[] = [];

  constructor(server: Server, private jwtSecret: string) {
    const wss = new WebSocketServer({ server, path: "/sync/live" });
    wss.on("connection", (ws, req) => {
      try {
        const url = new URL(req.url ?? "", "http://localhost");
        const token = url.searchParams.get("token") ?? "";
        const payload = jwt.verify(token, this.jwtSecret) as { userId: string };
        const deviceId = url.searchParams.get("deviceId") ?? "unknown";
        const conn: Connection = { ws, userId: payload.userId, deviceId };
        this.connections.push(conn);
        ws.on("close", () => {
          this.connections = this.connections.filter((c) => c !== conn);
        });
      } catch {
        ws.close(4001, "unauthorized");
      }
    });
  }

  broadcastToUser(userId: string, originDeviceId: string, changes: ChangeRecord[]): void {
    if (changes.length === 0) return;
    const payload = JSON.stringify({ type: "changes", changes });
    for (const conn of this.connections) {
      if (conn.userId !== userId) continue;
      if (conn.deviceId === originDeviceId) continue;
      if (conn.ws.readyState === WebSocket.OPEN) conn.ws.send(payload);
    }
  }
}
