import express from "express";
import http from "http";
import dotenv from "dotenv";
import { authMiddleware } from "./auth/auth";
import { createSyncRouter } from "./sync/router";
import { WSHub } from "./sync/wsHub";
import { DBAdapter } from "./db/adapter";
import { PostgresAdapter } from "./db/postgres";
import { MySQLAdapter } from "./db/mysql";
import { MongoDBAdapter } from "./db/mongodb";

dotenv.config();

function buildAdapter(): DBAdapter {
  const backend = process.env.DB_BACKEND ?? "postgres"; // postgres | mysql | mongodb
  switch (backend) {
    case "postgres":
      return new PostgresAdapter(process.env.POSTGRES_URL ?? "postgres://localhost/syncdb");
    case "mysql":
      return new MySQLAdapter({ uri: process.env.MYSQL_URL ?? "mysql://localhost/syncdb" });
    case "mongodb":
      return new MongoDBAdapter(process.env.MONGO_URL ?? "mongodb://localhost:27017", process.env.MONGO_DB ?? "syncdb");
    default:
      throw new Error(`Unknown DB_BACKEND: ${backend}`);
  }
}

async function main() {
  const app = express();
  app.use(express.json({ limit: "5mb" }));

  // Enable CORS for web apps
  app.use((req, res, next) => {
    res.header("Access-Control-Allow-Origin", process.env.CORS_ORIGIN ?? "*");
    res.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept, Authorization");
    if (req.method === "OPTIONS") {
      return res.sendStatus(200);
    }
    next();
  });

  const db = buildAdapter();
  await db.connect();
  console.log(`Connected to DB backend: ${process.env.DB_BACKEND ?? "postgres"}`);

  const { applied } = await db.runMigrations();
  console.log(applied.length ? `Applied migrations: ${applied.join(", ")}` : "Schema up to date, no migrations to apply");

  const server = http.createServer(app);
  const jwtSecret = process.env.JWT_SECRET ?? "dev-secret-change-me";
  const wsHub = new WSHub(server, jwtSecret);

  app.get("/health", (_req, res) => res.json({ ok: true }));
  app.use("/sync", authMiddleware(jwtSecret), createSyncRouter(db, wsHub));

  // Periodic tombstone GC (default: purge deletes older than 30 days)
  const gcIntervalMs = 24 * 60 * 60 * 1000;
  setInterval(() => {
    db.garbageCollectTombstones(30 * 24 * 60 * 60 * 1000).catch((e) =>
      console.error("tombstone GC failed", e)
    );
  }, gcIntervalMs);

  const port = Number(process.env.PORT ?? 4000);
  server.listen(port, () => console.log(`Sync server listening on :${port}`));
}

main().catch((e) => {
  console.error("Fatal startup error", e);
  process.exit(1);
});
