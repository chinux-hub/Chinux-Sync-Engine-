import fs from "fs";
import path from "path";

/** One migration file: `NNN_description.sql`, applied in ascending numeric order. */
export interface MigrationFile {
  version: number;
  name: string;
  sql: string;
}

export function loadMigrations(dir: string): MigrationFile[] {
  if (!fs.existsSync(dir)) return [];
  return fs
    .readdirSync(dir)
    .filter((f) => f.endsWith(".sql"))
    .map((f) => {
      const match = f.match(/^(\d+)_(.+)\.sql$/);
      if (!match) {
        throw new Error(`Migration filename "${f}" must match NNN_description.sql`);
      }
      return {
        version: parseInt(match[1], 10),
        name: match[2],
        sql: fs.readFileSync(path.join(dir, f), "utf8"),
      };
    })
    .sort((a, b) => a.version - b.version);
}

/**
 * Runs a SQL-based adapter's migrations against a `query(sql, params?)`
 * function the caller supplies (works for both `pg` and `mysql2` pools,
 * whose `.query()` signatures are compatible enough for this use).
 * Creates `schema_migrations` if it doesn't exist, then applies anything
 * with a version not already recorded there — safe to call on every boot.
 *
 * `paramStyle` picks the placeholder syntax for the tracking INSERT:
 * Postgres uses `$1, $2`, MySQL uses `?, ?`.
 */
export async function runSqlMigrations(
  dir: string,
  query: (sql: string, params?: unknown[]) => Promise<{ rows?: unknown[] }>,
  createTrackingTableSql: string,
  paramStyle: "dollar" | "question"
): Promise<{ applied: number[] }> {
  await query(createTrackingTableSql);

  const appliedRes = await query("SELECT version FROM schema_migrations");
  const appliedVersions = new Set(
    (appliedRes.rows ?? []).map((r: any) => Number(r.version))
  );

  const migrations = loadMigrations(dir);
  const applied: number[] = [];
  const insertSql =
    paramStyle === "dollar"
      ? "INSERT INTO schema_migrations (version, name) VALUES ($1, $2)"
      : "INSERT INTO schema_migrations (version, name) VALUES (?, ?)";

  for (const m of migrations) {
    if (appliedVersions.has(m.version)) continue;
    // Split on statement boundaries naively (each file should keep one
    // statement per line-group ending in `;`) — fine for the simple DDL
    // these migrations contain; a heavier migration tool would parse this
    // more robustly if migrations grow more complex.
    const statements = m.sql
      .split(/;\s*(?:\n|$)/)
      .map((s) => s.trim())
      .filter(Boolean);
    for (const stmt of statements) {
      await query(stmt);
    }
    await query(insertSql, [m.version, m.name]);
    applied.push(m.version);
  }

  return { applied };
}
