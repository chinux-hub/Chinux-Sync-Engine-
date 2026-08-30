// Frontend wrapper (React/Vue/vanilla) calling into the Tauri commands
// defined in tauri_commands.rs. Same public shape as the Kotlin/Swift
// wrappers, so app logic that talks to "the sync engine" reads the same
// across all three platforms.
import { invoke } from "@tauri-apps/api/tauri";
import { listen, UnlistenFn } from "@tauri-apps/api/event";

/** Exponential backoff with full jitter, mirroring core-rust/src/backoff.rs
 * — used here for the periodic sync timer so a server outage doesn't get
 * hammered every 30s by every open desktop client simultaneously. */
class Backoff {
  private attempt = 0;
  constructor(private baseMs: number, private capMs: number) {}
  next(): number {
    const exp = this.baseMs * Math.pow(2, Math.min(this.attempt, 20));
    const capped = Math.min(exp, this.capMs);
    this.attempt++;
    return Math.random() * capped;
  }
  reset() {
    this.attempt = 0;
  }
}

export class SyncEngine {
  private static backoff = new Backoff(1000, 30_000);
  private static timer: ReturnType<typeof setTimeout> | null = null;
  private static unlistenChanges: UnlistenFn | null = null;
  private static unlistenConnection: UnlistenFn | null = null;

  static async init(serverUrl: string, authToken: string, deviceId: string, userId: string): Promise<void> {
    await invoke("sync_init", { serverUrl, authToken, deviceId, userId });
  }

  static async upsert(
    table: string,
    rowId: string,
    fields: Record<string, string | number | boolean | null>,
    isInsert: boolean
  ): Promise<void> {
    await invoke("sync_upsert", { table, rowId, op: isInsert ? "insert" : "update", fields });
  }

  static async delete(table: string, rowId: string): Promise<void> {
    await invoke("sync_upsert", { table, rowId, op: "delete", fields: {} });
  }

  static async getRow<T = Record<string, unknown>>(table: string, rowId: string): Promise<T | null> {
    return invoke("sync_get_row", { table, rowId });
  }

  /** Push local changes, pull remote changes. Call directly on app focus /
   * a manual "sync now" action — for ongoing background freshness, use
   * startPeriodicSync() below instead of calling this on a naive timer. */
  static async syncNow(): Promise<{ pushed: number; pulled: number }> {
    const [pushed, pulled] = await invoke<[number, number]>("sync_now");
    return { pushed, pulled };
  }

  /** Starts a self-adjusting periodic sync: retries quickly at first, backs
   * off exponentially (with jitter) on repeated failure, and resets back to
   * the fast interval as soon as a sync succeeds. Call once, e.g. after
   * init(). Pairs with startLiveSync() for near-real-time updates on top of
   * this baseline. */
  static startPeriodicSync(baseIntervalMs = 30_000): void {
    this.stopPeriodicSync();
    const tick = async () => {
      try {
        await this.syncNow();
        this.backoff.reset();
        this.timer = setTimeout(tick, baseIntervalMs);
      } catch {
        this.timer = setTimeout(tick, this.backoff.next());
      }
    };
    this.timer = setTimeout(tick, 0);

    window.addEventListener("online", () => {
      // Network just came back — don't wait out a long backoff delay.
      if (this.timer) clearTimeout(this.timer);
      this.backoff.reset();
      tick();
    });
  }

  static stopPeriodicSync(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
  }

  /** Opens the near-real-time WebSocket connection (desktop apps generally
   * stay resident, so — unlike the mobile wrappers — there's no matching
   * "stop on background" call needed; call this once after init()). */
  static async startLiveSync(onChanges?: (count: number) => void, onConnectionChange?: (connected: boolean) => void): Promise<void> {
    if (onChanges) this.unlistenChanges = await listen<number>("sync://changes", (e) => onChanges(e.payload));
    if (onConnectionChange) this.unlistenConnection = await listen<boolean>("sync://connection", (e) => onConnectionChange(e.payload));
    await invoke("sync_start_live");
  }

  static async stopLiveSync(): Promise<void> {
    await invoke("sync_stop_live");
    this.unlistenChanges?.();
    this.unlistenConnection?.();
  }
}

/*
Example usage:

await SyncEngine.init("https://api.example.com", token, deviceId, userId);
await SyncEngine.upsert("tasks", taskId, { title: "Buy milk", done: false }, true);
const task = await SyncEngine.getRow("tasks", taskId);

SyncEngine.startPeriodicSync();               // baseline eventual-consistency sync
SyncEngine.startLiveSync(
  (count) => console.log(`${count} live change(s) applied`),
  (connected) => console.log("live socket:", connected)
);
*/
