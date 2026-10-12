import { Backoff } from "./backoff";
import { HlcClock } from "./hlc";
import { IdbStore } from "./idbStore";
import { ChangeRecord, SyncChangeEvent } from "./types";

export interface LiveSyncOptions {
  serverUrl: string;
  authToken: string;
  deviceId: string;
  store: IdbStore;
  clock: HlcClock;
  onChanges?: (count: number) => void;
  onConnectionChange?: (connected: boolean) => void;
  onEntityChange?: (event: SyncChangeEvent) => void;
}

export class LiveSyncManager {
  private ws: WebSocket | null = null;
  private backoff = new Backoff(500, 30_000);
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private isStopped = false;
  private readonly options: LiveSyncOptions;

  constructor(options: LiveSyncOptions) {
    this.options = options;
  }

  start(): void {
    this.isStopped = false;
    this.connect();
  }

  stop(): void {
    this.isStopped = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      this.ws.onclose = null;
      this.ws.onerror = null;
      this.ws.onmessage = null;
      this.ws.close();
      this.ws = null;
    }
    this.options.onConnectionChange?.(false);
  }

  private connect(): void {
    if (this.isStopped) return;

    try {
      const wsUrl = this.buildWsUrl(this.options.serverUrl);
      const url = new URL(`${wsUrl}/sync/live`);
      url.searchParams.set("token", this.options.authToken);
      url.searchParams.set("deviceId", this.options.deviceId);

      const ws = new WebSocket(url.toString());
      this.ws = ws;

      ws.onopen = () => {
        if (this.isStopped) {
          ws.close();
          return;
        }
        this.backoff.reset();
        this.options.onConnectionChange?.(true);
      };

      ws.onmessage = async (event) => {
        try {
          const data = JSON.parse(event.data);
          if (data.type === "changes" && Array.isArray(data.changes)) {
            await this.handleIncomingChanges(data.changes);
          }
        } catch (err) {
          console.error("[SyncEngine Web] Error handling live WebSocket message:", err);
        }
      };

      ws.onerror = () => {
        // Handled in onclose
      };

      ws.onclose = () => {
        this.options.onConnectionChange?.(false);
        this.ws = null;
        if (!this.isStopped) {
          const delay = this.backoff.nextDelay();
          this.reconnectTimer = setTimeout(() => this.connect(), delay);
        }
      };
    } catch (err) {
      console.error("[SyncEngine Web] Error creating WebSocket:", err);
      if (!this.isStopped) {
        const delay = this.backoff.nextDelay();
        this.reconnectTimer = setTimeout(() => this.connect(), delay);
      }
    }
  }

  private async handleIncomingChanges(changes: ChangeRecord[]): Promise<void> {
    let appliedCount = 0;

    for (const change of changes) {
      const applied = await this.options.store.applyRemoteChange(
        change.table,
        change.rowId,
        change.op,
        change.fields ?? {},
        change.fieldHlcs ?? {}
      );

      if (applied) {
        appliedCount++;
        this.options.clock.receive(change.hlc);

        // Notify specific entity subscriber
        const entityData = await this.options.store.getEntity(change.table, change.rowId);
        this.options.onEntityChange?.({
          table: change.table,
          rowId: change.rowId,
          op: change.op,
          source: "remote",
          data: entityData,
        });
      }
    }

    if (appliedCount > 0) {
      // Save clock state
      const { physical, counter } = this.options.clock.getPersistedState();
      await this.options.store.setMeta("hlc_physical", physical.toString());
      await this.options.store.setMeta("hlc_counter", counter.toString());

      this.options.onChanges?.(appliedCount);
    }
  }

  private buildWsUrl(serverUrl: string): string {
    // If relative URL (e.g., in browser running on same origin), construct absolute
    let parsed: URL;
    try {
      parsed = new URL(serverUrl);
    } catch {
      parsed = new URL(serverUrl, window.location.href);
    }

    const protocol = parsed.protocol === "https:" ? "wss:" : "ws:";
    return `${protocol}//${parsed.host}${parsed.pathname.replace(/\/$/, "")}`;
  }
}
