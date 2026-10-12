/**
 * Hybrid Logical Clock (HLC) implementation for the Web Sync Engine.
 *
 * Algorithmically identical to core-rust/src/hlc.rs and server/src/sync/hlc.ts.
 * Format: `${physical}.${counter}.${deviceId}`
 *
 * String comparison is sufficient for total ordering because physical time
 * and logical counter are zero-padded to fixed widths.
 */

const PHYSICAL_WIDTH = 15;
const COUNTER_WIDTH = 6;

export interface HlcState {
  physical: number;
  counter: number;
  deviceId: string;
}

export function encodeHLC(state: HlcState): string {
  const p = state.physical.toString().padStart(PHYSICAL_WIDTH, "0");
  const c = state.counter.toString().padStart(COUNTER_WIDTH, "0");
  return `${p}.${c}.${state.deviceId}`;
}

export function decodeHLC(hlc: string): HlcState | null {
  const parts = hlc.split(".");
  if (parts.length < 3) return null;
  const physical = parseInt(parts[0], 10);
  const counter = parseInt(parts[1], 10);
  const deviceId = parts.slice(2).join(".");
  if (isNaN(physical) || isNaN(counter)) return null;
  return { physical, counter, deviceId };
}

export function compareHLC(a: string, b: string): number {
  if (a === b) return 0;
  return a < b ? -1 : 1;
}

export class HlcClock {
  private physical: number;
  private counter: number;
  private deviceId: string;

  constructor(deviceId: string, restoredPhysical = 0, restoredCounter = 0) {
    this.deviceId = deviceId;
    this.physical = restoredPhysical;
    this.counter = restoredCounter;
  }

  /**
   * Generates a new HLC for a mutation originated locally on this web client.
   */
  tick(): string {
    const now = Date.now();
    if (now > this.physical) {
      this.physical = now;
      this.counter = 0;
    } else {
      this.counter += 1;
    }
    return encodeHLC({
      physical: this.physical,
      counter: this.counter,
      deviceId: this.deviceId,
    });
  }

  /**
   * Merges in an HLC observed from a remote device (via pull or WebSocket),
   * guaranteeing the local clock advances past both the local and remote clocks.
   */
  receive(remoteHlc: string): string {
    const remote = decodeHLC(remoteHlc) ?? {
      physical: 0,
      counter: 0,
      deviceId: "",
    };
    const now = Date.now();
    const maxPhysical = Math.max(this.physical, remote.physical, now);

    if (maxPhysical === this.physical && maxPhysical === remote.physical) {
      this.counter = Math.max(this.counter, remote.counter) + 1;
    } else if (maxPhysical === this.physical) {
      this.counter += 1;
    } else if (maxPhysical === remote.physical) {
      this.counter = remote.counter + 1;
    } else {
      this.counter = 0;
    }

    this.physical = maxPhysical;
    return encodeHLC({
      physical: this.physical,
      counter: this.counter,
      deviceId: this.deviceId,
    });
  }

  getPersistedState(): { physical: number; counter: number } {
    return { physical: this.physical, counter: this.counter };
  }
}
