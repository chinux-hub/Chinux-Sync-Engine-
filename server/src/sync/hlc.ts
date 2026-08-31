/**
 * Hybrid Logical Clock.
 *
 * Format: `${physicalMs}:${counter}:${deviceId}`
 * - physicalMs: wall clock ms at time of event
 * - counter: disambiguates multiple events in the same millisecond
 * - deviceId: final tie-breaker, guarantees total order across devices
 *
 * HLCs compare correctly with plain string comparison ONLY if physicalMs is
 * zero-padded to a fixed width and counter is zero-padded too, so we encode
 * that way instead of using ':' delimited raw numbers.
 */

const PHYSICAL_WIDTH = 15; // ms since epoch fits in 13 digits through year 2286; pad to 15 for safety
const COUNTER_WIDTH = 6;

export interface HLCState {
  physical: number;
  counter: number;
  deviceId: string;
}

export function encodeHLC(state: HLCState): string {
  const p = state.physical.toString().padStart(PHYSICAL_WIDTH, "0");
  const c = state.counter.toString().padStart(COUNTER_WIDTH, "0");
  return `${p}.${c}.${state.deviceId}`;
}

export function decodeHLC(hlc: string): HLCState {
  const [p, c, ...deviceParts] = hlc.split(".");
  return {
    physical: parseInt(p, 10),
    counter: parseInt(c, 10),
    deviceId: deviceParts.join("."),
  };
}

/** String comparison is sufficient for ordering because both numeric fields
 * are zero-padded fixed width. Returns <0 if a before b, >0 if a after b. */
export function compareHLC(a: string, b: string): number {
  if (a === b) return 0;
  return a < b ? -1 : 1;
}

/**
 * Server-side HLC clock: advances on every event it sees (local or remote),
 * so its physical component never goes backwards and always dominates any
 * HLC it has previously observed. This is the standard HLC receive-merge
 * algorithm (Kulkarni et al.).
 */
export class HLCClock {
  private physical = 0;
  private counter = 0;
  constructor(private readonly deviceId: string) {}

  /** Generate a new HLC for a locally-originated event. */
  tick(): string {
    const now = Date.now();
    if (now > this.physical) {
      this.physical = now;
      this.counter = 0;
    } else {
      this.counter += 1;
    }
    return encodeHLC({ physical: this.physical, counter: this.counter, deviceId: this.deviceId });
  }

  /** Merge in an HLC observed from a remote event, then generate a new local HLC
   * that is guaranteed to be greater than both the local clock and the remote one. */
  receive(remoteHlc: string): string {
    const remote = decodeHLC(remoteHlc);
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
    return encodeHLC({ physical: this.physical, counter: this.counter, deviceId: this.deviceId });
  }
}
