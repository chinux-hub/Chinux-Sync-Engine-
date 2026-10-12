/**
 * Exponential backoff with full jitter.
 *
 * Prevents "thundering herd" problems when web clients reconnect after a network
 * outage or server restart.
 */
export class Backoff {
  private attempt = 0;
  private readonly baseMs: number;
  private readonly capMs: number;

  constructor(baseMs = 300, capMs = 15_000) {
    this.baseMs = baseMs;
    this.capMs = capMs;
  }

  nextDelay(): number {
    const exp = this.baseMs * Math.pow(2, Math.min(this.attempt, 20));
    const capped = Math.min(exp, this.capMs);
    this.attempt++;
    // Full jitter: uniformly random between 0 and capped delay
    return Math.random() * capped;
  }

  reset(): void {
    this.attempt = 0;
  }
}
