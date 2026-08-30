//! Exponential backoff with full jitter (AWS-style: `random(0, min(cap, base * 2^attempt))`).
//! Used for both HTTP push/pull retries and WebSocket reconnects, so a
//! flaky network doesn't hammer the server on a fixed timer from every
//! client simultaneously (thundering herd).

use rand::Rng;
use std::time::Duration;

pub struct Backoff {
    attempt: u32,
    base_ms: u64,
    cap_ms: u64,
}

impl Backoff {
    pub fn new(base_ms: u64, cap_ms: u64) -> Self {
        Self { attempt: 0, base_ms, cap_ms }
    }

    /// Delay for the current attempt, then increments for next time.
    pub fn next_delay(&mut self) -> Duration {
        let exp = self.base_ms.saturating_mul(1u64 << self.attempt.min(20));
        let capped = exp.min(self.cap_ms);
        self.attempt += 1;
        let jittered = rand::thread_rng().gen_range(0..=capped.max(1));
        Duration::from_millis(jittered)
    }

    /// Call after a successful operation to reset the attempt counter.
    pub fn reset(&mut self) {
        self.attempt = 0;
    }
}
