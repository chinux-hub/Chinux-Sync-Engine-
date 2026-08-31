//! Hybrid Logical Clock — identical algorithm to server/src/sync/hlc.ts.
//! Keeping both implementations algorithmically identical is what makes
//! client-generated HLCs comparable with server-generated ones via plain
//! string ordering.

use std::time::{SystemTime, UNIX_EPOCH};

const PHYSICAL_WIDTH: usize = 15;
const COUNTER_WIDTH: usize = 6;

#[derive(Debug, Clone)]
pub struct HlcState {
    pub physical: u64,
    pub counter: u32,
    pub device_id: String,
}

pub fn encode(state: &HlcState) -> String {
    format!(
        "{:0pw$}.{:0cw$}.{}",
        state.physical,
        state.counter,
        state.device_id,
        pw = PHYSICAL_WIDTH,
        cw = COUNTER_WIDTH
    )
}

pub fn decode(hlc: &str) -> Option<HlcState> {
    let mut parts = hlc.splitn(3, '.');
    let physical: u64 = parts.next()?.parse().ok()?;
    let counter: u32 = parts.next()?.parse().ok()?;
    let device_id = parts.next()?.to_string();
    Some(HlcState { physical, counter, device_id })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Per-device HLC clock. One instance lives for the lifetime of the local
/// sync engine and is persisted (physical, counter) across app restarts so
/// it never regresses even if the process restarts within the same ms.
pub struct HlcClock {
    physical: u64,
    counter: u32,
    device_id: String,
}

impl HlcClock {
    pub fn new(device_id: String, restored_physical: u64, restored_counter: u32) -> Self {
        Self { physical: restored_physical, counter: restored_counter, device_id }
    }

    /// New HLC for a change originated on this device.
    pub fn tick(&mut self) -> String {
        let now = now_ms();
        if now > self.physical {
            self.physical = now;
            self.counter = 0;
        } else {
            self.counter += 1;
        }
        encode(&HlcState { physical: self.physical, counter: self.counter, device_id: self.device_id.clone() })
    }

    /// Merge in a remote HLC (from a pulled change) and return a fresh local
    /// HLC guaranteed greater than both. Standard HLC receive algorithm.
    pub fn receive(&mut self, remote_hlc: &str) -> String {
        let remote = decode(remote_hlc).unwrap_or(HlcState { physical: 0, counter: 0, device_id: String::new() });
        let now = now_ms();
        let max_physical = self.physical.max(remote.physical).max(now);

        if max_physical == self.physical && max_physical == remote.physical {
            self.counter = self.counter.max(remote.counter) + 1;
        } else if max_physical == self.physical {
            self.counter += 1;
        } else if max_physical == remote.physical {
            self.counter = remote.counter + 1;
        } else {
            self.counter = 0;
        }
        self.physical = max_physical;
        encode(&HlcState { physical: self.physical, counter: self.counter, device_id: self.device_id.clone() })
    }

    pub fn persisted_state(&self) -> (u64, u32) {
        (self.physical, self.counter)
    }
}

pub fn compare(a: &str, b: &str) -> std::cmp::Ordering {
    a.cmp(b)
}
