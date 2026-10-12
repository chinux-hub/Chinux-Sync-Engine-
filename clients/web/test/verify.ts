import { HlcClock, compareHLC, decodeHLC, encodeHLC } from "../src/hlc.ts";
import { Backoff } from "../src/backoff.ts";

function assert(condition: boolean, msg: string) {
  if (!condition) {
    throw new Error(`Assertion failed: ${msg}`);
  }
}

console.log("=== Running Web Sync Engine Verifications ===");

// 1. Verify HLC Clock
console.log("1. Testing Hybrid Logical Clock (HLC)...");
const clockA = new HlcClock("device-A");
const clockB = new HlcClock("device-B");

const hlcA1 = clockA.tick();
const hlcA2 = clockA.tick();
assert(compareHLC(hlcA1, hlcA2) < 0, "Clock A ticks must be strictly monotonic");

// Receive on clock B
const hlcB1 = clockB.receive(hlcA2);
assert(compareHLC(hlcA2, hlcB1) < 0, "Received HLC on B must dominate A's HLC");

const decoded = decodeHLC(hlcB1);
assert(decoded !== null, "Decoded HLC should not be null");
assert(decoded!.deviceId === "device-B", "Decoded deviceId must match device-B");
console.log("   HLC passed! Sample HLC:", hlcB1);

// 2. Verify Backoff with Jitter
console.log("2. Testing Exponential Backoff...");
const backoff = new Backoff(100, 1000);
for (let i = 0; i < 5; i++) {
  const delay = backoff.nextDelay();
  assert(delay >= 0 && delay <= 1000, `Delay ${delay} must be within bounds [0, 1000]`);
}
backoff.reset();
console.log("   Backoff passed!");

// 3. Verify Field-level LWW Conflict Resolution algorithm
console.log("3. Testing Field-level LWW Conflict Resolution logic...");
interface EntityState {
  data: Record<string, unknown>;
  fieldHlcs: Record<string, string>;
}

function applyFieldLww(
  state: EntityState,
  incomingFields: Record<string, unknown>,
  incomingHlcs: Record<string, string>
): boolean {
  let applied = false;
  for (const [col, val] of Object.entries(incomingFields)) {
    const incHlc = incomingHlcs[col] || "";
    const existingHlc = state.fieldHlcs[col];

    if (!existingHlc || incHlc > existingHlc) {
      state.data[col] = val;
      state.fieldHlcs[col] = incHlc;
      applied = true;
    }
  }
  return applied;
}

const entity: EntityState = { data: {}, fieldHlcs: {} };

// Step A: Device A writes title = "Initial Title" at hlc1
const hlc1 = clockA.tick();
applyFieldLww(entity, { title: "Initial Title" }, { title: hlc1 });
assert(entity.data["title"] === "Initial Title", "title should be Initial Title");

// Step B: Device B writes description = "A great product" at hlc2
const hlc2 = clockB.receive(hlc1);
applyFieldLww(entity, { description: "A great product" }, { description: hlc2 });
assert(entity.data["title"] === "Initial Title", "title should still survive");
assert(entity.data["description"] === "A great product", "description should be added");

// Step C: Concurrent edit: Older write to title comes in (e.g. hlc0)
const olderHlc = "000000000000001.000000.device-C";
const modifiedOlder = applyFieldLww(entity, { title: "Older stale title" }, { title: olderHlc });
assert(!modifiedOlder, "Older title write should NOT be applied");
assert(entity.data["title"] === "Initial Title", "Title should remain winning Initial Title");

// Step D: Newer write to title comes in
const hlc3 = clockA.receive(hlc2);
const modifiedNewer = applyFieldLww(entity, { title: "Updated Title" }, { title: hlc3 });
assert(modifiedNewer, "Newer title write must be applied");
assert(entity.data["title"] === "Updated Title", "Title must be updated to Updated Title");
assert(entity.data["description"] === "A great product", "Description must still be intact");

console.log("   Field-level LWW conflict resolution passed!");
console.log("=== All verifications passed successfully! ===");
