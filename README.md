# microvm-warm-pool

[![crates.io](https://img.shields.io/crates/v/microvm-warm-pool.svg)](https://crates.io/crates/microvm-warm-pool)
[![docs.rs](https://docs.rs/microvm-warm-pool/badge.svg)](https://docs.rs/microvm-warm-pool)

Pre-restored Firecracker microVM pool harness for fast warm handoff.

A pure-Rust primitive that sits on top of [`microvm-runtime`] and turns a
snapshot-restore-then-boot flow into a hashmap lookup. Acquire wallclock drops
from ~150 ms (cold) to ~10 ms (warm). The pool itself never speaks the
Firecracker API directly — every state change is delegated to a `VmProvider`
implementation.

[`microvm-runtime`]: https://crates.io/crates/microvm-runtime

## Why this exists

Every Tangle blueprint that needs sub-second tenant handoff (sandbox blueprint,
microvm blueprint, future cloud-style blueprints) wants the same pool harness.
This crate is that harness, extracted into a single primitive so it can be
hardened in one place and consumed as a Cargo dependency.

## Status

`0.1.0-alpha.1` — extracted from the TypeScript warm-pool that ships in the
sandbox host-agent. Supports:

- Per-`StackKey` buckets (`stack_name`, `version`, `vcpu_count`, `mem_size_mib`)
- Single background refill thread, low/high-water marks
- Age-based eviction
- Per-acquire `EntryValidator` health probe with eviction-on-unhealthy
- Atomic counters exposed via `WarmPool::metrics()` (Prometheus-friendly)
- Idempotent shutdown with 200 ms join budget + best-effort destroy of all
  remaining entries

Not yet:

- Adaptive depth from observed acquire EWMA (today: fixed `[min, max]`)
- Per-bucket refill coalescing across multiple pools
- Cross-thread metric labelling hook (today: counters only)

## Usage

```rust,no_run
use std::sync::Arc;
use std::time::Duration;
use microvm_runtime::{
    InMemoryVmProvider,
    model::{NetworkInterface, SnapshotRef, VmSpec},
    VmProvider,
};
use microvm_warm_pool::{
    EntryValidator, StackKey, ValidationResult, WarmPool, WarmPoolConfig,
};

struct AlwaysHealthy;
impl EntryValidator for AlwaysHealthy {
    fn validate(&self, _vm_id: &str) -> ValidationResult {
        ValidationResult::Healthy
    }
}

let provider = InMemoryVmProvider::default();
let pool = WarmPool::start(
    provider.clone(),
    WarmPoolConfig {
        min_depth: 2,
        max_depth: 4,
        refill_interval: Duration::from_secs(5),
        entry_max_age: Duration::from_secs(600),
    },
    Arc::new(AlwaysHealthy),
);

let stack = StackKey {
    stack_name: "node20".into(),
    version: "1.4.2".into(),
    vcpu_count: 2,
    mem_size_mib: 512,
};
pool.register(
    stack.clone(),
    SnapshotRef {
        vm_id: "template-vm".into(),
        snapshot_id: "template-snap".into(),
        resume_immediately: true,
        network_overrides: Vec::new(),
    },
);

// Caller pops a warm entry, then brings up a per-tenant VM by restoring the
// pool's snapshot with their own network override (FC 1.10+ feature).
if let Some(handle) = pool.acquire(&stack) {
    let tenant_id = "tenant-abc";
    let mut snap = handle.source_snapshot.clone();
    snap.network_overrides = vec![NetworkInterface {
        iface_id: "eth0".into(),
        host_dev_name: "tap-tenant-abc".into(),
        guest_mac: None,
        rx_rate_limiter: None,
        tx_rate_limiter: None,
    }];
    provider.create_vm_with_spec(
        tenant_id,
        &VmSpec {
            restore_from: Some(snap),
            ..VmSpec::default()
        },
    ).unwrap();
}
```

## Host requirements

The pool itself is platform-agnostic and tests pass on any host that runs Rust
1.91. The Firecracker provider it consumes requires:

- Linux with KVM (`/dev/kvm` accessible to the running user)
- Firecracker **1.10 or newer** for the warm-handoff path —
  `SnapshotRef::network_overrides` is rejected by older versions
- A snapshot template (a VM that has been booted, configured, and snapshotted
  by the operator out-of-band)

## API surface

| Type | Purpose |
| --- | --- |
| `WarmPool<P>` | Owns the bucket map, refill thread, and validator |
| `WarmPoolConfig` | `min_depth`, `max_depth`, `refill_interval`, `entry_max_age` |
| `WarmPoolHandle` | Returned from `acquire`: `source_vm_id`, `stack`, `source_snapshot` |
| `WarmPoolMetrics` | Counter snapshot for Prometheus / observability |
| `EntryValidator` | Per-entry health probe trait |
| `ValidationResult` | `Healthy` or `Unhealthy(reason)` |
| `StackKey` | Bucket identifier (stack/version + machine shape) |

## Cargo features

- `default = []` — no providers pulled in
- `firecracker` — forwards `microvm-runtime/firecracker`, enabling the in-process
  Firecracker driver alongside the warm-pool harness

## License

[Unlicense](LICENSE) — public domain.
