//! Pre-restored Firecracker microVM pool harness.
//!
//! Sits on top of [`microvm_runtime`] and turns a snapshot-restore-then-boot
//! flow into a hashmap lookup. Acquire wallclock drops from ~150 ms (cold) to
//! ~10 ms (warm).
//!
//! The pool is bucketed by [`StackKey`] (`stack_name`, `version`, `vcpu_count`,
//! `mem_size_mib`) — different identities are not interchangeable, so each
//! gets its own queue. A single background refill thread keeps every
//! registered bucket between `min_depth` and `max_depth` entries, evicts
//! entries older than `entry_max_age`, and drops entries whose
//! [`EntryValidator`] reports them unhealthy.
//!
//! See [`pool::WarmPool`] for the entry point.
//!
//! # Required host environment
//!
//! Warm handoff with network rewrite requires Firecracker 1.10+ (the version
//! that honors `network_interfaces` overrides in `PUT /snapshot/load`). The
//! pool itself runs anywhere; it is the provider you pass in that needs the
//! KVM-capable Linux host.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use std::time::Duration;
//! use microvm_runtime::{
//!     InMemoryVmProvider,
//!     model::{NetworkInterface, SnapshotRef},
//! };
//! use microvm_warm_pool::{
//!     EntryValidator, StackKey, ValidationResult, WarmPool, WarmPoolConfig,
//! };
//!
//! struct AlwaysHealthy;
//! impl EntryValidator for AlwaysHealthy {
//!     fn validate(&self, _vm_id: &str) -> ValidationResult {
//!         ValidationResult::Healthy
//!     }
//! }
//!
//! let provider = InMemoryVmProvider::default();
//! let pool = WarmPool::start(
//!     provider,
//!     WarmPoolConfig {
//!         min_depth: 2,
//!         max_depth: 4,
//!         refill_interval: Duration::from_secs(5),
//!         entry_max_age: Duration::from_secs(600),
//!     },
//!     Arc::new(AlwaysHealthy),
//! );
//!
//! let stack = StackKey {
//!     stack_name: "node20".into(),
//!     version: "1.4.2".into(),
//!     vcpu_count: 2,
//!     mem_size_mib: 512,
//! };
//! pool.register(
//!     stack.clone(),
//!     SnapshotRef {
//!         vm_id: "template-vm".into(),
//!         snapshot_id: "template-snap".into(),
//!         resume_immediately: true,
//!         network_overrides: Vec::new(),
//!     },
//! );
//!
//! // Caller side at acquire time: pop a warm entry, then bring up a
//! // tenant-specific VM by restoring the same snapshot with the per-tenant
//! // network override. If `acquire` returns `None`, fall back to cold boot.
//! if let Some(handle) = pool.acquire(&stack) {
//!     let _ = handle; // restore with handle.source_snapshot + your overrides
//! }
//! ```

pub mod pool;
pub mod stack_key;

pub use pool::{
    EntryValidator, ValidationResult, WarmPool, WarmPoolConfig, WarmPoolHandle, WarmPoolMetrics,
};
pub use stack_key::StackKey;
