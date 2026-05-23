//! Bucket identifier for warm pool entries.

/// Identifier for a pool entry's bucket. Same `(stack_name, version,
/// vcpu_count, mem_size_mib)` share a pool; different identities have
/// separate pools so handoff doesn't mismatch.
///
/// The four-tuple matches what a downstream blueprint can guarantee about a
/// pre-restored VM:
/// - `stack_name` + `version` pin the rootfs / sidecar image,
/// - `vcpu_count` + `mem_size_mib` pin the machine config baked into the
///   snapshot (Firecracker rejects restores into a differently-shaped VM).
///
/// All four must match for a warm entry to be valid for the caller's request.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct StackKey {
    /// Logical stack identifier (e.g. `"node20"`, `"python311-pytorch"`).
    pub stack_name: String,
    /// Version of the stack (e.g. sidecar version, rootfs revision).
    pub version: String,
    /// vCPU count the snapshot was taken with — must match the restore target.
    pub vcpu_count: u8,
    /// Memory size (MiB) the snapshot was taken with — must match on restore.
    pub mem_size_mib: u32,
}
