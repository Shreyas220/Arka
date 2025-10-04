use std::sync::atomic::{AtomicU64, Ordering};

/// Log Sequence Number (LSN) Manager for Arka's write path
///
/// ## Purpose
/// LSNs provide a total ordering of all operations in the system. Every write gets
/// a unique, monotonically increasing LSN. This enables:
/// - Consistent snapshots (read all data up to LSN X)
/// - Recovery (replay from last checkpoint)
/// - Replication (sync to remote LSN)
///
/// ## Architecture
/// The LSN manager tracks three watermarks:
/// 1. `next_lsn`: Next LSN to assign (in-memory operations)
/// 2. `flushed_lsn`: Data durably written to Arrow segments on disk
/// 3. `committed_lsn`: Data committed to Iceberg (can prune hot tier)
///
/// ## Example Flow
/// ```text
/// Time  Action                    next_lsn  flushed  committed
/// ----  ------                    --------  -------  ---------
/// T1    assign_range(100)         101       0        0
/// T2    flush segment             101       100      0
/// T3    assign_range(50)          151       100      0
/// T4    commit to Iceberg         151       100      100
/// T5    flush segment             151       150      100
/// ```
///
/// ## Thread Safety
/// All operations are lock-free using atomic operations.
pub struct LSNManager {
    /// Next LSN to be assigned to incoming data
    ///
    /// Starts at 1 (LSN 0 is reserved for "no LSN" sentinel value).
    /// Incremented atomically on every assign() or assign_range() call.
    next_lsn: AtomicU64,

    /// Highest LSN that has been durably written to disk
    ///
    /// Updated by the segment flusher after successfully writing an Arrow IPC file.
    /// Guarantees: All data with LSN <= flushed_lsn is on disk and survives crashes.
    flushed_lsn: AtomicU64,

    /// Highest LSN that has been committed to Iceberg
    ///
    /// Updated after successfully committing a batch of segments to Iceberg.
    /// Segments with LSN <= committed_lsn can be safely pruned from hot tier.
    committed_lsn: AtomicU64,
}

impl LSNManager {
    /// Create a new LSN manager starting from LSN 1
    ///
    /// Use this for fresh starts. For recovery, use `with_start_lsn()`.
    pub fn new() -> Self {
        Self {
            next_lsn: AtomicU64::new(1),
            flushed_lsn: AtomicU64::new(0),
            committed_lsn: AtomicU64::new(0),
        }
    }

    /// Create LSN manager starting from a specific LSN
    ///
    /// Used during recovery to resume from last known LSN + 1.
    ///
    /// # Arguments
    /// * `start_lsn` - First LSN to assign (should be last checkpoint LSN + 1)
    pub fn with_start_lsn(start_lsn: u64) -> Self {
        Self {
            next_lsn: AtomicU64::new(start_lsn),
            flushed_lsn: AtomicU64::new(0),
            committed_lsn: AtomicU64::new(0),
        }
    }

    /// Assign a single LSN
    ///
    /// Returns the assigned LSN. Thread-safe and lock-free.
    ///
    /// # Example
    /// ```
    /// let lsn_mgr = LSNManager::new();
    /// let lsn1 = lsn_mgr.assign(); // Returns 1
    /// let lsn2 = lsn_mgr.assign(); // Returns 2
    /// ```
    pub fn assign(&self) -> u64 {
        self.next_lsn.fetch_add(1, Ordering::SeqCst)
    }

    /// Assign a contiguous range of LSNs for a batch
    ///
    /// More efficient than calling assign() N times. Returns inclusive range.
    ///
    /// # Arguments
    /// * `count` - Number of LSNs to assign
    ///
    /// # Returns
    /// Tuple of (start_lsn, end_lsn) where both are inclusive.
    ///
    /// # Example
    /// ```
    /// let lsn_mgr = LSNManager::new();
    /// let (start, end) = lsn_mgr.assign_range(5); // (1, 5)
    /// // Next assign() will return 6
    /// ```
    pub fn assign_range(&self, count: u64) -> (u64, u64) {
        let start = self.next_lsn.fetch_add(count, Ordering::SeqCst);
        (start, start + count - 1)
    }

    /// Get the current LSN counter (next LSN to be assigned)
    ///
    /// Note: This is a snapshot - the value may change immediately after reading
    /// if another thread assigns LSNs concurrently.
    pub fn current(&self) -> u64 {
        self.next_lsn.load(Ordering::SeqCst)
    }

    /// Mark an LSN as durably flushed to disk
    ///
    /// Called by the segment flusher after fsync completes. Uses fetch_max to
    /// ensure watermark only moves forward even with concurrent flushes.
    ///
    /// # Arguments
    /// * `lsn` - The highest LSN in the segment that was flushed
    pub fn mark_flushed(&self, lsn: u64) {
        self.flushed_lsn.fetch_max(lsn, Ordering::SeqCst);
    }

    /// Mark an LSN as committed to Iceberg
    ///
    /// Called after successfully committing segments to Iceberg. Segments
    /// with LSN <= this value can be pruned from the hot tier.
    ///
    /// # Arguments
    /// * `lsn` - The highest LSN in the Iceberg commit batch
    pub fn mark_committed(&self, lsn: u64) {
        self.committed_lsn.fetch_max(lsn, Ordering::SeqCst);
    }

    /// Get the last flushed LSN watermark
    ///
    /// All data with LSN <= this value is guaranteed to be on disk.
    pub fn get_flushed(&self) -> u64 {
        self.flushed_lsn.load(Ordering::SeqCst)
    }

    /// Get the last committed LSN watermark
    ///
    /// All data with LSN <= this value is in Iceberg and can be pruned.
    pub fn get_committed(&self) -> u64 {
        self.committed_lsn.load(Ordering::SeqCst)
    }

    /// Calculate how many LSNs are pending disk flush
    ///
    /// This is the "memory lag" - data that's assigned an LSN but not yet durable.
    /// High values indicate the flusher is falling behind.
    ///
    /// # Returns
    /// Number of LSNs between current and flushed watermarks
    pub fn pending_flush(&self) -> u64 {
        let current = self.current();
        let flushed = self.get_flushed();
        current.saturating_sub(flushed)
    }

    /// Calculate how many LSNs are pending Iceberg commit
    ///
    /// This is the "hot tier size" - data on disk but not yet in Iceberg.
    /// High values indicate the Iceberg committer is falling behind.
    ///
    /// # Returns
    /// Number of LSNs between flushed and committed watermarks
    pub fn pending_commit(&self) -> u64 {
        let flushed = self.get_flushed();
        let committed = self.get_committed();
        flushed.saturating_sub(committed)
    }
}

impl Default for LSNManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_assignment() {
        let lsn = LSNManager::new();
        assert_eq!(lsn.current(), 1);
        assert_eq!(lsn.assign(), 1);
        assert_eq!(lsn.assign(), 2);
        assert_eq!(lsn.current(), 3);
    }

    #[test]
    fn test_range_assignment() {
        let lsn = LSNManager::new();
        let (start, end) = lsn.assign_range(5);
        assert_eq!(start, 1);
        assert_eq!(end, 5);
        assert_eq!(lsn.current(), 6);
    }

    #[test]
    fn test_watermarks() {
        let lsn = LSNManager::new();
        lsn.assign_range(10);

        assert_eq!(lsn.get_flushed(), 0);
        lsn.mark_flushed(5);
        assert_eq!(lsn.get_flushed(), 5);

        assert_eq!(lsn.get_committed(), 0);
        lsn.mark_committed(3);
        assert_eq!(lsn.get_committed(), 3);
    }

    #[test]
    fn test_pending_counts() {
        let lsn = LSNManager::new();
        lsn.assign_range(100);

        lsn.mark_flushed(50);
        lsn.mark_committed(20);

        assert_eq!(lsn.pending_flush(), 51); // 101 - 50
        assert_eq!(lsn.pending_commit(), 30); // 50 - 20
    }
}
