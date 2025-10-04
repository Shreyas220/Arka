use std::collections::BTreeMap;
use crate::storage::segment::{SegmentId, SegmentMeta};

/// In-memory index of all segments in the system
///
/// This provides fast lookups for:
/// - Finding segments by ID
/// - Finding segments in a time range
/// - Finding uncommitted segments
/// - Memory management (tracking what can be pruned)
pub struct SegmentIndex {
    /// Segments ordered by ID (epoch/LSN)
    /// BTreeMap provides ordered iteration and range queries
    segments: BTreeMap<SegmentId, SegmentMeta>,
}

impl SegmentIndex {
    /// Create a new empty segment index
    pub fn new() -> Self {
        Self {
            segments: BTreeMap::new(),
        }
    }

    /// Insert or update segment metadata
    pub fn insert(&mut self, meta: SegmentMeta) {
        self.segments.insert(meta.id, meta);
    }

    /// Get segment metadata by ID
    pub fn get(&self, id: SegmentId) -> Option<&SegmentMeta> {
        self.segments.get(&id)
    }

    /// Get mutable reference to segment metadata
    pub fn get_mut(&mut self, id: SegmentId) -> Option<&mut SegmentMeta> {
        self.segments.get_mut(&id)
    }

    /// Find all segments that overlap with the given time range
    ///
    /// This is the primary query optimization - we only scan segments
    /// that could contain data in the requested time range
    pub fn find_by_time_range(&self, start: i64, end: i64) -> Vec<SegmentId> {
        self.segments
            .values()
            .filter(|seg| seg.overlaps_time_range(start, end))
            .map(|seg| seg.id)
            .collect()
    }

    /// Find all segments that contain the given LSN
    pub fn find_by_lsn(&self, lsn: u64) -> Vec<SegmentId> {
        self.segments
            .values()
            .filter(|seg| seg.contains_lsn(lsn))
            .map(|seg| seg.id)
            .collect()
    }

    /// Get all segment IDs in order
    pub fn all_ids(&self) -> Vec<SegmentId> {
        self.segments.keys().copied().collect()
    }

    /// Get all segment metadata
    pub fn all(&self) -> Vec<&SegmentMeta> {
        self.segments.values().collect()
    }

    /// Find segments that haven't been committed to Iceberg
    /// These are candidates for the next Iceberg commit batch
    pub fn uncommitted_segments(&self) -> Vec<SegmentId> {
        self.segments
            .values()
            .filter(|seg| !seg.committed_to_iceberg)
            .map(|seg| seg.id)
            .collect()
    }

    /// Find segments that are committed and old enough to prune
    pub fn prunable_segments(&self, min_age_seconds: i64) -> Vec<SegmentId> {
        self.segments
            .values()
            .filter(|seg| seg.can_prune(min_age_seconds))
            .map(|seg| seg.id)
            .collect()
    }

    /// Mark a segment as committed to Iceberg
    pub fn mark_committed(&mut self, id: SegmentId) -> bool {
        if let Some(segment) = self.segments.get_mut(&id) {
            segment.committed_to_iceberg = true;
            true
        } else {
            false
        }
    }

    /// Remove a segment from the index
    /// Returns the removed metadata if it existed
    pub fn remove(&mut self, id: SegmentId) -> Option<SegmentMeta> {
        self.segments.remove(&id)
    }

    /// Number of segments being tracked
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Check if index is empty
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Total number of rows across all segments
    pub fn total_rows(&self) -> u64 {
        self.segments.values().map(|s| s.row_count as u64).sum()
    }

    /// Total bytes across all segments
    pub fn total_bytes(&self) -> u64 {
        self.segments.values().map(|s| s.byte_size).sum()
    }

    /// Estimate memory usage of this index
    pub fn memory_usage(&self) -> usize {
        // BTreeMap overhead + segment metadata
        let btree_overhead = self.segments.len() * 64; // Rough BTreeMap node overhead
        let metadata_size: usize = self.segments.values().map(|s| s.memory_usage()).sum();
        btree_overhead + metadata_size
    }
}

impl Default for SegmentIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn create_test_segment(id: u64, time_min: i64, time_max: i64, committed: bool) -> SegmentMeta {
        SegmentMeta {
            id,
            file_path: PathBuf::from(format!("segment_{}.arrow", id)),
            row_count: 1000,
            byte_size: 10000,
            time_range: (time_min, time_max),
            lsn_range: (id * 1000, (id + 1) * 1000 - 1),
            created_at: chrono::Utc::now().timestamp() - (100 - id as i64), // Older segments have earlier timestamp
            committed_to_iceberg: committed,
        }
    }

    #[test]
    fn test_basic_operations() {
        let mut index = SegmentIndex::new();
        assert_eq!(index.len(), 0);
        assert!(index.is_empty());

        let segment = create_test_segment(1000, 0, 100, false);
        index.insert(segment);

        assert_eq!(index.len(), 1);
        assert!(!index.is_empty());
        assert!(index.get(1000).is_some());
        assert!(index.get(1001).is_none());
    }

    #[test]
    fn test_time_range_queries() {
        let mut index = SegmentIndex::new();

        // Insert segments with different time ranges
        index.insert(create_test_segment(1, 0, 100, false));
        index.insert(create_test_segment(2, 100, 200, false));
        index.insert(create_test_segment(3, 200, 300, false));
        index.insert(create_test_segment(4, 300, 400, false));

        // Query overlapping segments
        let results = index.find_by_time_range(150, 250);
        assert_eq!(results.len(), 2);
        assert!(results.contains(&2));
        assert!(results.contains(&3));

        // Query before all segments
        let results = index.find_by_time_range(-100, -50);
        assert_eq!(results.len(), 0);

        // Query spanning all segments
        let results = index.find_by_time_range(-100, 500);
        assert_eq!(results.len(), 4);
    }

    #[test]
    fn test_uncommitted_segments() {
        let mut index = SegmentIndex::new();

        index.insert(create_test_segment(1, 0, 100, true));
        index.insert(create_test_segment(2, 100, 200, false));
        index.insert(create_test_segment(3, 200, 300, false));
        index.insert(create_test_segment(4, 300, 400, true));

        let uncommitted = index.uncommitted_segments();
        assert_eq!(uncommitted.len(), 2);
        assert!(uncommitted.contains(&2));
        assert!(uncommitted.contains(&3));
    }

    #[test]
    fn test_mark_committed() {
        let mut index = SegmentIndex::new();
        index.insert(create_test_segment(1, 0, 100, false));

        assert!(!index.get(1).unwrap().committed_to_iceberg);
        assert!(index.mark_committed(1));
        assert!(index.get(1).unwrap().committed_to_iceberg);
    }
}
