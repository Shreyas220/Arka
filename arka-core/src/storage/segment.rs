use std::path::PathBuf;
use serde::{Deserialize, Serialize};

/// Unique segment identifier (epoch-based)
pub type SegmentId = u64;

/// Metadata for a single WAL segment on disk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMeta {
    /// Unique segment ID (typically epoch/LSN)
    pub id: SegmentId,

    /// Path to the Arrow IPC file
    pub file_path: PathBuf,

    /// Number of rows in this segment
    pub row_count: u32,

    /// Size in bytes on disk
    pub byte_size: u64,

    /// Time range of data in this segment (min_timestamp, max_timestamp)
    /// Used for time-range query optimization
    pub time_range: (i64, i64),

    /// LSN range covered by this segment (min_lsn, max_lsn)
    /// Used for consistency and recovery
    pub lsn_range: (u64, u64),

    /// Unix timestamp when this segment was created
    pub created_at: i64,

    /// Whether this segment has been committed to Iceberg
    /// Once true, segment can be pruned from hot tier
    pub committed_to_iceberg: bool,
}

impl SegmentMeta {
    /// Create a new segment metadata with defaults
    pub fn new(id: SegmentId, file_path: PathBuf, row_count: u32) -> Self {
        Self {
            id,
            file_path,
            row_count,
            byte_size: 0,
            time_range: (i64::MAX, i64::MIN),
            lsn_range: (u64::MAX, u64::MIN),
            created_at: chrono::Utc::now().timestamp(),
            committed_to_iceberg: false,
        }
    }

    /// Check if this segment overlaps with a time range
    pub fn overlaps_time_range(&self, start: i64, end: i64) -> bool {
        // Segment overlaps if:
        // segment.min <= query.end AND segment.max >= query.start
        self.time_range.0 <= end && self.time_range.1 >= start
    }

    /// Check if this segment contains a specific LSN
    pub fn contains_lsn(&self, lsn: u64) -> bool {
        lsn >= self.lsn_range.0 && lsn <= self.lsn_range.1
    }

    /// Check if segment is old enough to prune (after Iceberg commit)
    pub fn can_prune(&self, min_age_seconds: i64) -> bool {
        if !self.committed_to_iceberg {
            return false;
        }

        let age = chrono::Utc::now().timestamp() - self.created_at;
        age >= min_age_seconds
    }

    /// Estimate memory usage of this metadata structure
    pub fn memory_usage(&self) -> usize {
        // Rough estimate:
        // - id: 8 bytes
        // - file_path: ~50 bytes average
        // - row_count: 4 bytes
        // - byte_size: 8 bytes
        // - time_range: 16 bytes
        // - lsn_range: 16 bytes
        // - created_at: 8 bytes
        // - committed_to_iceberg: 1 byte
        // - struct overhead: ~20 bytes
        // Total: ~130 bytes
        130 + self.file_path.as_os_str().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_overlaps_time_range() {
        let segment = SegmentMeta {
            id: 1000,
            file_path: PathBuf::from("segment_1000.arrow"),
            row_count: 1000,
            byte_size: 10000,
            time_range: (100, 200),  // Segment covers 100-200
            lsn_range: (1000, 2000),
            created_at: 0,
            committed_to_iceberg: false,
        };

        // Query entirely before segment
        assert!(!segment.overlaps_time_range(50, 90));

        // Query entirely after segment
        assert!(!segment.overlaps_time_range(250, 300));

        // Query overlaps start
        assert!(segment.overlaps_time_range(90, 150));

        // Query overlaps end
        assert!(segment.overlaps_time_range(150, 250));

        // Query contains segment
        assert!(segment.overlaps_time_range(50, 250));

        // Segment contains query
        assert!(segment.overlaps_time_range(120, 180));
    }

    #[test]
    fn test_contains_lsn() {
        let segment = SegmentMeta {
            id: 1000,
            file_path: PathBuf::from("segment_1000.arrow"),
            row_count: 1000,
            byte_size: 10000,
            time_range: (0, 0),
            lsn_range: (1000, 2000),
            created_at: 0,
            committed_to_iceberg: false,
        };

        assert!(!segment.contains_lsn(999));
        assert!(segment.contains_lsn(1000));
        assert!(segment.contains_lsn(1500));
        assert!(segment.contains_lsn(2000));
        assert!(!segment.contains_lsn(2001));
    }
}
