use arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Unique segment identifier (epoch-based)
pub type SegmentId = u64;

/// Default data type for deserialization (when skipped)
fn default_data_type() -> DataType {
    DataType::Null
}

/// Statistics for a single column in a segment
/// Only tracks min/max for numeric and temporal types (useful for range pruning)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnStats {
    /// Column name
    pub column_name: String,

    /// Column data type (for reference, not used in comparison)
    #[serde(skip, default = "default_data_type")]
    pub data_type: DataType,

    /// Minimum value encoded as big-endian bytes for comparison
    /// None if all values are null
    pub min_value: Option<Vec<u8>>,

    /// Maximum value encoded as big-endian bytes for comparison
    /// None if all values are null
    pub max_value: Option<Vec<u8>>,

    /// Number of null values in this column
    pub null_count: usize,

    /// Total number of values in this column
    pub value_count: usize,
}

impl ColumnStats {
    /// Check if this column's range overlaps with a query range [min, max]
    /// Values must be encoded in the same format (big-endian bytes)
    pub fn overlaps_range(&self, min: &[u8], max: &[u8]) -> bool {
        match (&self.min_value, &self.max_value) {
            (Some(col_min), Some(col_max)) => {
                // Segment range: [col_min, col_max]
                // Query range: [min, max]
                // Overlaps if: col_max >= min AND col_min <= max
                col_max.as_slice() >= min && col_min.as_slice() <= max
            }
            _ => true, // No bounds available, can't prune
        }
    }

    /// Check if a specific value might exist in this column
    /// Value must be encoded in the same format (big-endian bytes)
    pub fn might_contain(&self, value: &[u8]) -> bool {
        match (&self.min_value, &self.max_value) {
            (Some(col_min), Some(col_max)) => {
                value >= col_min.as_slice() && value <= col_max.as_slice()
            }
            _ => true, // No bounds, can't exclude
        }
    }

    /// Check if all values are null
    pub fn is_all_null(&self) -> bool {
        self.null_count == self.value_count
    }

    /// Get null percentage
    pub fn null_percentage(&self) -> f64 {
        if self.value_count == 0 {
            0.0
        } else {
            (self.null_count as f64) / (self.value_count as f64)
        }
    }
}

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

    /// Per-column statistics for numeric/temporal columns
    /// Used for segment pruning during queries
    #[serde(default)]
    pub column_stats: HashMap<String, ColumnStats>,
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
            column_stats: HashMap::new(),
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
        let base = 130 + self.file_path.as_os_str().len();

        // Add column stats memory (~100 bytes per column)
        let stats_size = self.column_stats.len() * 100;

        base + stats_size
    }

    /// Get statistics for a specific column
    pub fn get_column_stats(&self, column_name: &str) -> Option<&ColumnStats> {
        self.column_stats.get(column_name)
    }

    /// Check if segment might contain a value in a specific column
    /// Value must be encoded as big-endian bytes
    pub fn column_might_contain(&self, column: &str, value: &[u8]) -> bool {
        match self.get_column_stats(column) {
            Some(stats) => stats.might_contain(value),
            None => true, // No stats for this column, can't prune
        }
    }

    /// Check if segment's column range overlaps with query range [min, max]
    /// Values must be encoded as big-endian bytes
    pub fn column_overlaps_range(&self, column: &str, min: &[u8], max: &[u8]) -> bool {
        match self.get_column_stats(column) {
            Some(stats) => stats.overlaps_range(min, max),
            None => true, // No stats, can't prune
        }
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
            time_range: (100, 200), // Segment covers 100-200
            lsn_range: (1000, 2000),
            created_at: 0,
            committed_to_iceberg: false,
            column_stats: HashMap::new(),
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
            column_stats: HashMap::new(),
        };

        assert!(!segment.contains_lsn(999));
        assert!(segment.contains_lsn(1000));
        assert!(segment.contains_lsn(1500));
        assert!(segment.contains_lsn(2000));
        assert!(!segment.contains_lsn(2001));
    }
}
