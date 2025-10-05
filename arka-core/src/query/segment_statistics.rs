use crate::indexes::segment_index::SegmentIndex;
use crate::storage::segment::SegmentMeta;
use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::{Column, ScalarValue};
use datafusion::physical_optimizer::pruning::PruningStatistics;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

/// Adapter that exposes SegmentIndex to DataFusion's PruningPredicate
///
/// DataFusion's pruning model:
/// - Each "container" = one segment file
/// - For each column, provide arrays of [min, max, null_count, row_count] across all containers
///
/// Memory efficiency:
/// - Stores Vec<Arc<SegmentMeta>> snapshot (cheap! ~8 bytes per segment)
/// - Example: 10,000 segments = 80KB snapshot
/// - All segments share the same SegmentMeta data via Arc
pub struct SegmentPruningStatistics {
    /// Snapshot of segment metadata (Arc makes this ultra-cheap to clone)
    /// Cost: num_segments × 8 bytes
    segments: Vec<Arc<SegmentMeta>>,

    /// Schema for the table
    schema: SchemaRef,
}

impl SegmentPruningStatistics {
    /// Create a new statistics adapter
    ///
    /// Takes a snapshot of current segments. This is cheap because segments
    /// are stored as Arc<SegmentMeta> in the index.
    pub fn new(segment_index: Arc<RwLock<SegmentIndex>>, schema: SchemaRef) -> Self {
        // Snapshot all segments (each clone is just 8 bytes!)
        let segments = segment_index.read().unwrap().all();

        Self { segments, schema }
    }

    /// Get the segments that survived pruning (by index)
    pub fn get_segments(&self) -> &[Arc<SegmentMeta>] {
        &self.segments
    }
}

impl PruningStatistics for SegmentPruningStatistics {
    /// Return min values for each segment for the given column
    ///
    /// Example: column "amount", 3 segments
    /// Returns: Int64Array([100, 500, 200])
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        let col_name = &column.name;

        // Find data type from schema
        let field = self.schema.field_with_name(col_name).ok()?;

        match field.data_type() {
            DataType::Int64 => {
                let mins: Vec<Option<i64>> = self
                    .segments
                    .iter()
                    .map(|seg| {
                        seg.get_column_stats(col_name)
                            .and_then(|stats| stats.min_value.as_ref())
                            .and_then(|bytes| {
                                // Convert big-endian bytes to i64
                                if bytes.len() >= 8 {
                                    Some(i64::from_be_bytes([
                                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
                                        bytes[6], bytes[7],
                                    ]))
                                } else {
                                    None
                                }
                            })
                    })
                    .collect();

                Some(Arc::new(Int64Array::from(mins)))
            }
            DataType::Utf8 => {
                let mins: Vec<Option<String>> = self
                    .segments
                    .iter()
                    .map(|seg| {
                        seg.get_column_stats(col_name)
                            .and_then(|stats| stats.min_value.as_ref())
                            .map(|bytes| String::from_utf8_lossy(bytes).to_string())
                    })
                    .collect();

                Some(Arc::new(StringArray::from(mins)))
            }
            DataType::Timestamp(..) => {
                // Timestamps stored as i64 microseconds in time_range.0
                let mins: Vec<Option<i64>> = self
                    .segments
                    .iter()
                    .map(|seg| Some(seg.time_range.0))
                    .collect();

                Some(Arc::new(Int64Array::from(mins)))
            }
            _ => None, // Unsupported type = no pruning for this column
        }
    }

    /// Return max values for each segment for the given column
    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        let col_name = &column.name;
        let field = self.schema.field_with_name(col_name).ok()?;

        match field.data_type() {
            DataType::Int64 => {
                let maxs: Vec<Option<i64>> = self
                    .segments
                    .iter()
                    .map(|seg| {
                        seg.get_column_stats(col_name)
                            .and_then(|stats| stats.max_value.as_ref())
                            .and_then(|bytes| {
                                if bytes.len() >= 8 {
                                    Some(i64::from_be_bytes([
                                        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
                                        bytes[6], bytes[7],
                                    ]))
                                } else {
                                    None
                                }
                            })
                    })
                    .collect();

                Some(Arc::new(Int64Array::from(maxs)))
            }
            DataType::Utf8 => {
                let maxs: Vec<Option<String>> = self
                    .segments
                    .iter()
                    .map(|seg| {
                        seg.get_column_stats(col_name)
                            .and_then(|stats| stats.max_value.as_ref())
                            .map(|bytes| String::from_utf8_lossy(bytes).to_string())
                    })
                    .collect();

                Some(Arc::new(StringArray::from(maxs)))
            }
            DataType::Timestamp(..) => {
                let maxs: Vec<Option<i64>> = self
                    .segments
                    .iter()
                    .map(|seg| Some(seg.time_range.1))
                    .collect();

                Some(Arc::new(Int64Array::from(maxs)))
            }
            _ => None,
        }
    }

    /// Number of containers (segments)
    fn num_containers(&self) -> usize {
        self.segments.len()
    }

    /// Null counts (we don't track this, return None = conservative)
    fn null_counts(&self, _column: &Column) -> Option<ArrayRef> {
        None // Conservative: assume nulls might exist
    }

    /// Row counts per segment
    fn row_counts(&self, _column: &Column) -> Option<ArrayRef> {
        let counts: Vec<u64> = self
            .segments
            .iter()
            .map(|seg| seg.row_count as u64)
            .collect();

        Some(Arc::new(UInt64Array::from(counts)))
    }

    /// Contained method (required by trait in newer DataFusion versions)
    /// This is used for bloom filter/set membership pruning
    /// We don't support this yet, so return None (conservative)
    fn contained(&self, _column: &Column, _values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None // Conservative: assume value might be contained
    }
}
