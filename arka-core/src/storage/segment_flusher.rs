use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use arrow::array::{Array, StringArray};
use arrow::record_batch::RecordBatch;
use thiserror::Error;

use super::segment_writer::{SegmentWriter, SegmentWriterError};
use crate::storage::segment::SegmentId;
use crate::indexes::segment_index::SegmentIndex;
use crate::indexes::pk_index::PKIndex;

/// Errors that can occur during segment flushing
#[derive(Error, Debug)]
pub enum SegmentFlusherError {
    #[error("Writer error: {0}")]
    WriterError(#[from] SegmentWriterError),

    #[error("Primary key column '{0}' not found in batch")]
    MissingPKColumn(String),

    #[error("Primary key column is not a string array")]
    InvalidPKType,

    #[error("Index lock error")]
    LockError,
}

/// Coordinates segment writing with index updates
///
/// ## Purpose
/// The SegmentFlusher is the bridge between the write buffer and the storage layer.
/// It ensures that every segment write is atomically tracked in both indexes:
/// 1. PKIndex: Maps primary keys to exact row locations (segment_id, row_index)
/// 2. SegmentIndex: Tracks all segments with their metadata for time-range queries
///
/// ## Architecture
/// ```text
/// RecordBatch
///     ↓
/// SegmentFlusher
///     ├─→ SegmentWriter (writes Arrow IPC file)
///     ├─→ PKIndex (updates key→location mapping)
///     └─→ SegmentIndex (registers new segment)
/// ```
///
/// ## Thread Safety
/// Uses Arc<RwLock<>> for shared index access. Multiple flushers can write
/// concurrently (each gets unique segment IDs from the writer).
///
/// ## Example Flow
/// ```text
/// 1. flush(batch) called with 1000 rows
/// 2. SegmentWriter writes segment_0000000042.arrow to disk
/// 3. Extract primary keys from batch (e.g., "user_123", "user_456", ...)
/// 4. Update PKIndex: "user_123" → (42, 0), "user_456" → (42, 1), ...
/// 5. Update SegmentIndex with segment metadata (time range, row count, etc.)
/// 6. Return segment_id=42 to caller
/// ```
pub struct SegmentFlusher {
    /// Underlying segment writer
    writer: Arc<SegmentWriter>,

    /// Shared segment index (tracks all segments)
    segment_index: Arc<RwLock<SegmentIndex>>,

    /// Shared primary key index (maps keys to locations)
    pk_index: Arc<RwLock<PKIndex>>,

    /// Name of the primary key column in the schema
    /// Defaults to "id" but can be configured per table
    pk_column_name: String,
}

impl SegmentFlusher {
    /// Create a new segment flusher
    ///
    /// # Arguments
    /// * `base_dir` - Directory for segment files
    /// * `segment_index` - Shared segment index
    /// * `pk_index` - Shared primary key index
    /// * `pk_column_name` - Name of the primary key column
    pub fn new(
        base_dir: PathBuf,
        segment_index: Arc<RwLock<SegmentIndex>>,
        pk_index: Arc<RwLock<PKIndex>>,
        pk_column_name: String,
    ) -> Result<Self, SegmentFlusherError> {
        let writer = Arc::new(SegmentWriter::new(base_dir)?);
        Ok(Self {
            writer,
            segment_index,
            pk_index,
            pk_column_name,
        })
    }

    /// Create with recovery (scans existing segments)
    ///
    /// Use this on startup to resume from the last segment ID
    pub fn with_recovery(
        base_dir: PathBuf,
        segment_index: Arc<RwLock<SegmentIndex>>,
        pk_index: Arc<RwLock<PKIndex>>,
        pk_column_name: String,
    ) -> Result<Self, SegmentFlusherError> {
        let recovered_writer = SegmentWriter::with_recovery(base_dir)?;
        let writer = Arc::new(recovered_writer);
        Ok(Self {
            writer,
            segment_index,
            pk_index,
            pk_column_name,
        })
    }

    /// Flush a RecordBatch to disk and update indexes
    ///
    /// This is the main entry point for the write path. It performs:
    /// 1. Write Arrow IPC file to disk
    /// 2. Extract primary keys from batch
    /// 3. Update PKIndex with key→location mappings
    /// 4. Update SegmentIndex with segment metadata
    ///
    /// # Arguments
    /// * `batch` - The RecordBatch to flush
    ///
    /// # Returns
    /// The segment ID that was created
    ///
    /// # Errors
    /// - WriterError if disk write fails
    /// - MissingPKColumn if primary key column not found
    /// - InvalidPKType if primary key is not a string
    /// - LockError if index locks can't be acquired
    pub fn flush(&self, batch: RecordBatch) -> Result<SegmentId, SegmentFlusherError> {
        // 1. Write segment to disk
        let segment_meta = self.writer.write_batch(&batch)?;
        let segment_id = segment_meta.id;

        // 2. Extract primary keys from batch
        let pk_mappings = self.extract_primary_keys(&batch, segment_id)?;

        // 3. Update PKIndex (acquire write lock)
        {
            let mut pk_index = self.pk_index.write()
                .map_err(|_| SegmentFlusherError::LockError)?;
            pk_index.insert_batch(segment_id, pk_mappings);
        }

        // 4. Update SegmentIndex (acquire write lock)
        {
            let mut segment_index = self.segment_index.write()
                .map_err(|_| SegmentFlusherError::LockError)?;
            segment_index.insert(segment_meta);
        }

        Ok(segment_id)
    }

    /// Flush multiple batches as a single segment
    ///
    /// More efficient than calling flush() multiple times when you have
    /// accumulated multiple batches in a write buffer.
    ///
    /// # Arguments
    /// * `batches` - Vector of RecordBatches to write
    ///
    /// # Returns
    /// The segment ID that was created
    pub fn flush_batches(&self, batches: Vec<RecordBatch>) -> Result<SegmentId, SegmentFlusherError> {
        if batches.is_empty() {
            return Err(SegmentFlusherError::WriterError(
                SegmentWriterError::EmptyBatch
            ));
        }

        // 1. Write all batches to single segment
        let segment_meta = self.writer.write_batches(&batches)?;
        let segment_id = segment_meta.id;

        // 2. Extract primary keys from all batches
        let mut all_pk_mappings = Vec::new();
        let mut row_offset = 0u32;

        for batch in &batches {
            let mut batch_mappings = self.extract_primary_keys(batch, segment_id)?;

            // Adjust row indices by current offset
            for (_, row_index) in &mut batch_mappings {
                *row_index += row_offset;
            }

            all_pk_mappings.extend(batch_mappings);
            row_offset += batch.num_rows() as u32;
        }

        // 3. Update PKIndex
        {
            let mut pk_index = self.pk_index.write()
                .map_err(|_| SegmentFlusherError::LockError)?;
            pk_index.insert_batch(segment_id, all_pk_mappings);
        }

        // 4. Update SegmentIndex
        {
            let mut segment_index = self.segment_index.write()
                .map_err(|_| SegmentFlusherError::LockError)?;
            segment_index.insert(segment_meta);
        }

        Ok(segment_id)
    }

    /// Extract primary keys from a batch
    ///
    /// Returns Vec<(key, row_index)> for indexing
    ///
    /// # Arguments
    /// * `batch` - The RecordBatch to extract keys from
    /// * `segment_id` - The segment ID (for error context)
    ///
    /// # Errors
    /// - MissingPKColumn if the primary key column doesn't exist
    /// - InvalidPKType if the column is not a StringArray
    fn extract_primary_keys(
        &self,
        batch: &RecordBatch,
        _segment_id: SegmentId,
    ) -> Result<Vec<(String, u32)>, SegmentFlusherError> {
        // Find the primary key column
        let pk_column = batch
            .column_by_name(&self.pk_column_name)
            .ok_or_else(|| SegmentFlusherError::MissingPKColumn(self.pk_column_name.clone()))?;

        // Downcast to StringArray
        let pk_array = pk_column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(SegmentFlusherError::InvalidPKType)?;

        // Extract keys with their row indices
        let mut keys = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            if let Some(key) = pk_array.value(row_idx).into() {
                keys.push((key.to_string(), row_idx as u32));
            }
            // Skip null keys (should rarely happen for primary keys)
        }

        Ok(keys)
    }

    /// Get reference to underlying segment writer
    pub fn writer(&self) -> &Arc<SegmentWriter> {
        &self.writer
    }

    /// Get the next segment ID that will be assigned
    pub fn next_segment_id(&self) -> SegmentId {
        self.writer.current_segment_id()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, TimestampMicrosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_batch(start_id: u64, count: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("__lsn", DataType::UInt64, false),
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        ]));

        let ids: Vec<String> = (start_id..start_id + count as u64)
            .map(|i| format!("key_{}", i))
            .collect();
        let lsns: Vec<u64> = (start_id..start_id + count as u64).collect();
        let timestamps: Vec<i64> = (0..count)
            .map(|i| 1700000000000000 + i as i64 * 1000)
            .collect();

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(UInt64Array::from(lsns)),
                Arc::new(TimestampMicrosecondArray::from(timestamps)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_basic_flush() {
        let temp_dir = TempDir::new().unwrap();
        let segment_index = Arc::new(RwLock::new(SegmentIndex::new()));
        let pk_index = Arc::new(RwLock::new(PKIndex::new()));

        let flusher = SegmentFlusher::new(
            temp_dir.path().to_path_buf(),
            segment_index.clone(),
            pk_index.clone(),
            "id".to_string(),
        ).unwrap();

        let batch = create_test_batch(1, 100);
        let segment_id = flusher.flush(batch).unwrap();

        // Verify segment was added to SegmentIndex
        let seg_index = segment_index.read().unwrap();
        assert_eq!(seg_index.len(), 1);
        let segment = seg_index.get(segment_id).unwrap();
        assert_eq!(segment.row_count, 100);

        // Verify keys were added to PKIndex
        let pk_idx = pk_index.read().unwrap();
        assert_eq!(pk_idx.len(), 100);

        let location = pk_idx.get("key_1").unwrap();
        assert_eq!(location.segment_id, segment_id);
        assert_eq!(location.row_index, 0);

        let location = pk_idx.get("key_50").unwrap();
        assert_eq!(location.segment_id, segment_id);
        assert_eq!(location.row_index, 49);
    }

    #[test]
    fn test_multiple_flushes() {
        let temp_dir = TempDir::new().unwrap();
        let segment_index = Arc::new(RwLock::new(SegmentIndex::new()));
        let pk_index = Arc::new(RwLock::new(PKIndex::new()));

        let flusher = SegmentFlusher::new(
            temp_dir.path().to_path_buf(),
            segment_index.clone(),
            pk_index.clone(),
            "id".to_string(),
        ).unwrap();

        // Flush 3 batches
        let seg1 = flusher.flush(create_test_batch(1, 50)).unwrap();
        let seg2 = flusher.flush(create_test_batch(51, 50)).unwrap();
        let seg3 = flusher.flush(create_test_batch(101, 50)).unwrap();

        // Verify unique segment IDs
        assert!(seg1 < seg2);
        assert!(seg2 < seg3);

        // Verify all segments tracked
        let seg_index = segment_index.read().unwrap();
        assert_eq!(seg_index.len(), 3);

        // Verify all keys tracked across segments
        let pk_idx = pk_index.read().unwrap();
        assert_eq!(pk_idx.len(), 150);
        assert_eq!(pk_idx.segment_count(), 3);

        // Verify keys point to correct segments
        assert_eq!(pk_idx.get("key_1").unwrap().segment_id, seg1);
        assert_eq!(pk_idx.get("key_51").unwrap().segment_id, seg2);
        assert_eq!(pk_idx.get("key_101").unwrap().segment_id, seg3);
    }

    #[test]
    fn test_flush_batches() {
        let temp_dir = TempDir::new().unwrap();
        let segment_index = Arc::new(RwLock::new(SegmentIndex::new()));
        let pk_index = Arc::new(RwLock::new(PKIndex::new()));

        let flusher = SegmentFlusher::new(
            temp_dir.path().to_path_buf(),
            segment_index.clone(),
            pk_index.clone(),
            "id".to_string(),
        ).unwrap();

        // Flush 3 batches as single segment
        let batches = vec![
            create_test_batch(1, 50),
            create_test_batch(51, 50),
            create_test_batch(101, 50),
        ];
        let segment_id = flusher.flush_batches(batches).unwrap();

        // Verify only 1 segment created
        let seg_index = segment_index.read().unwrap();
        assert_eq!(seg_index.len(), 1);
        let segment = seg_index.get(segment_id).unwrap();
        assert_eq!(segment.row_count, 150);

        // Verify all keys tracked with correct row indices
        let pk_idx = pk_index.read().unwrap();
        assert_eq!(pk_idx.len(), 150);
        assert_eq!(pk_idx.segment_count(), 1);

        // Check row indices are correctly offset
        assert_eq!(pk_idx.get("key_1").unwrap().row_index, 0);
        assert_eq!(pk_idx.get("key_51").unwrap().row_index, 50);
        assert_eq!(pk_idx.get("key_101").unwrap().row_index, 100);
    }

    #[test]
    fn test_missing_pk_column() {
        let temp_dir = TempDir::new().unwrap();
        let segment_index = Arc::new(RwLock::new(SegmentIndex::new()));
        let pk_index = Arc::new(RwLock::new(PKIndex::new()));

        // Configure with wrong PK column name
        let flusher = SegmentFlusher::new(
            temp_dir.path().to_path_buf(),
            segment_index,
            pk_index,
            "wrong_column".to_string(),
        ).unwrap();

        let batch = create_test_batch(1, 10);
        let result = flusher.flush(batch);

        assert!(result.is_err());
        match result.unwrap_err() {
            SegmentFlusherError::MissingPKColumn(col) => {
                assert_eq!(col, "wrong_column");
            }
            _ => panic!("Expected MissingPKColumn error"),
        }
    }

    #[test]
    fn test_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let segment_index = Arc::new(RwLock::new(SegmentIndex::new()));
        let pk_index = Arc::new(RwLock::new(PKIndex::new()));

        // Create first flusher and write some segments
        {
            let flusher = SegmentFlusher::new(
                temp_dir.path().to_path_buf(),
                segment_index.clone(),
                pk_index.clone(),
                "id".to_string(),
            ).unwrap();
            flusher.flush(create_test_batch(1, 10)).unwrap();
            flusher.flush(create_test_batch(11, 10)).unwrap();
        }

        // Create new flusher with recovery
        let segment_index2 = Arc::new(RwLock::new(SegmentIndex::new()));
        let pk_index2 = Arc::new(RwLock::new(PKIndex::new()));

        let flusher2 = SegmentFlusher::with_recovery(
            temp_dir.path().to_path_buf(),
            segment_index2.clone(),
            pk_index2.clone(),
            "id".to_string(),
        ).unwrap();

        // Next segment ID should be after the recovered ones
        let next_id = flusher2.next_segment_id();
        assert!(next_id > 1);

        // Write new segment
        let new_segment = flusher2.flush(create_test_batch(21, 10)).unwrap();
        assert_eq!(new_segment, next_id);
    }
}
