use super::segment_writer::{SegmentWriter, SegmentWriterError};
use super::write_buffer::{WriteBuffer, WriteBufferError};
use crate::indexes::pk_index::PKIndex;
use crate::indexes::segment_index::SegmentIndex;
use arrow::record_batch::RecordBatch;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use tokio::task::JoinHandle;

/// Background flusher that periodically writes buffers to disk
///
/// ## Architecture
///
/// ```text
/// ┌─────────────────────────────────────────────────┐
/// │  Background Flusher Thread                      │
/// │                                                 │
/// │  loop:                                          │
/// │    1. Wait for trigger (interval OR manual)     │
/// │    2. Check if current buffer should freeze     │
/// │    3. Freeze current buffer                     │
/// │    4. Pop from immutable queue                  │
/// │    5. Write to disk (single segment)            │
/// │    6. Update indexes                            │
/// │    7. Mark bytes as flushed                     │
/// └─────────────────────────────────────────────────┘
/// ```
///
/// ## Flush Triggers
/// - **Time-based**: Every `flush_interval` (e.g., 100ms)
/// - **Size-based**: When buffer size exceeds `max_buffer_size_bytes`
/// - **Count-based**: When batch count exceeds `max_batches_per_buffer`
/// - **Manual**: Via `flush_now()` call
pub struct BufferFlusher {
    /// Write buffer to flush
    write_buffer: Arc<WriteBuffer>,

    /// Segment writer for writing to disk
    segment_writer: Arc<SegmentWriter>,

    /// Primary key index to update
    pk_index: Arc<RwLock<PKIndex>>,

    /// Segment index to update
    segment_index: Arc<RwLock<SegmentIndex>>,

    /// Shutdown signal
    shutdown: Arc<AtomicBool>,

    /// Handle to the background task
    task_handle: Option<JoinHandle<()>>,
}

impl BufferFlusher {
    /// Create a new BufferFlusher
    ///
    /// # Arguments
    /// * `write_buffer` - The write buffer to flush
    /// * `segment_writer` - Writer for creating segment files
    /// * `pk_index` - Primary key index to update after flush
    /// * `segment_index` - Segment index to update after flush
    pub fn new(
        write_buffer: Arc<WriteBuffer>,
        segment_writer: Arc<SegmentWriter>,
        pk_index: Arc<RwLock<PKIndex>>,
        segment_index: Arc<RwLock<SegmentIndex>>,
    ) -> Self {
        Self {
            write_buffer,
            segment_writer,
            pk_index,
            segment_index,
            shutdown: Arc::new(AtomicBool::new(false)),
            task_handle: None,
        }
    }

    /// Start the background flusher thread
    ///
    /// This spawns a tokio task that runs the flush loop.
    pub fn start(&mut self) {
        let write_buffer = self.write_buffer.clone();
        let segment_writer = self.segment_writer.clone();
        let pk_index = self.pk_index.clone();
        let segment_index = self.segment_index.clone();
        let shutdown = self.shutdown.clone();

        let handle = tokio::spawn(async move {
            Self::flush_loop(
                write_buffer,
                segment_writer,
                pk_index,
                segment_index,
                shutdown,
            )
            .await;
        });

        self.task_handle = Some(handle);
    }

    /// Stop the background flusher
    ///
    /// This signals the background task to shutdown and waits for it to complete.
    /// All remaining buffers will be flushed before shutdown.
    pub async fn stop(self) -> Result<(), BufferFlusherError> {
        // Signal shutdown
        self.shutdown.store(true, Ordering::SeqCst);

        // Wait for task to complete
        if let Some(handle) = self.task_handle {
            handle
                .await
                .map_err(|_| BufferFlusherError::TaskJoinError)?;
        }

        Ok(())
    }

    /// Main flush loop (runs in background task)
    async fn flush_loop(
        write_buffer: Arc<WriteBuffer>,
        segment_writer: Arc<SegmentWriter>,
        pk_index: Arc<RwLock<PKIndex>>,
        segment_index: Arc<RwLock<SegmentIndex>>,
        shutdown: Arc<AtomicBool>,
    ) {
        let flush_interval = write_buffer.config().flush_interval;
        let mut interval = tokio::time::interval(flush_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Time-based trigger
                    if let Err(e) = Self::maybe_flush(
                        &write_buffer,
                        &segment_writer,
                        &pk_index,
                        &segment_index,
                    ).await {
                        eprintln!("Flush error: {}", e);
                    }
                }
                _ = tokio::signal::ctrl_c() => {
                    // Graceful shutdown on Ctrl+C
                    break;
                }
            }

            // Check shutdown signal
            if shutdown.load(Ordering::SeqCst) {
                // Flush all remaining buffers before shutting down
                if let Err(e) =
                    Self::flush_all(&write_buffer, &segment_writer, &pk_index, &segment_index).await
                {
                    eprintln!("Final flush error: {}", e);
                }
                break;
            }
        }
    }

    /// Check if we should flush and do so if needed
    async fn maybe_flush(
        write_buffer: &Arc<WriteBuffer>,
        segment_writer: &Arc<SegmentWriter>,
        pk_index: &Arc<RwLock<PKIndex>>,
        segment_index: &Arc<RwLock<SegmentIndex>>,
    ) -> Result<(), BufferFlusherError> {
        // Check if current buffer should be frozen
        let should_freeze = write_buffer
            .current_size_bytes()
            .map_err(BufferFlusherError::WriteBuffer)?
            >= write_buffer.config().max_buffer_size_bytes
            || write_buffer
                .current_batch_count()
                .map_err(BufferFlusherError::WriteBuffer)?
                >= write_buffer.config().max_batches_per_buffer
            || !write_buffer
                .is_current_empty()
                .map_err(BufferFlusherError::WriteBuffer)?;

        if should_freeze {
            // Freeze current buffer
            write_buffer
                .freeze_current()
                .map_err(BufferFlusherError::WriteBuffer)?;
        }

        // Flush all frozen buffers
        Self::flush_all(write_buffer, segment_writer, pk_index, segment_index).await
    }

    /// Flush all frozen buffers
    async fn flush_all(
        write_buffer: &Arc<WriteBuffer>,
        segment_writer: &Arc<SegmentWriter>,
        pk_index: &Arc<RwLock<PKIndex>>,
        segment_index: &Arc<RwLock<SegmentIndex>>,
    ) -> Result<(), BufferFlusherError> {
        while let Some(frozen) = write_buffer
            .pop_frozen()
            .map_err(BufferFlusherError::WriteBuffer)?
        {
            Self::flush_one(
                frozen.batches,
                frozen.size_bytes,
                segment_writer,
                pk_index,
                segment_index,
                write_buffer,
            )
            .await?;
        }
        Ok(())
    }

    /// Flush a single frozen buffer to disk
    async fn flush_one(
        batches: Vec<RecordBatch>,
        size_bytes: usize,
        segment_writer: &Arc<SegmentWriter>,
        pk_index: &Arc<RwLock<PKIndex>>,
        segment_index: &Arc<RwLock<SegmentIndex>>,
        write_buffer: &Arc<WriteBuffer>,
    ) -> Result<(), BufferFlusherError> {
        if batches.is_empty() {
            return Ok(());
        }

        // Write all batches as a single segment
        let segment_meta = if batches.len() == 1 {
            segment_writer
                .write_batch(&batches[0])
                .map_err(BufferFlusherError::SegmentWriter)?
        } else {
            segment_writer
                .write_batches(&batches)
                .map_err(BufferFlusherError::SegmentWriter)?
        };

        let segment_id = segment_meta.id;

        // Extract primary keys from all batches
        let mut all_pk_mappings = Vec::new();
        for batch in &batches {
            let pk_mappings = extract_primary_keys(batch)?;
            all_pk_mappings.extend(pk_mappings);
        }

        // Update PKIndex
        {
            let mut pk_index = pk_index
                .write()
                .map_err(|_| BufferFlusherError::LockError)?;
            pk_index.insert_batch(segment_id, all_pk_mappings);
        }

        // Update SegmentIndex
        {
            let mut segment_index = segment_index
                .write()
                .map_err(|_| BufferFlusherError::LockError)?;
            segment_index.insert(segment_meta);
        }

        // Mark bytes as flushed (releases backpressure)
        write_buffer.mark_flushed(size_bytes);

        Ok(())
    }
}

/// Extract primary keys from a RecordBatch
///
/// Looks for the `id` column and maps each value to its row offset.
/// If the `id` column is not found, returns an empty vector (no PK indexing).
fn extract_primary_keys(batch: &RecordBatch) -> Result<Vec<(String, u32)>, BufferFlusherError> {
    use arrow::array::*;

    // Try to find "id" column - if not present, skip PK indexing
    let id_col = match batch.column_by_name("id") {
        Some(col) => col,
        None => return Ok(Vec::new()), // No "id" column, skip PK extraction
    };

    let id_array = id_col
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or(BufferFlusherError::InvalidPrimaryKeyType)?;

    let mut mappings = Vec::new();
    for (row_offset, id) in id_array.iter().enumerate() {
        if let Some(id) = id {
            mappings.push((id.to_string(), row_offset as u32));
        }
    }

    Ok(mappings)
}

/// Errors that can occur during buffer flushing
#[derive(Debug, thiserror::Error)]
pub enum BufferFlusherError {
    #[error("WriteBuffer error: {0}")]
    WriteBuffer(#[from] WriteBufferError),

    #[error("SegmentWriter error: {0}")]
    SegmentWriter(#[from] SegmentWriterError),

    #[error("Failed to acquire lock")]
    LockError,

    #[error("Missing primary key column 'id' in batch")]
    MissingPrimaryKey,

    #[error("Primary key column 'id' is not Int64 type")]
    InvalidPrimaryKeyType,

    #[error("Failed to join background task")]
    TaskJoinError,
}

#[cfg(test)]
mod tests {
    use super::super::config::{DurabilityLevel, SegmentWriterConfig};
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_batch(start_id: i64, num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int64Array::from((start_id..start_id + num_rows as i64).collect::<Vec<_>>());
        let name_array = StringArray::from(
            (0..num_rows)
                .map(|i| format!("name_{}", i))
                .collect::<Vec<_>>(),
        );

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    #[tokio::test]
    async fn test_flush_one_buffer() {
        let temp_dir = TempDir::new().unwrap();

        let writer_config = SegmentWriterConfig {
            durability: DurabilityLevel::Buffered,
        };
        let segment_writer =
            Arc::new(SegmentWriter::with_config(temp_dir.path(), writer_config).unwrap());

        let pk_index = Arc::new(RwLock::new(PKIndex::new()));
        let segment_index = Arc::new(RwLock::new(SegmentIndex::new()));
        let write_buffer = Arc::new(WriteBuffer::new());

        let batch = create_test_batch(1, 100);
        let batches = vec![batch];
        let size = 1000;

        BufferFlusher::flush_one(
            batches,
            size,
            &segment_writer,
            &pk_index,
            &segment_index,
            &write_buffer,
        )
        .await
        .unwrap();

        // Verify segment was written
        assert_eq!(segment_writer.current_segment_id(), 2);

        // Verify index was updated
        let segment_index = segment_index.read().unwrap();
        assert_eq!(segment_index.len(), 1);

        let pk_index = pk_index.read().unwrap();
        assert_eq!(pk_index.get("1").unwrap().segment_id, 1);
    }

    #[tokio::test]
    async fn test_extract_primary_keys() {
        let batch = create_test_batch(10, 5);
        let mappings = extract_primary_keys(&batch).unwrap();

        assert_eq!(mappings.len(), 5);
        assert_eq!(mappings[0], ("10".to_string(), 0));
        assert_eq!(mappings[1], ("11".to_string(), 1));
        assert_eq!(mappings[4], ("14".to_string(), 4));
    }
}
