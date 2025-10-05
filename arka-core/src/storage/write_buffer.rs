use super::config::WriteBufferConfig;
use arrow::record_batch::RecordBatch;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::Notify;

/// A frozen buffer ready to be flushed to disk
///
/// Once a buffer is frozen, it becomes immutable and is queued for flushing.
#[derive(Debug, Clone)]
pub struct FrozenBuffer {
    /// The batches in this buffer
    pub batches: Vec<RecordBatch>,

    /// Total size in bytes
    pub size_bytes: usize,

    /// Timestamp when this buffer was frozen (Unix epoch seconds)
    pub frozen_at: i64,

    /// LSN range covered by batches in this buffer
    /// Used for durability tracking - allows marking LSNs as flushed after write
    pub lsn_range: Option<(u64, u64)>,
}

/// Internal state of the current mutable buffer
#[derive(Debug)]
struct BufferState {
    /// RecordBatches accumulated in this buffer
    batches: Vec<RecordBatch>,

    /// Total size in bytes of all batches
    size_bytes: usize,

    /// LSN range for batches in this buffer
    min_lsn: Option<u64>,
    max_lsn: Option<u64>,
}

impl BufferState {
    fn new() -> Self {
        Self {
            batches: Vec::new(),
            size_bytes: 0,
            min_lsn: None,
            max_lsn: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    fn len(&self) -> usize {
        self.batches.len()
    }

    fn should_freeze(&self, config: &WriteBufferConfig) -> bool {
        self.size_bytes >= config.max_buffer_size_bytes
            || self.batches.len() >= config.max_batches_per_buffer
    }
}

/// WriteBuffer accumulates RecordBatches in memory before flushing
///
/// ## Architecture
///
/// ```text
/// ┌─────────────────────────────────────────┐
/// │  current_buffer (mutable)               │
/// │  - accepts new writes                   │
/// │  - grows until freeze trigger           │
/// └─────────────────────────────────────────┘
///              │ freeze
///              ↓
/// ┌─────────────────────────────────────────┐
/// │  immutable_buffers (FIFO queue)         │
/// │  - frozen buffers awaiting flush        │
/// │  - processed by background flusher      │
/// └─────────────────────────────────────────┘
/// ```
///
/// ## Freeze Triggers
/// A buffer is frozen when:
/// - Size exceeds `max_buffer_size_bytes`
/// - Batch count exceeds `max_batches_per_buffer`
/// - Time interval expires (handled by flusher)
///
/// ## Backpressure
/// When `unflushed_bytes` exceeds `max_unflushed_bytes`:
/// - New `append()` calls block
/// - Waits for flusher to make space
/// - Prevents OOM
pub struct WriteBuffer {
    /// Current mutable buffer accepting writes
    current: Arc<RwLock<BufferState>>,

    /// Frozen buffers waiting to be flushed (FIFO)
    immutable: Arc<RwLock<VecDeque<FrozenBuffer>>>,

    /// Total bytes not yet flushed to disk
    /// Includes both current and immutable buffers
    unflushed_bytes: Arc<AtomicU64>,

    /// Configuration
    config: WriteBufferConfig,

    /// Notifier for when space becomes available (backpressure release)
    space_available: Arc<Notify>,
}

impl WriteBuffer {
    /// Create a new WriteBuffer with default config
    pub fn new() -> Self {
        Self::with_config(WriteBufferConfig::default())
    }

    /// Create a new WriteBuffer with custom config
    pub fn with_config(config: WriteBufferConfig) -> Self {
        Self {
            current: Arc::new(RwLock::new(BufferState::new())),
            immutable: Arc::new(RwLock::new(VecDeque::new())),
            unflushed_bytes: Arc::new(AtomicU64::new(0)),
            config,
            space_available: Arc::new(Notify::new()),
        }
    }

    /// Append a RecordBatch to the buffer
    ///
    /// This may block if unflushed bytes exceed the configured limit (backpressure).
    ///
    /// # Arguments
    /// * `batch` - RecordBatch to append
    ///
    /// # Returns
    /// Returns true if the buffer should be frozen after this append
    pub async fn append(&self, batch: RecordBatch, lsn: u64) -> Result<bool, WriteBufferError> {
        if batch.num_rows() == 0 {
            return Err(WriteBufferError::EmptyBatch);
        }

        let batch_size = estimate_batch_size(&batch);

        // Backpressure: wait if we're over the limit
        while self.unflushed_bytes.load(Ordering::SeqCst) as usize + batch_size
            > self.config.max_unflushed_bytes
        {
            self.space_available.notified().await;
        }

        // Add to current buffer and track LSN range
        let should_freeze = {
            let mut current = self
                .current
                .write()
                .map_err(|_| WriteBufferError::LockError)?;
            current.batches.push(batch);
            current.size_bytes += batch_size;

            // Track LSN range
            current.min_lsn = Some(current.min_lsn.map_or(lsn, |min| min.min(lsn)));
            current.max_lsn = Some(current.max_lsn.map_or(lsn, |max| max.max(lsn)));

            // Check if we should freeze after this append
            current.should_freeze(&self.config)
        };

        // Update unflushed bytes
        self.unflushed_bytes
            .fetch_add(batch_size as u64, Ordering::SeqCst);

        Ok(should_freeze)
    }

    /// Freeze the current buffer and move it to the immutable queue
    ///
    /// This uses the freeze-and-swap pattern:
    /// 1. Atomically swap current buffer with empty buffer
    /// 2. Move old current to immutable queue
    /// 3. New writes go to the new empty buffer
    ///
    /// Returns true if a buffer was frozen, false if current was empty
    pub fn freeze_current(&self) -> Result<bool, WriteBufferError> {
        let mut current = self
            .current
            .write()
            .map_err(|_| WriteBufferError::LockError)?;
        let mut immutable = self
            .immutable
            .write()
            .map_err(|_| WriteBufferError::LockError)?;

        // Don't freeze if current is empty
        if current.is_empty() {
            return Ok(false);
        }

        // Atomic swap: replace current with empty buffer
        let frozen_state = std::mem::replace(&mut *current, BufferState::new());

        // Build LSN range from min/max
        let lsn_range = match (frozen_state.min_lsn, frozen_state.max_lsn) {
            (Some(min), Some(max)) => Some((min, max)),
            _ => None,
        };

        // Move to immutable queue
        immutable.push_back(FrozenBuffer {
            batches: frozen_state.batches,
            size_bytes: frozen_state.size_bytes,
            frozen_at: chrono::Utc::now().timestamp(),
            lsn_range,
        });

        Ok(true)
    }

    /// Pop the next frozen buffer for flushing (FIFO)
    ///
    /// Returns None if no frozen buffers available
    pub fn pop_frozen(&self) -> Result<Option<FrozenBuffer>, WriteBufferError> {
        let mut immutable = self
            .immutable
            .write()
            .map_err(|_| WriteBufferError::LockError)?;
        Ok(immutable.pop_front())
    }

    /// Mark bytes as flushed (called after successful flush to disk)
    ///
    /// This decrements unflushed_bytes and notifies waiting writers.
    pub fn mark_flushed(&self, bytes: usize) {
        self.unflushed_bytes
            .fetch_sub(bytes as u64, Ordering::SeqCst);

        // Notify waiting writers that space is available
        self.space_available.notify_waiters();
    }

    /// Get the number of frozen buffers waiting to be flushed
    pub fn frozen_count(&self) -> Result<usize, WriteBufferError> {
        let immutable = self
            .immutable
            .read()
            .map_err(|_| WriteBufferError::LockError)?;
        Ok(immutable.len())
    }

    /// Get the number of batches in the current buffer
    pub fn current_batch_count(&self) -> Result<usize, WriteBufferError> {
        let current = self
            .current
            .read()
            .map_err(|_| WriteBufferError::LockError)?;
        Ok(current.len())
    }

    /// Get the size of the current buffer in bytes
    pub fn current_size_bytes(&self) -> Result<usize, WriteBufferError> {
        let current = self
            .current
            .read()
            .map_err(|_| WriteBufferError::LockError)?;
        Ok(current.size_bytes)
    }

    /// Get total unflushed bytes across all buffers
    pub fn unflushed_bytes(&self) -> usize {
        self.unflushed_bytes.load(Ordering::SeqCst) as usize
    }

    /// Check if current buffer is empty
    pub fn is_current_empty(&self) -> Result<bool, WriteBufferError> {
        let current = self
            .current
            .read()
            .map_err(|_| WriteBufferError::LockError)?;
        Ok(current.is_empty())
    }

    /// Get the config
    pub fn config(&self) -> &WriteBufferConfig {
        &self.config
    }
}

impl Default for WriteBuffer {
    fn default() -> Self {
        Self::new()
    }
}

/// Estimate the size of a RecordBatch in bytes
///
/// This includes:
/// - Array data buffers
/// - Null bitmaps
/// - Offset buffers
/// - Metadata overhead
fn estimate_batch_size(batch: &RecordBatch) -> usize {
    use arrow::array::Array;

    let mut total = 0;

    for column in batch.columns() {
        // Add data buffer size
        total += column.get_array_memory_size();
    }

    // Add schema metadata overhead (rough estimate)
    total += 1024;

    total
}

/// Errors that can occur in WriteBuffer operations
#[derive(Debug, thiserror::Error)]
pub enum WriteBufferError {
    #[error("Cannot append empty batch")]
    EmptyBatch,

    #[error("Failed to acquire lock")]
    LockError,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int64Array::from((0..num_rows as i64).collect::<Vec<_>>());
        let name_array = StringArray::from(
            (0..num_rows)
                .map(|i| format!("name_{}", i))
                .collect::<Vec<_>>(),
        );

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    #[tokio::test]
    async fn test_append_and_freeze() {
        let buffer = WriteBuffer::new();

        let batch = create_test_batch(100);
        let should_freeze = buffer.append(batch.clone(), 0).await.unwrap();

        assert!(!should_freeze); // Small batch shouldn't trigger freeze
        assert_eq!(buffer.current_batch_count().unwrap(), 1);
        assert!(buffer.current_size_bytes().unwrap() > 0);
        assert_eq!(buffer.frozen_count().unwrap(), 0);

        // Manually freeze
        let was_frozen = buffer.freeze_current().unwrap();
        assert!(was_frozen);
        assert_eq!(buffer.frozen_count().unwrap(), 1);
        assert_eq!(buffer.current_batch_count().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_freeze_and_pop() {
        let buffer = WriteBuffer::new();

        let batch = create_test_batch(100);
        buffer.append(batch.clone(), 0).await.unwrap();
        buffer.freeze_current().unwrap();

        // Pop frozen buffer
        let frozen = buffer.pop_frozen().unwrap();
        assert!(frozen.is_some());

        let frozen = frozen.unwrap();
        assert_eq!(frozen.batches.len(), 1);
        assert_eq!(frozen.batches[0].num_rows(), 100);

        // Queue should be empty now
        assert_eq!(buffer.frozen_count().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_empty_batch_rejected() {
        let buffer = WriteBuffer::new();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let empty_batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(Vec::<i64>::new()))])
                .unwrap();

        let result = buffer.append(empty_batch, 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_freeze_empty_buffer() {
        let buffer = WriteBuffer::new();

        // Freezing empty buffer should return false
        let was_frozen = buffer.freeze_current().unwrap();
        assert!(!was_frozen);
        assert_eq!(buffer.frozen_count().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_auto_freeze_on_size() {
        let config = WriteBufferConfig {
            max_buffer_size_bytes: 1024, // Very small limit
            max_batches_per_buffer: 1000,
            ..Default::default()
        };

        let buffer = WriteBuffer::with_config(config);

        let batch = create_test_batch(1000); // Large batch
        let should_freeze = buffer.append(batch, 0).await.unwrap();

        // Should trigger freeze due to size
        assert!(should_freeze);
    }

    #[tokio::test]
    async fn test_auto_freeze_on_count() {
        let config = WriteBufferConfig {
            max_buffer_size_bytes: 1024 * 1024 * 1024, // Large size limit
            max_batches_per_buffer: 2,                 // Small count limit
            ..Default::default()
        };

        let buffer = WriteBuffer::with_config(config);

        buffer.append(create_test_batch(10), 0).await.unwrap();
        let should_freeze = buffer.append(create_test_batch(10), 0).await.unwrap();

        // Should trigger freeze on count
        assert!(should_freeze);
    }

    #[tokio::test]
    async fn test_mark_flushed() {
        let buffer = WriteBuffer::new();

        let batch = create_test_batch(100);
        buffer.append(batch, 0).await.unwrap();

        let initial_unflushed = buffer.unflushed_bytes();
        assert!(initial_unflushed > 0);

        buffer.mark_flushed(initial_unflushed);
        assert_eq!(buffer.unflushed_bytes(), 0);
    }

    #[tokio::test]
    async fn test_multiple_freeze_and_pop_fifo() {
        let buffer = WriteBuffer::new();

        // Add and freeze 3 buffers
        buffer.append(create_test_batch(10), 0).await.unwrap();
        buffer.freeze_current().unwrap();

        buffer.append(create_test_batch(20), 0).await.unwrap();
        buffer.freeze_current().unwrap();

        buffer.append(create_test_batch(30), 0).await.unwrap();
        buffer.freeze_current().unwrap();

        assert_eq!(buffer.frozen_count().unwrap(), 3);

        // Pop in FIFO order
        let first = buffer.pop_frozen().unwrap().unwrap();
        assert_eq!(first.batches[0].num_rows(), 10);

        let second = buffer.pop_frozen().unwrap().unwrap();
        assert_eq!(second.batches[0].num_rows(), 20);

        let third = buffer.pop_frozen().unwrap().unwrap();
        assert_eq!(third.batches[0].num_rows(), 30);

        assert_eq!(buffer.frozen_count().unwrap(), 0);
    }
}
