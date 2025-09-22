use crate::lsn::{LSNcoordinator, LSN};
use arrow::array::{
    Array, BinaryBuilder, StringArray, TimestampMicrosecondArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};
use std::fs::{create_dir_all, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

/// WAL record representing a single operation
#[derive(Debug, Clone)]
pub struct WalRecord {
    pub lsn: LSN,
    pub timestamp: u64,
    pub change_type: ChangeType,
    pub table_id: u32,
    pub key: Option<Vec<u8>>,
    pub data: Vec<u8>,
}

/// Type of change operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChangeType {
    Insert,
    UpdateBefore,
    UpdateAfter,
    Delete,
}

impl ChangeType {
    fn as_str(&self) -> &'static str {
        match self {
            ChangeType::Insert => "INSERT",
            ChangeType::UpdateBefore => "UPDATE_BEFORE",
            ChangeType::UpdateAfter => "UPDATE_AFTER",
            ChangeType::Delete => "DELETE",
        }
    }
}

/// Configuration for WAL behavior
#[derive(Debug, Clone)]
pub struct WalConfig {
    pub base_path: PathBuf,
    pub segment_size_limit: u64,      // 128MB default
    pub segment_time_limit: Duration, // 5 minutes default
    pub flush_batch_size: usize,      // 1000 records default
    pub flush_interval: Duration,     // 100ms default
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            base_path: PathBuf::from("./wal"),
            segment_size_limit: 128 * 1024 * 1024, // 128MB
            segment_time_limit: Duration::from_secs(300), // 5 minutes
            flush_batch_size: 1000,
            flush_interval: Duration::from_millis(100),
        }
    }
}

/// Current segment being written to
struct CurrentSegment {
    writer: StreamWriter<File>,
    path: PathBuf,
    base_lsn: LSN,
    record_count: usize,
    size_bytes: u64,
    created_at: Instant,
}

/// WAL writer with memory buffering
pub struct WalWriter {
    config: WalConfig,
    current_segment: Option<CurrentSegment>,
    write_buffer: Vec<WalRecord>,
    last_flush: Instant,
    lsn_coordinator: Arc<LSNcoordinator>,
    schema: Arc<Schema>,
}

impl WalWriter {
    /// Create new WAL writer
    pub fn new(
        config: WalConfig,
        lsn_coordinator: Arc<LSNcoordinator>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Create WAL directory structure
        create_dir_all(&config.base_path)?;
        create_dir_all(config.base_path.join("segments"))?;

        // Define Arrow schema for WAL records
        let schema = Arc::new(Schema::new(vec![
            Field::new("__lsn", DataType::UInt64, false),
            Field::new(
                "__timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("__change_type", DataType::Utf8, false),
            Field::new("__table_id", DataType::UInt32, false),
            Field::new("__key", DataType::Binary, true), // Nullable for append-only tables
            Field::new("__data", DataType::Binary, false),
        ]));

        let mut writer = Self {
            config,
            current_segment: None,
            write_buffer: Vec::with_capacity(1000),
            last_flush: Instant::now(),
            lsn_coordinator,
            schema,
        };

        // Create initial segment
        writer.create_new_segment()?;

        Ok(writer)
    }

    /// Write a record to WAL (buffered)
    pub async fn write_record(
        &mut self,
        mut record: WalRecord,
    ) -> Result<LSN, Box<dyn std::error::Error>> {
        // Assign LSN if not already set
        if record.lsn == LSN::ZERO {
            record.lsn = self.lsn_coordinator.assign_lsn();
        }

        // Set timestamp if not already set
        if record.timestamp == 0 {
            record.timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64;
        }

        // Add to buffer
        let lsn = record.lsn;
        self.write_buffer.push(record);

        // Check flush conditions
        if self.should_flush() {
            self.flush_buffer().await?;
        }

        // Check roll conditions
        if self.should_roll() {
            self.roll_segment().await?;
        }

        Ok(lsn)
    }

    /// Write record with immediate durability (forces flush)
    pub async fn write_record_durable(
        &mut self,
        record: WalRecord,
    ) -> Result<LSN, Box<dyn std::error::Error>> {
        let lsn = self.write_record(record).await?;
        self.flush_buffer().await?;
        Ok(lsn)
    }

    /// Check if buffer should be flushed
    fn should_flush(&self) -> bool {
        self.write_buffer.len() >= self.config.flush_batch_size
            || self.last_flush.elapsed() >= self.config.flush_interval
    }

    /// Check if segment should be rolled
    pub fn should_roll(&self) -> bool {
        if let Some(ref segment) = self.current_segment {
            segment.size_bytes >= self.config.segment_size_limit
                || segment.created_at.elapsed() >= self.config.segment_time_limit
        } else {
            false
        }
    }

    /// Flush write buffer to disk
    async fn flush_buffer(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }

        // Convert records to Arrow RecordBatch
        let batch = self.records_to_arrow_batch(&self.write_buffer)?;

        // Write to current segment
        if let Some(ref mut segment) = self.current_segment {
            segment.writer.write(&batch)?;
            segment.writer.get_mut().sync_all()?; // Force fsync

            // Update segment metadata
            segment.record_count += self.write_buffer.len();
            segment.size_bytes += batch.get_array_memory_size() as u64;

            // Update LSN coordinator
            if let Some(max_lsn) = self.write_buffer.iter().map(|r| r.lsn).max() {
                self.lsn_coordinator.mark_wal_durable(max_lsn);
            }
        }

        // Clear buffer and update flush time
        self.write_buffer.clear();
        self.last_flush = Instant::now();

        Ok(())
    }

    /// Convert WAL records to Arrow RecordBatch
    fn records_to_arrow_batch(
        &self,
        records: &[WalRecord],
    ) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        let len = records.len();

        // Build arrays for each column
        let lsn_array = UInt64Array::from(records.iter().map(|r| r.lsn.0).collect::<Vec<_>>());
        let timestamp_array = TimestampMicrosecondArray::from(
            records
                .iter()
                .map(|r| r.timestamp as i64)
                .collect::<Vec<_>>(),
        );
        let change_type_array = StringArray::from(
            records
                .iter()
                .map(|r| r.change_type.as_str())
                .collect::<Vec<_>>(),
        );
        let table_id_array =
            UInt32Array::from(records.iter().map(|r| r.table_id).collect::<Vec<_>>());

        // Build key array (nullable)
        let mut key_builder = BinaryBuilder::new();
        for record in records {
            match &record.key {
                Some(key) => key_builder.append_value(key),
                None => key_builder.append_null(),
            }
        }
        let key_array = key_builder.finish();

        // Build data array
        let mut data_builder = BinaryBuilder::new();
        for record in records {
            data_builder.append_value(&record.data);
        }
        let data_array = data_builder.finish();

        // Create RecordBatch
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(lsn_array),
                Arc::new(timestamp_array),
                Arc::new(change_type_array),
                Arc::new(table_id_array),
                Arc::new(key_array),
                Arc::new(data_array),
            ],
        )?;

        Ok(batch)
    }

    /// Roll to new segment
    async fn roll_segment(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Flush any remaining data
        self.flush_buffer().await?;

        // Close current segment (already in segments directory)
        if let Some(segment) = self.current_segment.take() {
            drop(segment.writer); // Close file - it's already in final location
        }

        // Create new segment
        self.create_new_segment()?;

        Ok(())
    }

    /// Create new segment directly in segments directory
    fn create_new_segment(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let next_lsn = LSN(self.lsn_coordinator.assign_lsn().0);
        let filename = format!("{:020}.arrow", next_lsn.0);
        let path = self.config.base_path.join("segments").join(&filename);

        let file = File::create(&path)?;
        let writer = StreamWriter::try_new(file, &self.schema)?;

        self.current_segment = Some(CurrentSegment {
            writer,
            path,
            base_lsn: next_lsn,
            record_count: 0,
            size_bytes: 0,
            created_at: Instant::now(),
        });

        Ok(())
    }

    /// Force flush (for shutdown)
    pub async fn flush(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.flush_buffer().await
    }

    /// Close WAL writer gracefully
    pub async fn close(mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.flush_buffer().await?;

        if let Some(segment) = self.current_segment.take() {
            drop(segment.writer);
        }

        Ok(())
    }

    /// Get current segment info
    pub fn get_current_segment_info(&self) -> Option<(LSN, usize, u64)> {
        self.current_segment
            .as_ref()
            .map(|s| (s.base_lsn, s.record_count, s.size_bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_wal_write_basic() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            base_path: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        let lsn_coordinator = Arc::new(LSNcoordinator::new());
        let mut wal = WalWriter::new(config, lsn_coordinator).unwrap();

        let record = WalRecord {
            lsn: LSN::ZERO, // Will be assigned
            timestamp: 0,   // Will be assigned
            change_type: ChangeType::Insert,
            table_id: 1,
            key: Some(b"user_123".to_vec()),
            data: b"{'name': 'Alice', 'age': 30}".to_vec(),
        };

        let lsn = wal.write_record(record).await.unwrap();
        assert_ne!(lsn, LSN::ZERO);

        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wal_flush_on_batch_size() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            base_path: temp_dir.path().to_path_buf(),
            flush_batch_size: 2, // Small batch for testing
            ..Default::default()
        };

        let lsn_coordinator = Arc::new(LSNcoordinator::new());
        let mut wal = WalWriter::new(config, lsn_coordinator.clone()).unwrap();

        // Write first record - should not flush
        let record1 = WalRecord {
            lsn: LSN::ZERO,
            timestamp: 0,
            change_type: ChangeType::Insert,
            table_id: 1,
            key: Some(b"key1".to_vec()),
            data: b"data1".to_vec(),
        };
        wal.write_record(record1).await.unwrap();
        assert_eq!(lsn_coordinator.get_wal_durable_lsn().0, 0);

        // Write second record - should trigger flush
        let record2: WalRecord = WalRecord {
            lsn: LSN::ZERO,
            timestamp: 0,
            change_type: ChangeType::Insert,
            table_id: 1,
            key: Some(b"key2".to_vec()),
            data: b"data2".to_vec(),
        };
        wal.write_record(record2).await.unwrap();
        assert!(lsn_coordinator.get_wal_durable_lsn().0 > 0);

        wal.close().await.unwrap();
    }
}
