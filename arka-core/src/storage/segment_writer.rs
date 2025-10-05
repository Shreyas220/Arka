use super::config::{DurabilityLevel, SegmentWriterConfig};
use super::segment::{ColumnStats, SegmentId, SegmentMeta};
use arrow::array::*;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs::{create_dir_all, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Writes RecordBatches to Arrow IPC segment files
///
/// ## Purpose
/// The segment writer is responsible for:
/// 1. Writing RecordBatches to disk as Arrow IPC files
/// 2. Extracting metadata (time range, row count, byte size)
/// 3. Generating unique segment IDs
/// 4. Configurable durability (fsync vs buffered)
///
/// ## Durability Modes
/// - **Durable**: Calls fsync after each write (5-10ms latency, 200 writes/sec)
/// - **Buffered**: Skips fsync, relies on OS buffer cache (microseconds, 100k+ writes/sec)
///
/// ## File Format
/// Segments are written as Arrow IPC files which are:
/// - Columnar format (efficient queries)
/// - Zero-copy readable (memory mapping)
/// - Schema-aware (self-describing)
/// - Compressed (configurable)
///
/// ## Naming Convention
/// Files are named: `segment_{id:010}.arrow`
/// Example: segment_0000001000.arrow
///
/// This ensures:
/// - Lexicographic ordering matches creation order
/// - Easy to parse segment ID from filename
/// - Fixed-width for alignment in directory listings
pub struct SegmentWriter {
    /// Base directory for segment files (e.g., "data/segments/")
    base_dir: PathBuf,

    /// Next segment ID to assign
    /// Monotonically increasing, starts from 1
    next_segment_id: AtomicU64,

    /// Writer configuration (durability level, etc.)
    config: SegmentWriterConfig,
}

impl SegmentWriter {
    /// Create a new segment writer with default config (Durable mode)
    ///
    /// # Arguments
    /// * `base_dir` - Directory where segment files will be written
    ///
    /// # Errors
    /// Returns error if directory cannot be created
    pub fn new<P: AsRef<Path>>(base_dir: P) -> Result<Self, SegmentWriterError> {
        Self::with_config(base_dir, SegmentWriterConfig::default())
    }

    /// Create a new segment writer with custom config
    ///
    /// # Arguments
    /// * `base_dir` - Directory where segment files will be written
    /// * `config` - Writer configuration (durability level, etc.)
    ///
    /// # Errors
    /// Returns error if directory cannot be created
    pub fn with_config<P: AsRef<Path>>(
        base_dir: P,
        config: SegmentWriterConfig,
    ) -> Result<Self, SegmentWriterError> {
        let base_dir = base_dir.as_ref().to_path_buf();

        // Create directory if it doesn't exist
        create_dir_all(&base_dir)
            .map_err(|e| SegmentWriterError::DirectoryCreation(base_dir.clone(), e))?;

        Ok(Self {
            base_dir,
            next_segment_id: AtomicU64::new(1),
            config,
        })
    }

    /// Create segment writer and recover from existing segments
    ///
    /// Scans the directory for existing segments and resumes from the highest ID + 1.
    /// Use this on startup to avoid segment ID collisions.
    pub fn with_recovery<P: AsRef<Path>>(base_dir: P) -> Result<Self, SegmentWriterError> {
        Self::with_recovery_and_config(base_dir, SegmentWriterConfig::default())
    }

    /// Create segment writer with recovery and custom config
    pub fn with_recovery_and_config<P: AsRef<Path>>(
        base_dir: P,
        config: SegmentWriterConfig,
    ) -> Result<Self, SegmentWriterError> {
        let base_dir = base_dir.as_ref().to_path_buf();
        create_dir_all(&base_dir)
            .map_err(|e| SegmentWriterError::DirectoryCreation(base_dir.clone(), e))?;

        // Find highest existing segment ID
        let max_id = Self::find_max_segment_id(&base_dir)?;

        Ok(Self {
            base_dir,
            next_segment_id: AtomicU64::new(max_id + 1),
            config,
        })
    }

    /// Write a batch to a new segment
    ///
    /// This is the main entry point. It:
    /// 1. Generates a new segment ID
    /// 2. Writes the RecordBatch to an Arrow IPC file
    /// 3. Extracts metadata (time range, LSN range, row count)
    /// 4. Returns SegmentMeta for index updates
    ///
    /// # Arguments
    /// * `batch` - RecordBatch to write (must have schema with internal columns)
    /// * `lsn_manager` - Optional LSN manager to extract LSN range
    ///
    /// # Returns
    /// SegmentMeta containing all metadata about the written segment
    pub fn write_batch(&self, batch: &RecordBatch) -> Result<SegmentMeta, SegmentWriterError> {
        // Validate batch is not empty
        if batch.num_rows() == 0 {
            return Err(SegmentWriterError::EmptyBatch);
        }

        // Generate segment ID and file path
        let segment_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
        let file_path = self.segment_path(segment_id);

        // Extract metadata before writing
        let meta = self.build_metadata(segment_id, batch, &file_path)?;

        // Write the Arrow IPC file
        self.write_arrow_file(&file_path, batch)?;

        Ok(meta)
    }

    /// Write multiple batches to a single segment
    ///
    /// More efficient than writing batches individually when you have multiple
    /// batches that logically belong together.
    ///
    /// # Arguments
    /// * `batches` - Vec of RecordBatches (must all have same schema)
    /// * `lsn_override` - Optional LSN range to use instead of extracting from batches
    pub fn write_batches(
        &self,
        batches: &[RecordBatch],
    ) -> Result<SegmentMeta, SegmentWriterError> {
        self.write_batches_with_lsn(batches, None)
    }

    /// Write multiple batches with explicit LSN range
    pub fn write_batches_with_lsn(
        &self,
        batches: &[RecordBatch],
        lsn_override: Option<(u64, u64)>,
    ) -> Result<SegmentMeta, SegmentWriterError> {
        if batches.is_empty() {
            return Err(SegmentWriterError::EmptyBatch);
        }

        let segment_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
        let file_path = self.segment_path(segment_id);

        // Aggregate metadata across all batches
        let mut total_rows = 0;
        let mut min_timestamp = i64::MAX;
        let mut max_timestamp = i64::MIN;
        let mut min_lsn = u64::MAX;
        let mut max_lsn = u64::MIN;

        // Column statistics accumulator
        let mut column_stats_accumulators: HashMap<String, GlobalColumnAccumulator> =
            HashMap::new();

        for batch in batches {
            total_rows += batch.num_rows();

            // Extract time range if timestamp column exists
            if let Some((min, max)) = extract_time_range(batch)? {
                min_timestamp = min_timestamp.min(min);
                max_timestamp = max_timestamp.max(max);
            }

            // Extract LSN range if __lsn column exists (unless overridden)
            if lsn_override.is_none() {
                if let Some((min, max)) = extract_lsn_range(batch)? {
                    min_lsn = min_lsn.min(min);
                    max_lsn = max_lsn.max(max);
                }
            }

            // Extract column statistics with adaptive parallelization
            let batch_stats = extract_column_statistics(batch)?;

            // Merge batch statistics into global accumulators
            for (col_name, batch_stat) in batch_stats {
                column_stats_accumulators
                    .entry(col_name)
                    .or_insert_with(|| GlobalColumnAccumulator::new(batch_stat.data_type.clone()))
                    .merge(batch_stat);
            }
        }

        // Use LSN override if provided
        if let Some((min, max)) = lsn_override {
            min_lsn = min;
            max_lsn = max;
        }

        // Write all batches to file
        let file = File::create(&file_path)
            .map_err(|e| SegmentWriterError::FileCreation(file_path.clone(), e))?;

        let mut writer = FileWriter::try_new(file, &batches[0].schema())
            .map_err(SegmentWriterError::ArrowWrite)?;

        for batch in batches {
            writer
                .write(batch)
                .map_err(SegmentWriterError::ArrowWrite)?;
        }

        writer.finish().map_err(SegmentWriterError::ArrowWrite)?;

        // Conditionally fsync based on durability level
        let file = writer
            .into_inner()
            .map_err(SegmentWriterError::ArrowWrite)?;

        match self.config.durability {
            DurabilityLevel::Durable => {
                file.sync_all()
                    .map_err(|e| SegmentWriterError::Fsync(file_path.clone(), e))?;
            }
            DurabilityLevel::Buffered => {
                // Skip fsync
            }
        }

        // Get file size
        let byte_size = std::fs::metadata(&file_path)
            .map_err(|e| SegmentWriterError::FileMetadata(file_path.clone(), e))?
            .len();

        // Finalize column statistics
        let column_stats: HashMap<String, ColumnStats> = column_stats_accumulators
            .into_iter()
            .map(|(name, acc)| {
                let mut stats = acc.finalize();
                stats.column_name = name.clone();
                (name, stats)
            })
            .collect();

        Ok(SegmentMeta {
            id: segment_id,
            file_path,
            row_count: total_rows as u32,
            byte_size,
            time_range: (min_timestamp, max_timestamp),
            lsn_range: (min_lsn, max_lsn),
            created_at: chrono::Utc::now().timestamp(),
            committed_to_iceberg: false,
            column_stats,
        })
    }

    /// Build segment metadata from a RecordBatch
    fn build_metadata(
        &self,
        segment_id: SegmentId,
        batch: &RecordBatch,
        file_path: &Path,
    ) -> Result<SegmentMeta, SegmentWriterError> {
        let row_count = batch.num_rows() as u32;

        // Extract time range (min, max timestamp)
        let time_range = extract_time_range(batch)?.unwrap_or((i64::MAX, i64::MIN));

        // Extract LSN range if __lsn column exists
        let lsn_range = extract_lsn_range(batch)?.unwrap_or((u64::MAX, u64::MIN));

        // Extract column statistics
        let column_stats = extract_column_statistics(batch)?;

        Ok(SegmentMeta {
            id: segment_id,
            file_path: file_path.to_path_buf(),
            row_count,
            byte_size: 0, // Will be updated after writing
            time_range,
            lsn_range,
            created_at: chrono::Utc::now().timestamp(),
            committed_to_iceberg: false,
            column_stats,
        })
    }

    /// Write RecordBatch to Arrow IPC file with optional fsync
    fn write_arrow_file(
        &self,
        file_path: &Path,
        batch: &RecordBatch,
    ) -> Result<(), SegmentWriterError> {
        let file = File::create(file_path)
            .map_err(|e| SegmentWriterError::FileCreation(file_path.to_path_buf(), e))?;

        let mut writer =
            FileWriter::try_new(file, &batch.schema()).map_err(SegmentWriterError::ArrowWrite)?;

        writer
            .write(batch)
            .map_err(SegmentWriterError::ArrowWrite)?;
        writer.finish().map_err(SegmentWriterError::ArrowWrite)?;

        // Conditionally fsync based on durability level
        let file = writer
            .into_inner()
            .map_err(SegmentWriterError::ArrowWrite)?;

        match self.config.durability {
            DurabilityLevel::Durable => {
                // Force fsync to ensure durability (5-10ms)
                file.sync_all()
                    .map_err(|e| SegmentWriterError::Fsync(file_path.to_path_buf(), e))?;
            }
            DurabilityLevel::Buffered => {
                // Skip fsync - rely on OS buffer cache (microseconds)
                // Data will be flushed eventually by OS or background flusher
            }
        }

        Ok(())
    }

    /// Generate segment file path from ID
    fn segment_path(&self, segment_id: SegmentId) -> PathBuf {
        self.base_dir
            .join(format!("segment_{:010}.arrow", segment_id))
    }

    /// Find the highest segment ID in the directory (for recovery)
    fn find_max_segment_id(base_dir: &Path) -> Result<u64, SegmentWriterError> {
        if !base_dir.exists() {
            return Ok(0);
        }

        let mut max_id = 0;

        for entry in std::fs::read_dir(base_dir)
            .map_err(|e| SegmentWriterError::DirectoryRead(base_dir.to_path_buf(), e))?
        {
            let entry =
                entry.map_err(|e| SegmentWriterError::DirectoryRead(base_dir.to_path_buf(), e))?;
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            // Parse segment ID from filename: segment_{id}.arrow
            if filename_str.starts_with("segment_") && filename_str.ends_with(".arrow") {
                let id_str = &filename_str[8..filename_str.len() - 6]; // Extract ID part
                if let Ok(id) = id_str.parse::<u64>() {
                    max_id = max_id.max(id);
                }
            }
        }

        Ok(max_id)
    }

    /// Get current segment ID (next to be assigned)
    pub fn current_segment_id(&self) -> u64 {
        self.next_segment_id.load(Ordering::SeqCst)
    }
}

/// Extract time range from RecordBatch
///
/// Looks for columns named "timestamp", "__timestamp", or "ts" and computes min/max.
/// Returns None if no timestamp column found.
fn extract_time_range(batch: &RecordBatch) -> Result<Option<(i64, i64)>, SegmentWriterError> {
    use arrow::array::*;
    use arrow::datatypes::*;

    // Try different timestamp column names
    let timestamp_col = batch
        .column_by_name("timestamp")
        .or_else(|| batch.column_by_name("__timestamp"))
        .or_else(|| batch.column_by_name("ts"));

    if let Some(col) = timestamp_col {
        // Handle different timestamp types
        let (min, max) = match col.data_type() {
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                let array = col
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                let min = arrow::compute::min(array).unwrap_or(i64::MAX);
                let max = arrow::compute::max(array).unwrap_or(i64::MIN);
                (min, max)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                let array = col
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let min = arrow::compute::min(array).unwrap_or(i64::MAX);
                let max = arrow::compute::max(array).unwrap_or(i64::MIN);
                (min, max)
            }
            DataType::Int64 => {
                let array = col.as_any().downcast_ref::<Int64Array>().unwrap();
                let min = arrow::compute::min(array).unwrap_or(i64::MAX);
                let max = arrow::compute::max(array).unwrap_or(i64::MIN);
                (min, max)
            }
            _ => return Ok(None),
        };

        Ok(Some((min, max)))
    } else {
        Ok(None)
    }
}

/// Extract LSN range from RecordBatch
///
/// Looks for "__lsn" column and computes min/max.
/// Returns None if no LSN column found.
fn extract_lsn_range(batch: &RecordBatch) -> Result<Option<(u64, u64)>, SegmentWriterError> {
    use arrow::array::*;

    if let Some(col) = batch.column_by_name("__lsn") {
        if let Some(array) = col.as_any().downcast_ref::<UInt64Array>() {
            let min = arrow::compute::min(array).unwrap_or(u64::MAX);
            let max = arrow::compute::max(array).unwrap_or(u64::MIN);
            return Ok(Some((min, max)));
        }
    }
    Ok(None)
}

/// Extract column statistics from a RecordBatch with adaptive parallelization
fn extract_column_statistics(
    batch: &RecordBatch,
) -> Result<HashMap<String, ColumnStats>, SegmentWriterError> {
    let schema = batch.schema();
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| is_comparable_type(f.data_type()))
        .collect();

    // Adaptive parallelization: use Rayon if workload is large enough
    let should_parallelize = fields.len() > 5 || batch.num_rows() > 10_000;

    if should_parallelize {
        // Parallel extraction for wide tables or large batches
        Ok(fields
            .par_iter()
            .filter_map(|(idx, field)| {
                let array = batch.column(*idx);
                extract_stats_for_column(field, array.as_ref())
                    .ok()
                    .map(|stats| (field.name().clone(), stats))
            })
            .collect())
    } else {
        // Sequential extraction for small batches (avoid thread overhead)
        Ok(fields
            .iter()
            .filter_map(|(idx, field)| {
                let array = batch.column(*idx);
                extract_stats_for_column(field, array.as_ref())
                    .ok()
                    .map(|stats| (field.name().clone(), stats))
            })
            .collect())
    }
}

/// Check if a data type supports min/max statistics
/// Only numeric and temporal types are useful for range pruning
fn is_comparable_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    )
}

/// Extract statistics for a single column
fn extract_stats_for_column(
    field: &arrow::datatypes::Field,
    array: &dyn arrow::array::Array,
) -> Result<ColumnStats, SegmentWriterError> {
    let (min_value, max_value) = extract_min_max_for_type(array, field.data_type())?;

    Ok(ColumnStats {
        column_name: field.name().clone(),
        data_type: field.data_type().clone(),
        min_value,
        max_value,
        null_count: array.null_count(),
        value_count: array.len(),
    })
}

/// Dispatch to type-specific min/max extraction
fn extract_min_max_for_type(
    array: &dyn arrow::array::Array,
    data_type: &DataType,
) -> Result<(Option<Vec<u8>>, Option<Vec<u8>>), SegmentWriterError> {
    match data_type {
        DataType::Int8 => extract_primitive_min_max::<arrow::datatypes::Int8Type>(array),
        DataType::Int16 => extract_primitive_min_max::<arrow::datatypes::Int16Type>(array),
        DataType::Int32 => extract_primitive_min_max::<arrow::datatypes::Int32Type>(array),
        DataType::Int64 => extract_primitive_min_max::<arrow::datatypes::Int64Type>(array),
        DataType::UInt8 => extract_primitive_min_max::<arrow::datatypes::UInt8Type>(array),
        DataType::UInt16 => extract_primitive_min_max::<arrow::datatypes::UInt16Type>(array),
        DataType::UInt32 => extract_primitive_min_max::<arrow::datatypes::UInt32Type>(array),
        DataType::UInt64 => extract_primitive_min_max::<arrow::datatypes::UInt64Type>(array),
        DataType::Float32 => extract_float_min_max::<arrow::datatypes::Float32Type>(array),
        DataType::Float64 => extract_float_min_max::<arrow::datatypes::Float64Type>(array),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            extract_primitive_min_max::<arrow::datatypes::TimestampNanosecondType>(array)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            extract_primitive_min_max::<arrow::datatypes::TimestampMicrosecondType>(array)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            extract_primitive_min_max::<arrow::datatypes::TimestampMillisecondType>(array)
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            extract_primitive_min_max::<arrow::datatypes::TimestampSecondType>(array)
        }
        DataType::Date32 => extract_primitive_min_max::<arrow::datatypes::Date32Type>(array),
        DataType::Date64 => extract_primitive_min_max::<arrow::datatypes::Date64Type>(array),
        _ => Ok((None, None)), // Unsupported type
    }
}

/// Extract min/max for primitive types using BIG-ENDIAN encoding for correct byte comparison
fn extract_primitive_min_max<T>(
    array: &dyn arrow::array::Array,
) -> Result<(Option<Vec<u8>>, Option<Vec<u8>>), SegmentWriterError>
where
    T: arrow::datatypes::ArrowPrimitiveType,
    T::Native: std::cmp::Ord,
{
    let primitive_array = array
        .as_any()
        .downcast_ref::<arrow::array::PrimitiveArray<T>>()
        .ok_or(SegmentWriterError::InvalidArrayType)?;

    if primitive_array.null_count() == primitive_array.len() {
        return Ok((None, None)); // All nulls
    }

    // Use Rayon for large arrays
    if primitive_array.len() > 10_000 {
        let (min_val, max_val) = (0..primitive_array.len())
            .into_par_iter()
            .filter(|&i| primitive_array.is_valid(i))
            .map(|i| {
                let v = primitive_array.value(i);
                (v, v)
            })
            .reduce(
                || (T::Native::default(), T::Native::default()),
                |(min1, max1), (min2, max2)| {
                    (
                        if min1 < min2 { min1 } else { min2 },
                        if max1 > max2 { max1 } else { max2 },
                    )
                },
            );

        return Ok((
            Some(value_to_be_bytes(&min_val)),
            Some(value_to_be_bytes(&max_val)),
        ));
    }

    // Sequential for small arrays
    let mut min = None;
    let mut max = None;

    for i in 0..primitive_array.len() {
        if primitive_array.is_valid(i) {
            let val = primitive_array.value(i);
            min = Some(min.map_or(val, |m: T::Native| m.min(val)));
            max = Some(max.map_or(val, |m: T::Native| m.max(val)));
        }
    }

    Ok((
        min.map(|v| value_to_be_bytes(&v)),
        max.map(|v| value_to_be_bytes(&v)),
    ))
}

/// Convert a native type to big-endian bytes
fn value_to_be_bytes<T: Copy>(value: &T) -> Vec<u8> {
    let size = std::mem::size_of::<T>();
    let ptr = value as *const T as *const u8;
    let bytes = unsafe { std::slice::from_raw_parts(ptr, size) };

    // Convert to big-endian if necessary
    let mut result = bytes.to_vec();
    if cfg!(target_endian = "little") {
        result.reverse();
    }
    result
}

/// Extract min/max for float types (handles NaN, uses BIG-ENDIAN)
fn extract_float_min_max<T>(
    array: &dyn arrow::array::Array,
) -> Result<(Option<Vec<u8>>, Option<Vec<u8>>), SegmentWriterError>
where
    T: arrow::datatypes::ArrowPrimitiveType,
    T::Native: num::Float + Copy,
{
    use num::Float;
    let float_array = array
        .as_any()
        .downcast_ref::<arrow::array::PrimitiveArray<T>>()
        .ok_or(SegmentWriterError::InvalidArrayType)?;

    if float_array.null_count() == float_array.len() {
        return Ok((None, None));
    }

    let mut min = None;
    let mut max = None;

    for i in 0..float_array.len() {
        if float_array.is_valid(i) {
            let val = float_array.value(i);
            if !val.is_nan() {
                min = Some(min.map_or(val, |m: T::Native| if val < m { val } else { m }));
                max = Some(max.map_or(val, |m: T::Native| if val > m { val } else { m }));
            }
        }
    }

    Ok((
        min.map(|v| value_to_be_bytes(&v)),
        max.map(|v| value_to_be_bytes(&v)),
    ))
}

/// Accumulator for merging statistics across batches
struct GlobalColumnAccumulator {
    data_type: DataType,
    min_value: Option<Vec<u8>>,
    max_value: Option<Vec<u8>>,
    null_count: usize,
    value_count: usize,
}

impl GlobalColumnAccumulator {
    fn new(data_type: DataType) -> Self {
        Self {
            data_type,
            min_value: None,
            max_value: None,
            null_count: 0,
            value_count: 0,
        }
    }

    fn merge(&mut self, batch_stats: ColumnStats) {
        // Update min
        if let Some(batch_min) = batch_stats.min_value {
            self.min_value = Some(match &self.min_value {
                Some(current_min) if current_min.as_slice() <= batch_min.as_slice() => {
                    current_min.clone()
                }
                _ => batch_min,
            });
        }

        // Update max
        if let Some(batch_max) = batch_stats.max_value {
            self.max_value = Some(match &self.max_value {
                Some(current_max) if current_max.as_slice() >= batch_max.as_slice() => {
                    current_max.clone()
                }
                _ => batch_max,
            });
        }

        self.null_count += batch_stats.null_count;
        self.value_count += batch_stats.value_count;
    }

    fn finalize(self) -> ColumnStats {
        ColumnStats {
            column_name: String::new(), // Set by caller
            data_type: self.data_type,
            min_value: self.min_value,
            max_value: self.max_value,
            null_count: self.null_count,
            value_count: self.value_count,
        }
    }
}

/// Errors that can occur during segment writing
#[derive(Debug, thiserror::Error)]
pub enum SegmentWriterError {
    #[error("Failed to create directory {0}: {1}")]
    DirectoryCreation(PathBuf, std::io::Error),

    #[error("Failed to read directory {0}: {1}")]
    DirectoryRead(PathBuf, std::io::Error),

    #[error("Failed to create file {0}: {1}")]
    FileCreation(PathBuf, std::io::Error),

    #[error("Failed to read file metadata {0}: {1}")]
    FileMetadata(PathBuf, std::io::Error),

    #[error("Arrow write error: {0}")]
    ArrowWrite(#[from] arrow::error::ArrowError),

    #[error("Failed to fsync file {0}: {1}")]
    Fsync(PathBuf, std::io::Error),

    #[error("Cannot write empty batch")]
    EmptyBatch,

    #[error("Invalid array type for statistics extraction")]
    InvalidArrayType,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("__lsn", DataType::UInt64, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("user_id", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ]));

        let lsn_array = arrow::array::UInt64Array::from((1..=num_rows as u64).collect::<Vec<_>>());
        let ts_array =
            TimestampMicrosecondArray::from((1000..1000 + num_rows as i64).collect::<Vec<_>>());
        let user_array = StringArray::from(
            (0..num_rows)
                .map(|i| format!("user_{}", i))
                .collect::<Vec<_>>(),
        );
        let amount_array =
            Int64Array::from((0..num_rows as i64).map(|i| i * 100).collect::<Vec<_>>());

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(lsn_array),
                Arc::new(ts_array),
                Arc::new(user_array),
                Arc::new(amount_array),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_basic_write() {
        let temp_dir = TempDir::new().unwrap();
        let writer = SegmentWriter::new(temp_dir.path()).unwrap();

        let batch = create_test_batch(100);
        let meta = writer.write_batch(&batch).unwrap();

        assert_eq!(meta.id, 1);
        assert_eq!(meta.row_count, 100);
        assert!(meta.file_path.exists());
        assert_eq!(meta.time_range, (1000, 1099));
        assert_eq!(meta.lsn_range, (1, 100));
    }

    #[test]
    fn test_multiple_writes() {
        let temp_dir = TempDir::new().unwrap();
        let writer = SegmentWriter::new(temp_dir.path()).unwrap();

        let batch1 = create_test_batch(50);
        let meta1 = writer.write_batch(&batch1).unwrap();
        assert_eq!(meta1.id, 1);

        let batch2 = create_test_batch(75);
        let meta2 = writer.write_batch(&batch2).unwrap();
        assert_eq!(meta2.id, 2);

        assert!(meta1.file_path.exists());
        assert!(meta2.file_path.exists());
    }

    #[test]
    fn test_recovery() {
        let temp_dir = TempDir::new().unwrap();

        // Write some segments
        {
            let writer = SegmentWriter::new(temp_dir.path()).unwrap();
            let batch = create_test_batch(100);
            writer.write_batch(&batch).unwrap();
            writer.write_batch(&batch).unwrap();
            writer.write_batch(&batch).unwrap();
        }

        // Recover and verify we resume from correct ID
        let writer = SegmentWriter::with_recovery(temp_dir.path()).unwrap();
        assert_eq!(writer.current_segment_id(), 4); // Should be 3 + 1

        let batch = create_test_batch(100);
        let meta = writer.write_batch(&batch).unwrap();
        assert_eq!(meta.id, 4);
    }

    #[test]
    fn test_write_batches() {
        let temp_dir = TempDir::new().unwrap();
        let writer = SegmentWriter::new(temp_dir.path()).unwrap();

        let batches = vec![
            create_test_batch(50),
            create_test_batch(75),
            create_test_batch(100),
        ];

        let meta = writer.write_batches(&batches).unwrap();
        assert_eq!(meta.id, 1);
        assert_eq!(meta.row_count, 225); // 50 + 75 + 100
        assert!(meta.file_path.exists());
    }
}
