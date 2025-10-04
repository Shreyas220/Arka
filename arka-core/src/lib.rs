use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{Duration, Instant};
use tokio::sync::watch;
// pub mod lsn;

// New modular architecture
pub mod catalog;
pub mod flight;
pub mod indexes;
pub mod storage;

#[derive(Debug, Clone)]
pub enum ChangeOp {
    Delete {
        key: Vec<u8>,
        old_value_lsn: u64, // LSN of the value being deleted
    },
    Update {
        key: Vec<u8>,
        old_value_lsn: u64, // LSN of the value being updated
        old_value: Vec<u8>,
        new_value: Vec<u8>,
    },
}

// LSN management is now handled by storage::LSNManager
// (removed ArkaLsnGenerator - use storage::LSNManager instead)

// Visibility LSN tracking for read consistency
#[derive(Debug, Clone, Copy)]
pub struct ArkaVisibilityLsn {
    pub commit_lsn: u64, // Last committed LSN visible to reads
    pub write_lsn: u64,  // Last written LSN (may be uncommitted)
}

// Optimized hash function (from Moonlink)
pub fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

// Fast hash for binary keys (general purpose)
pub fn hash_key(key: &[u8]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    splitmix64(hasher.finish())
}

// Type-aware hash function that matches indexing logic
pub fn hash_key_typed(key: &[u8], data_type: &arrow::datatypes::DataType) -> u64 {
    match data_type {
        arrow::datatypes::DataType::Int64 => {
            // For Int64, parse bytes as i64 and use splitmix64 directly (same as indexing)
            if key.len() == 8 {
                let value = i64::from_le_bytes([
                    key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
                ]);
                splitmix64(value as u64)
            } else {
                // Fallback to general hash
                hash_key(key)
            }
        }
        arrow::datatypes::DataType::UInt64 => {
            // For UInt64, parse bytes as u64 and use splitmix64 directly (same as indexing)
            if key.len() == 8 {
                let value = u64::from_le_bytes([
                    key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
                ]);
                splitmix64(value)
            } else {
                // Fallback to general hash
                hash_key(key)
            }
        }
        arrow::datatypes::DataType::Binary | arrow::datatypes::DataType::Utf8 => {
            // For Binary and Utf8, use general hash_key (same as indexing)
            hash_key(key)
        }
        _ => {
            // For all other types, use general hash (same as indexing fallback)
            hash_key(key)
        }
    }
}

// Location of a row within a batch for fast CDC lookups
#[derive(Debug, Clone)]
pub struct BatchLocation {
    pub batch_id: u64,
    pub row_index: u32,
    pub lsn: u64,
}

// Production-level index entry (space optimized)
#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub key_hash: u64, // 8 bytes instead of full key (major space saving)
    pub location: BatchLocation,
}

// Index configuration for different table types
#[derive(Debug, Clone)]
pub enum IndexType {
    None,                   // Append-only tables
    SinglePrimitive(usize), // Single column primary key (column index)
    Composite(Vec<usize>),  // Multi-column primary key (column indices)
}

// Production index system
#[derive(Debug)]
pub struct ArkMemorySliceIndex {
    pub index_type: IndexType,

    // Hash-based index
    hash_to_location: DashMap<u64, BatchLocation>,

    // Bloom filter for negative lookups
    pub bloom_filter: BloomFilter,

    // Statistics
    total_entries: AtomicUsize,
    hash_collisions: AtomicUsize,
}

// Simple Bloom filter implementation
#[derive(Debug)]
pub struct BloomFilter {
    bits: Vec<AtomicU64>,
    num_hash_functions: usize,
    size_bits: usize,
}

impl BloomFilter {
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let size_bits = (-(expected_items as f64 * false_positive_rate.ln())
            / (2.0_f64.ln().powi(2)))
        .ceil() as usize;
        let num_hash_functions =
            ((size_bits as f64 / expected_items as f64) * 2.0_f64.ln()).round() as usize;

        Self {
            bits: (0..((size_bits + 63) / 64))
                .map(|_| AtomicU64::new(0))
                .collect(),
            num_hash_functions,
            size_bits,
        }
    }

    pub fn insert(&self, hash: u64) {
        for i in 0..self.num_hash_functions {
            let bit_pos =
                (splitmix64(hash.wrapping_add(i as u64)) % self.size_bits as u64) as usize;
            let word_idx = bit_pos / 64;
            let bit_offset = bit_pos % 64;

            if word_idx < self.bits.len() {
                self.bits[word_idx].fetch_or(1u64 << bit_offset, Ordering::Relaxed);
            }
        }
    }

    pub fn might_contain(&self, hash: u64) -> bool {
        for i in 0..self.num_hash_functions {
            let bit_pos =
                (splitmix64(hash.wrapping_add(i as u64)) % self.size_bits as u64) as usize;
            let word_idx = bit_pos / 64;
            let bit_offset = bit_pos % 64;

            if word_idx >= self.bits.len() {
                return false;
            }

            if (self.bits[word_idx].load(Ordering::Relaxed) & (1u64 << bit_offset)) == 0 {
                return false;
            }
        }
        true
    }
}

impl ArkMemorySliceIndex {
    pub fn new(index_type: IndexType, expected_items: usize) -> Self {
        Self {
            index_type,
            hash_to_location: DashMap::new(),
            bloom_filter: BloomFilter::new(expected_items, 0.01), // 1% false positive rate
            total_entries: AtomicUsize::new(0),
            hash_collisions: AtomicUsize::new(0),
        }
    }

    pub fn insert(&self, key_hash: u64, location: BatchLocation) {
        // Update bloom filter
        self.bloom_filter.insert(key_hash);

        // Check for hash collision
        if self.hash_to_location.contains_key(&key_hash) {
            self.hash_collisions.fetch_add(1, Ordering::Relaxed);
        }

        // Insert into main index
        self.hash_to_location.insert(key_hash, location);
        self.total_entries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn lookup(&self, key_hash: u64) -> Option<BatchLocation> {
        // Fast negative lookup using bloom filter
        if !self.bloom_filter.might_contain(key_hash) {
            return None;
        }

        // Actual lookup
        self.hash_to_location
            .get(&key_hash)
            .map(|entry| entry.value().clone())
    }

    pub fn remove(&self, key_hash: u64) -> Option<BatchLocation> {
        if let Some((_, location)) = self.hash_to_location.remove(&key_hash) {
            self.total_entries.fetch_sub(1, Ordering::Relaxed);
            Some(location)
        } else {
            None
        }
    }

    pub fn stats(&self) -> IndexStats {
        IndexStats {
            total_entries: self.total_entries.load(Ordering::Relaxed),
            hash_collisions: self.hash_collisions.load(Ordering::Relaxed),
            memory_usage_bytes: self.estimate_memory_usage(),
        }
    }

    fn estimate_memory_usage(&self) -> usize {
        let entries = self.total_entries.load(Ordering::Relaxed);
        // Each entry: u64 (8) + BatchLocation (24) + DashMap overhead (~16)
        let index_size = entries * 48;

        // Bloom filter size
        let bloom_size = self.bloom_filter.bits.len() * 8;

        index_size + bloom_size
    }
}

#[derive(Debug)]
pub struct IndexStats {
    pub total_entries: usize,
    pub hash_collisions: usize,
    pub memory_usage_bytes: usize,
}

// Pending changes for disk propagation
#[derive(Debug, Clone)]
pub struct PendingChange {
    pub change_lsn: u64,
    pub target_data_lsn: u64,
    pub operation: ChangeOp,
}

// Memory batch with LSN tracking
#[derive(Debug)]
pub struct MemoryBatch {
    pub data: RecordBatch,
    pub lsn: u64,
    pub batch_id: u64,
    pub created_at: Instant,
    // Deletion vector for this batch
    pub deletions: DeletionVector,
}

// Deletion vector implementation
#[derive(Debug, Clone)]
pub struct DeletionVector {
    pub batch_id: u64,
    pub deleted_rows: Vec<u32>, // Row indices that are deleted
    pub delete_lsn: u64,        // LSN when deletion occurred
}

impl DeletionVector {
    pub fn new(batch_id: u64) -> Self {
        Self {
            batch_id,
            deleted_rows: Vec::new(),
            delete_lsn: 0,
        }
    }

    pub fn mark_deleted(&mut self, row_index: u32, lsn: u64) {
        if !self.deleted_rows.contains(&row_index) {
            self.deleted_rows.push(row_index);
            self.delete_lsn = lsn;
        }
    }

    pub fn is_deleted(&self, row_index: u32) -> bool {
        self.deleted_rows.contains(&row_index)
    }
}

// Enhanced Memory Buffer with LSN-based ordering
pub struct MemorySlice {
    //append only
    append_only: bool,
    // Ordered batches by LSN
    batches: RwLock<Vec<MemoryBatch>>,

    // LSN tracking using storage::LSNManager
    lsn_manager: Arc<storage::LSNManager>,
    latest_commit_lsn: AtomicU64,
    latest_write_lsn: AtomicU64,

    // Batch management
    next_batch_id: AtomicU64,

    // Metadata
    row_count: AtomicUsize,
    byte_size: AtomicUsize,

    oldest_timestamp: RwLock<Option<i64>>,
    newest_timestamp: RwLock<Option<i64>>,

    // Flushing
    last_flush_time: RwLock<Instant>,

    // Visibility for reads
    pub visibility_lsn_tx: watch::Sender<ArkaVisibilityLsn>,

    // NEW: Production-level index system
    pub memory_slice_index: Arc<ArkMemorySliceIndex>,

    // NEW: Track CDC changes for disk propagation
    pending_disk_changes: RwLock<Vec<PendingChange>>,
}

impl MemorySlice {
    pub fn new(lsn_manager: Arc<storage::LSNManager>, config: &ArkConfig) -> Self {
        let (visibility_tx, _) = watch::channel(ArkaVisibilityLsn {
            commit_lsn: 0,
            write_lsn: 0,
        });

        // Determine index type based on configuration
        let index_type = if config.append_only
            || (!config.enable_point_lookups && !config.enable_cdc)
            || config.primary_key_columns.is_none()
        {
            IndexType::None
        } else if let Some(ref primary_cols) = config.primary_key_columns {
            if primary_cols.len() == 1 {
                IndexType::SinglePrimitive(primary_cols[0])
            } else {
                IndexType::Composite(primary_cols.clone())
            }
        } else {
            IndexType::None
        };

        Self {
            append_only: config.append_only,
            batches: RwLock::new(Vec::new()),
            lsn_manager,
            latest_commit_lsn: AtomicU64::new(0),
            latest_write_lsn: AtomicU64::new(0),
            next_batch_id: AtomicU64::new(1),
            row_count: AtomicUsize::new(0),
            byte_size: AtomicUsize::new(0),
            oldest_timestamp: RwLock::new(None),
            newest_timestamp: RwLock::new(None),
            last_flush_time: RwLock::new(Instant::now()),
            visibility_lsn_tx: visibility_tx,

            // Initialize index based on configuration
            memory_slice_index: Arc::new(ArkMemorySliceIndex::new(index_type, 1_000_000)), // Expect 1M items
            pending_disk_changes: RwLock::new(Vec::new()),
        }
    }

    // Append a new batch and build key index
    pub async fn append_batch(&self, batch: RecordBatch) -> Result<u64, String> {
        let lsn = self.lsn_manager.assign();
        let batch_id = self.next_batch_id.fetch_add(1, Ordering::SeqCst);

        self.build_key_index_for_batch(&batch, batch_id, lsn).await;

        let memory_batch = MemoryBatch {
            data: batch.clone(),
            lsn,
            batch_id,
            created_at: Instant::now(),
            deletions: DeletionVector::new(batch_id),
        };

        // Update metadata
        self.row_count.fetch_add(batch.num_rows(), Ordering::SeqCst);
        self.byte_size
            .fetch_add(batch.num_rows() * 100, Ordering::SeqCst);

        // Add to batches in LSN order
        {
            let mut batches = self.batches.write().unwrap();
            batches.push(memory_batch);
            batches.sort_by_key(|b| b.lsn);
        }

        self.latest_write_lsn.store(lsn, Ordering::SeqCst);
        Ok(lsn)
    }

    // Production-level key index builder with configurable key extraction
    async fn build_key_index_for_batch(&self, batch: &RecordBatch, batch_id: u64, batch_lsn: u64) {
        // Skip indexing for append-only tables
        if matches!(self.memory_slice_index.index_type, IndexType::None) {
            return;
        }

        match &self.memory_slice_index.index_type {
            IndexType::SinglePrimitive(column_idx) => {
                self.build_single_primitive_index(batch, *column_idx, batch_id, batch_lsn)
                    .await;
            }
            IndexType::Composite(column_indices) => {
                self.build_composite_index(batch, column_indices, batch_id, batch_lsn)
                    .await;
            }
            IndexType::None => {} // Already handled above
        }
    }

    async fn build_single_primitive_index(
        &self,
        batch: &RecordBatch,
        column_idx: usize,
        batch_id: u64,
        batch_lsn: u64,
    ) {
        if column_idx >= batch.num_columns() {
            return;
        }

        let column = batch.column(column_idx);

        // Handle different Arrow data types
        match column.data_type() {
            arrow::datatypes::DataType::Binary => {
                if let Some(binary_array) =
                    column.as_any().downcast_ref::<arrow::array::BinaryArray>()
                {
                    for (row_index, key_bytes) in binary_array.iter().enumerate() {
                        if let Some(key) = key_bytes {
                            let key_hash = hash_key(key);
                            let location = BatchLocation {
                                batch_id,
                                row_index: row_index as u32,
                                lsn: batch_lsn,
                            };

                            self.memory_slice_index.insert(key_hash, location);
                        }
                    }
                }
            }
            arrow::datatypes::DataType::Utf8 => {
                if let Some(string_array) =
                    column.as_any().downcast_ref::<arrow::array::StringArray>()
                {
                    for (row_index, key_str) in string_array.iter().enumerate() {
                        if let Some(key) = key_str {
                            let key_hash = hash_key(key.as_bytes());
                            let location = BatchLocation {
                                batch_id,
                                row_index: row_index as u32,
                                lsn: batch_lsn,
                            };

                            self.memory_slice_index.insert(key_hash, location);
                        }
                    }
                }
            }
            arrow::datatypes::DataType::Int64 => {
                if let Some(int64_array) =
                    column.as_any().downcast_ref::<arrow::array::Int64Array>()
                {
                    for (row_index, key_value) in int64_array.iter().enumerate() {
                        if let Some(key) = key_value {
                            let key_hash = splitmix64(key as u64);
                            let location = BatchLocation {
                                batch_id,
                                row_index: row_index as u32,
                                lsn: batch_lsn,
                            };

                            self.memory_slice_index.insert(key_hash, location);
                        }
                    }
                }
            }
            arrow::datatypes::DataType::UInt64 => {
                if let Some(uint64_array) =
                    column.as_any().downcast_ref::<arrow::array::UInt64Array>()
                {
                    for (row_index, key_value) in uint64_array.iter().enumerate() {
                        if let Some(key) = key_value {
                            let key_hash = splitmix64(key);
                            let location = BatchLocation {
                                batch_id,
                                row_index: row_index as u32,
                                lsn: batch_lsn,
                            };

                            self.memory_slice_index.insert(key_hash, location);
                        }
                    }
                }
            }
            _ => {
                // Fallback: convert to string and hash
                // This is slower but handles any data type
                for row_index in 0..batch.num_rows() {
                    let value_str = format!("{:?}", column.slice(row_index, 1));
                    let key_hash = hash_key(value_str.as_bytes());
                    let location = BatchLocation {
                        batch_id,
                        row_index: row_index as u32,
                        lsn: batch_lsn,
                    };

                    self.memory_slice_index.insert(key_hash, location);
                }
            }
        }
    }

    async fn build_composite_index(
        &self,
        batch: &RecordBatch,
        column_indices: &[usize],
        batch_id: u64,
        batch_lsn: u64,
    ) {
        for row_index in 0..batch.num_rows() {
            let mut composite_key = Vec::new();

            // Build composite key from multiple columns
            for &col_idx in column_indices {
                if col_idx < batch.num_columns() {
                    let column = batch.column(col_idx);
                    let value_str = format!("{:?}", column.slice(row_index, 1));
                    composite_key.extend_from_slice(value_str.as_bytes());
                    composite_key.push(b'|'); // Separator
                }
            }

            if !composite_key.is_empty() {
                composite_key.pop(); // Remove last separator

                let key_hash = hash_key(&composite_key);
                let location = BatchLocation {
                    batch_id,
                    row_index: row_index as u32,
                    lsn: batch_lsn,
                };

                self.memory_slice_index.insert(key_hash, location);
            }
        }
    }

    // Commit up to a specific LSN (makes data visible to readers)
    pub async fn commit(&self, commit_lsn: u64) -> Result<(), String> {
        let current_commit = self.latest_commit_lsn.load(Ordering::SeqCst);
        if commit_lsn <= current_commit {
            return Ok(()); // Already committed
        }

        self.latest_commit_lsn.store(commit_lsn, Ordering::SeqCst);

        // Update visibility for readers
        let visibility = ArkaVisibilityLsn {
            commit_lsn,
            write_lsn: self.latest_write_lsn.load(Ordering::SeqCst),
        };

        let _ = self.visibility_lsn_tx.send(visibility);
        Ok(())
    }

    // Get read snapshot with deletion vectors applied
    pub async fn get_read_snapshot(&self, as_of_lsn: Option<u64>) -> Vec<RecordBatch> {
        let effective_lsn =
            as_of_lsn.unwrap_or_else(|| self.latest_commit_lsn.load(Ordering::SeqCst));

        let batches = self.batches.read().unwrap();
        let mut results = Vec::new();

        for batch in batches.iter().filter(|batch| batch.lsn <= effective_lsn) {
            // Apply deletion vectors if they exist and are visible at effective_lsn
            if batch.deletions.delete_lsn > 0 && batch.deletions.delete_lsn <= effective_lsn {
                let filtered_batch = self.apply_deletion_vector(&batch.data, &batch.deletions);
                results.push(filtered_batch);
            } else {
                // No deletions to apply
                results.push(batch.data.clone());
            }
        }

        results
    }

    // Apply deletion vector to filter out deleted rows
    fn apply_deletion_vector(
        &self,
        batch: &RecordBatch,
        deletions: &DeletionVector,
    ) -> RecordBatch {
        if deletions.deleted_rows.is_empty() {
            return batch.clone();
        }

        // Create boolean mask: true = keep, false = delete
        let mut keep_mask = vec![true; batch.num_rows()];
        for &deleted_idx in &deletions.deleted_rows {
            if (deleted_idx as usize) < batch.num_rows() {
                keep_mask[deleted_idx as usize] = false;
            }
        }

        let mask_array = arrow::array::BooleanArray::from(keep_mask);

        let filtered_columns: Result<
            Vec<std::sync::Arc<dyn arrow::array::Array>>,
            arrow::error::ArrowError,
        > = batch
            .columns()
            .iter()
            .map(|column| arrow::compute::filter(column, &mask_array))
            .collect();

        match filtered_columns {
            Ok(columns) => {
                match RecordBatch::try_new(batch.schema(), columns) {
                    Ok(filtered_batch) => filtered_batch,
                    Err(_e) => batch.clone(), // Fallback on error
                }
            }
            Err(_e) => batch.clone(), // Fallback on error
        }
    }

    // Production CDC delete with hash-based lookup - FIXED with type-aware hashing
    pub async fn apply_cdc_delete(
        &self,
        key: &[u8],
        key_column_type: &arrow::datatypes::DataType,
        target_lsn: u64,
    ) -> Result<u64, String> {
        let change_lsn = self.lsn_manager.assign();
        let key_hash = hash_key_typed(key, key_column_type);

        // Fast O(1) hash lookup with bloom filter optimization
        if let Some(location) = self.memory_slice_index.lookup(key_hash) {
            if location.lsn == target_lsn {
                // Apply to specific batch and row
                let mut batches = self.batches.write().unwrap();
                if let Some(batch) = batches.iter_mut().find(|b| b.batch_id == location.batch_id) {
                    batch.deletions.mark_deleted(location.row_index, change_lsn);

                    // Remove from index (the key no longer exists)
                    self.memory_slice_index.remove(key_hash);

                    // Track for disk propagation
                    let pending_change = PendingChange {
                        change_lsn,
                        target_data_lsn: target_lsn,
                        operation: ChangeOp::Delete {
                            key: key.to_vec(),
                            old_value_lsn: target_lsn,
                        },
                    };
                    self.pending_disk_changes
                        .write()
                        .unwrap()
                        .push(pending_change);

                    self.latest_write_lsn.store(change_lsn, Ordering::SeqCst);
                    return Ok(change_lsn);
                }
            }
        }

        Err("Key not found at target LSN".to_string())
    }

    // Get production index statistics
    pub fn get_index_stats(&self) -> IndexStats {
        self.memory_slice_index.stats()
    }

    // Check if a key might exist (bloom filter check) - FIXED with type-aware hashing
    pub fn might_contain_key(
        &self,
        key: &[u8],
        key_column_type: &arrow::datatypes::DataType,
    ) -> bool {
        let key_hash = hash_key_typed(key, key_column_type);
        self.memory_slice_index.bloom_filter.might_contain(key_hash)
    }

    // Production-level point lookup - FIXED with type-aware hashing
    pub async fn lookup_key(
        &self,
        key: &[u8],
        key_column_type: &arrow::datatypes::DataType,
    ) -> Option<BatchLocation> {
        let key_hash = hash_key_typed(key, key_column_type);
        self.memory_slice_index.lookup(key_hash)
    }

    // Convenience method to get primary key column data type
    pub fn get_primary_key_type(
        &self,
        schema: &arrow::datatypes::Schema,
    ) -> Option<arrow::datatypes::DataType> {
        match &self.memory_slice_index.index_type {
            IndexType::SinglePrimitive(column_idx) => schema
                .fields()
                .get(*column_idx)
                .map(|field| field.data_type().clone()),
            IndexType::Composite(column_indices) => {
                // For composite keys, we could return a tuple type or the first column type
                // For now, return the first column type
                if let Some(&first_col) = column_indices.first() {
                    schema
                        .fields()
                        .get(first_col)
                        .map(|field| field.data_type().clone())
                } else {
                    None
                }
            }
            IndexType::None => None,
        }
    }

    // Apply any CDC change operation - FIXED with type-aware hashing
    pub async fn apply_change_operation(
        &self,
        change: ChangeOp,
        key_column_type: &arrow::datatypes::DataType,
    ) -> Result<u64, String> {
        match change {
            ChangeOp::Delete { key, old_value_lsn } => {
                self.apply_cdc_delete(&key, key_column_type, old_value_lsn)
                    .await
            }
            ChangeOp::Update {
                key, old_value_lsn, ..
            } => {
                // For now, updates are handled as delete + insert
                // TODO: Implement proper update logic
                self.apply_cdc_delete(&key, key_column_type, old_value_lsn)
                    .await
            }
        }
    }
}

// Main table structure
pub struct ArkTable {
    name: String,
    schema: Arc<Schema>,
    prev_schema: Vec<Arc<Schema>>,
    config: Arc<ArkConfig>,

    // Memory layer (hot)
    pub memory_slice: Arc<MemorySlice>,

    // LSN management - single source of truth
    pub lsn_manager: Arc<storage::LSNManager>,

    // Storage layer for disk persistence
    write_buffer: Arc<storage::WriteBuffer>,
    buffer_flusher: Arc<RwLock<Option<storage::BufferFlusher>>>,

    // Visibility for readers
    visibility_lsn_rx: watch::Receiver<ArkaVisibilityLsn>,
}

impl ArkTable {
    pub fn new(
        name: String,
        schema: Arc<Schema>,
        config: Arc<ArkConfig>,
        base_path: String,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Create single LSNManager for both memory and storage
        let lsn_manager = Arc::new(storage::LSNManager::new());

        // Create MemorySlice using the shared LSNManager
        let memory_slice = Arc::new(MemorySlice::new(lsn_manager.clone(), &config));
        let visibility_rx = memory_slice.visibility_lsn_tx.subscribe();

        // Create WriteBuffer for batching writes
        let write_buffer = Arc::new(storage::WriteBuffer::new());

        // BufferFlusher will be initialized and started separately
        let buffer_flusher = Arc::new(RwLock::new(None));

        Ok(Self {
            name,
            schema,
            prev_schema: Vec::new(),
            config,
            memory_slice,
            lsn_manager,
            write_buffer,
            buffer_flusher,
            visibility_lsn_rx: visibility_rx,
        })
    }

    /// Start background buffer flusher with LSN tracking
    ///
    /// This creates a SegmentWriter and BufferFlusher, then starts the background
    /// flush loop that writes buffers to disk and updates LSNcoordinator.
    pub fn start_buffer_flusher(
        &self,
        base_path: String,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use std::path::PathBuf;

        // Create segment writer
        let segments_dir = PathBuf::from(&base_path).join("segments");
        let segment_writer = Arc::new(storage::SegmentWriter::new(segments_dir)?);

        // Create indexes (for now, empty - TODO: integrate properly)
        let pk_index = Arc::new(RwLock::new(crate::indexes::pk_index::PKIndex::new()));
        let segment_index = Arc::new(RwLock::new(
            crate::indexes::segment_index::SegmentIndex::new(),
        ));

        // Create and start buffer flusher
        let mut flusher = storage::BufferFlusher::new(
            self.write_buffer.clone(),
            segment_writer,
            pk_index,
            segment_index,
        );

        flusher.start();

        // Store flusher so we can stop it later
        if let Ok(mut flusher_guard) = self.buffer_flusher.write() {
            *flusher_guard = Some(flusher);
        }

        // TODO: Hook into flush completion to call lsn_coordinator.mark_wal_durable()
        // For now, we'll use a separate polling mechanism in Flight service

        Ok(())
    }

    // Ingest Arrow Flight batch
    pub async fn ingest_batch(&self, batch: RecordBatch) -> Result<u64, String> {
        // 1. Append to memory (for queries)
        let lsn = self.memory_slice.append_batch(batch.clone()).await?;

        // 2. Append to write buffer (for disk persistence)
        self.write_buffer
            .append(batch)
            .await
            .map_err(|e| format!("WriteBuffer append failed: {}", e))?;

        // 3. Auto-commit
        self.memory_slice.commit(lsn).await?;

        Ok(lsn)
    }

    // Query at specific LSN
    pub async fn query(&self, as_of_lsn: Option<u64>) -> Vec<RecordBatch> {
        self.memory_slice.get_read_snapshot(as_of_lsn).await
    }

    // Apply CDC change to the table - FIXED with type-aware hashing
    pub async fn apply_cdc_change(&self, change: ChangeOp) -> Result<u64, String> {
        if let Some(key_type) = self.memory_slice.get_primary_key_type(&self.schema) {
            let change_lsn = self
                .memory_slice
                .apply_change_operation(change, &key_type)
                .await?;
            // Auto-commit CDC changes
            self.memory_slice.commit(change_lsn).await?;
            Ok(change_lsn)
        } else {
            Err("Table has no primary key for CDC operations".to_string())
        }
    }

    // Get production index statistics
    pub fn get_index_stats(&self) -> IndexStats {
        self.memory_slice.get_index_stats()
    }

    // Production-level key lookup - FIXED with type-aware hashing
    pub async fn lookup_key(&self, key: &[u8]) -> Option<BatchLocation> {
        if let Some(key_type) = self.memory_slice.get_primary_key_type(&self.schema) {
            self.memory_slice.lookup_key(key, &key_type).await
        } else {
            None
        }
    }

    // Check if key might exist (fast bloom filter check) - FIXED with type-aware hashing
    pub fn key_might_exist(&self, key: &[u8]) -> bool {
        if let Some(key_type) = self.memory_slice.get_primary_key_type(&self.schema) {
            self.memory_slice.might_contain_key(key, &key_type)
        } else {
            false
        }
    }

    // ===== NEW: Type-Safe Lookup APIs =====

    // Type-safe lookup for Int64 keys
    pub async fn lookup_by_int64(&self, column_idx: usize, value: i64) -> Option<BatchLocation> {
        match &self.memory_slice.memory_slice_index.index_type {
            IndexType::SinglePrimitive(idx) if *idx == column_idx => {
                let key_hash = splitmix64(value as u64); // Consistent with indexing
                self.memory_slice.memory_slice_index.lookup(key_hash)
            }
            _ => None,
        }
    }

    // Type-safe lookup for UInt64 keys
    pub async fn lookup_by_uint64(&self, column_idx: usize, value: u64) -> Option<BatchLocation> {
        match &self.memory_slice.memory_slice_index.index_type {
            IndexType::SinglePrimitive(idx) if *idx == column_idx => {
                let key_hash = splitmix64(value); // Consistent with indexing
                self.memory_slice.memory_slice_index.lookup(key_hash)
            }
            _ => None,
        }
    }

    // Type-safe lookup for String keys
    pub async fn lookup_by_string(&self, column_idx: usize, value: &str) -> Option<BatchLocation> {
        match &self.memory_slice.memory_slice_index.index_type {
            IndexType::SinglePrimitive(idx) if *idx == column_idx => {
                let key_hash = hash_key(value.as_bytes()); // Consistent with indexing
                self.memory_slice.memory_slice_index.lookup(key_hash)
            }
            _ => None,
        }
    }

    // Type-safe bloom filter check for Int64 keys
    pub fn int64_might_exist(&self, column_idx: usize, value: i64) -> bool {
        match &self.memory_slice.memory_slice_index.index_type {
            IndexType::SinglePrimitive(idx) if *idx == column_idx => {
                let key_hash = splitmix64(value as u64);
                self.memory_slice
                    .memory_slice_index
                    .bloom_filter
                    .might_contain(key_hash)
            }
            _ => false,
        }
    }

    // Type-safe bloom filter check for String keys
    pub fn string_might_exist(&self, column_idx: usize, value: &str) -> bool {
        match &self.memory_slice.memory_slice_index.index_type {
            IndexType::SinglePrimitive(idx) if *idx == column_idx => {
                let key_hash = hash_key(value.as_bytes());
                self.memory_slice
                    .memory_slice_index
                    .bloom_filter
                    .might_contain(key_hash)
            }
            _ => false,
        }
    }

    // Type-safe CDC delete for Int64 keys
    pub async fn delete_by_int64(
        &self,
        column_idx: usize,
        value: i64,
        target_lsn: u64,
    ) -> Result<u64, String> {
        match &self.memory_slice.memory_slice_index.index_type {
            IndexType::SinglePrimitive(idx) if *idx == column_idx => {
                let key_bytes = value.to_le_bytes();
                let key_type = arrow::datatypes::DataType::Int64;
                let change_lsn = self
                    .memory_slice
                    .apply_cdc_delete(&key_bytes, &key_type, target_lsn)
                    .await?;
                // Auto-commit CDC changes (same as apply_cdc_change)
                self.memory_slice.commit(change_lsn).await?;
                Ok(change_lsn)
            }
            _ => Err("Column index does not match primary key".to_string()),
        }
    }

    // Type-safe CDC delete for String keys
    pub async fn delete_by_string(
        &self,
        column_idx: usize,
        value: &str,
        target_lsn: u64,
    ) -> Result<u64, String> {
        match &self.memory_slice.memory_slice_index.index_type {
            IndexType::SinglePrimitive(idx) if *idx == column_idx => {
                let key_bytes = value.as_bytes();
                let key_type = arrow::datatypes::DataType::Utf8;
                let change_lsn = self
                    .memory_slice
                    .apply_cdc_delete(key_bytes, &key_type, target_lsn)
                    .await?;
                // Auto-commit CDC changes (same as apply_cdc_change)
                self.memory_slice.commit(change_lsn).await?;
                Ok(change_lsn)
            }
            _ => Err("Column index does not match primary key".to_string()),
        }
    }
}

pub struct ArkConfig {
    // Memory thresholds
    pub memory_threshold: usize,
    pub row_threshold: usize,
    pub memory_max_size: usize,

    // Flushing
    pub flush_interval: Duration,
    pub parquet_file_target_size: usize,

    // Table properties
    pub append_only: bool,

    // NEW: Indexing control
    pub enable_point_lookups: bool, // Enable fast key-based lookups
    pub enable_cdc: bool,           // Enable Change Data Capture operations
    pub primary_key_columns: Option<Vec<usize>>, // Which columns to index (None = no indexing)

    // LSN settings
    pub commit_batch_size: usize, // How many operations before auto-commit
}

impl ArkConfig {
    /// Configuration for append-only event/log tables (no indexing)
    pub fn append_only_events() -> Self {
        Self {
            memory_threshold: 1024 * 1024, // 1MB
            row_threshold: 1000,
            memory_max_size: 10 * 1024 * 1024, // 10MB
            flush_interval: Duration::from_secs(60),
            parquet_file_target_size: 128 * 1024 * 1024, // 128MB
            append_only: true,
            enable_point_lookups: false,
            enable_cdc: false,
            primary_key_columns: None,
            commit_batch_size: 100,
        }
    }

    /// Configuration for CDC-enabled tables with indexing
    pub fn cdc_enabled_table(primary_key_columns: Vec<usize>) -> Self {
        Self {
            memory_threshold: 1024 * 1024, // 1MB
            row_threshold: 1000,
            memory_max_size: 10 * 1024 * 1024, // 10MB
            flush_interval: Duration::from_secs(60),
            parquet_file_target_size: 128 * 1024 * 1024, // 128MB
            append_only: false,
            enable_point_lookups: true,
            enable_cdc: true,
            primary_key_columns: Some(primary_key_columns),
            commit_batch_size: 100,
        }
    }

    /// Configuration for analytical tables with optional point lookups
    pub fn analytical_with_lookups(primary_key_columns: Vec<usize>) -> Self {
        Self {
            memory_threshold: 1024 * 1024, // 1MB
            row_threshold: 1000,
            memory_max_size: 10 * 1024 * 1024, // 10MB
            flush_interval: Duration::from_secs(60),
            parquet_file_target_size: 128 * 1024 * 1024, // 128MB
            append_only: false,
            enable_point_lookups: true,
            enable_cdc: false, // No CDC, just fast lookups
            primary_key_columns: Some(primary_key_columns),
            commit_batch_size: 100,
        }
    }
}
