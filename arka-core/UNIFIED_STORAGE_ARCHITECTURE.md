# Arka Unified Storage Architecture: Complete Design Document

## Table of Contents
1. [Executive Summary](#executive-summary)
2. [Architecture Overview](#architecture-overview)
3. [Core Components](#core-components)
4. [Data Flow](#data-flow)
5. [Implementation Details](#implementation-details)
6. [Performance Analysis](#performance-analysis)
7. [Operational Procedures](#operational-procedures)
8. [Trade-offs and Decisions](#trade-offs-and-decisions)

---

## Executive Summary

### Goal
Build a unified storage layer that provides sub-second queries on streaming data while efficiently batching to Apache Iceberg for long-term storage.

### Key Design Decisions
- **Arrow IPC as WAL**: All data written as immutable Arrow files
- **Separated CDC**: CDC operations stored in separate lightweight files
- **In-Memory Metadata**: Only metadata in memory, data stays on disk
- **DataFusion Query Engine**: Leverage Arrow-native query processing
- **Lazy Materialization**: Apply CDC operations at query time

### Performance Targets
- Point lookups: < 1ms
- Small range queries (< 100K rows): < 100ms
- Large scans: < 1 second for 1M rows
- Write latency: < 10ms
- Memory usage: < 500MB for 10M keys

---

## Architecture Overview

### High-Level Design

```
┌─────────────────────────────────────────────────────────────┐
│                     Client Applications                      │
└──────────────────────────┬──────────────────────────────────┘
                           │
                    ┌──────▼──────┐
                    │  Query API   │
                    │ (DataFusion) │
                    └──────┬──────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
┌───────▼─────┐   ┌────────▼───────┐   ┌─────▼──────┐
│    WAL      │   │   In-Memory    │   │   Query    │
│  (Arrow)    │   │    Metadata    │   │  Executor  │
└──────┬──────┘   └────────────────┘   └────────────┘
       │
       ├─── segment_001.arrow (Data)
       ├─── segment_002_cdc.arrow (CDC Operations)
       └─── segment_003.arrow (Data)
```

### Storage Layout

```
/data/
├── /wal/
│   ├── segment_00001.arrow         # INSERT data (100MB)
│   ├── segment_00002_cdc.arrow     # CDC operations (1MB)
│   ├── segment_00003.arrow         # INSERT data (100MB)
│   └── segment_00004_cdc.arrow     # CDC operations (1MB)
├── /checkpoint/
│   └── metadata_checkpoint.bin      # Periodic metadata snapshots
└── /iceberg/
    └── (Iceberg table format)      # Long-term storage
```

---

## Core Components

### 1. WAL Manager

#### Purpose
Handles all writes to the Arrow IPC write-ahead log with durability guarantees.

#### Schema Design

**Data Segment Schema**
```
Fields:
- __lsn: UInt64 (Log Sequence Number)
- __timestamp: Timestamp(Microsecond)
- __key: Binary (Primary key)
- [user columns...]

Example:
┌────────┬──────────────┬────────┬────────┬──────────┐
│ __lsn  │ __timestamp  │ __key  │  name  │ balance  │
├────────┼──────────────┼────────┼────────┼──────────┤
│   1    │ 1699000000   │  b"1"  │ Alice  │   1000   │
│   2    │ 1699000001   │  b"2"  │  Bob   │   2000   │
└────────┴──────────────┴────────┴────────┴──────────┘
```

**CDC Segment Schema**
```
Fields:
- __lsn: UInt64
- __timestamp: Timestamp(Microsecond)
- __op: Utf8 (UPDATE/DELETE)
- __key: Binary
- __updates: Binary (Serialized column updates)

Example:
┌────────┬──────────────┬────────┬────────┬───────────────┐
│ __lsn  │ __timestamp  │  __op  │ __key  │   __updates   │
├────────┼──────────────┼────────┼────────┼───────────────┤
│  50    │ 1699000050   │ UPDATE │  b"1"  │ {balance:1500}│
│  51    │ 1699000051   │ DELETE │  b"2"  │     NULL      │
└────────┴──────────────┴────────┴────────┴───────────────┘
```

#### Implementation

```rust
pub struct WalManager {
    // Configuration
    config: WalConfig,

    // Current write segment
    current_segment: Arc<RwLock<Segment>>,
    current_cdc_segment: Arc<RwLock<Segment>>,

    // Segment management
    segment_size_limit: usize,      // 100MB default
    segment_time_limit: Duration,   // 5 minutes default

    // Write buffering
    write_buffer: Arc<RwLock<Vec<Record>>>,
    buffer_size: usize,             // 1000 records
    flush_interval: Duration,       // 100ms

    // LSN assignment
    next_lsn: AtomicU64,
}

impl WalManager {
    pub async fn write(&self, operation: Operation) -> Result<LSN> {
        let lsn = self.assign_lsn();

        match operation {
            Operation::Insert(record) => {
                self.write_to_data_segment(lsn, record).await?;
            }
            Operation::Update { key, updates } => {
                self.write_to_cdc_segment(lsn, "UPDATE", key, updates).await?;
            }
            Operation::Delete { key } => {
                self.write_to_cdc_segment(lsn, "DELETE", key, None).await?;
            }
        }

        Ok(lsn)
    }
}
```

### 2. In-Memory Metadata

#### Purpose
Maintains all metadata needed for efficient query execution without loading actual data.

#### Structure

```rust
pub struct InMemoryMetadata {
    // Segment tracking
    segments: SegmentRegistry,

    // Key indexes
    primary_key_index: KeyIndex,

    // CDC operations
    cdc_memory: CDCMemory,

    // Statistics
    statistics: TableStatistics,

    // Transaction state
    watermarks: Watermarks,
}

pub struct SegmentRegistry {
    // All segments sorted by LSN
    data_segments: BTreeMap<LSN, SegmentMetadata>,
    cdc_segments: BTreeMap<LSN, SegmentMetadata>,

    // Quick lookups
    segment_by_id: HashMap<SegmentId, SegmentMetadata>,
    segments_by_time: BTreeMap<Timestamp, Vec<SegmentId>>,
}

pub struct SegmentMetadata {
    segment_id: u64,
    file_path: PathBuf,
    segment_type: SegmentType,

    // Bounds for pruning
    min_lsn: LSN,
    max_lsn: LSN,
    min_timestamp: i64,
    max_timestamp: i64,
    min_key: Vec<u8>,
    max_key: Vec<u8>,

    // Statistics
    row_count: u32,
    file_size_bytes: u64,

    // Bloom filter for key existence (1KB per 1000 keys at 0.1% FPR)
    bloom_filter: BloomFilter,
}

pub struct KeyIndex {
    // Primary key → physical location
    // HashMap with 32 bytes per entry
    key_to_location: HashMap<Vec<u8>, KeyLocation>,

    // Reverse index for segment cleanup
    segment_to_keys: HashMap<SegmentId, HashSet<Vec<u8>>>,
}

pub struct KeyLocation {
    data_segment: SegmentId,    // 8 bytes
    row_number: u32,            // 4 bytes
    lsn: LSN,                   // 8 bytes
    is_deleted: bool,           // 1 bit
    has_updates: bool,          // 1 bit
    // Total: ~32 bytes with overhead
}

pub struct CDCMemory {
    // All CDC operations in memory
    // ~40 bytes per operation
    operations: Vec<CDCOperation>,

    // Index by key for O(1) lookup
    key_operations: HashMap<Vec<u8>, Vec<usize>>,

    // Index by LSN for time-travel
    lsn_index: BTreeMap<LSN, usize>,
}

pub struct CDCOperation {
    lsn: LSN,                   // 8 bytes
    timestamp: i64,             // 8 bytes
    op_type: OperationType,     // 1 byte
    key: Vec<u8>,               // ~8 bytes (reference)
    updates: Option<Updates>,    // ~20 bytes when present
}
```

#### Memory Usage Calculation

```
For 10M keys, 1000 segments, 500K CDC operations:

Segment Metadata:
  1000 segments × 200 bytes = 200KB
  1000 bloom filters × 1KB = 1MB

Key Index:
  10M entries × 32 bytes = 320MB

CDC Operations:
  500K operations × 40 bytes = 20MB

Statistics & Other:
  ~10MB

Total: ~351MB (well within 500MB target)
```

### 3. Query Executor

#### Purpose
Executes queries using DataFusion while applying CDC operations transparently.

#### Implementation

```rust
pub struct QueryExecutor {
    metadata: Arc<InMemoryMetadata>,
    datafusion_ctx: SessionContext,
    segment_cache: Arc<SegmentCache>,
}

impl QueryExecutor {
    pub async fn execute_query(
        &self,
        sql: &str,
        target_lsn: Option<LSN>,
    ) -> Result<SendableRecordBatchStream> {
        // Register custom table provider
        let provider = Arc::new(CDCTableProvider {
            metadata: self.metadata.clone(),
            target_lsn: target_lsn.unwrap_or_else(|| self.metadata.current_lsn()),
        });

        self.datafusion_ctx.register_table("t", provider)?;

        // Execute query
        let df = self.datafusion_ctx.sql(sql).await?;
        df.execute_stream().await
    }
}

pub struct CDCTableProvider {
    metadata: Arc<InMemoryMetadata>,
    target_lsn: LSN,
}

impl TableProvider for CDCTableProvider {
    async fn scan(
        &self,
        projection: &Option<Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Use metadata to select segments
        let segments = self.select_segments(filters)?;

        // Create custom execution plan
        Ok(Arc::new(CDCExecutionPlan {
            segments,
            cdc_operations: self.metadata.cdc_memory.clone(),
            target_lsn: self.target_lsn,
            projection: projection.clone(),
        }))
    }
}

pub struct CDCExecutionPlan {
    segments: Vec<SegmentMetadata>,
    cdc_operations: Arc<CDCMemory>,
    target_lsn: LSN,
    projection: Option<Vec<usize>>,
}

impl ExecutionPlan for CDCExecutionPlan {
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Create streaming iterator that applies CDC
        Ok(Box::pin(CDCStream::new(
            self.segments.clone(),
            self.cdc_operations.clone(),
            self.target_lsn,
        )))
    }
}
```

### 4. CDC Application Engine

#### Purpose
Applies CDC operations to Arrow batches during query execution.

#### Algorithm

```rust
pub struct CDCApplier {
    cdc_operations: Arc<CDCMemory>,
}

impl CDCApplier {
    pub fn apply_to_batch(
        &self,
        batch: RecordBatch,
        target_lsn: LSN,
    ) -> Result<RecordBatch> {
        // Build deletion bitmap
        let mut deletions = vec![true; batch.num_rows()];

        // Build update map
        let mut updates: HashMap<usize, Vec<Update>> = HashMap::new();

        // Process each row
        for row_idx in 0..batch.num_rows() {
            let key = extract_key(&batch, row_idx);

            // Find CDC operations for this key
            if let Some(ops) = self.cdc_operations.key_operations.get(&key) {
                for &op_idx in ops {
                    let op = &self.cdc_operations.operations[op_idx];

                    // Skip operations after target LSN
                    if op.lsn > target_lsn {
                        continue;
                    }

                    match op.op_type {
                        OperationType::Delete => {
                            deletions[row_idx] = false;
                        }
                        OperationType::Update => {
                            updates.entry(row_idx)
                                .or_default()
                                .push(op.updates.clone());
                        }
                    }
                }
            }
        }

        // Apply deletions
        let filtered = filter_batch(batch, &deletions)?;

        // Apply updates
        let updated = apply_updates(filtered, updates)?;

        Ok(updated)
    }
}
```

---

## Data Flow

### Write Path

```
1. Client Request
   └─> Operation (INSERT/UPDATE/DELETE)

2. LSN Assignment
   └─> Atomic increment, guaranteed unique

3. Route to Appropriate Segment
   ├─> INSERT → Data segment (segment_NNN.arrow)
   └─> UPDATE/DELETE → CDC segment (segment_NNN_cdc.arrow)

4. Buffer Management
   ├─> Add to write buffer
   ├─> Check buffer size (1000 records)
   └─> Check time limit (100ms)

5. Flush to Disk
   ├─> Create Arrow RecordBatch
   ├─> Write to segment file
   ├─> Fsync for durability
   └─> Update metadata

6. Metadata Update
   ├─> Update segment registry
   ├─> Update key index (INSERT only)
   ├─> Add CDC operation to memory (UPDATE/DELETE)
   └─> Update statistics

7. Response to Client
   └─> Return assigned LSN
```

### Read Path

```
1. SQL Query Received
   └─> Parse and analyze

2. Segment Selection (Using Metadata)
   ├─> Apply time range filters
   ├─> Apply key range filters
   ├─> Check bloom filters
   └─> Build segment list

3. Create Execution Plan
   ├─> CDCTableProvider registered
   ├─> DataFusion optimizes plan
   └─> CDCExecutionPlan created

4. Stream Execution
   For each selected segment:
   ├─> Read Arrow file (streaming)
   ├─> Apply CDC operations
   │   ├─> Check deletions
   │   └─> Apply updates
   ├─> Apply SQL filters
   └─> Yield batch to client

5. Result Streaming
   └─> Results streamed as Arrow batches
```

### CDC Application Flow

```
For each Arrow batch from disk:

1. Extract Keys
   └─> Get primary key for each row

2. Lookup CDC Operations
   └─> O(1) HashMap lookup per key

3. Build Change Sets
   ├─> Deletion bitmap (deleted rows)
   └─> Update map (modified values)

4. Apply Changes
   ├─> Filter deleted rows (Arrow compute kernel)
   └─> Update modified columns
   └─> Return transformed batch
```

---

## Implementation Details

### Segment Management

#### Segment Rotation

```rust
pub struct SegmentRotation {
    // Triggers
    size_threshold: usize,        // 100MB
    time_threshold: Duration,     // 5 minutes
    record_threshold: usize,      // 1M records
}

impl SegmentRotation {
    pub async fn should_rotate(&self, segment: &Segment) -> bool {
        segment.size_bytes > self.size_threshold ||
        segment.created_at.elapsed() > self.time_threshold ||
        segment.record_count > self.record_threshold
    }

    pub async fn rotate(&self, manager: &WalManager) -> Result<()> {
        // 1. Flush current segment
        manager.flush_current().await?;

        // 2. Close current file
        manager.close_segment().await?;

        // 3. Create new segment
        let new_segment = Segment::new(manager.next_segment_id());
        manager.set_current(new_segment);

        // 4. Update metadata
        manager.metadata.register_segment(new_segment);

        Ok(())
    }
}
```

#### Compaction Strategy

```rust
pub struct CompactionStrategy {
    // CDC compaction triggers
    max_cdc_operations: usize,           // 100K operations
    max_cdc_segments: usize,              // 10 segments
    cdc_size_threshold: usize,           // 100MB total

    // Data compaction triggers
    small_segment_threshold: usize,      // < 10MB
    max_small_segments: usize,           // 100 small segments
}

impl CompactionStrategy {
    pub async fn compact_cdc(&self, engine: &StorageEngine) -> Result<()> {
        // 1. Create new data segment with CDC applied
        let compacted = self.materialize_with_cdc().await?;

        // 2. Write compacted segment
        let segment_id = engine.write_segment(compacted).await?;

        // 3. Update metadata
        engine.metadata.replace_segments(old_segments, segment_id);

        // 4. Clear CDC operations
        engine.metadata.cdc_memory.clear_before(lsn);

        // 5. Delete old segments (after grace period)
        engine.schedule_deletion(old_segments);

        Ok(())
    }
}
```

### Memory Management

#### Memory Limits

```rust
pub struct MemoryManager {
    // Limits
    total_limit: usize,              // 500MB
    metadata_limit: usize,           // 350MB
    cache_limit: usize,              // 150MB

    // Current usage
    metadata_used: AtomicUsize,
    cache_used: AtomicUsize,

    // Eviction
    eviction_policy: EvictionPolicy,
}

pub enum EvictionPolicy {
    LRU,
    LFU,
    ARC,  // Adaptive Replacement Cache
}

impl MemoryManager {
    pub fn check_memory_pressure(&self) -> MemoryPressure {
        let total = self.metadata_used.load() + self.cache_used.load();

        if total > self.total_limit * 0.9 {
            MemoryPressure::Critical
        } else if total > self.total_limit * 0.7 {
            MemoryPressure::High
        } else {
            MemoryPressure::Normal
        }
    }

    pub async fn handle_pressure(&self, pressure: MemoryPressure) {
        match pressure {
            MemoryPressure::Critical => {
                // Immediately evict cache
                self.evict_cache(0.5).await;
                // Trigger emergency compaction
                self.trigger_compaction().await;
            }
            MemoryPressure::High => {
                // Gradual cache eviction
                self.evict_cache(0.2).await;
            }
            MemoryPressure::Normal => {
                // No action needed
            }
        }
    }
}
```

### Consistency Management

#### Watermarks and Checkpointing

```rust
pub struct ConsistencyManager {
    // Watermarks
    write_lsn: AtomicU64,        // Latest write
    durable_lsn: AtomicU64,      // Persisted to disk
    indexed_lsn: AtomicU64,      // Indexed in memory
    iceberg_lsn: AtomicU64,      // Flushed to Iceberg

    // Checkpointing
    checkpoint_interval: Duration,  // 5 minutes
    last_checkpoint: RwLock<Checkpoint>,
}

pub struct Checkpoint {
    timestamp: SystemTime,
    lsn: LSN,
    metadata_snapshot: Vec<u8>,  // Compressed metadata
    segment_list: Vec<SegmentId>,
}

impl ConsistencyManager {
    pub async fn checkpoint(&self) -> Result<()> {
        // 1. Pause writes briefly
        let write_lock = self.write_lock.lock().await;

        // 2. Get consistent snapshot
        let snapshot = Checkpoint {
            timestamp: SystemTime::now(),
            lsn: self.write_lsn.load(),
            metadata_snapshot: self.serialize_metadata()?,
            segment_list: self.get_all_segments(),
        };

        // 3. Write checkpoint atomically
        let checkpoint_file = "checkpoint.tmp";
        serialize_to_file(&snapshot, checkpoint_file)?;
        fs::rename(checkpoint_file, "checkpoint.bin")?;

        // 4. Resume writes
        drop(write_lock);

        Ok(())
    }

    pub async fn recover(&self) -> Result<()> {
        // 1. Load checkpoint
        let checkpoint = load_checkpoint("checkpoint.bin")?;

        // 2. Restore metadata
        self.restore_metadata(checkpoint.metadata_snapshot)?;

        // 3. Replay WAL from checkpoint
        for segment in self.find_segments_after(checkpoint.lsn) {
            self.replay_segment(segment).await?;
        }

        Ok(())
    }
}
```

### Iceberg Integration

#### Flush to Iceberg

```rust
pub struct IcebergWriter {
    config: IcebergConfig,
    catalog: IcebergCatalog,
}

impl IcebergWriter {
    pub async fn flush(
        &self,
        engine: &StorageEngine,
        target_lsn: LSN,
    ) -> Result<()> {
        // 1. Get all data up to target LSN
        let segments = engine.get_segments_before(target_lsn);

        // 2. Build Iceberg data files
        let mut data_files = Vec::new();

        for segment in segments {
            // Read segment
            let batch = read_arrow_file(&segment.path)?;

            // Apply CDC if needed
            let materialized = engine.apply_cdc(batch, target_lsn)?;

            // Convert to Parquet
            let parquet_path = self.write_parquet(materialized)?;

            // Create Iceberg DataFile
            data_files.push(DataFile {
                path: parquet_path,
                format: FileFormat::Parquet,
                record_count: materialized.num_rows(),
                file_size_bytes: fs::metadata(&parquet_path)?.len(),
            });
        }

        // 3. Write deletion vectors for CDC deletes
        let deletion_vector = self.build_deletion_vector(engine)?;

        // 4. Commit to Iceberg
        let txn = self.catalog.new_transaction()?;
        txn.append_files(data_files)?;
        txn.add_deletion_vector(deletion_vector)?;
        txn.commit().await?;

        // 5. Update watermark
        engine.update_iceberg_watermark(target_lsn);

        // 6. Clean up flushed segments
        engine.cleanup_before(target_lsn).await?;

        Ok(())
    }
}
```

---

## Performance Analysis

### Query Performance Breakdown

#### Point Lookup
```
Operation                     Time        Cumulative
─────────────────────────────────────────────────────
Key index lookup              1μs         1μs
Segment location read         1μs         2μs
Disk seek (SSD)              50μs        52μs
Read Arrow row               50μs        102μs
Apply CDC operations         10μs        112μs
─────────────────────────────────────────────────────
Total:                                   ~110μs ✅
```

#### Range Query (1000 rows)
```
Operation                     Time        Cumulative
─────────────────────────────────────────────────────
Segment selection            10μs         10μs
Read 2 segments (SSD)        10ms         10.01ms
Apply CDC (1K rows)          1ms          11.01ms
DataFusion processing        5ms          16.01ms
─────────────────────────────────────────────────────
Total:                                   ~16ms ✅
```

#### Full Table Scan (10M rows)
```
Operation                     Time        Cumulative
─────────────────────────────────────────────────────
Read 100 segments            500ms        500ms
Apply CDC (500K ops)         200ms        700ms
DataFusion filtering         100ms        800ms
Result materialization       200ms        1000ms
─────────────────────────────────────────────────────
Total:                                   ~1s ⚠️
```

### Bottleneck Analysis

```
1. CDC Application Cost
   - Cost per row: ~0.1μs (in-memory lookup)
   - Cost with many CDC ops: O(num_rows × avg_cdc_per_key)
   - Mitigation: Compact when CDC ops > 100K

2. Disk I/O
   - Sequential read: 500MB/s (SSD)
   - Random read: 100MB/s (SSD)
   - Mitigation: Segment caching, columnar projection

3. Memory Pressure
   - Metadata growth: O(unique_keys)
   - CDC growth: O(updates)
   - Mitigation: Periodic compaction, eviction

4. DataFusion Overhead
   - Plan creation: ~5ms
   - Execution setup: ~10ms
   - Mitigation: Plan caching, prepared statements
```

### Optimization Strategies

```rust
pub struct OptimizationConfig {
    // Caching
    segment_cache_size: usize,       // 1GB
    cache_hot_segments: bool,        // true

    // Parallelism
    query_parallelism: usize,        // 8 threads
    compaction_parallelism: usize,   // 4 threads

    // Prefetching
    prefetch_segments: bool,         // true
    prefetch_size: usize,            // 10MB

    // Compaction
    auto_compact: bool,              // true
    compact_threshold: f64,          // 0.3 (30% CDC)
}
```

---

## Operational Procedures

### Startup Sequence

```
1. Load Configuration
   └─> Parse config file, validate settings

2. Recovery Check
   ├─> Check for checkpoint file
   └─> Check for crash recovery needed

3. Load Checkpoint (if exists)
   ├─> Deserialize metadata
   ├─> Rebuild indexes
   └─> Verify segment files

4. Scan WAL Directory
   ├─> Find all segment files
   ├─> Load segment metadata
   └─> Rebuild bloom filters

5. Load CDC Operations
   ├─> Read all CDC segments
   ├─> Build in-memory CDC index
   └─> Verify consistency

6. Initialize Services
   ├─> Start WAL writer
   ├─> Start query executor
   ├─> Start compaction scheduler
   └─> Start Iceberg writer

7. Ready for Operations
   └─> Accept client connections
```

### Shutdown Sequence

```
1. Stop Accepting New Requests
   └─> Graceful connection draining

2. Flush Pending Writes
   ├─> Flush write buffers
   └─> Ensure durability

3. Create Checkpoint
   ├─> Serialize metadata
   ├─> Write checkpoint file
   └─> Sync to disk

4. Close Resources
   ├─> Close segment files
   ├─> Release memory
   └─> Shutdown thread pools

5. Final Sync
   └─> Ensure all data on disk
```

### Monitoring Metrics

```yaml
System Metrics:
  - wal_write_latency_ms: p50, p99, p999
  - query_latency_ms: p50, p99, p999
  - memory_usage_mb: current, limit
  - disk_usage_gb: wal, iceberg

Throughput Metrics:
  - writes_per_second: rate
  - queries_per_second: rate
  - bytes_written_per_second: rate
  - bytes_read_per_second: rate

CDC Metrics:
  - cdc_operations_count: current
  - cdc_memory_mb: current
  - cdc_apply_latency_ms: p50, p99
  - compaction_pending: count

Health Metrics:
  - segment_count: data, cdc
  - key_count: total
  - watermark_lag: write-durable, durable-iceberg
  - last_checkpoint_age_seconds: age
```

---

## Trade-offs and Decisions

### Design Trade-offs

#### 1. Arrow IPC vs Custom Format
```
Chosen: Arrow IPC
Pros:
  + Native DataFusion support
  + Columnar format benefits
  + Standard tooling
Cons:
  - 20-30% space overhead
  - Less control over layout
```

#### 2. CDC in Memory vs On Disk Only
```
Chosen: CDC in memory + on disk
Pros:
  + Fast query-time application
  + Point lookup optimization
  + Efficient compaction decisions
Cons:
  - Memory usage (40MB per 1M ops)
  - Startup time to load
```

#### 3. Lazy vs Eager Materialization
```
Chosen: Lazy (query-time)
Pros:
  + Lower write latency
  + Flexible consistency
  + Memory efficient
Cons:
  - Higher query latency with many CDC ops
  - Complex query execution
```

#### 4. Separate CDC Files vs Mixed
```
Chosen: Separate CDC files
Pros:
  + Small CDC files (fast loads)
  + Clear separation of concerns
  + Efficient compaction
Cons:
  - More files to manage
  - Two write paths
```

### Future Optimizations

```markdown
1. Tiered Storage
   - Hot tier: NVMe SSD
   - Warm tier: SATA SSD
   - Cold tier: Object storage

2. Compression
   - Zstd for data segments
   - Dictionary compression for CDC
   - Compression-aware query execution

3. Distributed Architecture
   - Partitioned tables
   - Distributed metadata
   - Coordinated compaction

4. Advanced Indexing
   - Secondary indexes
   - Multi-dimensional indexes
   - Learned indexes

5. Query Optimization
   - Cost-based optimization
   - Materialized views
   - Query result caching
```

---

## Appendix

### Configuration Reference

```toml
[wal]
segment_size_mb = 100
segment_time_seconds = 300
buffer_size = 1000
flush_interval_ms = 100

[metadata]
memory_limit_mb = 500
checkpoint_interval_seconds = 300
bloom_filter_fpr = 0.001

[query]
cache_size_mb = 1000
parallelism = 8
timeout_seconds = 30

[compaction]
auto_compact = true
cdc_threshold = 100000
compact_interval_seconds = 600

[iceberg]
flush_interval_seconds = 300
flush_size_mb = 1000
catalog_uri = "s3://bucket/catalog"
```

### Error Handling

```rust
pub enum StorageError {
    // WAL errors
    WalWriteFailed(io::Error),
    SegmentCorrupted(SegmentId),

    // Metadata errors
    MetadataInconsistent(String),
    IndexCorrupted(String),

    // Query errors
    QueryTimeout(Duration),
    CDCApplicationFailed(String),

    // System errors
    OutOfMemory(usize),
    DiskFull(PathBuf),
}
```

### Testing Strategy

```markdown
1. Unit Tests
   - Segment management
   - CDC application
   - Index operations

2. Integration Tests
   - End-to-end writes
   - Query correctness
   - Crash recovery

3. Performance Tests
   - Throughput benchmarks
   - Latency measurements
   - Memory usage

4. Chaos Tests
   - Random failures
   - Network partitions
   - Disk failures
```

---

## Conclusion

This architecture provides:
- ✅ Sub-second queries for most workloads
- ✅ Efficient CDC handling
- ✅ Memory-bounded operation
- ✅ Clean Iceberg integration
- ✅ Crash recovery
- ✅ Production-ready monitoring

The key insight: Separate immutable data (Arrow files) from mutable metadata (in-memory), apply changes lazily at query time, and maintain clean separation between data and CDC operations.