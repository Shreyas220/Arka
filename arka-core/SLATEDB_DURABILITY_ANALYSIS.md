# SlateDB Durability Architecture - Analysis for Arka

## Executive Summary

SlateDB provides **configurable durability** with a multi-layered approach:
1. **Memory** - In-memory WAL buffer (current_wal)
2. **Disk** - Immutable WAL queue (immutable_wals)
3. **Object Storage** - S3/GCS/etc. (durable_watcher)

Key insight: **Durability is decoupled from write acknowledgment** via `await_durable` flag.

---

## SlateDB's Write Path Architecture

### 1. Three-Layer Write Pipeline

```text
User Write
    ↓
┌─────────────────────────────────────────────────────┐
│ LAYER 1: Memory (Instant - No Durability)           │
│ • WriteBatch → current_wal (in-memory KVTable)      │
│ • Assign sequence number (commit_seq)               │
│ • Update last_committed_seq → visible to readers    │
│ • Return immediately if await_durable=false         │
└─────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────┐
│ LAYER 2: Freeze & Queue (Still in Memory)           │
│ • maybe_trigger_flush() checks size/time limits     │
│ • freeze_current_wal() → move to immutable_wals     │
│ • Create new empty current_wal                      │
│ • Send WalFlushWork to background task              │
└─────────────────────────────────────────────────────┘
    ↓
┌─────────────────────────────────────────────────────┐
│ LAYER 3: Object Storage (Durable)                   │
│ • Background task flushes immutable_wals to S3      │
│ • Write as SST file (SsTableId::Wal(wal_id))        │
│ • Update recent_flushed_wal_id                      │
│ • Update last_remote_persisted_seq                  │
│ • Notify durable_watcher → await resolves           │
└─────────────────────────────────────────────────────┘
```

### 2. Key Data Structures

```rust
struct WalBufferManager {
    inner: Arc<RwLock<WalBufferManagerInner>>,
    max_wal_bytes_size: usize,           // Soft limit (default: l0_sst_size_bytes)
    max_flush_interval: Option<Duration>, // Time-based flush
    // ... other fields
}

struct WalBufferManagerInner {
    current_wal: Arc<KVTable>,                    // Active writes go here
    immutable_wals: VecDeque<(u64, Arc<KVTable>)>, // Frozen, waiting for flush
    recent_flushed_wal_id: u64,                   // Last WAL flushed to S3
    last_applied_seq: Option<u64>,               // Last seq applied to memtable
    flush_tx: Option<mpsc::UnboundedSender<WalFlushWork>>, // Background task channel
    oracle: Arc<Oracle>,                          // Sequence number tracking
}
```

### 3. Durability Guarantees

#### Default Behavior (`await_durable=true`)
```rust
db.put(b"key", b"value").await?; // Blocks until S3 write completes
```

**Guarantees:**
- ✅ Data survives process crash
- ✅ Data survives machine crash
- ✅ Data survives disk failure
- ✅ Data in object storage (S3/GCS)

**Latency:** Object storage PUT latency (~10-100ms)

#### Fast Path (`await_durable=false`)
```rust
let mut batch = WriteBatch::new();
batch.put_with_options(key, value, &PutOptions { await_durable: false });
db.write(batch).await?; // Returns after memory write
```

**Guarantees:**
- ✅ Data visible to readers immediately
- ✅ Data in memory (current_wal or immutable_wals)
- ❌ Lost on process crash (if not yet flushed)

**Latency:** Microseconds (just memory write + lock)

---

## Key Mechanisms for Crash Safety

### 1. Flush Triggers (Size + Time)

```rust
pub async fn maybe_trigger_flush(&self) -> Result<Arc<KVTable>, SlateDBError> {
    let current_wal_size = self.table_store.estimate_encoded_size(
        inner.current_wal.metadata().entry_num,
        inner.current_wal.metadata().entries_size_in_bytes,
    );

    let need_flush = current_wal_size >= self.max_wal_bytes_size;

    if need_flush {
        flush_tx.send(WalFlushWork { result_tx: None })?
    }
    Ok(current_wal)
}
```

**Size-based:** Flush when WAL exceeds `max_wal_bytes_size` (default: `l0_sst_size_bytes`)

**Time-based:** Background task uses `max_flush_interval` timer
```rust
struct WalFlushHandler {
    max_flush_interval: Option<Duration>, // e.g., 100ms
    wal_buffer: Arc<WalBufferManager>,
}
```

### 2. WAL Replay on Startup

SlateDB reads **all unflushed WALs from object storage** and replays them into the memtable:

```rust
// From manifest:
let recent_flushed_wal_id = state.read().state().core().replay_after_wal_id;

// Replay all WALs > recent_flushed_wal_id
let wal_replay_iter = WalReplayIterator::new(/*...*/);
while let Some(entry) = wal_replay_iter.next().await? {
    memtable.put(entry);
}
```

This ensures **no data loss** even if process crashes between WAL flush and memtable flush.

### 3. Immutable WAL Queue Pattern

```rust
async fn freeze_current_wal(&self) -> Result<(), SlateDBError> {
    let next_wal_id = self.wal_id_incrementor.next_wal_id();
    let mut inner = self.inner.write();

    // Atomic swap: old current becomes immutable
    let current_wal = std::mem::replace(
        &mut inner.current_wal,
        Arc::new(KVTable::new())
    );

    // Add to flush queue
    inner.immutable_wals.push_back((next_wal_id, current_wal));
    Ok(())
}
```

**Why this pattern?**
- Writes never block on flushes (fast path)
- Multiple WALs can be in-flight simultaneously
- Batching: multiple writes go to same WAL before flush

### 4. Sequence Number Watermarks

```rust
struct Oracle {
    last_seq: MonotonicSeq,                    // Last assigned seq
    last_committed_seq: MonotonicSeq,          // Last visible seq
    last_remote_persisted_seq: MonotonicSeq,   // Last durable seq
}
```

**Flow:**
1. Write assigned `seq=100` → `last_seq=100`
2. Write committed to memtable → `last_committed_seq=100` (now visible)
3. WAL flushed to S3 → `last_remote_persisted_seq=100` (now durable)

**Benefit:** Readers can query "give me all data up to seq=100" and know exactly what's durable.

---

## How SlateDB Handles Crashes

### Scenario 1: Crash Before WAL Flush
```
1. User writes key=foo, value=bar
2. Data in current_wal (memory only)
3. Process crashes ⚡
```
**Result:** ❌ Data lost (acceptable with `await_durable=false`)

### Scenario 2: Crash After WAL Flush
```
1. User writes key=foo, value=bar
2. Data flushed to S3 as WAL file
3. Process crashes ⚡
4. Restart: replay WAL from S3 → data recovered
```
**Result:** ✅ Data recovered

### Scenario 3: Crash During Memtable Flush
```
1. WAL flushed to S3 (wal_id=5)
2. Memtable flush starts → writes L0 SST
3. Process crashes ⚡ (L0 SST incomplete)
4. Restart:
   - manifest says last_l0_seq=100, replay_after_wal_id=4
   - Replays wal_id=5 → data recovered
```
**Result:** ✅ Data recovered (WAL is source of truth until memtable flush completes)

---

## Performance Characteristics

### Memory Usage
- **Current WAL:** ~0 bytes (empty) to `max_wal_bytes_size` (e.g., 4MB)
- **Immutable WALs Queue:** Bounded by flush rate
  - Example: 100 writes/sec, 10ms S3 latency = ~1 immutable WAL at a time
  - Worst case: slow S3 = multiple immutable WALs queued

### Latency Profile
| Operation       | await_durable=false    |     await_durable=true   |
|-----------------|------------------------|--------------------------|
| Single put      | ~10μs                  | ~50ms (S3 PUT)           |
| Batch 1000 puts | ~100μs                 | ~50ms (S3 PUT)           |
| Crash recovery  | N/A                    | Replay all unflushed WALs|

### Object Storage API Costs
- **Without batching:** 1 PUT per write = $$$
- **With WAL batching:** 1 PUT per `max_wal_bytes_size` (e.g., 4MB)
  - 1000 writes × 100 bytes = 100KB → **1 PUT**
  - Saves 999 PUTs!

---

## What Arka Should Learn from SlateDB

### 1. ✅ **Decouple Durability from Acknowledgment**

**SlateDB:**
```rust
pub struct WriteOptions {
    pub await_durable: bool, // Default: true
}
```

**Arka should implement:**
```rust
pub struct FlushOptions {
    pub await_durable: bool,
    pub fsync: bool, // For local disk durability
}

// Fast path: visible but not durable
flusher.flush_async(batch, FlushOptions {
    await_durable: false,
    fsync: false
})?;

// Safe path: durable on disk
flusher.flush(batch, FlushOptions {
    await_durable: true,
    fsync: true
})?;
```

### 2. ✅ **Implement Immutable WAL Queue**

**Current Arka:** Direct write to disk per flush

**Proposed:**
```rust
pub struct WriteBuffer {
    current_buffer: Arc<RwLock<Vec<RecordBatch>>>,
    immutable_buffers: Arc<RwLock<VecDeque<Vec<RecordBatch>>>>,
    flusher: Arc<SegmentFlusher>,
}

impl WriteBuffer {
    pub async fn append(&self, batch: RecordBatch) {
        self.current_buffer.write().push(batch);
        self.maybe_trigger_flush().await?;
    }

    async fn maybe_trigger_flush(&self) {
        if self.should_flush() {
            self.freeze_current_buffer().await?;
            tokio::spawn(async { self.flush_oldest_buffer().await });
        }
    }
}
```

**Benefits:**
- Writes never block on fsync
- Multiple flushes can happen concurrently
- Batching reduces disk I/O

### 3. ✅ **Add Sequence Number Watermarks**

**Arka currently:** No sequence number tracking

**Should add:**
```rust
pub struct DurabilityTracker {
    last_written_lsn: AtomicU64,      // Last LSN assigned
    last_flushed_lsn: AtomicU64,      // Last LSN written to disk
    last_synced_lsn: AtomicU64,       // Last LSN fsync'd
}

impl SegmentFlusher {
    pub async fn flush_with_sync(&self, batch: RecordBatch) -> Result<WaitHandle> {
        let lsn = self.tracker.last_written_lsn.fetch_add(1);

        // Write to disk (buffered)
        self.writer.write_batch(&batch)?;
        self.tracker.last_flushed_lsn.store(lsn);

        // Return handle that waits for fsync
        Ok(WaitHandle {
            lsn,
            tracker: self.tracker.clone()
        })
    }
}

pub struct WaitHandle {
    lsn: u64,
    tracker: Arc<DurabilityTracker>,
}

impl WaitHandle {
    pub async fn await_durable(&self) {
        while self.tracker.last_synced_lsn.load() < self.lsn {
            tokio::time::sleep(Duration::from_micros(100)).await;
        }
    }
}
```

### 4. ✅ **Add Time-Based Flush**

**Arka currently:** Only size-based flush

**Should add:**
```rust
pub struct WriteBuffer {
    max_buffer_size: usize,           // Size trigger
    max_buffer_age: Duration,         // Time trigger (e.g., 100ms)
    last_flush_time: Instant,
}

impl WriteBuffer {
    fn should_flush(&self) -> bool {
        let size_exceeded = self.current_size() >= self.max_buffer_size;
        let age_exceeded = self.last_flush_time.elapsed() >= self.max_buffer_age;

        size_exceeded || age_exceeded
    }
}
```

**Why?** Ensures low latency even with low write rate:
- High write rate: flush on size
- Low write rate: flush on time (e.g., every 100ms)

### 5. ✅ **Add Crash Recovery (WAL Replay)**

**Arka currently:** No recovery mechanism

**Should add:**
```rust
impl SegmentFlusher {
    pub fn with_recovery(base_dir: PathBuf) -> Result<Self> {
        // 1. Scan for existing segments
        let segments = Self::scan_segments(&base_dir)?;

        // 2. Rebuild indexes from segments
        let mut pk_index = PKIndex::new();
        let mut seg_index = SegmentIndex::new();

        for seg_meta in segments {
            // Read segment and rebuild PK index
            let reader = FileReader::try_new(File::open(&seg_meta.file_path)?)?;
            for batch in reader {
                for row_idx in 0..batch.num_rows() {
                    let pk = extract_pk(&batch, row_idx)?;
                    pk_index.insert(pk, (seg_meta.id, row_idx));
                }
            }
            seg_index.insert(seg_meta);
        }

        Ok(Self { /* ... */ })
    }
}
```

**Critical for durability:** Without replay, any crash loses data!

---

## Recommended Architecture for Arka

### Proposed Write Path with Durability

```rust
// src/write_buffer.rs
pub struct WriteBuffer {
    current: Arc<RwLock<WriteBufferInner>>,
    flusher: Arc<SegmentFlusher>,
    durability_tracker: Arc<DurabilityTracker>,
    fsync_handle: Option<JoinHandle<()>>,
}

struct WriteBufferInner {
    active_buffer: Vec<RecordBatch>,
    frozen_buffers: VecDeque<FrozenBuffer>,
    current_size: usize,
    last_flush_time: Instant,
}

struct FrozenBuffer {
    batches: Vec<RecordBatch>,
    durable_notifier: Arc<tokio::sync::Notify>,
}

impl WriteBuffer {
    pub async fn write(&self, batch: RecordBatch, opts: WriteOptions) -> Result<DurableHandle> {
        // 1. Append to active buffer
        let durable_notifier = {
            let mut inner = self.current.write();
            inner.active_buffer.push(batch.clone());
            inner.current_size += batch_size(&batch);

            // Get notifier for durability tracking
            if inner.should_flush() {
                self.freeze_active_buffer(&mut inner)
            } else {
                inner.frozen_buffers.back()
                    .map(|fb| fb.durable_notifier.clone())
                    .unwrap_or_else(|| Arc::new(tokio::sync::Notify::new()))
            }
        };

        // 2. Trigger background flush if needed
        self.maybe_trigger_flush().await?;

        // 3. Return handle
        Ok(DurableHandle {
            notifier: durable_notifier,
            await_durable: opts.await_durable
        })
    }

    async fn background_flusher(&self) {
        loop {
            // Dequeue oldest frozen buffer
            let frozen = {
                let mut inner = self.current.write();
                inner.frozen_buffers.pop_front()
            };

            if let Some(frozen) = frozen {
                // Flush to disk
                let segment_id = self.flusher
                    .flush_batches(frozen.batches)
                    .await
                    .expect("flush failed");

                // Fsync if configured
                if self.fsync_enabled {
                    self.fsync_segment(segment_id).await?;
                }

                // Notify waiters
                frozen.durable_notifier.notify_waiters();
            } else {
                // No work, sleep
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
}

pub struct DurableHandle {
    notifier: Arc<tokio::sync::Notify>,
    await_durable: bool,
}

impl DurableHandle {
    pub async fn wait(self) -> Result<()> {
        if self.await_durable {
            self.notifier.notified().await;
        }
        Ok(())
    }
}
```

### Usage Examples

#### Fast Path (Async Durability)
```rust
let write_buffer = WriteBuffer::new(flusher, WriteBufferConfig {
    max_buffer_size: 4 * 1024 * 1024, // 4MB
    max_buffer_age: Duration::from_millis(100),
    fsync: false, // Background fsync
});

// Returns immediately after memory write
let handle = write_buffer.write(batch, WriteOptions {
    await_durable: false
}).await?;

// Continue working...
// handle.wait() if you need to wait later
```

#### Safe Path (Synchronous Durability)
```rust
// Blocks until fsync completes
let handle = write_buffer.write(batch, WriteOptions {
    await_durable: true
}).await?;

handle.wait().await?; // Guaranteed durable now
```

---

## Testing Durability

### Test 1: No Data Loss on Clean Shutdown
```rust
#[tokio::test]
async fn test_clean_shutdown() {
    let dir = TempDir::new()?;

    // Write 1000 records
    {
        let buffer = WriteBuffer::new(dir.path());
        for i in 0..1000 {
            buffer.write(create_batch(i)).await?;
        }
        buffer.close().await?; // Flush all buffers
    }

    // Reopen and verify
    let buffer = WriteBuffer::with_recovery(dir.path())?;
    assert_eq!(buffer.pk_index.len(), 1000);
}
```

### Test 2: Some Data Loss on Crash (await_durable=false)
```rust
#[tokio::test]
async fn test_crash_async() {
    let dir = TempDir::new()?;

    // Write 1000 records without waiting
    {
        let buffer = WriteBuffer::new(dir.path());
        for i in 0..1000 {
            buffer.write(create_batch(i), WriteOptions {
                await_durable: false
            }).await?;
        }
        // Simulate crash: drop without close()
    }

    // Reopen
    let buffer = WriteBuffer::with_recovery(dir.path())?;
    // Some records lost (ones not yet flushed)
    assert!(buffer.pk_index.len() < 1000);
    assert!(buffer.pk_index.len() > 0); // But not all lost
}
```

### Test 3: No Data Loss on Crash (await_durable=true)
```rust
#[tokio::test]
async fn test_crash_sync() {
    let dir = TempDir::new()?;

    // Write 1000 records with fsync
    {
        let buffer = WriteBuffer::new(dir.path());
        for i in 0..1000 {
            buffer.write(create_batch(i), WriteOptions {
                await_durable: true
            }).await?.wait().await?;
        }
        // Simulate crash: drop without close()
    }

    // Reopen
    let buffer = WriteBuffer::with_recovery(dir.path())?;
    assert_eq!(buffer.pk_index.len(), 1000); // No data loss!
}
```

---

## Summary: Key Takeaways for Arka

| Feature | SlateDB | Arka (Current) | Arka (Proposed) |
|---------|---------|----------------|-----------------|
| **In-memory buffer** | ✅ current_wal | ❌ Direct flush | ✅ WriteBuffer |
| **Immutable queue** | ✅ immutable_wals | ❌ No queue | ✅ frozen_buffers |
| **Durability options** | ✅ await_durable | ❌ Always sync | ✅ WriteOptions |
| **Crash recovery** | ✅ WAL replay | ❌ No recovery | ✅ Segment replay |
| **Sequence watermarks** | ✅ Oracle | ❌ No tracking | ✅ DurabilityTracker |
| **Time-based flush** | ✅ max_flush_interval | ❌ Size only | ✅ max_buffer_age |
| **Fsync control** | ✅ Object storage | ❌ Always fsync? | ✅ Configurable |

### Next Steps for Arka

1. **Implement WriteBuffer** with frozen buffer queue
2. **Add DurabilityTracker** with sequence watermarks
3. **Add WriteOptions** with `await_durable` flag
4. **Implement recovery** by scanning segments and rebuilding indexes
5. **Add time-based flush** (e.g., every 100ms)
6. **Test crash scenarios** extensively

This will give Arka **production-grade durability** with **configurable latency tradeoffs**.
