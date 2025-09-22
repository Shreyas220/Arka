# Arka: High-Performance Streaming Buffer for Apache Iceberg

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org/)
[![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-compatible-brightgreen.svg)](https://iceberg.apache.org/)

## Overview

Arka is a high-performance streaming ingestion buffer designed to bridge the fundamental impedance mismatch between real-time data streams and batch-optimized lakehouse storage. It enables **microsecond-latency queries** on streaming data while ensuring optimal batching for **Apache Iceberg** tables.

### The Problem

Modern data architectures face an impossible choice:
- **Stream Processing**: Sub-millisecond latency but poor analytical capabilities
- **Lakehouse (Iceberg)**: Excellent analytics but seconds-to-minutes latency

You cannot write individual events to Iceberg (too slow, creates tiny files), but batching for optimal file sizes (128MB) means losing real-time query capability.

### The Solution

Arka provides a multi-tier storage architecture that delivers:
- **Microsecond queries** on recent data (in-memory tier)
- **Millisecond queries** on warm data (disk cache tier)
- **Automatic batching** for optimal Iceberg files (128MB Parquet)
- **Full CDC support** with temporal consistency
- **Unified queries** across all tiers

## Architecture

### Core Design Principles

1. **Lakehouse-First**: Every decision optimizes for eventual Iceberg storage
2. **Memory as Buffer**: Memory is temporary (minutes), not a cache (hours/days)
3. **Streaming Everywhere**: Data streams between tiers without full loading
4. **Multi-Signal Intelligence**: Tier migration based on size, time, pressure, and cost

### Four-Tier Storage Hierarchy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            ARKA STORAGE TIERS                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Tier 0: WAL (Write-Ahead Log)                                             │
│  ├─ Purpose: Durability, CDC source, crash recovery                        │
│  ├─ Location: Local SSD/NVMe (persistent)                                  │
│  ├─ Latency: <1ms writes                                                   │
│  ├─ Retention: Until confirmed in Iceberg                                  │
│  └─ Size: Small (write buffer: 4KB, files: segmented)                     │
│                                                                              │
│  Tier 1: MemSlice (Hot Data)                                              │
│  ├─ Purpose: Microsecond queries, CDC materialization                      │
│  ├─ Location: RAM (Arrow columnar format)                                  │
│  ├─ Latency: <100μs queries                                               │
│  ├─ Retention: 5-60 minutes (configurable)                               │
│  └─ Size: 1-2GB (hard limit with back-pressure)                          │
│                                                                              │
│  Tier 2: Disk Cache (Warm Data) ← CRITICAL FOR PERFORMANCE               │
│  ├─ Purpose: Query cache + Iceberg staging + CDC deltas                   │
│  ├─ Location: Local SSD (Parquet files)                                   │
│  ├─ Latency: 1-10ms queries                                               │
│  ├─ Retention: Hours to days (LRU eviction)                              │
│  └─ Size: 100-500GB (configurable)                                        │
│                                                                              │
│  Tier 3: Iceberg (Cold Data)                                              │
│  ├─ Purpose: Long-term storage, analytics                                 │
│  ├─ Location: S3/GCS/HDFS (object storage)                               │
│  ├─ Latency: 100ms-1s queries                                            │
│  ├─ Retention: Indefinite                                                 │
│  └─ Size: Unlimited                                                       │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

#### Write Path

```
Client Write Request
       │
       ▼
1. Write to WAL (Durability)
   ├─ Assign global LSN (Log Sequence Number)
   ├─ Append to active segment file
   └─ Optional: fsync for immediate durability
       │
       ▼
2. Apply to MemSlice (Fast Queries)
   ├─ Route to partition (e.g., date=2024-01-01/hour=12)
   ├─ Append to Arrow RecordBatch
   ├─ Update hash indexes
   └─ Track in CDC buffer if needed
       │
       ▼
3. Return LSN to Client (Write acknowledged)
```

#### Tier Migration Flow

```
Memory → Disk Migration (ANY condition triggers):
├─ Memory usage > 80% (prevent OOM)
├─ Partition size ≥ 128MB (optimal Parquet size)
├─ Age > 5 minutes (prevent staleness)
├─ WAL lag > 100K LSNs (maintain durability)
└─ CDC operations > 10K (prevent backlog)

Disk → Iceberg Migration (ANY condition triggers):
├─ File size ≥ 128MB (optimal for queries)
├─ Time since last commit > 60s (bounded latency)
├─ Small files > 10 (needs compaction)
├─ Pending deletes > 10K (CDC pressure)
└─ Total staged > 1GB (cost optimization)
```

#### Query Path

```
Query Request
       │
       ▼
Query Router (intelligently routes based on time range)
       │
       ├─ Memory Tier (<1ms)
       │  └─ Direct Arrow RecordBatch scan
       │
       ├─ Disk Cache (1-10ms)
       │  └─ Local Parquet file scan
       │
       └─ Iceberg/S3 (100ms-1s)
          └─ Remote Parquet scan (last resort)
```

## Key Features

### 1. Multi-Purpose Disk Cache

The disk tier isn't just staging for Iceberg - it serves multiple critical purposes:

```rust
enum DiskTierPurpose {
    QueryCache,        // Avoid S3 latency for recent queries
    IcebergStaging,    // Batch data to optimal file sizes
    CDCDeltaStorage,   // Store updates/deletes after base flush
    CompactionBuffer,  // Workspace for merging small files
}
```

**Performance Impact:**
- Queries from disk: **1-10ms**
- Queries from S3: **100-500ms**
- **10-50x faster** for warm data queries

### 2. CDC (Change Data Capture) Support

Arka handles CDC operations correctly across all tiers:

```rust
// CDC operations respect tier boundaries
enum CDCRoute {
    ApplyToMemory,      // Data still in memory
    CreateDiskDelta,    // Data on disk - create delta file
    QueueForIceberg,    // Data in Iceberg - queue for next commit
}

// Example: UPDATE arrives after data flushed to disk
UPDATE key=k1, new_value=v2, lsn=2001
├─ Check: Where is k1? → Already on disk
├─ Action: Create delta file with update
└─ Query: Merge base file + delta at query time
```

### 3. Memory Management & Back-Pressure

Three-zone system prevents OOM while maintaining performance:

```
┌────────────────────────────────────────────────────────┐
│                  MEMORY ZONES                          │
├────────────────────────────────────────────────────────┤
│                                                        │
│  GREEN (0-60%): Normal operation                      │
│    └─ Full speed, no throttling                       │
│                                                        │
│  YELLOW (60-80%): Soft back-pressure                  │
│    ├─ Progressive delay: (usage - 60) * 5ms           │
│    └─ Start aggressive eviction                       │
│                                                        │
│  RED (80-100%): Hard back-pressure                    │
│    ├─ Block writes or severe throttling               │
│    └─ Emergency flush all partitions                  │
│                                                        │
└────────────────────────────────────────────────────────┘
```

### 4. Streaming Uploads to Iceberg

Data moves from disk to Iceberg without loading entire files into memory:

```rust
// Stream large files in chunks (no memory explosion)
async fn upload_to_iceberg(file_path: &Path) {
    let file = File::open(file_path)?;
    let upload = s3_client.create_multipart_upload();

    // Stream in 1MB chunks
    let mut buffer = vec![0u8; 1_048_576];
    while file.read(&mut buffer)? > 0 {
        upload.upload_part(buffer);  // Only 1MB in memory
    }

    upload.complete();
}
```

### 5. Unified Query Federation

Queries transparently span all tiers:

```rust
// Single query API regardless of data location
let results = arka.query("SELECT * FROM events WHERE time > NOW() - '1 hour'");

// Internally routes to appropriate tiers:
// - Last 5 minutes → Memory (microseconds)
// - Last hour → Disk cache (milliseconds)
// - Older → Iceberg/S3 (hundreds of milliseconds)
```

## Technical Decisions & Rationale

### Why WAL on Disk (Not Memory)?

- **Durability**: Must survive crashes for recovery
- **Infinite capacity**: Can grow without memory limits
- **CDC source**: Natural change stream for downstream
- **Small footprint**: Only 4KB write buffer in memory

### Why Memory as Temporary Buffer (Not Cache)?

- **Bounded memory**: Can aggressively evict without data loss
- **Cost effective**: Don't need massive RAM
- **Simple model**: Memory = buffer, Iceberg = storage
- **Fast eviction**: No cache invalidation complexity

### Why Disk Cache Tier (Not Direct to Iceberg)?

- **Query performance**: 10-50x faster than S3 queries
- **Batching buffer**: Accumulate to optimal 128MB files
- **CDC workspace**: Apply deltas before Iceberg commit
- **Cost savings**: Reduce S3 API calls and data transfer
- **Recovery buffer**: Fast recovery without S3 access

### Why Arrow/Parquet Format?

- **Zero-copy**: Arrow to Parquet is just serialization
- **Columnar benefits**: Better compression, vectorized operations
- **Ecosystem**: Works with DataFusion, DuckDB, Iceberg
- **Schema evolution**: Handles changes gracefully

### Why LSN-Based Ordering?

- **Global order**: Establishes happened-before relationships
- **CDC correctness**: Exact replay order for consistency
- **Tier boundaries**: Know exactly what's persisted where
- **Distributed consensus**: Works as logical timestamp

## Performance Characteristics

| Metric | Target | Notes |
|--------|--------|-------|
| **Write Latency** | <10ms p99 | Time to LSN acknowledgment |
| **Memory Query Latency** | <1ms p99 | Point queries on hot data |
| **Disk Query Latency** | <10ms p99 | Parquet scans on warm data |
| **Federated Query Latency** | <100ms p99 | Across all tiers |
| **Throughput** | 100K+ writes/sec | Per node |
| **Memory Usage** | <2GB | Hard limit with back-pressure |
| **Iceberg File Size** | 128MB ± 20% | Optimal for analytics |
| **CDC Lag** | <1 second | Event to visibility |

## Implementation Roadmap

### Phase 1: Core Foundation (Weeks 1-2)
- [x] Basic WAL implementation
- [x] Simple MemSlice with HashMap
- [ ] LSN assignment and ordering
- [ ] Basic tier migration

### Phase 2: Performance (Weeks 3-4)
- [ ] Arrow columnar format
- [ ] Partition-aligned organization
- [ ] Streaming Parquet writer
- [ ] Back-pressure implementation

### Phase 3: Disk Cache (Weeks 5-6)
- [ ] Local Parquet storage
- [ ] Query routing to disk
- [ ] LRU eviction policy
- [ ] CDC delta files

### Phase 4: Iceberg Integration (Weeks 7-8)
- [ ] Iceberg writer
- [ ] Catalog integration
- [ ] Streaming S3 uploads
- [ ] Query federation

### Phase 5: Production (Weeks 9-10)
- [ ] Monitoring & metrics
- [ ] Recovery & fault tolerance
- [ ] Configuration management
- [ ] Performance tuning

## Usage Example

```rust
use arka::{Arka, Config};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize Arka
    let config = Config::builder()
        .memory_limit_gb(2)
        .disk_cache_gb(100)
        .iceberg_table("catalog.database.events")
        .build();

    let arka = Arka::new(config).await?;

    // Write data (microsecond acknowledgment)
    let lsn = arka.append(json!({
        "user_id": 123,
        "action": "click",
        "timestamp": "2024-01-01T12:00:00Z"
    })).await?;

    // Query recent data (microsecond latency)
    let recent = arka.query(
        "SELECT * FROM events WHERE timestamp > NOW() - INTERVAL '5 minutes'"
    ).await?;

    // CDC operations
    arka.update("user_id", 123, json!({"action": "purchase"})).await?;
    arka.delete("user_id", 456).await?;

    // Data automatically flows to Iceberg
    // Recent data: Memory (microseconds)
    // Warm data: Disk cache (milliseconds)
    // Cold data: Iceberg/S3 (seconds)

    Ok(())
}
```

## Comparison with Existing Systems

| Feature | Arka | Fluss | Moonlink | Kafka | ClickHouse |
|---------|------|-------|----------|-------|------------|
| **Primary Use Case** | Streaming→Lakehouse | Real-time KV+CDC | Iceberg Ingestion | Message Queue | Real-time Analytics |
| **Query Latency (Hot)** | <1ms | <1ms | 10ms | N/A | <10ms |
| **Query Latency (Warm)** | 1-10ms | 10ms | 100ms | N/A | 10-100ms |
| **Iceberg Native** | ✅ | ❌ | ✅ | ❌ | ❌ |
| **CDC Support** | ✅ Full | ✅ Full | ✅ Basic | ✅ Log | ✅ Basic |
| **Memory Management** | 3-zone | TODO | Row limits | Paging | Settings |
| **Disk Cache** | Query+Stage | RocksDB | Stage only | Log only | MergeTree |
| **Cloud Native** | ✅ | ⚠️ | ✅ | ⚠️ | ⚠️ |

## Key Innovations

1. **Multi-Signal Tier Migration**: More intelligent than simple thresholds
2. **Three-Zone Back-Pressure**: Gradual degradation vs hard limits
3. **Disk as Query Cache**: Not just staging but active performance tier
4. **CDC-Aware Architecture**: Correct handling across tier boundaries
5. **Streaming Everywhere**: No full file loads during transfers

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

Arka is licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

## Acknowledgments

Arka's design is inspired by production systems including:
- Apache Fluss (WAL patterns, KV architecture)
- Moonlink (Iceberg integration, streaming patterns)
- Apache Kafka (log-based architecture)
- Apache Iceberg (table format and catalog)

## Contact

- GitHub Issues: [github.com/arka/arka](https://github.com/arka/arka)
- Discord: [Join our community](https://discord.gg/arka)
- Email: team@arka.io

---

*Arka: Where streams meet lakes at the speed of memory*