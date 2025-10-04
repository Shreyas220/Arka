# Arka Architecture Decisions

## Overview

This document captures the key architectural decisions made for Arka, a high-performance streaming buffer for Apache Iceberg with microsecond-latency queries on streaming data.

## Core Architecture

```
Streaming Data → WAL (Arrow IPC) → DuckDB 
                 ↓     ↓            ↓
               LSN  iceberg       Query Engine
```

## 1. WAL Storage Format: Arrow IPC

### Decision
Use **Arrow IPC format** for WAL instead of custom binary format or JSON.

### Rationale
- **Appendable**: Can append records incrementally (unlike Parquet)
- **Zero-copy reads**: Memory-mappable for fast access
- **DataFusion compatible**: Can query directly without conversion
- **CDC support**: Add metadata columns for change types

### Implementation
```
WAL Segment Structure:
- Format: Arrow IPC (streaming format)
- Files: segment_<base_lsn>.arrow
- Metadata columns:
  - __lsn: u64 (Log Sequence Number)
  - __change_type: string (INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE)
  - __timestamp: timestamp
```

## 2. LSN (Log Sequence Number) Coordination

### Decision
Use LSN as global ordering mechanism across all tiers.

### Implementation
```rust
struct LSNCoordinator {
    next_lsn: AtomicU64,           // Next LSN to assign
    wal_durable_lsn: AtomicU64,    // Persisted to WAL
    duckdb_lsn: AtomicU64,         // Materialized in DuckDB
    iceberg_lsn: AtomicU64,        // Flushed to Iceberg
}
```

### LSN Flow
1. **Write Path**: Assign LSN → Write to WAL → Update DuckDB → Eventually Iceberg
2. **Recovery**: Read checkpoint → Replay from WAL after checkpoint LSN
3. **Deduplication**: Check if LSN already processed to avoid duplicates

## 3. Storage Engine: DuckDB

### Decision
Use **DuckDB in persistent mode** as primary storage engine instead of custom StateTable implementation.

### Rationale
- **Time to market**: Weeks instead of months
- **Built-in features**: Incremental materialized views, SQL engine, memory management
- **Automatic tiering**: Hot data in memory, cold on disk
- **Production ready**: Battle-tested, handles edge cases

### Configuration
```sql
-- DuckDB setup
CREATE DATABASE arka.db;
SET memory_limit = '2GB';
SET threads = 4;

-- Base table for streaming data
CREATE TABLE wal_data (
    -- User columns
    user_id INTEGER,
    name VARCHAR,
    age INTEGER,

    -- CDC metadata columns
    __lsn BIGINT,
    __change_type VARCHAR,
    __timestamp TIMESTAMP
);
```

## 4. Table Types and Storage Strategy

### Decision
Different storage strategies based on table type.

### Table Types
```
1. AppendOnly Tables
   - No primary key
   - No CDC operations
   - Query directly from WAL/DuckDB
   - No KV index needed

2. PrimaryKey Tables
   - Has primary key
   - Supports CDC operations
   - Maintains current state in DuckDB
   - Point lookups optimized

3. Materialized Views
   - Derived from base tables
   - Incrementally maintained
   - Stored in DuckDB
   - Fully queryable via SQL
```

## 5. CDC Handling

### Decision
Store CDC operations in WAL with metadata, materialize in DuckDB.

### CDC Flow
```
1. WAL stores all operations:
   - INSERT: New record
   - UPDATE_BEFORE: Previous state
   - UPDATE_AFTER: New state
   - DELETE: Removed record

2. DuckDB materialization:
   - Latest state per key
   - Incremental updates
   - Point lookups via indexes

3. Iceberg push:
   - Periodic snapshots
   - CDC operations as row-level updates
```

### Example CDC in WAL
```
| user_id | name    | age | __lsn | __change_type  |
|---------|---------|-----|-------|----------------|
| 1       | Alice   | 30  | 100   | INSERT         |
| 1       | Alice   | 30  | 200   | UPDATE_BEFORE  |
| 1       | Alice   | 31  | 201   | UPDATE_AFTER   |
| 2       | Bob     | 25  | 300   | INSERT         |
| 2       | Bob     | 25  | 400   | DELETE         |
```

### Materialized State in DuckDB
```
| user_id | name    | age | __current_lsn |
|---------|---------|-----|---------------|
| 1       | Alice   | 31  | 201           |
-- User 2 deleted, not present
```

## 6. Query Architecture

### Decision
Use DuckDB SQL for all queries, with query routing optimization.

### Query Types and Execution

#### Point Lookups
```sql
-- Query
SELECT * FROM users WHERE user_id = 123;

-- Execution
1. Check if primary key table
2. Use DuckDB index (automatic)
3. Sub-second response
```

#### Range Queries
```sql
-- Query
SELECT * FROM events WHERE timestamp > NOW() - INTERVAL '1 hour';

-- Execution
1. DuckDB scans relevant partitions
2. Uses statistics for optimization
3. Memory cache for hot data
```

#### Materialized Views
```sql
-- Create view
CREATE MATERIALIZED VIEW user_summary AS
SELECT user_id, COUNT(*), SUM(amount)
FROM events
GROUP BY user_id;

-- Query view (fully SQL capable!)
SELECT * FROM user_summary WHERE sum_amount > 1000;
```

#### Complex Analytics
```sql
-- Joins, aggregations, window functions all work
SELECT u.name, us.total_amount,
       RANK() OVER (ORDER BY us.total_amount DESC)
FROM user_summary us
JOIN users u ON us.user_id = u.user_id;
```

## 7. Data Flow Pipeline

### Write Path
```
1. Client write request
2. Assign LSN (monotonic, global)
3. Append to WAL (Arrow IPC format)
4. Return LSN to client (durability guaranteed)
5. Async: Apply to DuckDB
6. Async: Eventually flush to Iceberg
```

### Read Path
```
Query Type:
├─ Point Lookup → DuckDB index → Fast response
├─ Recent Data → DuckDB (memory cached) → Low latency
├─ Historical → Iceberg via DuckDB external table → Higher latency
└─ Materialized View → DuckDB table → Fast response
```

## 8. Memory Management

### Decision
Let DuckDB handle memory management with configuration.

### Strategy
```
DuckDB Buffer Pool (2GB default)
├─ Hot data (frequently accessed) → Kept in memory
├─ Warm data (recent) → Cached based on LRU
└─ Cold data → Spilled to disk

Configurable:
- memory_limit: Maximum memory usage
- temp_directory: Spill location
```

## 9. Migration to Iceberg

### Decision
Periodic batch migration with CDC preserved.

### Process
```
Every 5 minutes or 128MB:
1. Read batch from DuckDB WHERE lsn > last_iceberg_lsn
2. Convert to Parquet
3. Write to Iceberg with CDC metadata
4. Update iceberg_lsn
5. Optionally clean old WAL segments
```

## 10. Recovery and Consistency

### Checkpoint Strategy
```json
{
  "checkpoint": {
    "wal_lsn": 10000,
    "duckdb_lsn": 9950,
    "iceberg_lsn": 8000,
    "timestamp": "2024-01-01T12:00:00Z"
  }
}
```

### Recovery Process
1. Load checkpoint
2. Open DuckDB (automatic recovery via its WAL)
3. Replay Arrow IPC WAL from checkpoint LSN
4. Resume normal operations

## 11. Future Optimizations

### Phase 1 (Current)
- DuckDB for everything
- Simple, working solution

### Phase 2 (If needed)
- Custom StateTable for hot paths
- Keep DuckDB for complex queries
- Hybrid approach

### Phase 3 (Scale)
- Distributed processing
- Partitioned state
- Multiple DuckDB instances

## Summary of Key Decisions

1. **WAL Format**: Arrow IPC (appendable, queryable)
2. **Storage Engine**: DuckDB (persistent mode)
3. **CDC Handling**: Metadata columns in WAL, materialized in DuckDB
4. **Query Engine**: DuckDB SQL for everything
5. **Materialized Views**: DuckDB native support
6. **Memory Management**: DuckDB automatic with configuration
7. **Iceberg Integration**: Periodic batch with CDC metadata
8. **LSN Coordination**: Global ordering across all components

## Benefits of This Architecture

- **Fast time to market**: Leverage DuckDB's capabilities
- **Simple mental model**: SQL for everything
- **Production ready**: Built on proven components
- **Flexible**: Can optimize specific paths later
- **Unified queries**: Single SQL interface for all data

## Trade-offs Accepted

- **Less control**: DuckDB is somewhat of a black box
- **Memory overhead**: DuckDB's general-purpose vs specialized
- **Two storage formats**: Arrow IPC (WAL) + DuckDB internal
- **Limited customization**: Can't optimize every CDC pattern

These trade-offs are acceptable given the massive reduction in implementation complexity and time to market.