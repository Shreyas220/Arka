# Arka Arrow Flight Examples

This directory contains examples demonstrating how to use Arka's Arrow Flight streaming interface.

## Overview

Arka uses **Arrow Flight** for fast, language-agnostic data ingestion with real-time durability notifications:

- **Bidirectional streaming**: Client sends data, server sends acknowledgments simultaneously
- **LSN tracking**: Every write gets a unique Log Sequence Number (LSN)
- **Durability tiers**: Memory → Disk → Object Storage → Iceberg
- **Multi-language**: Works with Python, Java, Rust, Go, JavaScript, etc.

## Quick Start

### 1. Start the Server

```bash
cargo run --example flight_server
```

This starts an Arrow Flight server on `localhost:8815` with:
- **Persistent catalog** at `/tmp/arka_data/`
- **Pre-registered tables**: `users` (primary key) and `events` (append-only)
- **Flight actions**: CreateTable, ListTables, DropTable
- **DoExchange**: Bidirectional streaming writes

### 2. Run Python Client

```bash
# Install dependencies
pip install pyarrow pandas

# Run example
python examples/flight_client.py
```

The Python client demonstrates:
- Connecting to Arka Flight server
- Creating tables via Flight actions
- Writing data using DoExchange streaming
- Receiving durability ACKs in real-time

## Examples

### Python Client (`flight_client.py`)

Complete example showing:
- Table creation
- Batch writes
- ACK processing
- Error handling

**Key Code Snippet:**
```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:8815")

# Write data with durability tracking
write_request = {"table_name": "users", "requested_durability": "disk"}
descriptor = flight.FlightDescriptor.for_path("users")

writer, reader = client.do_exchange(descriptor)
writer.write_batch(batch)

# Receive ACKs
for chunk in reader:
    ack = json.loads(chunk.app_metadata)
    print(f"LSN {ack['lsn']} reached {ack['durability_level']}")
```

### Server (`flight_server.rs`)

Production-ready server showing:
- Catalog initialization and persistence
- Table registration with partition specs
- Flight service setup
- gRPC server configuration

## Architecture

```text
┌─────────────────────────────────────────────────┐
│  Client (Python/Java/Rust/Go)                   │
│                                                 │
│  DoExchange Stream:                             │
│  ├─> WriteRequest (metadata)                    │
│  ├─> RecordBatch #1 ───────┐                    │
│  ├─> RecordBatch #2         │                   │
│  └─< AckMessage stream ─────┼───┐               │
└──────────────────────────────┼───┼───────────────┘
                               │   │
                               ↓   ↑
┌──────────────────────────────────────────────────┐
│  Arka Flight Server                              │
│                                                  │
│  1. Parse WriteRequest                           │
│  2. Lookup table in catalog                      │
│  3. For each batch:                              │
│     - Append to ArkTable                         │
│     - Assign LSN                                 │
│     - Send Memory ACK (0.1ms)                    │
│  4. Background:                                  │
│     - Send Disk ACK (10ms)                       │
│     - Send S3 ACK (100ms)                        │
│     - Send Iceberg ACK (500ms)                   │
└──────────────────────────────────────────────────┘
```

## Durability Guarantees

| Tier | Latency | Guarantee |
|------|---------|-----------|
| **Memory** | ~0.1ms | Data in write buffer (volatile) |
| **Disk** | ~10ms | Data fsynced to local SSD (survives process crash) |
| **Object Storage** | ~100ms | Data in S3/R2 (survives node failure) |
| **Iceberg Committed** | ~500ms | Data visible in Iceberg queries (immutable snapshot) |

## Message Formats

### WriteRequest (JSON in app_metadata)
```json
{
  "table_name": "users",
  "requested_durability": "disk",
  "idempotency_key": "batch-2024-01-15-001"
}
```

### AckMessage (JSON in app_metadata)
```json
{
  "lsn": 12345,
  "durability_level": "memory",
  "timestamp_us": 1705334400000000,
  "error": null
}
```

## Flight Actions

### CreateTable
```python
create_req = {
    "name": "users",
    "schema_json": json.dumps({"fields": [...]}),
    "primary_key": "id",
    "table_type": "primary_key",
    "partition_spec": {
        "spec_id": 1,
        "fields": [{
            "source_id": 3,
            "field_id": 1000,
            "name": "signup_day",
            "transform": "Day"
        }]
    }
}

result = client.do_action(
    flight.Action("CreateTable", json.dumps(create_req).encode())
)
```

### ListTables
```python
result = client.do_action(flight.Action("ListTables", b""))
for response in result:
    tables = json.loads(response.body.to_pybytes())
    print(tables)  # ["default.users", "default.events"]
```

### DropTable
```python
drop_req = {"name": "users"}
client.do_action(
    flight.Action("DropTable", json.dumps(drop_req).encode())
)
```

## Advanced Usage

### Partition-Aware Writes

Tables can be partitioned using Iceberg partition specs:

```rust
let partition_spec = PartitionSpec::new(1, vec![
    PartitionField::new(
        2,  // Column index (0-based)
        1000,  // Unique field ID
        "event_day".to_string(),
        Transform::Day,  // Extract day from timestamp
    ),
]);
```

Supported transforms:
- `Identity`: Partition by exact value
- `Year`, `Month`, `Day`, `Hour`: Extract from timestamp
- `Bucket(N)`: Hash bucket into N buckets
- `Truncate(N)`: Truncate strings/ints to width N

### Conditional Durability

Request specific durability levels:

```python
# Only wait for memory ACK (fastest)
write_req = {"table_name": "events", "requested_durability": "memory"}

# Wait for disk ACK (durable)
write_req = {"table_name": "users", "requested_durability": "disk"}

# Wait for Iceberg commit (queryable)
write_req = {"table_name": "analytics", "requested_durability": "iceberg_committed"}
```

### Idempotent Writes

Use idempotency keys to prevent duplicate writes:

```python
write_req = {
    "table_name": "users",
    "idempotency_key": f"batch-{date}-{sequence_number}"
}
```

## Performance Tuning

### Batch Size
- **Recommended**: 10,000 - 100,000 rows per batch
- **Too small**: Network overhead dominates
- **Too large**: Memory pressure, slower acks

### Streaming Strategy
- **Single large batch**: Simple, but all-or-nothing
- **Multiple small batches**: Progressive acks, better flow control
- **Concurrent streams**: Multiple DoExchange connections (use different tables)

### Backpressure
Server automatically applies backpressure when:
- Write buffer is full
- Disk I/O is saturated
- Object storage upload is slow

Client should handle slow `writer.write_batch()` gracefully.

## Troubleshooting

### Connection Refused
```
Error: Connection refused
```
**Solution**: Make sure server is running on `localhost:8815`

### Table Not Found
```
Error: Table not found: users
```
**Solution**: Create table first using `CreateTable` action or start server (it pre-registers example tables)

### No Acknowledgments
```
Warning: No acknowledgments received
```
**Solution**: Check that `app_metadata` is being read correctly. Current implementation sends memory ACKs immediately.

### Schema Mismatch
```
Error: Schema mismatch
```
**Solution**: Ensure RecordBatch schema matches registered table schema

## Next Steps

- **Java Example**: See `examples/FlightClient.java` (TODO)
- **Go Example**: See `examples/flight_client.go` (TODO)
- **Production Deployment**: See `docs/deployment.md` (TODO)
- **Monitoring**: See `docs/monitoring.md` (TODO)

## References

- [Arrow Flight Documentation](https://arrow.apache.org/docs/format/Flight.html)
- [PyArrow Flight Guide](https://arrow.apache.org/docs/python/flight.html)
- [Iceberg Partition Spec](https://iceberg.apache.org/spec/#partitioning)
- [Arka RFC 001: Streaming Durability](../rfc/001-arrow-flight-streaming-durability.md)
