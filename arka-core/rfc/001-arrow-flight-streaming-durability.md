# RFC 001: Arrow Flight Streaming Write with Async Durability Notifications

**Status:** Draft
**Author:** Arka Team
**Created:** 2024-10-04
**Updated:** 2024-10-04

---

## Summary

Implement a bidirectional streaming write protocol using Arrow Flight that decouples write acknowledgment from durability guarantees. This enables high-throughput ingestion while providing clients with fine-grained durability notifications as data progresses through storage tiers (memory → local disk → object storage → Iceberg).

---

## Motivation

### Current Limitations

1. **Synchronous write bottleneck**: Traditional RPC requires clients to wait for fsync before receiving acknowledgment (~10ms per write)
2. **No durability visibility**: Clients don't know when their data has reached object storage or Iceberg
3. **Language lock-in**: Custom protocols limit client language support

### Goals

1. **High-throughput ingestion**: 10,000+ writes/sec/client (100x improvement)
2. **Async durability tracking**: Clients receive notifications as data reaches each tier
3. **Multi-language support**: Python, Java, Go, Rust, JavaScript clients using standard Arrow Flight
4. **Flexible guarantees**: Clients choose their own durability/latency trade-off

---

## Design Overview

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Client (Any Language)                         │
│  ┌──────────────┐         ┌──────────────────────────┐          │
│  │ Write Thread │────────→│ Background Ack Processor │          │
│  │ (non-block)  │         │ (process notifications)  │          │
│  └──────────────┘         └──────────────────────────┘          │
│         │                              ↑                         │
│         │ FlightData                   │ WriteAck               │
│         ↓                              │                         │
└─────────────────────────────────────────────────────────────────┘
              │                              ↑
              │ gRPC Bidirectional Stream    │
              ↓                              │
┌─────────────────────────────────────────────────────────────────┐
│                    Server (Rust)                                 │
│  ┌──────────────┐         ┌──────────────────────────┐          │
│  │ Write Handler│────────→│ Durability Notifier      │          │
│  │ (buffer)     │         │ (track tier progress)    │          │
│  └──────────────┘         └──────────────────────────┘          │
│                                     │                            │
│  ┌──────────────────────────────────┼─────────────────────────┐ │
│  │ Storage Tiers                    ↓                         │ │
│  │  Memory → Disk → S3 → Iceberg                              │ │
│  │    (notify subscribers on each tier completion)            │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### Durability Tiers

Data progresses through four tiers, each with different guarantees:

| Tier | Durability | Latency | Survives |
|------|-----------|---------|----------|
| **Memory** | WriteBuffer | ~0.1ms | Process restart: ❌<br>Machine failure: ❌ |
| **Local Disk** | Segment files (fsync) | ~10ms | Process restart: ✓<br>Machine failure: ❌ |
| **Object Storage** | S3/R2 replication | ~100ms | Process restart: ✓<br>Machine failure: ✓ |
| **Committed** | Iceberg catalog | ~60s | External queries: ✓ |

---

## Detailed Design

### Protocol: Arrow Flight DoExchange

Use Arrow Flight's `DoExchange` RPC for bidirectional streaming:

```protobuf
// Standard Arrow Flight protocol (no custom extensions needed!)
service FlightService {
    rpc DoExchange(stream FlightData) returns (stream FlightData);
}
```

**Client → Server:** RecordBatch writes
**Server → Client:** Acknowledgments + durability notifications

### Message Formats

#### Write Message (Client → Server)

```
FlightData {
    descriptor: "streaming_write"
    schema: Arrow Schema (first message only)
    batches: RecordBatch (Arrow IPC format)
}
```

#### Acknowledgment Message (Server → Client)

Server sends back acknowledgments as RecordBatch with this schema:

```rust
Schema {
    lsn: UInt64,                      // Globally unique log sequence number
    durability_level: Utf8,           // "MEMORY" | "LOCAL_DISK" | "OBJECT_STORAGE" | "COMMITTED"
    is_durability_update: Boolean,    // false = initial ack, true = tier progression
    timestamp: Timestamp(Microsecond) // When this tier was reached
}
```

**Example acknowledgment progression:**

```
// Immediate ack (write accepted)
{ lsn: 1000, durability_level: "MEMORY", is_durability_update: false, ... }

// 10ms later (disk flush completed)
{ lsn: 1000, durability_level: "LOCAL_DISK", is_durability_update: true, ... }

// 100ms later (S3 upload completed)
{ lsn: 1000, durability_level: "OBJECT_STORAGE", is_durability_update: true, ... }

// 60s later (Iceberg commit)
{ lsn: 1000, durability_level: "COMMITTED", is_durability_update: true, ... }
```

### LSN Coordination

Extend `LSNCoordinator` to track tier watermarks:

```rust
pub struct LSNCoordinator {
    // LSN assignment
    next_lsn: AtomicU64,

    // Durability watermarks (monotonically increasing)
    latest_write_lsn: AtomicU64,        // Assigned to buffer
    flushed_to_disk_lsn: AtomicU64,     // Persisted to local disk
    flushed_to_s3_lsn: AtomicU64,       // Replicated to object storage
    flushed_to_iceberg_lsn: AtomicU64,  // Committed to Iceberg catalog

    // Notification channels
    disk_flush_tx: watch::Sender<LSN>,
    s3_upload_tx: watch::Sender<LSN>,
    iceberg_commit_tx: watch::Sender<LSN>,
}
```

**Invariant:** `latest_write_lsn ≥ flushed_to_disk_lsn ≥ flushed_to_s3_lsn ≥ flushed_to_iceberg_lsn`

### Subscriber Management

Track which Flight clients care about which LSNs:

```rust
pub struct DurabilitySubscribers {
    // Per-LSN subscriptions
    subscribers: RwLock<HashMap<LSN, Vec<AckSender>>>,

    // Optional: Per-client watermark subscriptions (less precise, lower overhead)
    watermark_subscribers: RwLock<Vec<AckSender>>,
}

impl DurabilitySubscribers {
    /// Register client interest in LSN durability
    pub fn subscribe(&self, lsn: LSN, sender: AckSender) {
        self.subscribers.write().unwrap()
            .entry(lsn)
            .or_insert_with(Vec::new)
            .push(sender);
    }

    /// Notify all subscribers when LSN reaches tier
    pub async fn notify(&self, lsn: LSN, tier: DurabilityLevel) {
        if let Some(senders) = self.subscribers.read().unwrap().get(&lsn) {
            for sender in senders {
                sender.send(create_durability_ack(lsn, tier)).await;
            }
        }
    }

    /// Cleanup subscribers for old LSNs
    pub fn prune(&self, before_lsn: LSN) {
        self.subscribers.write().unwrap()
            .retain(|&lsn, _| lsn >= before_lsn);
    }
}
```

### Write Path Flow

```
1. Client sends RecordBatch
   ↓
2. Server receives FlightData
   ↓
3. Assign LSN from coordinator
   ↓
4. Append to WriteBuffer (in-memory)
   ↓
5. Send immediate ack: { lsn, durability: "MEMORY" }
   ↓
6. Register client as subscriber for this LSN
   ↓
7. Background: BufferFlusher writes to disk
   ↓
8. On disk flush completion:
   - coordinator.mark_flushed_to_disk(lsn)
   - subscribers.notify(lsn, "LOCAL_DISK")
   ↓
9. Background: S3Uploader uploads segment
   ↓
10. On S3 upload completion:
    - coordinator.mark_uploaded_to_s3(lsn)
    - subscribers.notify(lsn, "OBJECT_STORAGE")
    ↓
11. Background: IcebergCommitter commits snapshot
    ↓
12. On Iceberg commit:
    - coordinator.mark_committed_to_iceberg(lsn)
    - subscribers.notify(lsn, "COMMITTED")
    - subscribers.prune(lsn)  // Cleanup old subscriptions
```

---

## Client API Design

### Python Client

```python
import pyarrow as pa
import pyarrow.flight as flight

# Connect to Arka
client = flight.connect("grpc://localhost:8815")

# Start streaming write session
writer, reader = client.do_exchange(
    flight.FlightDescriptor.for_path("streaming_write")
)

# Send batches (fast, non-blocking)
batch = pa.RecordBatch.from_pandas(df)
writer.begin(batch.schema)
writer.write_batch(batch)

# Process acknowledgments asynchronously
for ack_batch in reader:
    ack_df = ack_batch.to_pandas()

    for _, row in ack_df.iterrows():
        lsn = row['lsn']
        durability = row['durability_level']

        if row['is_durability_update']:
            print(f"LSN {lsn} reached {durability}")

            if durability == 'OBJECT_STORAGE':
                # Safe to delete local copy
                cleanup_local_cache(lsn)

writer.done_writing()
```

### Java Client

```java
FlightClient client = FlightClient.builder()
    .location(Location.forGrpcInsecure("localhost", 8815))
    .build();

ExchangeReaderWriter stream = client.doExchange(
    FlightDescriptor.path("streaming_write")
);

// Send batches
stream.getWriter().start(root);
stream.getWriter().putNext();

// Receive acks
while (stream.getReader().next()) {
    VectorSchemaRoot ackRoot = stream.getReader().getRoot();
    processAcknowledgments(ackRoot);
}
```

### Rust Client

```rust
let mut client = FlightClient::new(channel);
let (tx, rx) = client.do_exchange(descriptor).await?;

// Send batches
tx.send(batch).await?;

// Receive acks
while let Some(ack) = rx.recv().await? {
    let lsn = ack.column(0).as_any().downcast_ref::<UInt64Array>()?;
    let durability = ack.column(1).as_any().downcast_ref::<StringArray>()?;

    println!("LSN {} reached {}", lsn.value(0), durability.value(0));
}
```

---

## Alternative Designs Considered

### 1. Per-LSN Subscriptions vs Watermark Broadcasting

**Per-LSN (Chosen):**
- ✓ Precise: Client knows exact LSN durability
- ✓ Flexible: Different clients can subscribe to different LSNs
- ✗ Overhead: Need to track subscribers per LSN
- ✗ Memory: Grows with number of in-flight writes

**Watermark Broadcasting:**
- ✓ Simple: Just broadcast `{ disk_lsn: 1000, s3_lsn: 950 }` periodically
- ✓ Low overhead: No per-LSN tracking
- ✗ Imprecise: Client must check if their LSN ≤ watermark
- ✗ Delayed: Only updated on interval (e.g., every 100ms)

**Decision:** Implement **both**. Use per-LSN for critical writes, watermarks for bulk ingestion.

### 2. Custom Protocol vs Arrow Flight

**Custom Protocol:**
- ✓ Full control over message format
- ✗ Must implement in every language
- ✗ No zero-copy benefits
- ✗ Reinventing the wheel

**Arrow Flight (Chosen):**
- ✓ Standard protocol (gRPC + Arrow IPC)
- ✓ Multi-language support out-of-the-box
- ✓ Zero-copy across languages
- ✓ Battle-tested (used by Snowflake, BigQuery, Spark)

**Decision:** Arrow Flight. No need to reinvent.

### 3. Synchronous Ack API vs Async Notifications

**Synchronous (Traditional):**
```python
lsn = client.write(batch)  # Blocks until durable
```
- ✓ Simple mental model
- ✗ Low throughput (blocking)
- ✗ Couples write and durability

**Async Notifications (Chosen):**
```python
lsn = client.write(batch)  # Returns immediately
# ... later ...
on_durable(lsn, lambda: cleanup())  # Callback when durable
```
- ✓ High throughput (non-blocking)
- ✓ Flexible durability guarantees
- ✗ More complex client code

**Decision:** Async notifications. Provide both APIs:
- `write()` - Fire and forget
- `write_durable()` - Helper that waits internally

---

## Performance Analysis

### Throughput Comparison

**Synchronous API (baseline):**
```
Client → Server: write(batch)
Server: append to buffer → flush to disk (10ms) → ack
Client: receives ack (total: 10ms)

Throughput: 1 / 10ms = 100 writes/sec
```

**Async API (this RFC):**
```
Client → Server: write(batch)
Server: append to buffer → immediate ack (0.1ms)
Background: flush to disk → notify

Client receives ack (total: 0.1ms)

Throughput: 1 / 0.1ms = 10,000 writes/sec
```

**Improvement: 100x**

### Latency Breakdown

| Operation | Latency | Notes |
|-----------|---------|-------|
| Arrow IPC serialization | ~10μs | Zero-copy in most cases |
| gRPC send | ~100μs | Network + syscall |
| Buffer append | ~1μs | HashMap insert |
| Ack serialization | ~10μs | Small RecordBatch |
| gRPC receive | ~100μs | Network + syscall |
| **Total (client write)** | **~0.22ms** | 98% faster than sync (10ms) |

### Memory Overhead

**Per LSN subscription:**
```
Subscriber {
    lsn: u64,              // 8 bytes
    sender: AckSender,     // 16 bytes (channel ptr)
    client_id: u64,        // 8 bytes
}
Total: ~32 bytes per LSN
```

**For 1M in-flight writes:** 32 MB

**Mitigation:** Prune subscriptions after LSN reaches final tier (Iceberg).

---

## Implementation Plan

### Phase 1: Core Streaming (Week 1)

**Goals:**
- Bidirectional streaming works
- Immediate acks sent
- Python client can write batches

**Deliverables:**
1. `ArkaFlightService` with `do_exchange()` implementation
2. LSN assignment on write
3. Immediate memory-tier acks
4. Python example client

**Success Metrics:**
- 10,000 writes/sec from Python client
- Acks received within 1ms

### Phase 2: Durability Notifications (Week 2)

**Goals:**
- Track durability tiers
- Notify clients on tier progression
- Watermark broadcasting

**Deliverables:**
1. Extended `LSNCoordinator` with tier watermarks
2. `DurabilitySubscribers` implementation
3. Disk flush notifications
4. Watermark periodic broadcasts

**Success Metrics:**
- Clients receive disk notifications within 50ms of flush
- Memory overhead < 50 MB for 1M in-flight writes

### Phase 3: Multi-Tier Support (Week 3)

**Goals:**
- S3 upload notifications
- Iceberg commit notifications
- Client reconnection handling

**Deliverables:**
1. S3 uploader integration with notifications
2. Iceberg committer integration
3. Durability log for reconnection
4. Java example client

**Success Metrics:**
- Clients receive S3 notifications within 200ms of upload
- Reconnection recovers missed notifications

### Phase 4: Production Hardening (Week 4)

**Goals:**
- Error handling
- Backpressure
- Metrics/monitoring

**Deliverables:**
1. Graceful client disconnection
2. Server-side backpressure (when buffer full)
3. Prometheus metrics for durability lag
4. Load testing (1M writes/sec aggregate)

**Success Metrics:**
- Zero data loss under load
- Graceful degradation when overwhelmed

---

## Testing Strategy

### Unit Tests

```rust
#[tokio::test]
async fn test_immediate_ack() {
    let server = ArkaFlightServer::new(...);
    let client = create_test_client();

    let (tx, rx) = client.do_exchange(...).await?;

    tx.send(test_batch).await?;

    let ack = rx.recv().await?;
    assert_eq!(ack.durability_level, "MEMORY");
    assert!(ack.lsn > 0);
}

#[tokio::test]
async fn test_durability_progression() {
    let server = ArkaFlightServer::new(...);
    let (tx, rx) = client.do_exchange(...).await?;

    tx.send(test_batch).await?;

    // Immediate ack
    let ack1 = rx.recv().await?;
    assert_eq!(ack1.durability_level, "MEMORY");

    // Trigger disk flush
    server.buffer_flusher.flush_now().await?;

    // Disk notification
    let ack2 = rx.recv().await?;
    assert_eq!(ack2.durability_level, "LOCAL_DISK");
    assert_eq!(ack2.lsn, ack1.lsn);
}
```

### Integration Tests

```python
# Python integration test
def test_python_client_streaming():
    client = flight.connect("grpc://localhost:8815")

    writer, reader = client.do_exchange(...)

    # Send 1000 batches
    for i in range(1000):
        batch = create_test_batch(i)
        writer.write_batch(batch)

    # Verify all acks received
    acks = list(reader)
    assert len(acks) >= 1000  # At least initial acks

    # Wait for disk durability
    disk_acks = [a for a in acks if a['durability_level'] == 'LOCAL_DISK']
    assert len(disk_acks) == 1000
```

### Load Tests

```
Scenario 1: Single client throughput
  - 1 client, 100K batches
  - Measure: writes/sec, ack latency p50/p99
  - Goal: 10,000 writes/sec, p99 < 2ms

Scenario 2: Concurrent clients
  - 100 clients, 1K batches each
  - Measure: aggregate throughput, fairness
  - Goal: 500K writes/sec aggregate

Scenario 3: Durability lag
  - Write 1M batches
  - Measure: time until all reach OBJECT_STORAGE
  - Goal: < 5 minutes
```

---

## Rollout Plan

### Stage 1: Internal Alpha (Week 1-2)
- Deploy to dev environment
- Team testing with Python clients
- Collect performance metrics

### Stage 2: Beta (Week 3-4)
- Expose to select users
- Multi-language client examples published
- Monitor for edge cases

### Stage 3: GA (Week 5+)
- Full production rollout
- Documentation published
- Client libraries released

---

## Monitoring & Observability

### Key Metrics

```
# Write throughput
arka_flight_writes_total{tier="memory"}
arka_flight_writes_total{tier="disk"}

# Durability lag
arka_durability_lag_seconds{tier="disk"}
arka_durability_lag_seconds{tier="s3"}
arka_durability_lag_seconds{tier="iceberg"}

# Client tracking
arka_active_flight_clients
arka_pending_subscriptions{tier="disk|s3|iceberg"}

# Performance
arka_write_latency_seconds{quantile="0.5|0.99"}
arka_ack_latency_seconds{quantile="0.5|0.99"}
```

### Dashboards

1. **Write Performance Dashboard**
   - Writes/sec over time
   - Ack latency distribution
   - Active clients

2. **Durability Dashboard**
   - Watermark progression
   - Tier lag (current LSN - tier LSN)
   - Subscriber counts

3. **Client Health Dashboard**
   - Per-client write rates
   - Disconnection events
   - Backpressure events

---

## Open Questions

1. **Should we support filtering durability notifications?**
   - Client says "only notify me for OBJECT_STORAGE"
   - Pro: Reduces network traffic
   - Con: More complex API

2. **How long should we keep subscriber registrations?**
   - Current: Until Iceberg commit
   - Alternative: Configurable TTL
   - Risk: Memory leak if Iceberg committer fails

3. **Should we batch durability notifications?**
   - Current: One ack per LSN per tier
   - Alternative: Batch acks for LSNs 1000-1100 in single message
   - Trade-off: Latency vs throughput

4. **How to handle client reconnection after server restart?**
   - Option A: Persist durability log to disk
   - Option B: Client re-queries current watermarks
   - Option C: Client assumes worst-case (all lost)

---

## References

- [Arrow Flight Protocol Specification](https://arrow.apache.org/docs/format/Flight.html)
- [Kafka Producer Acks](https://kafka.apache.org/documentation/#producerconfigs_acks)
- [Fluss Bucket Lifecycle](https://github.com/alibaba/fluss)
- [RisingWave Epoch-based Consistency](https://www.risingwave.com/docs/)

---

## Appendix A: Full API Example

### Complete Python Client

```python
import pyarrow as pa
import pyarrow.flight as flight
import pandas as pd
from typing import Dict, Callable

class ArkaStreamingClient:
    def __init__(self, host: str, port: int):
        self.client = flight.connect(f"grpc://{host}:{port}")
        self.pending_writes: Dict[int, str] = {}  # lsn -> durability level
        self.callbacks: Dict[int, Callable] = {}

    def start_session(self):
        """Start bidirectional streaming session"""
        descriptor = flight.FlightDescriptor.for_path("streaming_write")
        self.writer, self.reader = self.client.do_exchange(descriptor)

        # Start background ack processor
        import threading
        self.ack_thread = threading.Thread(target=self._process_acks)
        self.ack_thread.start()

    def write(self, df: pd.DataFrame) -> int:
        """Write batch, return LSN (non-blocking)"""
        batch = pa.RecordBatch.from_pandas(df)

        if not hasattr(self, 'schema_sent'):
            self.writer.begin(batch.schema)
            self.schema_sent = True

        self.writer.write_batch(batch)

        # Return optimistic LSN (will be confirmed in ack)
        lsn = self._next_lsn()
        self.pending_writes[lsn] = "MEMORY"
        return lsn

    def on_durable(self, lsn: int, level: str, callback: Callable):
        """Register callback for when LSN reaches durability level"""
        self.callbacks[lsn] = (level, callback)

    def _process_acks(self):
        """Background thread: process server acks"""
        for ack_batch in self.reader:
            ack_df = ack_batch.to_pandas()

            for _, row in ack_df.iterrows():
                lsn = row['lsn']
                durability = row['durability_level']

                # Update tracking
                self.pending_writes[lsn] = durability

                # Trigger callbacks
                if lsn in self.callbacks:
                    target_level, callback = self.callbacks[lsn]
                    if self._durability_order(durability) >= self._durability_order(target_level):
                        callback()
                        del self.callbacks[lsn]

    def _durability_order(self, level: str) -> int:
        return {"MEMORY": 0, "LOCAL_DISK": 1, "OBJECT_STORAGE": 2, "COMMITTED": 3}[level]

    def close(self):
        """Close session gracefully"""
        self.writer.done_writing()
        self.ack_thread.join()

# Usage
client = ArkaStreamingClient("localhost", 8815)
client.start_session()

# Write 1000 batches without blocking
for i in range(1000):
    df = pd.DataFrame({'id': [i], 'value': [i * 100]})
    lsn = client.write(df)

    # Register callback for S3 durability
    client.on_durable(lsn, "OBJECT_STORAGE",
                     lambda: print(f"LSN {lsn} safe on S3!"))

client.close()
```

---

## Appendix B: Server Implementation Sketch

```rust
use arrow_flight::{
    flight_service_server::{FlightService, FlightServiceServer},
    FlightData, FlightDescriptor,
};
use tonic::{Request, Response, Status, Streaming};

pub struct ArkaFlightService {
    write_buffer: Arc<WriteBuffer>,
    lsn_coordinator: Arc<LSNCoordinator>,
    subscribers: Arc<DurabilitySubscribers>,
}

#[tonic::async_trait]
impl FlightService for ArkaFlightService {
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let mut incoming = request.into_inner();
        let (ack_tx, ack_rx) = mpsc::channel(1000);

        let write_buffer = self.write_buffer.clone();
        let lsn_coordinator = self.lsn_coordinator.clone();
        let subscribers = self.subscribers.clone();

        // Spawn write handler
        tokio::spawn(async move {
            while let Some(Ok(flight_data)) = incoming.next().await {
                let batch = flight_data_to_batch(&flight_data)?;

                // Assign LSN
                let lsn = lsn_coordinator.assign_lsn();

                // Append to buffer (fast!)
                write_buffer.append(batch).await?;

                // Send immediate ack
                let ack = create_ack(lsn, DurabilityLevel::Memory, false)?;
                ack_tx.send(Ok(ack)).await?;

                // Register for durability notifications
                subscribers.subscribe(lsn, ack_tx.clone());
            }
            Ok::<_, Status>(())
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(ack_rx))))
    }
}

fn create_ack(
    lsn: LSN,
    durability: DurabilityLevel,
    is_update: bool,
) -> Result<FlightData, Status> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("lsn", DataType::UInt64, false),
        Field::new("durability_level", DataType::Utf8, false),
        Field::new("is_durability_update", DataType::Boolean, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![lsn.0])),
            Arc::new(StringArray::from(vec![durability.as_str()])),
            Arc::new(BooleanArray::from(vec![is_update])),
        ],
    )?;

    Ok(FlightData::from(batch))
}
```
