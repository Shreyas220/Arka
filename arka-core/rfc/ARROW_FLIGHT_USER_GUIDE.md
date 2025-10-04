# Arka Arrow Flight User Guide

**Version:** 1.0.0-alpha
**Last Updated:** 2024-10-04

---

## Table of Contents

1. [Introduction](#introduction)
2. [Quick Start](#quick-start)
3. [Concepts](#concepts)
4. [Language-Specific Guides](#language-specific-guides)
5. [Advanced Usage](#advanced-usage)
6. [Best Practices](#best-practices)
7. [Troubleshooting](#troubleshooting)

---

## Introduction

Arka provides a high-performance Arrow Flight API for streaming data ingestion. Unlike traditional RPC-based systems, Arka's streaming API decouples write acknowledgment from durability, enabling:

- **10,000+ writes/sec per client** (100x faster than synchronous writes)
- **Multi-language support** (Python, Java, Rust, Go, JavaScript)
- **Fine-grained durability control** (choose your own guarantees)
- **Zero-copy data transfer** (Arrow IPC format across all languages)

### Key Features

```
✓ Bidirectional streaming (write batches, receive acks simultaneously)
✓ Async durability notifications (know when data is safe)
✓ Backpressure handling (never overwhelm the server)
✓ Graceful reconnection (recover from network failures)
```

---

## Quick Start

### Python (Recommended for Getting Started)

```python
import pyarrow as pa
import pyarrow.flight as flight
import pandas as pd

# 1. Connect to Arka
client = flight.connect("grpc://localhost:8815")

# 2. Start streaming session
descriptor = flight.FlightDescriptor.for_path("streaming_write")
writer, reader = client.do_exchange(descriptor)

# 3. Write data (fast!)
df = pd.DataFrame({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'timestamp': pd.Timestamp.now()
})
batch = pa.RecordBatch.from_pandas(df)

writer.begin(batch.schema)
writer.write_batch(batch)

# 4. Process acknowledgments
for ack_batch in reader:
    ack_df = ack_batch.to_pandas()
    print(ack_df)
    #     lsn durability_level  is_durability_update
    # 0  1000          MEMORY                 False
    # 1  1000      LOCAL_DISK                  True

writer.done_writing()
```

**Output:**
```
   lsn durability_level  is_durability_update              timestamp
0 1000          MEMORY                 False  2024-10-04 12:00:00.123
1 1000      LOCAL_DISK                  True  2024-10-04 12:00:00.135
2 1000  OBJECT_STORAGE                  True  2024-10-04 12:00:00.234
```

---

## Concepts

### Durability Tiers

Your data progresses through four tiers:

| Tier | Latency | Survives | When to Use |
|------|---------|----------|-------------|
| **MEMORY** | ~0.1ms | Process crash: ❌ | High-throughput logging |
| **LOCAL_DISK** | ~10ms | Process crash: ✓<br>Machine failure: ❌ | Application-level durability |
| **OBJECT_STORAGE** | ~100ms | Machine failure: ✓ | Production durability |
| **COMMITTED** | ~60s | External queries: ✓ | Data warehouse integration |

**Example Timeline:**

```
t=0ms:   Write LSN 1000 → acknowledged as MEMORY
t=10ms:  Flushed to disk → notification: MEMORY → LOCAL_DISK
t=150ms: Uploaded to S3  → notification: LOCAL_DISK → OBJECT_STORAGE
t=60s:   Iceberg commit  → notification: OBJECT_STORAGE → COMMITTED
```

### Log Sequence Number (LSN)

Every write receives a globally unique, monotonically increasing LSN:

```python
lsn_1 = client.write(batch_1)  # LSN: 1000
lsn_2 = client.write(batch_2)  # LSN: 1001
lsn_3 = client.write(batch_3)  # LSN: 1002

# LSNs are ordered:
assert lsn_1 < lsn_2 < lsn_3
```

**Use LSNs to:**
- Track which writes are durable
- Implement idempotent retries
- Coordinate distributed systems

### Acknowledgment Types

Arka sends two types of acknowledgments:

1. **Initial Ack** (`is_durability_update: false`)
   - Sent immediately when write is accepted
   - Always at `MEMORY` tier
   - Confirms LSN assignment

2. **Durability Update** (`is_durability_update: true`)
   - Sent when data reaches new tier
   - Progression: MEMORY → LOCAL_DISK → OBJECT_STORAGE → COMMITTED
   - Asynchronous (timing varies)

---

## Language-Specific Guides

### Python

#### Installation

```bash
pip install pyarrow
```

#### Basic Write Pattern

```python
import pyarrow as pa
import pyarrow.flight as flight

# Connect
client = flight.connect("grpc://localhost:8815")

# Write session
descriptor = flight.FlightDescriptor.for_path("streaming_write")
writer, reader = client.do_exchange(descriptor)

# Write multiple batches
for i in range(100):
    batch = create_batch(i)
    writer.write_batch(batch)

# Process acks
for ack in reader:
    print(f"Received {len(ack)} acks")

writer.done_writing()
```

#### Waiting for Durability

```python
import threading
import queue

# Track durability state
durability_state = {}
durable_queue = queue.Queue()

def process_acks(reader):
    """Background thread: process acks"""
    for ack_batch in reader:
        df = ack_batch.to_pandas()

        for _, row in df.iterrows():
            lsn = row['lsn']
            level = row['durability_level']

            durability_state[lsn] = level

            if level == 'OBJECT_STORAGE':
                durable_queue.put(lsn)

# Start ack processor
ack_thread = threading.Thread(target=process_acks, args=(reader,))
ack_thread.start()

# Write batches
lsns = []
for i in range(100):
    writer.write_batch(batch)
    lsns.append(i)  # Track LSNs

# Wait for all to reach S3
for expected_lsn in lsns:
    durable_lsn = durable_queue.get()
    print(f"LSN {durable_lsn} is safe on S3!")

writer.done_writing()
ack_thread.join()
```

#### Fire-and-Forget Pattern

```python
# For high-throughput logging (don't wait for durability)
for log_entry in log_stream:
    batch = log_to_arrow(log_entry)
    writer.write_batch(batch)
    # Don't process acks - maximum throughput!

# Acks are still sent, but client ignores them
```

---

### Java

#### Maven Dependency

```xml
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>flight-core</artifactId>
    <version>14.0.0</version>
</dependency>
<dependency>
    <groupId>org.apache.arrow</groupId>
    <artifactId>flight-grpc</artifactId>
    <version>14.0.0</version>
</dependency>
```

#### Basic Example

```java
import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;

public class ArkaClient {
    public static void main(String[] args) throws Exception {
        BufferAllocator allocator = new RootAllocator();

        // Connect
        FlightClient client = FlightClient.builder()
            .allocator(allocator)
            .location(Location.forGrpcInsecure("localhost", 8815))
            .build();

        // Start exchange
        FlightDescriptor descriptor = FlightDescriptor.path("streaming_write");
        ExchangeReaderWriter stream = client.doExchange(descriptor);

        // Write batches
        VectorSchemaRoot root = createBatch(allocator);
        stream.getWriter().start(root);

        for (int i = 0; i < 100; i++) {
            fillBatch(root, i);
            stream.getWriter().putNext();
        }

        // Read acks
        while (stream.getReader().next()) {
            VectorSchemaRoot ackRoot = stream.getReader().getRoot();
            BigIntVector lsnVector = (BigIntVector) ackRoot.getVector("lsn");
            VarCharVector durabilityVector = (VarCharVector) ackRoot.getVector("durability_level");

            for (int i = 0; i < ackRoot.getRowCount(); i++) {
                long lsn = lsnVector.get(i);
                String durability = new String(durabilityVector.get(i));
                System.out.println("LSN " + lsn + ": " + durability);
            }
        }

        stream.getWriter().completed();
        client.close();
    }
}
```

#### Async Ack Processing

```java
// Separate thread for ack processing
ExecutorService executor = Executors.newSingleThreadExecutor();

CompletableFuture<Void> ackProcessor = CompletableFuture.runAsync(() -> {
    try {
        while (stream.getReader().next()) {
            processAcks(stream.getReader().getRoot());
        }
    } catch (Exception e) {
        e.printStackTrace();
    }
}, executor);

// Main thread writes
for (int i = 0; i < 1000; i++) {
    stream.getWriter().putNext(createBatch(i));
}

stream.getWriter().completed();
ackProcessor.join();  // Wait for acks to finish
```

---

### Rust

#### Cargo Dependency

```toml
[dependencies]
arrow-flight = "53.0.0"
tokio = { version = "1", features = ["full"] }
futures = "0.3"
```

#### Example

```rust
use arrow_flight::FlightClient;
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect
    let channel = tonic::transport::Channel::from_static("http://localhost:8815")
        .connect()
        .await?;
    let mut client = FlightClient::new(channel);

    // Start exchange
    let descriptor = FlightDescriptor::new_path(vec!["streaming_write".into()]);
    let (mut tx, mut rx) = client.do_exchange(descriptor).await?;

    // Spawn ack processor
    tokio::spawn(async move {
        while let Some(Ok(ack_data)) = rx.next().await {
            let batch = flight_data_to_batch(ack_data).unwrap();
            println!("Received {} acks", batch.num_rows());
        }
    });

    // Write batches
    for i in 0..100 {
        let batch = create_batch(i)?;
        tx.send(batch).await?;
    }

    Ok(())
}
```

---

### Go

#### Installation

```bash
go get github.com/apache/arrow/go/v14/arrow
go get github.com/apache/arrow/go/v14/arrow/flight
```

#### Example

```go
package main

import (
    "context"
    "fmt"
    "github.com/apache/arrow/go/v14/arrow/flight"
)

func main() {
    client, err := flight.NewClientWithMiddleware(
        "localhost:8815",
        nil,
        nil,
    )
    if err != nil {
        panic(err)
    }
    defer client.Close()

    ctx := context.Background()

    // Start exchange
    stream, err := client.DoExchange(ctx)
    if err != nil {
        panic(err)
    }

    // Write batches
    go func() {
        for i := 0; i < 100; i++ {
            batch := createBatch(i)
            stream.Send(&flight.FlightData{
                DataBody: batch,
            })
        }
        stream.CloseSend()
    }()

    // Read acks
    for {
        ack, err := stream.Recv()
        if err != nil {
            break
        }
        fmt.Printf("Received ack: %v\n", ack)
    }
}
```

---

## Advanced Usage

### Pattern 1: Conditional Durability

Only wait for durability on critical writes:

```python
def write_with_policy(batch, is_critical):
    """Write batch with conditional durability check"""
    writer.write_batch(batch)

    if is_critical:
        # Critical data: wait for S3
        wait_for_durability(batch.lsn, 'OBJECT_STORAGE')
    # else: fire and forget
```

### Pattern 2: Batch Acknowledgment Tracking

```python
class AckTracker:
    def __init__(self):
        self.pending = {}  # lsn -> durability level

    def on_ack(self, lsn, durability):
        self.pending[lsn] = durability

    def wait_all_durable(self, lsns, target_level='OBJECT_STORAGE'):
        """Wait until all LSNs reach target durability"""
        while True:
            all_durable = all(
                self.pending.get(lsn, 'MEMORY') >= target_level
                for lsn in lsns
            )
            if all_durable:
                return
            time.sleep(0.1)

# Usage
tracker = AckTracker()
lsns = []

for batch in batches:
    writer.write_batch(batch)
    lsns.append(batch.lsn)

# Background: update tracker from acks
# ...

tracker.wait_all_durable(lsns, 'LOCAL_DISK')
print("All batches on disk!")
```

### Pattern 3: Watermark-Based Cleanup

```python
class LocalCache:
    def __init__(self):
        self.cache = {}  # lsn -> data
        self.durable_watermark = 0

    def on_watermark_update(self, s3_lsn):
        """Server broadcasts watermarks periodically"""
        self.durable_watermark = s3_lsn

        # Cleanup old data
        to_delete = [
            lsn for lsn in self.cache
            if lsn <= self.durable_watermark
        ]
        for lsn in to_delete:
            del self.cache[lsn]
            print(f"Deleted LSN {lsn} from cache (safe on S3)")

cache = LocalCache()

# Write to cache + Arka
data = generate_data()
cache.cache[lsn] = data
writer.write_batch(data)

# Background: process watermark updates
for ack in reader:
    if ack.is_watermark_update:
        cache.on_watermark_update(ack.s3_lsn)
```

### Pattern 4: Retry with Idempotency

```python
def write_with_retry(batch, max_retries=3):
    """Retry writes with idempotent semantics"""
    for attempt in range(max_retries):
        try:
            writer.write_batch(batch)

            # Wait for ack
            ack = next(reader)
            return ack.lsn

        except Exception as e:
            if attempt == max_retries - 1:
                raise
            print(f"Retry {attempt + 1}/{max_retries}")
            time.sleep(2 ** attempt)  # Exponential backoff
```

---

## Best Practices

### 1. Always Process Acks Asynchronously

**❌ Bad: Blocking on acks**
```python
for batch in batches:
    writer.write_batch(batch)
    ack = next(reader)  # Blocks! Slow!
```

**✅ Good: Separate threads**
```python
# Thread 1: Write batches
for batch in batches:
    writer.write_batch(batch)

# Thread 2: Process acks
for ack in reader:
    handle_ack(ack)
```

### 2. Batch Writes for Efficiency

**❌ Bad: One row at a time**
```python
for row in rows:
    batch = pa.RecordBatch.from_pydict({'id': [row.id]})
    writer.write_batch(batch)  # Overhead!
```

**✅ Good: Accumulate batches**
```python
rows_buffer = []
for row in rows:
    rows_buffer.append(row)

    if len(rows_buffer) >= 1000:
        batch = pa.RecordBatch.from_pydict(rows_buffer)
        writer.write_batch(batch)
        rows_buffer = []
```

### 3. Set Appropriate Durability Expectations

**For metrics/telemetry:**
```python
# Fire and forget (MEMORY is fine)
writer.write_batch(metrics_batch)
```

**For application state:**
```python
# Wait for disk (LOCAL_DISK)
writer.write_batch(user_data)
wait_for_durability(lsn, 'LOCAL_DISK')
```

**For financial transactions:**
```python
# Wait for replication (OBJECT_STORAGE)
writer.write_batch(payment)
wait_for_durability(lsn, 'OBJECT_STORAGE')
```

### 4. Handle Backpressure Gracefully

```python
import queue

write_queue = queue.Queue(maxsize=1000)

def writer_thread():
    while True:
        try:
            batch = write_queue.get(timeout=1)
            writer.write_batch(batch)
        except queue.Empty:
            continue

# Main thread
try:
    write_queue.put(batch, timeout=5)
except queue.Full:
    print("Backpressure! Server is overwhelmed.")
    # Option 1: Drop data (metrics)
    # Option 2: Block (critical data)
    # Option 3: Write to local buffer
```

### 5. Monitor Durability Lag

```python
import time

last_write_lsn = 0
last_durable_lsn = 0

# Write loop
for batch in batches:
    writer.write_batch(batch)
    last_write_lsn += 1

# Ack loop
for ack in reader:
    if ack.durability_level == 'OBJECT_STORAGE':
        last_durable_lsn = ack.lsn

        lag = last_write_lsn - last_durable_lsn
        if lag > 10000:
            print(f"WARNING: Durability lag = {lag}")
```

---

## Troubleshooting

### Issue: "Connection refused"

**Cause:** Arka server not running or wrong address

**Solution:**
```python
# Check server is running
curl http://localhost:8815

# Try different port
client = flight.connect("grpc://localhost:8816")
```

### Issue: "Schema mismatch"

**Cause:** Sent batch schema doesn't match server expectation

**Solution:**
```python
# Server expects these columns:
required_schema = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('name', pa.string()),
    pa.field('timestamp', pa.timestamp('us'))
])

# Ensure your batch matches
batch = pa.RecordBatch.from_pandas(df, schema=required_schema)
```

### Issue: "Acks received but never reach OBJECT_STORAGE"

**Cause:** S3 uploader might be disabled or failing

**Solution:**
```python
# Check server logs
# Query server status
status = client.get_flight_info(FlightDescriptor.for_path("status"))
print(status)

# Expected output:
# {
#   "disk_lsn": 1000,
#   "s3_lsn": 950,    ← Should be advancing
#   "lag_seconds": 10
# }
```

### Issue: "Client hangs on writer.done_writing()"

**Cause:** Server might be waiting for more data

**Solution:**
```python
# Set timeout
import signal

def timeout_handler(signum, frame):
    raise TimeoutError("Writer close timeout")

signal.signal(signal.SIGALRM, timeout_handler)
signal.alarm(10)  # 10 second timeout

try:
    writer.done_writing()
finally:
    signal.alarm(0)
```

### Issue: "Memory growing unbounded on client"

**Cause:** Not consuming acks fast enough

**Solution:**
```python
# Ensure ack processor is running
import threading

def ack_consumer(reader):
    for ack in reader:
        pass  # Consume and discard

threading.Thread(target=ack_consumer, args=(reader,), daemon=True).start()
```

---

## Performance Tuning

### Client-Side

```python
# Increase batch size (more throughput, higher latency)
batch_size = 10000  # rows per batch

# Adjust write buffer
write_queue = queue.Queue(maxsize=100)  # Buffer 100 batches

# Use multiprocessing for CPU-bound work
from multiprocessing import Pool

with Pool(4) as pool:
    batches = pool.map(create_batch, range(1000))
    for batch in batches:
        writer.write_batch(batch)
```

### Server-Side Configuration

Contact your Arka administrator to adjust:

```yaml
# Server config (example)
write_buffer:
  max_buffer_size: 128MB       # Larger = fewer flushes
  flush_interval: 100ms        # Shorter = lower latency
  max_unflushed_bytes: 512MB   # Higher = more throughput

durability:
  s3_upload_interval: 5s       # Shorter = faster S3 durability
  iceberg_commit_interval: 60s
```

---

## FAQ

**Q: Can I use Arrow Flight with Pandas?**

Yes! Convert via PyArrow:
```python
import pandas as pd
import pyarrow as pa

df = pd.DataFrame({'id': [1, 2, 3]})
batch = pa.RecordBatch.from_pandas(df)
writer.write_batch(batch)
```

**Q: Is Arrow Flight faster than REST/JSON?**

Yes, significantly:
- Arrow Flight: Zero-copy binary format
- REST/JSON: Serialize/deserialize overhead

Benchmark: 100x faster for large datasets.

**Q: Can I query data written via Arrow Flight?**

Yes! Use Arka's query API or wait for Iceberg commit:
```python
# Wait for COMMITTED tier
wait_for_durability(lsn, 'COMMITTED')

# Now queryable in Iceberg
spark.sql("SELECT * FROM arka.my_table WHERE ...")
```

**Q: What happens if my client crashes?**

Server buffers acks for ~5 minutes. On reconnect:
```python
# Query missed acks
status = client.get_durability_status(last_known_lsn)
for event in status.events:
    print(f"LSN {event.lsn} reached {event.durability}")
```

**Q: Can multiple clients write concurrently?**

Yes! Each client gets independent LSN assignments.

---

## Examples Repository

Find complete examples at:
- Python: `examples/python/arrow_flight_client.py`
- Java: `examples/java/ArkaFlightClient.java`
- Rust: `examples/rust/flight_client.rs`
- Go: `examples/go/flight_client.go`

---

## Support

- **Documentation:** https://arka.dev/docs
- **GitHub Issues:** https://github.com/arka/arka-core/issues
- **Community:** Discord #arrow-flight channel

---

**Next Steps:**
1. Try the [Quick Start](#quick-start)
2. Read [Concepts](#concepts)
3. Run examples for your language
4. Build your integration!
