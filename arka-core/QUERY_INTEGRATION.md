# Query Integration Guide

## Overview

This guide shows how to integrate the DataFusion pruning engine into Arka for querying.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Query Flow                                                 │
│                                                             │
│  SQL → DataFusion → ArkaTableProvider → PruningEngine →   │
│         SegmentPruningStatistics → Pruned Segments →       │
│         DataFusion Execution                                │
└─────────────────────────────────────────────────────────────┘
```

## Method 1: Arrow Flight DoGet (Recommended)

Implement `do_get` in `ArkaFlightService` to enable SQL queries via Flight.

### Implementation

Add to `src/flight/service.rs`:

```rust
use datafusion::prelude::*;
use crate::query::{ArkaTableProvider, PruningEngine};
use crate::indexes::{PKIndex, SegmentIndex};

impl ArkaFlightService {
    // ... existing code ...

    async fn do_get(
        &self,
        request: tonic::Request<Ticket>,
    ) -> Result<tonic::Response<Self::DoGetStream>, tonic::Status> {
        let ticket = request.into_inner();

        // Parse SQL from ticket
        let sql = String::from_utf8(ticket.ticket.to_vec())
            .map_err(|e| tonic::Status::invalid_argument(format!("Invalid SQL: {}", e)))?;

        println!("🔍 Executing SQL: {}", sql);

        // Step 1: Create DataFusion context
        let ctx = SessionContext::new();

        // Step 2: Register all tables
        for table_metadata in self.catalog.list_tables() {
            let table_name = &table_metadata.name;

            let table = self.catalog
                .get_table(&table_metadata.identifier())
                .ok_or_else(|| tonic::Status::not_found(format!("Table {} not found", table_name)))?;

            // Get indexes from table's storage layer
            // In production: Load from table.write_buffer or storage
            let pk_index = table.write_buffer.pk_index.clone();
            let segment_index = table.write_buffer.segment_index.clone();

            let pruning_engine = Arc::new(PruningEngine::new(pk_index, segment_index));

            let table_provider = Arc::new(ArkaTableProvider::new(
                table_name.clone(),
                table.schema(),
                pruning_engine,
            ));

            ctx.register_table(table_name.as_str(), table_provider)
                .map_err(|e| tonic::Status::internal(format!("Failed to register table: {}", e)))?;
        }

        // Step 3: Execute SQL query
        let df = ctx.sql(&sql).await
            .map_err(|e| tonic::Status::internal(format!("Query error: {}", e)))?;

        // Step 4: Collect results and convert to FlightData stream
        let results = df.collect().await
            .map_err(|e| tonic::Status::internal(format!("Execution error: {}", e)))?;

        // Convert RecordBatches to FlightData
        let (tx, rx) = mpsc::channel(results.len());

        tokio::spawn(async move {
            for batch in results {
                let flight_data = FlightData::from_batch(&batch);
                if tx.send(Ok(flight_data)).await.is_err() {
                    break;
                }
            }
        });

        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}
```

### Client Usage (Python)

```python
import pyarrow.flight as flight

# Connect to Arka
client = flight.FlightClient('grpc://localhost:8815')

# Execute SQL via do_get
sql = "SELECT * FROM orders WHERE amount > 100 AND status = 'shipped'"
ticket = flight.Ticket(sql.encode('utf-8'))

# Get results
flight_stream = client.do_get(ticket)
table = flight_stream.read_all()

print(table.to_pandas())
```

## Method 2: Standalone Query Example

For testing and development, use the example:

```bash
cargo run --example query_example
```

This shows:
- How to set up DataFusion context
- How to register ArkaTableProvider
- How to execute SQL with pruning
- How to inspect execution plans

### Example Code

```rust
use datafusion::prelude::*;
use arka_core::query::{ArkaTableProvider, PruningEngine};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load catalog
    let catalog = ArkCatalog::new("/tmp/arka_data")?;
    catalog.load()?;

    // Create DataFusion context
    let ctx = SessionContext::new();

    // Register table
    let table = catalog.get_table(&TableIdentifier::new("orders")).unwrap();
    let pruning_engine = Arc::new(PruningEngine::new(pk_index, segment_index));

    ctx.register_table("orders", Arc::new(ArkaTableProvider::new(
        "orders".to_string(),
        table.schema(),
        pruning_engine,
    )))?;

    // Execute SQL
    let df = ctx.sql("SELECT * FROM orders WHERE amount > 100").await?;
    let results = df.collect().await?;

    Ok(())
}
```

## Method 3: Direct API (Advanced)

Call pruning directly without SQL:

```rust
use datafusion::logical_expr::{Expr, col, lit};

// Create filters programmatically
let filters = vec![
    col("amount").gt(lit(100)),
    col("status").eq(lit("shipped")),
];

// Prune segments
let pruning_engine = PruningEngine::new(pk_index, segment_index);
let pruned_paths = pruning_engine.prune_with_datafusion(&filters, schema)?;

// Now scan only pruned segments manually
for path in pruned_paths {
    let reader = FileReader::try_new(File::open(path)?, None)?;
    // Process batches...
}
```

## Integration Checklist

To fully integrate querying:

1. ✅ **Pruning Engine** - Implemented with DataFusion
2. ✅ **SegmentPruningStatistics** - Adapter for min/max stats
3. ✅ **ArkaTableProvider** - DataFusion table provider
4. ⏳ **Flight DoGet** - Add SQL query endpoint
5. ⏳ **Index Loading** - Load segment metadata on startup
6. ⏳ **Memory + Disk Query** - Query both hot and cold tier

## Performance Tips

### Pruning Effectiveness

The pruning works like this:

```
Query: SELECT * FROM orders WHERE amount > 100

Segments: 1000 total

Layer 1 (PK Index):     If query has pk=X → O(1) → 1 segment
Layer 2 (Time Range):   If query has time filter → ~100 segments
Layer 3 (Column Stats): amount > 100 → ~10 segments

Final: Scan 10 segments instead of 1000 (99% reduction!)
```

### Best Practices

1. **Use time filters**: Most effective for pruning
2. **Use PK equality**: O(1) lookup for point queries
3. **Use column ranges**: Leverages min/max statistics
4. **Combine filters**: More filters = better pruning

### Example Queries

```sql
-- Best: PK + time (1 segment)
SELECT * FROM orders WHERE order_id = '12345' AND time > '2024-01-01'

-- Good: Time + range (10-50 segments)
SELECT * FROM orders WHERE time > '2024-01-01' AND amount > 100

-- OK: Range only (100-500 segments)
SELECT * FROM orders WHERE amount > 100 AND status = 'shipped'

-- Poor: No filters (all segments)
SELECT * FROM orders
```

## Next Steps

1. Implement `do_get` in Flight service
2. Add index loading on table open
3. Test with real workload
4. Optimize for your query patterns
