/// Arka Rust Client Example
///
/// Demonstrates:
/// 1. Connecting to Arka Flight server
/// 2. Creating a table
/// 3. Listing tables
/// 4. Sending RecordBatches via DoExchange
/// 5. Receiving durability acknowledgments
///
/// Run with: cargo run --example rust_client
use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{Action, FlightData};
use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WriteRequest {
    table_name: String,
    requested_durability: Option<String>,
    idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AckMessage {
    lsn: u64,
    durability_level: String,
    timestamp_us: i64,
    error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CreateTableRequest {
    name: String,
    schema_json: String,
    primary_key: Option<String>,
    table_type: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔌 Connecting to Arka Flight Server...\n");

    // 1. Connect to Arka server
    let mut client = FlightServiceClient::connect("http://localhost:8815").await?;
    println!("✅ Connected to Arka at localhost:8815\n");

    // 2. Create a table
    println!("📋 Creating 'orders' table...");

    // Convert schema to JSON for CreateTable action
    let schema_json = serde_json::json!({
        "fields": [
            {"name": "order_id", "type": "Int64", "nullable": false},
            {"name": "customer_id", "type": "Int64", "nullable": false},
            {"name": "product", "type": "Utf8", "nullable": false},
            {"name": "order_time", "type": "Timestamp", "nullable": false}
        ]
    })
    .to_string();

    let create_req = CreateTableRequest {
        name: "orders".to_string(),
        schema_json,
        primary_key: Some("order_id".to_string()),
        table_type: Some("PrimaryKey".to_string()),
    };

    let create_action = Action {
        r#type: "CreateTable".to_string(),
        body: Bytes::from(serde_json::to_vec(&create_req)?),
    };

    match client.do_action(create_action).await {
        Ok(response) => {
            let mut create_stream = response.into_inner();
            while let Some(result) = create_stream.next().await {
                let result = result?;
                let message = String::from_utf8_lossy(&result.body);
                println!("   ✓ {}\n", message);
            }
        }
        Err(e) => {
            if e.message().contains("already exists") {
                println!("   ℹ️  Table 'orders' already exists (skipping creation)\n");
            } else {
                return Err(e.into());
            }
        }
    }

    // 3. List all tables
    println!("📖 Listing all tables...");

    let list_action = Action {
        r#type: "ListTables".to_string(),
        body: Bytes::new(),
    };

    let mut list_stream = client.do_action(list_action).await?.into_inner();

    while let Some(result) = list_stream.next().await {
        let result = result?;
        let tables: Vec<String> = serde_json::from_slice(&result.body)?;
        println!("   Tables:");
        for table in tables {
            println!("   - {}", table);
        }
    }
    println!();

    // 4. Send batches via DoExchange
    println!("📤 Sending RecordBatches via DoExchange...\n");

    // Create channel for sending FlightData
    let (tx, rx) = tokio::sync::mpsc::channel::<FlightData>(100);

    // Spawn task to send data
    let send_handle = tokio::spawn(async move {
        // First message: WriteRequest metadata
        let write_req = WriteRequest {
            table_name: "orders".to_string(),
            requested_durability: Some("Disk".to_string()), // Wait for disk durability
            idempotency_key: None,
        };

        let write_req_bytes = serde_json::to_vec(&write_req).unwrap();

        let first_msg = FlightData {
            app_metadata: Bytes::from(write_req_bytes),
            data_body: Bytes::new(),
            data_header: Bytes::new(),
            flight_descriptor: None,
        };

        tx.send(first_msg).await.unwrap();
        println!("   ✓ Sent WriteRequest metadata");

        // Create and send batches
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("customer_id", DataType::Int64, false),
            Field::new("product", DataType::Utf8, false),
            Field::new(
                "order_time",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));

        // Batch 1: Orders 1-3
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![101, 102, 103])),
                Arc::new(StringArray::from(vec!["Laptop", "Mouse", "Keyboard"])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    1234567890000000,
                    1234567891000000,
                    1234567892000000,
                ])),
            ],
        )
        .unwrap();

        send_batch_to_channel(&tx, &batch1).await.unwrap();
        println!("   ✓ Sent batch 1 (3 rows)");

        // Batch 2: Orders 4-5
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![4, 5])),
                Arc::new(Int64Array::from(vec![104, 105])),
                Arc::new(StringArray::from(vec!["Monitor", "Headphones"])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    1234567893000000,
                    1234567894000000,
                ])),
            ],
        )
        .unwrap();

        send_batch_to_channel(&tx, &batch2).await.unwrap();
        println!("   ✓ Sent batch 2 (2 rows)");

        // Close the channel
        drop(tx);
    });

    // Convert receiver to stream
    let request_stream = ReceiverStream::new(rx);

    // Call do_exchange
    let mut response_stream = client.do_exchange(request_stream).await?.into_inner();

    // Wait for send task to complete
    send_handle.await?;
    println!("\n📥 Waiting for durability acknowledgments...\n");

    // 5. Receive acknowledgments
    let mut ack_count = 0;
    let mut memory_acks = 0;
    let mut disk_acks = 0;

    // Wait longer to receive all acks (memory + disk)
    let timeout = tokio::time::Duration::from_secs(5);
    let start = tokio::time::Instant::now();

    loop {
        tokio::select! {
            result = response_stream.next() => {
                match result {
                    Some(Ok(flight_data)) => {
                        if !flight_data.app_metadata.is_empty() {
                            let ack: AckMessage = serde_json::from_slice(&flight_data.app_metadata)?;

                            if let Some(error) = &ack.error {
                                println!("   ❌ Error: {}", error);
                            } else {
                                ack_count += 1;
                                match ack.durability_level.to_lowercase().as_str() {
                                    "memory" => memory_acks += 1,
                                    "disk" => disk_acks += 1,
                                    _ => {}
                                }
                                println!(
                                    "   ✅ ACK #{}: LSN={}, Durability={}, Timestamp={}",
                                    ack_count, ack.lsn, ack.durability_level, ack.timestamp_us
                                );
                            }
                        }
                    }
                    Some(Err(e)) => {
                        println!("   ❌ Stream error: {}", e);
                        break;
                    }
                    None => {
                        println!("   ℹ️  Stream closed");
                        break;
                    }
                }
            }
            _ = tokio::time::sleep_until(start + timeout) => {
                println!("   ℹ️  Timeout waiting for more acks");
                break;
            }
        }
    }

    println!(
        "\n🎉 Success! Received {} acknowledgment(s) ({} memory, {} disk)\n",
        ack_count, memory_acks, disk_acks
    );
    println!("💡 Data is now queryable in Arka");

    Ok(())
}

/// Helper function to encode and send a RecordBatch to a channel
async fn send_batch_to_channel(
    tx: &tokio::sync::mpsc::Sender<FlightData>,
    batch: &RecordBatch,
) -> Result<(), Box<dyn std::error::Error>> {
    // Encode batch using Arrow IPC
    let mut buffer = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }

    let flight_data = FlightData {
        app_metadata: Bytes::new(),
        data_body: Bytes::from(buffer),
        data_header: Bytes::new(),
        flight_descriptor: None,
    };

    tx.send(flight_data).await?;
    Ok(())
}
