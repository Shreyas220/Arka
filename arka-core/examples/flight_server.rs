/// Arka Flight Server Example
///
/// This example demonstrates how to:
/// 1. Set up an Arka catalog with persistent storage
/// 2. Start an Arrow Flight server
/// 3. Accept bidirectional streaming writes
/// 4. Send durability acknowledgments back to clients
///
/// Run with: cargo run --example flight_server
///
/// Then connect with Python client: python examples/flight_client.py
use arka_core::catalog::{
    ArkCatalog, PartitionField, PartitionSpec, TableIdentifier, TableMetadata, TableType, Transform,
};
use arka_core::flight::ArkaFlightService;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow_flight::flight_service_server::FlightServiceServer;
use std::sync::Arc;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Starting Arka Flight Server...\n");

    // 1. Initialize catalog
    let data_path = "/tmp/arka_data";
    println!("📁 Initializing catalog at: {}", data_path);

    let mut catalog = ArkCatalog::new(data_path)?;

    // Load existing catalog (if any)
    if let Err(e) = catalog.load() {
        println!("⚠️  Warning: Could not load existing catalog: {}", e);
    } else {
        let table_count = catalog.list_tables().len();
        println!("✅ Loaded catalog with {} existing tables", table_count);
    }

    // 2. Register example tables
    println!("\n📋 Registering example tables...");

    // Example 1: Users table (primary key, partitioned by signup_date)
    let users_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
        Field::new(
            "signup_date",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("age", DataType::Int32, true),
    ]));

    let users_partition_spec = PartitionSpec::new(
        1,
        vec![PartitionField::new(
            3, // signup_date column index (0-indexed: id, name, email, signup_date)
            1000,
            "signup_day".to_string(),
            Transform::Day,
        )],
    );

    let users_metadata = TableMetadata::new(
        TableIdentifier::new("users"),
        users_schema,
        format!("{}/users", data_path),
        TableType::PrimaryKey,
    )
    .with_primary_key("id".to_string())
    .with_partition_spec(users_partition_spec)
    .with_property("write.format.default".to_string(), "parquet".to_string());

    if catalog.get_table_by_name("users").is_none() {
        catalog.register_table(users_metadata)?;
        println!("  ✓ Registered 'users' table (primary key, day-partitioned)");
    } else {
        println!("  ✓ 'users' table already exists");
    }

    // Example 2: Events table (append-only, partitioned by event_hour)
    let events_schema = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Int64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("properties", DataType::Utf8, true),
    ]));

    let events_partition_spec = PartitionSpec::new(
        1,
        vec![PartitionField::new(
            3, // event_time column index
            1001,
            "event_hour".to_string(),
            Transform::Hour,
        )],
    );

    let events_metadata = TableMetadata::new(
        TableIdentifier::new("events"),
        events_schema,
        format!("{}/events", data_path),
        TableType::AppendOnly,
    )
    .with_partition_spec(events_partition_spec);

    if catalog.get_table_by_name("events").is_none() {
        catalog.register_table(events_metadata)?;
        println!("  ✓ Registered 'events' table (append-only, hour-partitioned)");
    } else {
        println!("  ✓ 'events' table already exists");
    }

    // 3. Create Flight service
    let catalog = Arc::new(catalog);
    let flight_service = ArkaFlightService::new(catalog.clone());

    let addr = "0.0.0.0:8815".parse()?;
    println!("\n🌐 Starting Arrow Flight server on {}", addr);
    println!("\n📖 Available tables:");
    for table in catalog.list_tables() {
        println!("   - {}", table.qualified_name());
    }

    println!("\n🔌 Server ready for connections!");
    println!("   Connect with: pyarrow.flight.FlightClient('grpc://localhost:8815')");
    println!("\n💡 Try the Python example: python examples/flight_client.py\n");

    // 4. Start gRPC server
    Server::builder()
        .add_service(FlightServiceServer::new(flight_service))
        .serve(addr)
        .await?;

    Ok(())
}
