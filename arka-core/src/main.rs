/// Arka - Real-time Analytical Storage Engine
///
/// Start the Arrow Flight server for accepting streaming writes
/// with LSN tracking and durability guarantees.
///
/// Run with: cargo run
use arka_core::catalog::ArkCatalog;
use arka_core::flight::ArkaFlightService;
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
        println!("   Starting with empty catalog");
    } else {
        let table_count = catalog.list_tables().len();
        if table_count > 0 {
            println!("✅ Loaded catalog with {} existing table(s)", table_count);
        } else {
            println!("✅ Loaded catalog (empty - create tables via CreateTable action)");
        }
    }

    // 2. Create Flight service
    let catalog = Arc::new(catalog);
    let flight_service = ArkaFlightService::new(catalog.clone());

    let addr = "0.0.0.0:8815".parse()?;
    println!("\n🌐 Starting Arrow Flight server on {}", addr);

    let table_count = catalog.list_tables().len();
    if table_count > 0 {
        println!("\n📖 Available tables:");
        for table in catalog.list_tables() {
            println!("   - {}", table.qualified_name());
        }
    }

    println!("\n🔌 Server ready for connections!");
    println!("   Connect with: pyarrow.flight.FlightClient('grpc://localhost:8815')");
    println!("\n💡 Use CreateTable action to create tables, then send batches via DoExchange\n");

    // 3. Start gRPC server
    Server::builder()
        .add_service(FlightServiceServer::new(flight_service))
        .serve(addr)
        .await?;

    Ok(())
}
