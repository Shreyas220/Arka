/// Example: Querying data using DataFusion with pruning
///
/// This example shows how to:
/// 1. Set up a query context with DataFusion
/// 2. Register Arka tables with the pruning engine
/// 3. Execute SQL queries with automatic segment pruning
///
/// Run with: cargo run --example query_example
use arka_core::catalog::ArkCatalog;
use arka_core::indexes::{pk_index::PKIndex, segment_index::SegmentIndex};
use arka_core::query::{ArkaTableProvider, PruningEngine};
use datafusion::prelude::*;
use std::sync::{Arc, RwLock};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Arka Query Example\n");

    // Step 1: Initialize catalog
    let data_path = "/tmp/arka_data";
    println!("📁 Loading catalog from: {}", data_path);

    let mut catalog = ArkCatalog::new(data_path)?;
    catalog.load()?;

    let tables = catalog.list_tables();
    if tables.is_empty() {
        println!("⚠️  No tables found! Create tables first using the Flight server.");
        return Ok(());
    }

    println!("✅ Found {} table(s)\n", tables.len());

    // Step 2: Create DataFusion session context
    let ctx = SessionContext::new();

    // Step 3: Register each table with DataFusion
    for table_metadata in &tables {
        let table_name = &table_metadata.name;
        println!("📊 Registering table: {}", table_name);

        // Get table from catalog
        let table = catalog
            .get_table(&table_metadata.identifier())
            .ok_or_else(|| format!("Table {} not found", table_name))?;

        // Access indexes from the table's write buffer
        // Note: In production, you'd get these from the table's storage layer
        let pk_index = Arc::new(RwLock::new(PKIndex::new()));
        let segment_index = Arc::new(RwLock::new(SegmentIndex::new()));

        // TODO: Load segment metadata into segment_index from disk
        // This would scan the table's data directory and populate the index
        // For now, we'll work with whatever segments were loaded during catalog.load()

        // Create pruning engine
        let pruning_engine = Arc::new(PruningEngine::new(pk_index, segment_index));

        // Create Arka table provider
        let schema = table.schema();
        let table_provider = Arc::new(ArkaTableProvider::new(
            table_name.clone(),
            schema,
            pruning_engine,
        ));

        // Register with DataFusion
        ctx.register_table(table_name.as_str(), table_provider)?;
        println!("   ✓ Registered {}", table_name);
    }

    println!("\n🎯 Ready to query! Example queries:\n");

    // Example 1: Simple SELECT with filter
    println!("=== Example 1: Simple filter ===");
    let sql1 = "SELECT * FROM orders WHERE amount > 100 LIMIT 10";
    println!("SQL: {}", sql1);

    match ctx.sql(sql1).await {
        Ok(df) => {
            println!("Executing query...");
            let results = df.collect().await?;
            println!("Results: {} batches", results.len());

            // Show first batch
            if let Some(batch) = results.first() {
                println!("First batch: {} rows", batch.num_rows());
                println!("{}", arrow::util::pretty::print_batches(&[batch.clone()])?);
            }
        }
        Err(e) => println!("Error: {}", e),
    }

    println!("\n=== Example 2: Multiple conditions ===");
    let sql2 = "SELECT * FROM orders WHERE amount > 100 AND status = 'shipped' LIMIT 10";
    println!("SQL: {}", sql2);

    match ctx.sql(sql2).await {
        Ok(df) => {
            println!("Executing query...");

            // Show the execution plan (includes pruning info)
            println!("\nExecution Plan:");
            println!("{}", df.clone().into_optimized_plan()?);

            let results = df.collect().await?;
            println!("\nResults: {} batches", results.len());

            if let Some(batch) = results.first() {
                println!("First batch: {} rows", batch.num_rows());
                println!("{}", arrow::util::pretty::print_batches(&[batch.clone()])?);
            }
        }
        Err(e) => println!("Error: {}", e),
    }

    println!("\n=== Example 3: Aggregation ===");
    let sql3 = "SELECT status, COUNT(*) as count, SUM(amount) as total \
                FROM orders \
                WHERE amount > 50 \
                GROUP BY status";
    println!("SQL: {}", sql3);

    match ctx.sql(sql3).await {
        Ok(df) => {
            println!("Executing query...");
            let results = df.collect().await?;
            println!("Results: {} batches", results.len());

            if let Some(batch) = results.first() {
                println!("{}", arrow::util::pretty::print_batches(&[batch.clone()])?);
            }
        }
        Err(e) => println!("Error: {}", e),
    }

    println!("\n✅ Query examples complete!");

    Ok(())
}
