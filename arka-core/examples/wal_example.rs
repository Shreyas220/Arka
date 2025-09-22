use arka_core::lsn::LSNcoordinator;
use arka_core::wal::{ChangeType, WalConfig, WalRecord, WalWriter};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Arka WAL Example");

    // Create temporary directory for WAL
    // let temp_dir = TempDir::new()?;
    let temp_dir = PathBuf::from("/Users/shreyas/personel-project/Arka/arka-core/wal");
    println!("📁 WAL directory: {:?}", temp_dir);

    // Configure WAL
    let config = WalConfig {
        segment_size_limit: 1024,                   // 1KB instead of 128MB
        segment_time_limit: Duration::from_secs(1), // 1 second instead of 5 minutes
        base_path: temp_dir.clone(),
        flush_batch_size: 3, // Small batch for demo
        ..Default::default()
    };

    // Create LSN coordinator
    let lsn_coordinator = Arc::new(LSNcoordinator::new());

    // Create WAL writer
    let mut wal = WalWriter::new(config, lsn_coordinator.clone())?;

    println!("\n📝 Writing records to WAL...");

    // Write some sample records
    let records = vec![
        WalRecord {
            lsn: arka_core::lsn::LSN::ZERO, // Will be assigned
            timestamp: 0,                   // Will be assigned
            change_type: ChangeType::Insert,
            table_id: 1,
            key: Some(b"user_123".to_vec()),
            data: b"{'name': 'Alice', 'age': 30}".to_vec(),
        },
        WalRecord {
            lsn: arka_core::lsn::LSN::ZERO,
            timestamp: 0,
            change_type: ChangeType::UpdateBefore,
            table_id: 1,
            key: Some(b"user_123".to_vec()),
            data: b"{'name': 'Alice', 'age': 30}".to_vec(),
        },
        WalRecord {
            lsn: arka_core::lsn::LSN::ZERO,
            timestamp: 0,
            change_type: ChangeType::UpdateAfter,
            table_id: 1,
            key: Some(b"user_123".to_vec()),
            data: b"{'name': 'Alice', 'age': 31}".to_vec(),
        },
        WalRecord {
            lsn: arka_core::lsn::LSN::ZERO,
            timestamp: 0,
            change_type: ChangeType::Insert,
            table_id: 1,
            key: Some(b"user_456".to_vec()),
            data: b"{'name': 'Bob', 'age': 25}".to_vec(),
        },
    ];

    // Write records
    for (i, record) in records.into_iter().enumerate() {
        let lsn = wal.write_record(record).await?;
        println!("  ✅ Record {} written with LSN: {}", i + 1, lsn.0);

        // Show flush behavior
        let durable_lsn = lsn_coordinator.get_wal_durable_lsn();
        println!("     Durable LSN: {}", durable_lsn.0);
    }

    // Force final flush
    wal.flush().await?;
    let final_durable_lsn = lsn_coordinator.get_wal_durable_lsn();
    println!("\n🔄 Final flush - Durable LSN: {}", final_durable_lsn.0);

    // Show current segment info
    if let Some((base_lsn, record_count, size_bytes)) = wal.get_current_segment_info() {
        println!("\n📊 Current segment info:");
        println!("   Base LSN: {}", base_lsn.0);
        println!("   Record count: {}", record_count);
        println!("   Size: {} bytes", size_bytes);
    }

    // Close WAL gracefully
    wal.close().await?;

    // List created files
    println!("\n📁 Created WAL files:");
    let segments_dir = temp_dir.join("segments");
    if segments_dir.exists() {
        println!("  📂 segments/");
        for entry in std::fs::read_dir(segments_dir)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            println!(
                "    📄 {} ({} bytes)",
                entry.file_name().to_string_lossy(),
                metadata.len()
            );
        }
    }

    println!("\n✨ WAL example completed successfully!");

    Ok(())
}
