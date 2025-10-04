use arrow::array::{StringArray, TimestampMicrosecondArray, UInt64Array, UInt8Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arka_core::indexes::pk_index::PKIndex;
use arka_core::indexes::segment_index::SegmentIndex;
use arka_core::storage::SegmentFlusher;
use std::sync::{Arc, RwLock};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Arka Segment Writer Example\n");

    // Create data directory
    let data_dir = std::path::PathBuf::from("./arka_data/segments");
    std::fs::create_dir_all(&data_dir)?;
    println!("📁 Data directory: {}", data_dir.display());

    // Initialize indexes
    let segment_index = Arc::new(RwLock::new(SegmentIndex::new()));
    let pk_index = Arc::new(RwLock::new(PKIndex::new()));

    // Create segment flusher
    let flusher = SegmentFlusher::new(
        data_dir.clone(),
        segment_index.clone(),
        pk_index.clone(),
        "id".to_string(),
    )?;

    println!("✅ Initialized segment flusher\n");

    // Create schema with internal columns + user columns
    let schema = Arc::new(Schema::new(vec![
        // User columns
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::UInt8, false),
        // Internal columns
        Field::new("__lsn", DataType::UInt64, false),
        Field::new("__change_type", DataType::UInt8, false), // 0=INSERT, 1=UPDATE, 2=DELETE
        Field::new("__table_id", DataType::UInt64, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
    ]));

    // Batch 1: Initial users
    println!("📝 Writing batch 1: Initial users");
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["user_001", "user_002", "user_003"])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(UInt8Array::from(vec![25, 30, 35])),
            Arc::new(UInt64Array::from(vec![1, 2, 3])),
            Arc::new(UInt8Array::from(vec![0, 0, 0])), // INSERT
            Arc::new(UInt64Array::from(vec![1, 1, 1])), // table_id=1
            Arc::new(TimestampMicrosecondArray::from(vec![
                1700000000000000,
                1700000001000000,
                1700000002000000,
            ])),
        ],
    )?;
    let seg1 = flusher.flush(batch1)?;
    println!("✅ Segment {} written", seg1);

    // Batch 2: More users
    println!("\n📝 Writing batch 2: More users");
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["user_004", "user_005"])),
            Arc::new(StringArray::from(vec!["Diana", "Eve"])),
            Arc::new(UInt8Array::from(vec![28, 32])),
            Arc::new(UInt64Array::from(vec![4, 5])),
            Arc::new(UInt8Array::from(vec![0, 0])),
            Arc::new(UInt64Array::from(vec![1, 1])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                1700000003000000,
                1700000004000000,
            ])),
        ],
    )?;
    let seg2 = flusher.flush(batch2)?;
    println!("✅ Segment {} written", seg2);

    // Batch 3: Updates (simulated as new inserts with UPDATE change_type)
    println!("\n📝 Writing batch 3: Updates");
    let batch3 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["user_002"])),
            Arc::new(StringArray::from(vec!["Bob Updated"])),
            Arc::new(UInt8Array::from(vec![31])),
            Arc::new(UInt64Array::from(vec![6])),
            Arc::new(UInt8Array::from(vec![1])), // UPDATE
            Arc::new(UInt64Array::from(vec![1])),
            Arc::new(TimestampMicrosecondArray::from(vec![1700000005000000])),
        ],
    )?;
    let seg3 = flusher.flush(batch3)?;
    println!("✅ Segment {} written", seg3);

    // Print statistics
    println!("\n📊 Statistics:");
    println!("─────────────────────────────────────");

    let seg_index = segment_index.read().unwrap();
    println!("Total segments: {}", seg_index.len());

    for segment_id in seg_index.all_ids() {
        let seg = seg_index.get(segment_id).unwrap();
        println!(
            "  Segment {}: {} rows, {} bytes, time_range: [{}, {}]",
            segment_id, seg.row_count, seg.byte_size, seg.time_range.0, seg.time_range.1
        );
    }

    let pk_idx = pk_index.read().unwrap();
    println!("\nPrimary key index:");
    println!("  Total keys: {}", pk_idx.len());
    println!("  Segments tracked: {}", pk_idx.segment_count());

    // Show some key lookups
    println!("\n🔍 Key lookups:");
    if let Some(loc) = pk_idx.get("user_001") {
        println!("  user_001 → segment={}, row={}", loc.segment_id, loc.row_index);
    }
    if let Some(loc) = pk_idx.get("user_002") {
        println!("  user_002 → segment={}, row={} (latest)", loc.segment_id, loc.row_index);
    }
    if let Some(loc) = pk_idx.get("user_005") {
        println!("  user_005 → segment={}, row={}", loc.segment_id, loc.row_index);
    }

    // List files created
    println!("\n📂 Files created:");
    for entry in std::fs::read_dir(&data_dir)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        println!(
            "  {} ({} bytes)",
            entry.file_name().to_string_lossy(),
            metadata.len()
        );
    }

    println!("\n✨ Done! Check {} for Arrow IPC files", data_dir.display());

    Ok(())
}
