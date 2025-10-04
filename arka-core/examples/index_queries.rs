use arka_core::indexes::pk_index::PKIndex;
use arka_core::indexes::segment_index::SegmentIndex;
use arka_core::storage::SegmentFlusher;
use arrow::array::{StringArray, TimestampMicrosecondArray, UInt64Array, UInt8Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use std::fs::File;
use std::sync::{Arc, RwLock};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Arka Index Query Example\n");

    // Setup
    let data_dir = std::path::PathBuf::from("./arka_data/index_demo");
    std::fs::create_dir_all(&data_dir)?;

    let segment_index = Arc::new(RwLock::new(SegmentIndex::new()));
    let pk_index = Arc::new(RwLock::new(PKIndex::new()));

    let flusher = SegmentFlusher::new(
        data_dir.clone(),
        segment_index.clone(),
        pk_index.clone(),
        "user_id".to_string(),
    )?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("value", DataType::UInt64, false),
        Field::new("__lsn", DataType::UInt64, false),
        Field::new("__change_type", DataType::UInt8, false),
        Field::new("__table_id", DataType::UInt64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));

    println!("📝 Writing test data across different time ranges...\n");

    // Segment 1: Morning events (9am - 10am)
    let morning_start = 1700038800000000i64; // 9:00 AM
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
            Arc::new(StringArray::from(vec!["login", "login", "login"])),
            Arc::new(UInt64Array::from(vec![1, 1, 1])),
            Arc::new(UInt64Array::from(vec![1, 2, 3])),
            Arc::new(UInt8Array::from(vec![0, 0, 0])),
            Arc::new(UInt64Array::from(vec![1, 1, 1])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                morning_start,
                morning_start + 1800000000, // 30 min later
                morning_start + 3600000000, // 1 hour later
            ])),
        ],
    )?;
    let seg1 = flusher.flush(batch1)?;
    println!("  ✅ Segment {}: Morning events (9am-10am)", seg1);

    // Segment 2: Afternoon events (2pm - 3pm)
    let afternoon_start = 1700056800000000i64; // 2:00 PM
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["alice", "diana", "eve"])),
            Arc::new(StringArray::from(vec!["purchase", "login", "purchase"])),
            Arc::new(UInt64Array::from(vec![100, 1, 250])),
            Arc::new(UInt64Array::from(vec![4, 5, 6])),
            Arc::new(UInt8Array::from(vec![0, 0, 0])),
            Arc::new(UInt64Array::from(vec![1, 1, 1])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                afternoon_start,
                afternoon_start + 1200000000, // 20 min later
                afternoon_start + 2400000000, // 40 min later
            ])),
        ],
    )?;
    let seg2 = flusher.flush(batch2)?;
    println!("  ✅ Segment {}: Afternoon events (2pm-3pm)", seg2);

    // Segment 3: Evening events (7pm - 8pm)
    let evening_start = 1700074800000000i64; // 7:00 PM
    let batch3 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["bob", "charlie", "frank"])),
            Arc::new(StringArray::from(vec!["logout", "purchase", "login"])),
            Arc::new(UInt64Array::from(vec![0, 500, 1])),
            Arc::new(UInt64Array::from(vec![7, 8, 9])),
            Arc::new(UInt8Array::from(vec![0, 0, 0])),
            Arc::new(UInt64Array::from(vec![1, 1, 1])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                evening_start,
                evening_start + 1800000000, // 30 min later
                evening_start + 3000000000, // 50 min later
            ])),
        ],
    )?;
    let seg3 = flusher.flush(batch3)?;
    println!("  ✅ Segment {}: Evening events (7pm-8pm)", seg3);

    println!("\n{}", "=".repeat(60));
    println!("📊 INDEX STATISTICS");
    println!("{}\n", "=".repeat(60));

    let seg_idx = segment_index.read().unwrap();
    let pk_idx = pk_index.read().unwrap();

    println!("Segment Index:");
    println!("  Total segments: {}", seg_idx.len());
    for seg_id in seg_idx.all_ids() {
        let seg = seg_idx.get(seg_id).unwrap();
        println!(
            "    Segment {}: {} rows | time_range=[{}, {}] | lsn_range=[{}, {}]",
            seg_id,
            seg.row_count,
            seg.time_range.0,
            seg.time_range.1,
            seg.lsn_range.0,
            seg.lsn_range.1
        );
    }

    println!("\nPrimary Key Index:");
    println!("  Total keys: {}", pk_idx.len());
    println!("  Segments tracked: {}", pk_idx.segment_count());
    println!("  Memory usage: ~{} bytes", pk_idx.memory_usage());

    // Show the key distribution
    for seg_id in seg_idx.all_ids() {
        if let Some(keys) = pk_idx.keys_for_segment(seg_id) {
            println!("    Segment {}: {} keys → {:?}", seg_id, keys.len(), keys);
        }
    }

    println!("\n{}", "=".repeat(60));
    println!("🔍 QUERY 1: Primary Key Lookups (O(1) operations)");
    println!("{}\n", "=".repeat(60));

    let test_keys = vec!["alice", "bob", "diana", "nonexistent"];
    for key in test_keys {
        print!("  Lookup '{}': ", key);
        if let Some(loc) = pk_idx.get(key) {
            println!(
                "✅ Found at segment={}, row={}",
                loc.segment_id, loc.row_index
            );

            // Read the actual data from the segment
            let segment_path = data_dir.join(format!("segment_{:010}.arrow", loc.segment_id));
            if let Ok(file) = File::open(&segment_path) {
                if let Ok(reader) = FileReader::try_new(file, None) {
                    for batch_result in reader {
                        if let Ok(batch) = batch_result {
                            if (loc.row_index as usize) < batch.num_rows() {
                                let user_col = batch
                                    .column(0)
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .unwrap();
                                let event_col = batch
                                    .column(1)
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .unwrap();
                                let value_col = batch
                                    .column(2)
                                    .as_any()
                                    .downcast_ref::<UInt64Array>()
                                    .unwrap();
                                let ts_col = batch
                                    .column(6)
                                    .as_any()
                                    .downcast_ref::<TimestampMicrosecondArray>()
                                    .unwrap();

                                println!(
                                    "       Data: user={}, event={}, value={}, timestamp={}",
                                    user_col.value(loc.row_index as usize),
                                    event_col.value(loc.row_index as usize),
                                    value_col.value(loc.row_index as usize),
                                    ts_col.value(loc.row_index as usize)
                                );
                            }
                        }
                    }
                }
            }
        } else {
            println!("❌ Not found");
        }
    }

    println!("\n{}", "=".repeat(60));
    println!("🔍 QUERY 2: Time-Range Queries (Segment Pruning)");
    println!("{}\n", "=".repeat(60));

    // Query 1: Morning only (should hit segment 1)
    println!("Query: Events between 9am-10am");
    let morning_segments = seg_idx.find_by_time_range(morning_start, morning_start + 3600000000);
    println!(
        "  📌 Segments to scan: {:?} (out of {} total)",
        morning_segments,
        seg_idx.len()
    );
    println!(
        "  💡 Segment pruning: Skipped {} segments!",
        seg_idx.len() - morning_segments.len()
    );

    // Query 2: Afternoon only (should hit segment 2)
    println!("\nQuery: Events between 2pm-3pm");
    let afternoon_segments =
        seg_idx.find_by_time_range(afternoon_start, afternoon_start + 3600000000);
    println!(
        "  📌 Segments to scan: {:?} (out of {} total)",
        afternoon_segments,
        seg_idx.len()
    );
    println!(
        "  💡 Segment pruning: Skipped {} segments!",
        seg_idx.len() - afternoon_segments.len()
    );

    // Query 3: Full day (should hit all segments)
    println!("\nQuery: All events (full day)");
    let all_segments = seg_idx.find_by_time_range(morning_start, evening_start + 3600000000);
    println!(
        "  📌 Segments to scan: {:?} (out of {} total)",
        all_segments,
        seg_idx.len()
    );
    println!("  💡 No pruning possible - query spans entire dataset");

    // Query 4: Narrow window that spans segments (afternoon to evening)
    println!("\nQuery: Events between 2pm-8pm (spans multiple segments)");
    let cross_segments = seg_idx.find_by_time_range(afternoon_start, evening_start + 3600000000);
    println!(
        "  📌 Segments to scan: {:?} (out of {} total)",
        cross_segments,
        seg_idx.len()
    );
    println!(
        "  💡 Segment pruning: Skipped {} segments!",
        seg_idx.len() - cross_segments.len()
    );

    println!("\n{}", "=".repeat(60));
    println!("🔍 QUERY 3: LSN-Based Queries (Point-in-time)");
    println!("{}\n", "=".repeat(60));

    println!("Query: Find segment containing LSN=5");
    let lsn_segments = seg_idx.find_by_lsn(5);
    println!("  📌 Segments containing LSN=5: {:?}", lsn_segments);

    println!("\nQuery: Find segment containing LSN=8");
    let lsn_segments = seg_idx.find_by_lsn(8);
    println!("  📌 Segments containing LSN=8: {:?}", lsn_segments);

    println!("\n{}", "=".repeat(60));
    println!("🧹 QUERY 4: Segment Pruning Simulation");
    println!("{}\n", "=".repeat(60));

    println!("Scenario: All segments committed to Iceberg, checking prunability");

    // Mark segments as committed
    drop(seg_idx);
    {
        let mut seg_idx_mut = segment_index.write().unwrap();
        for seg_id in seg_idx_mut.all_ids() {
            if let Some(seg) = seg_idx_mut.get_mut(seg_id) {
                seg.committed_to_iceberg = true;
            }
        }
    }

    let seg_idx = segment_index.read().unwrap();
    let min_age_secs = 0; // Prune immediately after commit
    let prunable = seg_idx.prunable_segments(min_age_secs);

    println!(
        "  Committed segments: {:?}",
        seg_idx.uncommitted_segments().is_empty()
    );
    println!(
        "  Prunable segments (age > {}s): {:?}",
        min_age_secs, prunable
    );
    println!(
        "  💡 {} segments ready to be deleted from hot storage",
        prunable.len()
    );

    // Show memory impact of pruning
    println!("\n📊 Memory Impact of Pruning:");
    let keys_before = pk_idx.len();
    let segments_before = seg_idx.len();

    // Simulate pruning segment 1
    drop(seg_idx);
    drop(pk_idx);
    {
        let mut pk_idx_mut = pk_index.write().unwrap();
        pk_idx_mut.prune_segment(1);
        println!("  After pruning segment 1:");
        println!(
            "    Keys: {} → {} (freed {} keys)",
            keys_before,
            pk_idx_mut.len(),
            keys_before - pk_idx_mut.len()
        );
        println!(
            "    Memory freed: ~{} bytes",
            (keys_before - pk_idx_mut.len()) * 84
        );
    }

    println!("\n{}", "=".repeat(60));
    println!("✨ Index Performance Summary");
    println!("{}\n", "=".repeat(60));

    let pk_idx = pk_index.read().unwrap();
    let seg_idx = segment_index.read().unwrap();

    println!("✅ Primary Key Index:");
    println!("   • O(1) lookups for point queries");
    println!(
        "   • {} keys tracked across {} segments",
        pk_idx.len(),
        pk_idx.segment_count()
    );
    println!(
        "   • Memory: ~{} bytes (~{} KB)",
        pk_idx.memory_usage(),
        pk_idx.memory_usage() / 1024
    );
    println!("   • Efficient pruning with reverse index");

    println!("\n✅ Segment Index:");
    println!("   • {} segments tracked", seg_idx.len());
    println!(
        "   • Time-range pruning: up to {}% segments skipped",
        ((seg_idx.len() - 1) * 100 / seg_idx.len().max(1))
    );
    println!("   • LSN-based point-in-time queries");
    println!("   • Lifecycle tracking (committed/uncommitted)");

    println!("\n✨ Done! Indexes are working perfectly.");

    Ok(())
}
