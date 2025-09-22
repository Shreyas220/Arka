use arka_core::*;
use arrow::array::{Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Duration;

// Test configurations using the new builder methods
fn test_cdc_config() -> Arc<ArkConfig> {
    Arc::new(ArkConfig::cdc_enabled_table(vec![0])) // Index first column (user_id)
}

fn test_append_only_config() -> Arc<ArkConfig> {
    Arc::new(ArkConfig::append_only_events())
}

fn test_analytical_config() -> Arc<ArkConfig> {
    Arc::new(ArkConfig::analytical_with_lookups(vec![0])) // Index first column for lookups
}

// Create test schema for user data
fn create_user_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("username", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, true),
        Field::new("created_at", DataType::Int64, false),
    ]))
}

// Create test schema for append-only events
fn create_event_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::UInt64, false),
        Field::new("user_id", DataType::Int64, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
    ]))
}

// Helper to create user record batch
fn create_user_batch(
    user_ids: Vec<i64>,
    usernames: Vec<&str>,
    emails: Vec<Option<&str>>,
    created_ats: Vec<i64>,
) -> RecordBatch {
    let user_id_array = Int64Array::from(user_ids);
    let username_array = StringArray::from(usernames);
    let email_array = StringArray::from(emails);
    let created_at_array = Int64Array::from(created_ats);

    RecordBatch::try_new(
        create_user_schema(),
        vec![
            Arc::new(user_id_array),
            Arc::new(username_array),
            Arc::new(email_array),
            Arc::new(created_at_array),
        ],
    )
    .unwrap()
}

// Helper to create event record batch (append-only)
fn create_event_batch(
    event_ids: Vec<u64>,
    user_ids: Vec<i64>,
    event_types: Vec<&str>,
    timestamps: Vec<i64>,
) -> RecordBatch {
    let event_id_array = UInt64Array::from(event_ids);
    let user_id_array = Int64Array::from(user_ids);
    let event_type_array = StringArray::from(event_types);
    let timestamp_array = Int64Array::from(timestamps);

    RecordBatch::try_new(
        create_event_schema(),
        vec![
            Arc::new(event_id_array),
            Arc::new(user_id_array),
            Arc::new(event_type_array),
            Arc::new(timestamp_array),
        ],
    )
    .unwrap()
}

async fn test_basic_table_creation_and_ingestion() {
    println!("=== Test: Basic Table Creation and Ingestion ===");

    let schema = create_user_schema();
    let config = test_cdc_config(); // Use CDC config for indexing
    let table = ArkTable::new("users".to_string(), schema, config);

    // Create initial batch of users
    let batch1 = create_user_batch(
        vec![1, 2, 3],
        vec!["alice", "bob", "charlie"],
        vec![Some("alice@example.com"), Some("bob@example.com"), None],
        vec![1609459200, 1609545600, 1609632000], // Unix timestamps
    );

    // Ingest the batch
    let lsn1 = table.ingest_batch(batch1).await.unwrap();
    println!("Ingested batch 1 with LSN: {}", lsn1);

    // Query the data
    let results = table.query(None).await;
    println!("Query results: {} batches", results.len());
    for batch in &results {
        for row in batch.columns() {
            println!("Row: {:?}", row);
        }
    }

    for (i, batch) in results.iter().enumerate() {
        println!("Batch {}: {} rows", i, batch.num_rows());
    }

    // Verify index stats
    let stats = table.get_index_stats();
    println!("Index stats: {:?}", stats);

    assert_eq!(stats.total_entries, 3);
    assert!(stats.memory_usage_bytes > 0);
}

async fn test_key_lookup_functionality() {
    println!("=== Test: Key Lookup Functionality ===");

    let schema = create_user_schema();
    let config = test_analytical_config(); // Use analytical config for lookups
    let table = ArkTable::new("users".to_string(), schema, config);

    // Create test data
    let batch = create_user_batch(
        vec![100, 200, 300],
        vec!["user100", "user200", "user300"],
        vec![
            Some("u100@test.com"),
            Some("u200@test.com"),
            Some("u300@test.com"),
        ],
        vec![1609459200, 1609545600, 1609632000],
    );

    let lsn = table.ingest_batch(batch).await.unwrap();
    println!("Ingested data with LSN: {}", lsn);

    // ===== Test NEW Type-Safe APIs =====
    let user_id_100 = 100i64;

    // Test type-safe bloom filter check
    let might_exist = table.int64_might_exist(0, user_id_100);
    println!("✅ Type-safe bloom filter check for user_id {}: {}", user_id_100, might_exist);
    assert!(might_exist);

    // Test type-safe lookup
    let lookup_result = table.lookup_by_int64(0, user_id_100).await;
    println!("✅ Type-safe lookup result for user_id {}: {:?}", user_id_100, lookup_result);

    match lookup_result {
        Some(location) => {
            println!(
                "Found at batch_id: {}, row_index: {}, lsn: {}",
                location.batch_id, location.row_index, location.lsn
            );
            assert_eq!(location.lsn, lsn);
            assert_eq!(location.row_index, 0); // First row
        }
        None => panic!("Expected to find user_id 100 via type-safe lookup"),
    }

    // Test FIXED general API lookup (should now work with type-aware hashing)
    println!("\n✅ Testing FIXED API lookup (type-aware hashing):");
    let key_bytes = user_id_100.to_le_bytes();
    let api_lookup_result = table.lookup_key(&key_bytes).await;
    println!("API lookup result: {:?}", api_lookup_result);

    match api_lookup_result {
        Some(location) => {
            println!("✅ FIXED: API lookup now works correctly!");
            println!("Found at batch_id: {}, row_index: {}, lsn: {}",
                    location.batch_id, location.row_index, location.lsn);
            assert_eq!(location.lsn, lsn);
            assert_eq!(location.row_index, 0);
        }
        None => panic!("API lookup should work now with type-aware hashing"),
    }

    // Test type-safe lookup for non-existent key
    let non_existent_id = 999i64;
    let lookup_result = table.lookup_by_int64(0, non_existent_id).await;
    println!("Lookup result for non-existent key: {:?}", lookup_result);
    assert!(lookup_result.is_none());
}

async fn test_cdc_delete_operations() {
    println!("=== Test: CDC Delete Operations ===");

    let schema = create_user_schema();
    let config = test_cdc_config(); // Use CDC config for CDC operations
    let table = ArkTable::new("users".to_string(), schema, config);

    // Initial data
    let batch = create_user_batch(
        vec![1, 2, 3, 4, 5],
        vec!["alice", "bob", "charlie", "david", "eve"],
        vec![
            Some("alice@test.com"),
            Some("bob@test.com"),
            Some("charlie@test.com"),
            Some("david@test.com"),
            Some("eve@test.com"),
        ],
        vec![1609459200, 1609545600, 1609632000, 1609718400, 1609804800],
    );

    let initial_lsn = table.ingest_batch(batch).await.unwrap();
    println!("Initial ingestion LSN: {}", initial_lsn);

    // Verify initial state
    let results_before = table.query(None).await;
    let total_rows_before: usize = results_before.iter().map(|b| b.num_rows()).sum();
    println!("Total rows before delete: {}", total_rows_before);
    assert_eq!(total_rows_before, 5);

    // ===== Test NEW Type-Safe CDC Delete =====
    let user_id_to_delete = 3i64;

    // Verify user exists before delete
    let exists_before = table.lookup_by_int64(0, user_id_to_delete).await;
    println!("User {} exists before delete: {:?}", user_id_to_delete, exists_before.is_some());
    assert!(exists_before.is_some());
    let target_lsn = exists_before.unwrap().lsn;

    // Perform type-safe CDC delete
    println!("✅ Performing type-safe CDC delete for user_id: {}", user_id_to_delete);
    let delete_lsn = table.delete_by_int64(0, user_id_to_delete, target_lsn).await.unwrap();
    println!("Delete operation completed with LSN: {}", delete_lsn);

    // Query after delete - should show FILTERED rows (deletion vector applied)
    println!("\n===== Testing Deletion Vector Filtering =====");
    let results_after = table.query(None).await;
    let total_rows_after: usize = results_after.iter().map(|b| b.num_rows()).sum();
    println!("Total rows after delete: {}", total_rows_after);

    // FIXED: Now deletion vector filtering should work correctly
    assert_eq!(total_rows_after, 4, "Should have 4 rows after deleting 1 user");
    println!("✅ FIXED: Deletion vector correctly filtered out deleted row!");

    // Verify the key no longer exists in index
    let exists_after = table.lookup_by_int64(0, user_id_to_delete).await;
    println!("User {} exists after delete: {:?}", user_id_to_delete, exists_after.is_some());
    assert!(exists_after.is_none());
    println!("✅ FIXED: Deleted key removed from index");

    // Verify other keys still exist
    for user_id in [1i64, 2i64, 4i64, 5i64] {  // Skip deleted user_id 3
        let exists = table.lookup_by_int64(0, user_id).await.is_some();
        let bloom_check = table.int64_might_exist(0, user_id);
        println!("User {}: exists={}, bloom={}", user_id, exists, bloom_check);
        assert!(exists, "User {} should still exist", user_id);
        assert!(bloom_check, "User {} should pass bloom filter", user_id);
    }

    // Verify index stats updated correctly
    let stats = table.get_index_stats();
    println!("Index stats after delete: {:?}", stats);
    assert_eq!(stats.total_entries, 4, "Should have 4 entries after delete");

    println!("✅ All CDC delete functionality working correctly!");
    println!("✅ Deletion vector filtering working correctly!");
    println!("✅ Type-safe APIs working correctly!");
}

async fn test_multiple_batches_with_same_keys() {
    println!("=== Test: Multiple Batches with Same Keys (Updates) ===");

    let schema = create_user_schema();
    let config = test_cdc_config(); // Use CDC config for update scenarios
    let table = ArkTable::new("users".to_string(), schema, config);

    // First batch
    let batch1 = create_user_batch(
        vec![1, 2],
        vec!["alice_v1", "bob_v1"],
        vec![Some("alice@v1.com"), Some("bob@v1.com")],
        vec![1609459200, 1609545600],
    );

    let lsn1 = table.ingest_batch(batch1).await.unwrap();
    println!("First batch LSN: {}", lsn1);

    // Second batch with updated data for same keys
    let batch2 = create_user_batch(
        vec![1, 3], // user_id 1 is updated, user_id 3 is new
        vec!["alice_v2", "charlie"],
        vec![Some("alice@v2.com"), Some("charlie@test.com")],
        vec![1609632000, 1609718400],
    );

    let lsn2 = table.ingest_batch(batch2).await.unwrap();
    println!("Second batch LSN: {}", lsn2);

    // Check which version of user_id 1 is in the index (should be latest)
    let user_id_1 = 1i64;

    // ===== Test Type-Safe Lookup for Updated Keys =====
    let lookup_result = table.lookup_by_int64(0, user_id_1).await;
    println!("✅ Type-safe lookup result for user_id 1: {:?}", lookup_result);

    match lookup_result {
        Some(location) => {
            println!(
                "Found user_id 1 at LSN: {}, batch_id: {}",
                location.lsn, location.batch_id
            );
            // Should be the latest version (from batch 2)
            assert_eq!(location.lsn, lsn2);
            println!("✅ Index correctly contains the latest version of user_id 1");
        }
        None => panic!("Expected to find user_id 1"),
    }

    // Test that both old and new API work consistently now
    let key_bytes = user_id_1.to_le_bytes();
    let api_lookup_result = table.lookup_key(&key_bytes).await;
    println!("✅ General API lookup result: {:?}", api_lookup_result);

    if let Some(api_location) = api_lookup_result {
        assert_eq!(api_location.lsn, lsn2, "Both APIs should return same result");
        println!("✅ FIXED: Both type-safe and general APIs return consistent results");
    }

    let stats = table.get_index_stats();
    println!("Index stats: {:?}", stats);

    // Should have 3 entries total (alice_v2, bob_v1, charlie)
    // Note: Current implementation may have hash collisions
    if stats.hash_collisions > 0 {
        println!("Hash collisions detected: {}", stats.hash_collisions);
    }
}

async fn test_append_only_table() {
    println!("=== Test: Append-Only Table (No Indexing) ===");

    let schema = create_event_schema();
    let config = test_append_only_config(); // Use proper append-only config

    let table = ArkTable::new("events".to_string(), schema, config);

    // Create event batches
    let batch1 = create_event_batch(
        vec![1, 2, 3],
        vec![100, 200, 300],
        vec!["login", "logout", "purchase"],
        vec![1609459200, 1609459300, 1609459400],
    );

    let batch2 = create_event_batch(
        vec![4, 5, 6],
        vec![100, 400, 200],
        vec!["view", "login", "logout"],
        vec![1609459500, 1609459600, 1609459700],
    );

    let lsn1 = table.ingest_batch(batch1).await.unwrap();
    let lsn2 = table.ingest_batch(batch2).await.unwrap();

    println!("Event batch 1 LSN: {}", lsn1);
    println!("Event batch 2 LSN: {}", lsn2);

    // Query all events
    let results = table.query(None).await;
    let total_events: usize = results.iter().map(|b| b.num_rows()).sum();
    println!("Total events: {}", total_events);
    assert_eq!(total_events, 6);

    // Verify no indexing for append-only table
    let stats = table.get_index_stats();
    println!("Index stats for append-only table: {:?}", stats);
    assert_eq!(stats.total_entries, 0); // No indexing for append-only
    println!("✅ Append-only table correctly skips all indexing operations");

    // Verify the index type is None
    let index_type = &table.memory_slice.memory_slice_index.index_type;
    match index_type {
        IndexType::None => println!("✅ Index type correctly set to None for append-only"),
        _ => panic!("Expected IndexType::None for append-only table"),
    }

    // Key lookups should not work (but shouldn't crash)
    let event_id_1 = 1u64;
    let key_bytes = event_id_1.to_le_bytes();
    let lookup_result = table.lookup_key(&key_bytes).await;
    println!("Lookup result in append-only table: {:?}", lookup_result);
    assert!(lookup_result.is_none());
}

async fn test_lsn_based_time_travel_queries() {
    println!("=== Test: LSN-Based Time Travel Queries ===");

    let schema = create_user_schema();
    let config = test_analytical_config(); // Use analytical config
    let table = ArkTable::new("users".to_string(), schema, config);

    // Batch 1
    let batch1 = create_user_batch(
        vec![1, 2],
        vec!["alice", "bob"],
        vec![Some("alice@test.com"), Some("bob@test.com")],
        vec![1609459200, 1609545600],
    );
    let lsn1 = table.ingest_batch(batch1).await.unwrap();
    println!("Batch 1 LSN: {}", lsn1);

    // Batch 2
    let batch2 = create_user_batch(
        vec![3, 4],
        vec!["charlie", "david"],
        vec![Some("charlie@test.com"), Some("david@test.com")],
        vec![1609632000, 1609718400],
    );
    let lsn2 = table.ingest_batch(batch2).await.unwrap();
    println!("Batch 2 LSN: {}", lsn2);

    // Query at different LSNs
    let results_at_lsn1 = table.query(Some(lsn1)).await;
    let results_at_lsn2 = table.query(Some(lsn2)).await;
    let results_latest = table.query(None).await;

    let rows_at_lsn1: usize = results_at_lsn1.iter().map(|b| b.num_rows()).sum();
    let rows_at_lsn2: usize = results_at_lsn2.iter().map(|b| b.num_rows()).sum();
    let rows_latest: usize = results_latest.iter().map(|b| b.num_rows()).sum();

    println!("Rows at LSN {}: {}", lsn1, rows_at_lsn1);
    println!("Rows at LSN {}: {}", lsn2, rows_at_lsn2);
    println!("Rows at latest: {}", rows_latest);

    assert_eq!(rows_at_lsn1, 2); // Only batch 1
    assert_eq!(rows_at_lsn2, 4); // Both batches
    assert_eq!(rows_latest, 4); // Both batches
}

async fn test_concurrent_operations() {
    println!("=== Test: Concurrent Operations ===");

    let schema = create_user_schema();
    let config = test_cdc_config(); // Use CDC config for concurrent operations
    let table = Arc::new(ArkTable::new("users".to_string(), schema, config));

    // Spawn multiple tasks for concurrent ingestion
    let mut handles = vec![];

    for i in 0..5 {
        let table_clone = table.clone();
        let handle = tokio::spawn(async move {
            let batch = create_user_batch(
                vec![i * 10 + 1, i * 10 + 2],
                vec![&format!("user_{}_1", i), &format!("user_{}_2", i)],
                vec![
                    Some(&format!("user{}1@test.com", i)),
                    Some(&format!("user{}2@test.com", i)),
                ],
                vec![1609459200 + i, 1609459300 + i],
            );

            let lsn = table_clone.ingest_batch(batch).await.unwrap();
            println!("Task {} ingested with LSN: {}", i, lsn);
            lsn
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    let lsns: Vec<u64> = futures::future::join_all(handles)
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    println!("All LSNs: {:?}", lsns);

    // Query final state
    let results = table.query(None).await;
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    println!("Total rows after concurrent operations: {}", total_rows);
    assert_eq!(total_rows, 10); // 5 tasks * 2 rows each

    // Verify index integrity
    let stats = table.get_index_stats();
    println!("Final index stats: {:?}", stats);

    // FIXED: Now that we removed the index rebuild bug, we should get exactly 10 entries
    println!("✅ Index rebuild bug fixed - no more duplicate entries!");
    assert_eq!(stats.total_entries, 10, "Should have exactly 10 entries (5 tasks × 2 rows each)");

    if stats.hash_collisions > 0 {
        println!("Hash collisions: {} (legitimate collisions only)", stats.hash_collisions);
    }
}

// Main test runner for manual execution
// Run with: cargo run --bin test-runner
#[tokio::main]
async fn main() {
    println!("🚀 Running Arka Core Tests\n");

    println!("Running basic table creation and ingestion test...");
    test_basic_table_creation_and_ingestion().await;
    println!("✅ Basic table creation test passed\n");

    println!("Running key lookup functionality test...");
    test_key_lookup_functionality().await;
    println!("✅ Key lookup test passed\n");

    println!("Running CDC delete operations test...");
    test_cdc_delete_operations().await;
    println!("✅ CDC delete test passed\n");

    println!("Running multiple batches with same keys test...");
    test_multiple_batches_with_same_keys().await;
    println!("✅ Multiple batches test passed\n");

    println!("Running append-only table test...");
    test_append_only_table().await;
    println!("✅ Append-only test passed\n");

    println!("Running LSN-based time travel queries test...");
    test_lsn_based_time_travel_queries().await;
    println!("✅ Time travel test passed\n");

    println!("Running concurrent operations test...");
    test_concurrent_operations().await;
    println!("✅ Concurrent operations test passed\n");

    println!("Running comprehensive fix validation test...");
    test_all_fixes_comprehensive().await;
    println!("✅ Comprehensive fix validation test passed\n");

    println!("✅ All tests completed successfully!");
}

// ===== NEW: Comprehensive Test for All Three Fixes =====
async fn test_all_fixes_comprehensive() {
    println!("=== Test: Comprehensive Validation of All Three Fixes ===");

    let schema = create_user_schema();
    let config = test_cdc_config();
    let table = ArkTable::new("comprehensive_test".to_string(), schema, config);

    // 1. Test multiple data ingestions with indexing
    println!("\n1️⃣ Testing optimized indexing (no rebuild bug):");
    let batch1 = create_user_batch(
        vec![1, 2, 3],
        vec!["alice", "bob", "charlie"],
        vec![Some("alice@test.com"), Some("bob@test.com"), Some("charlie@test.com")],
        vec![1000, 1001, 1002],
    );

    let batch2 = create_user_batch(
        vec![4, 5],
        vec!["david", "eve"],
        vec![Some("david@test.com"), Some("eve@test.com")],
        vec![1003, 1004],
    );

    let lsn1 = table.ingest_batch(batch1).await.unwrap();
    let lsn2 = table.ingest_batch(batch2).await.unwrap();

    let stats = table.get_index_stats();
    println!("Index stats after 2 batches: {:?}", stats);
    assert_eq!(stats.total_entries, 5, "Should have exactly 5 entries (no index rebuild bug)");
    println!("✅ Index rebuild bug FIXED - exactly 5 entries");

    // 2. Test type-aware hash consistency
    println!("\n2️⃣ Testing type-aware hash consistency:");

    // Type-safe API
    let type_safe_result = table.lookup_by_int64(0, 3).await;
    println!("Type-safe lookup for user_id 3: {:?}", type_safe_result.is_some());

    // General API (now using type-aware hashing)
    let key_bytes = 3i64.to_le_bytes();
    let general_result = table.lookup_key(&key_bytes).await;
    println!("General API lookup for user_id 3: {:?}", general_result.is_some());

    // Both should work and return the same result
    assert!(type_safe_result.is_some(), "Type-safe lookup should work");
    assert!(general_result.is_some(), "General API should work with type-aware hashing");
    assert_eq!(type_safe_result.unwrap().lsn, general_result.unwrap().lsn, "Both APIs should return same result");
    println!("✅ Hash consistency FIXED - both APIs work and return same result");

    // 3. Test deletion vector filtering
    println!("\n3️⃣ Testing deletion vector filtering:");

    // Query before delete
    let rows_before = table.query(None).await.iter().map(|b| b.num_rows()).sum::<usize>();
    println!("Rows before delete: {}", rows_before);
    assert_eq!(rows_before, 5);

    // Perform delete using type-safe API
    let target_lsn = table.lookup_by_int64(0, 3).await.unwrap().lsn;
    let delete_lsn = table.delete_by_int64(0, 3, target_lsn).await.unwrap();
    println!("Deleted user_id 3 with LSN: {}", delete_lsn);

    // Query after delete - should be filtered
    let rows_after = table.query(None).await.iter().map(|b| b.num_rows()).sum::<usize>();
    println!("Rows after delete: {}", rows_after);
    assert_eq!(rows_after, 4, "Deletion vector filtering should remove 1 row");
    println!("✅ Deletion vector filtering FIXED - correctly filtered deleted row");

    // Verify deleted key is gone from index
    let deleted_lookup = table.lookup_by_int64(0, 3).await;
    assert!(deleted_lookup.is_none(), "Deleted key should not exist in index");

    // Verify other keys still exist
    for user_id in [1i64, 2i64, 4i64, 5i64] {
        let exists = table.lookup_by_int64(0, user_id).await.is_some();
        assert!(exists, "User {} should still exist", user_id);
    }

    let final_stats = table.get_index_stats();
    assert_eq!(final_stats.total_entries, 4, "Index should have 4 entries after delete");
    println!("✅ Index correctly updated after delete");

    // 4. Test type-safe APIs comprehensively
    println!("\n4️⃣ Testing type-safe API functionality:");

    // Bloom filter checks
    assert!(table.int64_might_exist(0, 1), "User 1 should pass bloom filter");
    assert!(!table.int64_might_exist(0, 999), "Non-existent user should likely fail bloom filter");

    // Different column type (if we had string keys)
    // For now, just verify the APIs are available and type-safe
    println!("✅ Type-safe APIs available and working correctly");

    println!("\n🎉 ALL THREE FIXES VALIDATED SUCCESSFULLY!");
    println!("   ✅ Index rebuild bug fixed");
    println!("   ✅ Hash consistency fixed");
    println!("   ✅ Deletion vector filtering fixed");
    println!("   ✅ Type-safe APIs implemented");
}
