use std::collections::{HashMap, HashSet};
use crate::storage::segment::SegmentId;
use super::RowLocation;

/// Primary key index for O(1) row lookups
///
/// Maintains a mapping from primary key values to their exact location
/// in the storage system. Also tracks reverse mapping for efficient pruning.
///
/// Memory usage: ~64 bytes per key for single primary key
#[derive(Debug)]
pub struct PKIndex {
    /// Main index: primary key → row location
    /// This enables O(1) lookups for CDC operations and point queries
    key_to_location: HashMap<String, RowLocation>,

    /// Reverse index: segment → set of keys
    /// This enables O(1) cleanup when a segment is pruned
    segment_to_keys: HashMap<SegmentId, HashSet<String>>,

    /// Statistics
    total_keys: usize,
}

impl PKIndex {
    /// Create a new empty primary key index
    pub fn new() -> Self {
        Self {
            key_to_location: HashMap::new(),
            segment_to_keys: HashMap::new(),
            total_keys: 0,
        }
    }

    /// Create with initial capacity hint for better memory allocation
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            key_to_location: HashMap::with_capacity(capacity),
            segment_to_keys: HashMap::new(),
            total_keys: 0,
        }
    }

    /// Insert a single key-location mapping
    pub fn insert(&mut self, key: String, location: RowLocation) {
        // Update reverse index
        self.segment_to_keys
            .entry(location.segment_id)
            .or_insert_with(HashSet::new)
            .insert(key.clone());

        // Insert into main index
        self.key_to_location.insert(key, location);
        self.total_keys = self.key_to_location.len();
    }

    /// Batch insert for segment flush - more efficient than individual inserts
    ///
    /// # Arguments
    /// * `segment_id` - The segment these keys belong to
    /// * `keys` - Vector of (key, row_index) pairs
    pub fn insert_batch(&mut self, segment_id: SegmentId, keys: Vec<(String, u32)>) {
        if keys.is_empty() {
            return;
        }

        // Pre-allocate reverse index set
        let mut segment_keys = HashSet::with_capacity(keys.len());

        // Insert all keys
        for (key, row_index) in keys {
            let location = RowLocation::new(segment_id, row_index);
            self.key_to_location.insert(key.clone(), location);
            segment_keys.insert(key);
        }

        // Update reverse index once
        self.segment_to_keys.insert(segment_id, segment_keys);
        self.total_keys = self.key_to_location.len();
    }

    /// Lookup a key - O(1) operation
    ///
    /// Returns the exact location of the row if it exists
    pub fn get(&self, key: &str) -> Option<RowLocation> {
        self.key_to_location.get(key).copied()
    }

    /// Check if a key exists - O(1) operation
    pub fn contains_key(&self, key: &str) -> bool {
        self.key_to_location.contains_key(key)
    }

    /// Remove a key from the index (e.g., after a DELETE operation)
    ///
    /// Returns the location if the key existed
    pub fn remove(&mut self, key: &str) -> Option<RowLocation> {
        if let Some(location) = self.key_to_location.remove(key) {
            // Also remove from reverse index
            if let Some(segment_keys) = self.segment_to_keys.get_mut(&location.segment_id) {
                segment_keys.remove(key);
            }
            self.total_keys = self.key_to_location.len();
            Some(location)
        } else {
            None
        }
    }

    /// Prune all keys for a segment
    ///
    /// This is called after a segment is committed to Iceberg and can be
    /// removed from the hot tier. Efficiently removes all keys in the segment.
    pub fn prune_segment(&mut self, segment_id: SegmentId) -> usize {
        let removed_count = if let Some(keys) = self.segment_to_keys.remove(&segment_id) {
            let count = keys.len();
            // Remove all keys from main index
            for key in keys {
                self.key_to_location.remove(&key);
            }
            count
        } else {
            0
        };

        self.total_keys = self.key_to_location.len();
        removed_count
    }

    /// Get all keys for a segment (useful for debugging/inspection)
    pub fn keys_for_segment(&self, segment_id: SegmentId) -> Option<&HashSet<String>> {
        self.segment_to_keys.get(&segment_id)
    }

    /// Number of keys in the index
    pub fn len(&self) -> usize {
        self.total_keys
    }

    /// Check if index is empty
    pub fn is_empty(&self) -> bool {
        self.total_keys == 0
    }

    /// Number of segments tracked
    pub fn segment_count(&self) -> usize {
        self.segment_to_keys.len()
    }

    /// Estimate memory usage
    ///
    /// Each entry consumes approximately:
    /// - String key: ~20 bytes average
    /// - HashMap overhead: ~32 bytes
    /// - RowLocation: 12 bytes
    /// - Reverse index: ~20 bytes per key
    /// Total: ~84 bytes per key (conservative estimate)
    pub fn memory_usage(&self) -> usize {
        self.total_keys * 84
    }

    /// Get statistics about the index
    pub fn stats(&self) -> PKIndexStats {
        PKIndexStats {
            total_keys: self.total_keys,
            segment_count: self.segment_to_keys.len(),
            memory_bytes: self.memory_usage(),
        }
    }

    /// Clear the entire index
    pub fn clear(&mut self) {
        self.key_to_location.clear();
        self.segment_to_keys.clear();
        self.total_keys = 0;
    }
}

impl Default for PKIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the primary key index
#[derive(Debug, Clone)]
pub struct PKIndexStats {
    pub total_keys: usize,
    pub segment_count: usize,
    pub memory_bytes: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut index = PKIndex::new();
        assert_eq!(index.len(), 0);
        assert!(index.is_empty());

        let location = RowLocation::new(1000, 42);
        index.insert("key1".to_string(), location);

        assert_eq!(index.len(), 1);
        assert!(!index.is_empty());
        assert_eq!(index.get("key1"), Some(location));
        assert_eq!(index.get("key2"), None);
    }

    #[test]
    fn test_batch_insert() {
        let mut index = PKIndex::new();

        let keys = vec![
            ("key1".to_string(), 0),
            ("key2".to_string(), 1),
            ("key3".to_string(), 2),
        ];

        index.insert_batch(1000, keys);

        assert_eq!(index.len(), 3);
        assert_eq!(index.segment_count(), 1);
        assert_eq!(index.get("key1").unwrap().segment_id, 1000);
        assert_eq!(index.get("key1").unwrap().row_index, 0);
    }

    #[test]
    fn test_remove() {
        let mut index = PKIndex::new();
        let location = RowLocation::new(1000, 42);
        index.insert("key1".to_string(), location);

        assert!(index.contains_key("key1"));
        let removed = index.remove("key1");
        assert_eq!(removed, Some(location));
        assert!(!index.contains_key("key1"));
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_prune_segment() {
        let mut index = PKIndex::new();

        // Insert keys for multiple segments
        index.insert_batch(1000, vec![
            ("key1".to_string(), 0),
            ("key2".to_string(), 1),
        ]);
        index.insert_batch(2000, vec![
            ("key3".to_string(), 0),
            ("key4".to_string(), 1),
        ]);

        assert_eq!(index.len(), 4);
        assert_eq!(index.segment_count(), 2);

        // Prune first segment
        let removed = index.prune_segment(1000);
        assert_eq!(removed, 2);
        assert_eq!(index.len(), 2);
        assert_eq!(index.segment_count(), 1);

        // Keys from segment 1000 should be gone
        assert!(!index.contains_key("key1"));
        assert!(!index.contains_key("key2"));

        // Keys from segment 2000 should still exist
        assert!(index.contains_key("key3"));
        assert!(index.contains_key("key4"));
    }

    #[test]
    fn test_keys_for_segment() {
        let mut index = PKIndex::new();
        index.insert_batch(1000, vec![
            ("key1".to_string(), 0),
            ("key2".to_string(), 1),
        ]);

        let keys = index.keys_for_segment(1000).unwrap();
        assert_eq!(keys.len(), 2);
        assert!(keys.contains("key1"));
        assert!(keys.contains("key2"));
    }

    #[test]
    fn test_memory_estimation() {
        let mut index = PKIndex::new();
        let initial_memory = index.memory_usage();

        // Add 1000 keys
        for i in 0..1000 {
            index.insert(format!("key_{}", i), RowLocation::new(1000, i as u32));
        }

        let memory = index.memory_usage();
        assert!(memory > initial_memory);
        // Rough check: 1000 keys * ~64 bytes should be in ballpark
        assert!(memory > 50_000 && memory < 150_000);
    }
}
