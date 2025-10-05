use crate::indexes::{pk_index::PKIndex, segment_index::SegmentIndex};
use crate::query::predicates::{ColumnRange, ExtractedPredicates};
use crate::query::segment_statistics::SegmentPruningStatistics;
use crate::storage::segment::SegmentMeta;
use arrow::datatypes::SchemaRef;
use datafusion::common::DFSchema;
use datafusion::logical_expr::{Expr, Operator};
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::prelude::SessionContext;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

/// 3-layer pruning engine using all available indexes
///
/// Pruning order (fastest to slowest):
/// 1. PKIndex: O(1) point lookup - returns 1 segment
/// 2. SegmentIndex time range: time complexity -> O(segments)
/// 3. ColumnStats range: -> O(segments × columns)  
#[derive(Debug)]
pub struct PruningEngine {
    pk_index: Arc<RwLock<PKIndex>>,
    segment_index: Arc<RwLock<SegmentIndex>>,
}

impl PruningEngine {
    pub fn new(pk_index: Arc<RwLock<PKIndex>>, segment_index: Arc<RwLock<SegmentIndex>>) -> Self {
        Self {
            pk_index,
            segment_index,
        }
    }

    /// Prune segments using all available indexes
    /// Returns list of file paths to scan
    pub fn prune(&self, predicates: &ExtractedPredicates) -> Vec<PathBuf> {
        // Layer 1: Point lookup (highest priority)
        if let Some(pk_value) = &predicates.pk_equality {
            return self.try_point_lookup(pk_value);
        }

        // Layer 2: Get all segments, then filter
        let segment_index = self.segment_index.read().unwrap();
        let mut candidates: Vec<_> = segment_index.all();

        // Layer 2a: Time range pruning
        if let Some((start, end)) = predicates.time_range {
            candidates.retain(|seg| seg.overlaps_time_range(start, end));
        }

        // Layer 3: Column statistics pruning
        for (col_name, range) in &predicates.column_ranges {
            candidates.retain(|seg| self.segment_matches_column_range(seg, col_name, range));
        }

        // Return file paths
        candidates.iter().map(|seg| seg.file_path.clone()).collect()
    }

    fn try_point_lookup(&self, pk_value: &[u8]) -> Vec<PathBuf> {
        let pk_index = self.pk_index.read().unwrap();

        // Convert bytes to string (assumes string PK for now)
        let key = String::from_utf8_lossy(pk_value).to_string();

        if let Some(location) = pk_index.get(&key) {
            let segment_index = self.segment_index.read().unwrap();
            if let Some(seg) = segment_index.get(location.segment_id) {
                return vec![seg.file_path.clone()];
            }
        }

        // Key not found
        vec![]
    }

    fn segment_matches_column_range(
        &self,
        segment: &SegmentMeta,
        column: &str,
        range: &ColumnRange,
    ) -> bool {
        let Some(stats) = segment.get_column_stats(column) else {
            return true; // No stats, can't prune
        };

        match range {
            ColumnRange::Equals(val) => stats.might_contain(val),
            ColumnRange::GreaterThan(val) => {
                // Keep if max > val
                stats
                    .max_value
                    .as_ref()
                    .map_or(true, |max| max.as_slice() > val.as_slice())
            }
            ColumnRange::LessThan(val) => {
                // Keep if min < val
                stats
                    .min_value
                    .as_ref()
                    .map_or(true, |min| min.as_slice() < val.as_slice())
            }
            ColumnRange::Between(min, max) => stats.overlaps_range(min, max),
        }
    }

    /// Prune segments using DataFusion's PruningPredicate API
    ///
    /// This method uses DataFusion's native pruning which handles complex expressions
    /// automatically using min/max statistics from SegmentPruningStatistics.
    ///
    /// # Example Query Flow
    /// ```text
    /// Query: SELECT * FROM orders WHERE amount > 100 AND status = 'shipped'
    ///
    /// Input filters: [amount > 100, status = 'shipped']
    /// Combined: (amount > 100) AND (status = 'shipped')
    ///
    /// Segments:
    /// - seg_1: amount [50..150],   status ['pending'..'shipped']  → KEEP (max > 100)
    /// - seg_2: amount [10..80],    status ['pending'..'pending']  → SKIP (max ≤ 100)
    /// - seg_3: amount [200..500],  status ['shipped'..'shipped']  → KEEP (max > 100)
    ///
    /// Result: [seg_1.arrow, seg_3.arrow]
    /// ```
    ///
    /// # Arguments
    /// * `filters` - Vector of logical filter expressions from DataFusion
    /// * `schema` - Arrow schema for the table
    ///
    /// # Returns
    /// List of file paths to scan after pruning
    pub fn prune_with_datafusion(
        &self,
        filters: &[Expr],
        schema: SchemaRef,
    ) -> datafusion::error::Result<Vec<PathBuf>> {
        if filters.is_empty() {
            // No filters = no pruning, scan all segments
            let segment_index = self.segment_index.read().unwrap();
            return Ok(segment_index
                .all()
                .iter()
                .map(|seg| seg.file_path.clone())
                .collect());
        }

        // Step 1: Combine all filter expressions with AND
        //
        // Example: [amount > 100, status = 'shipped']
        //       → (amount > 100) AND (status = 'shipped')
        //
        // This creates a single expression tree that DataFusion can optimize
        let combined_expr = filters
            .iter()
            .cloned()
            .reduce(|acc, expr| {
                Expr::BinaryExpr(datafusion::logical_expr::BinaryExpr {
                    left: Box::new(acc),
                    op: Operator::And,
                    right: Box::new(expr),
                })
            })
            .unwrap();

        // Step 2: Create DFSchema from Arrow Schema
        //
        // DFSchema is DataFusion's wrapper that adds metadata needed for
        // expression evaluation (column types, nullability, etc.)
        //
        // Example: Arrow Schema {amount: Int64, status: Utf8}
        //       → DFSchema with qualified column info
        let df_schema = DFSchema::try_from(schema.as_ref().clone())?;

        // Step 3: Convert logical Expr to PhysicalExpr
        //
        // Logical Expr (from SQL parsing): "amount > 100"
        // Physical Expr (executable code): Binary comparison that can evaluate arrays
        //
        // This conversion:
        // - Resolves column references to actual indices
        // - Infers data types for literals
        // - Creates executable operators
        //
        // Example: Expr::BinaryExpr{col("amount"), >, lit(100)}
        //       → PhysicalBinaryExpr with typed comparison function
        let session_ctx = SessionContext::new();
        let physical_expr = create_physical_expr(
            &combined_expr,
            &df_schema,
            session_ctx.state().execution_props(),
        )?;

        // Step 4: Create PruningPredicate with PhysicalExpr
        //
        // PruningPredicate rewrites the expression to work with min/max statistics:
        // - "amount > 100" becomes "max(amount) > 100" (if max ≤ 100, skip segment)
        // - "amount < 500" becomes "min(amount) < 500" (if min ≥ 500, skip segment)
        // - "status = 'shipped'" becomes "min(status) ≤ 'shipped' AND max(status) ≥ 'shipped'"
        //
        // This is the magic of zone map pruning!
        let pruning_predicate = PruningPredicate::try_new(physical_expr, schema.clone())?;

        // Step 5: Create statistics adapter and prune
        //
        // SegmentPruningStatistics provides min/max arrays for each column:
        //
        // Example for 3 segments:
        // - min_values("amount"): [50, 10, 200]
        // - max_values("amount"): [150, 80, 500]
        //
        // PruningPredicate evaluates: max(amount) > 100
        // Result: [true, false, true] → keep seg_1 and seg_3
        let segment_statistics = SegmentPruningStatistics::new(self.segment_index.clone(), schema);

        let keep_segments = pruning_predicate.prune(&segment_statistics)?;

        // Step 6: Filter segments based on pruning results
        //
        // keep_segments is a boolean array: [true, false, true, false, ...]
        // We enumerate and keep only the segments where keep=true
        //
        // Example:
        // - segments: [seg_1, seg_2, seg_3]
        // - keep_segments: [true, false, true]
        // - result: [seg_1.arrow, seg_3.arrow]
        let segments = segment_statistics.get_segments();
        let pruned_paths: Vec<PathBuf> = keep_segments
            .iter()
            .enumerate()
            .filter_map(|(idx, &keep)| {
                if keep {
                    Some(segments[idx].file_path.clone())
                } else {
                    None
                }
            })
            .collect();

        Ok(pruned_paths)
    }
}
