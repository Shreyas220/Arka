//! Query execution module using DataFusion
//!
//! This module provides SQL query capabilities over Arka's segment storage
//! by integrating with DataFusion's query engine. The key components are:
//!
//! - `segment_statistics`: Adapter for DataFusion's PruningStatistics
//! - `pruning`: Pruning engine using PKIndex + DataFusion's PruningPredicate
//! - `table_provider`: Bridge between Arka storage and DataFusion
//! - `predicates`: (deprecated - using DataFusion's pruning instead)

pub mod predicates;
pub mod pruning;
pub mod segment_statistics;
pub mod table_provider;

pub use predicates::ExtractedPredicates;
pub use pruning::PruningEngine;
pub use segment_statistics::SegmentPruningStatistics;
pub use table_provider::ArkaTableProvider;
