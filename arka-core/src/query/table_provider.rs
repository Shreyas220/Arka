use crate::query::PruningEngine;
use arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::datasource::file_format::arrow::ArrowFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::{empty::EmptyExec, ExecutionPlan};
use std::any::Any;
use std::sync::Arc;

/// Arka's TableProvider for DataFusion integration
///
/// Architecture:
/// ```
/// SQL Query → ArkaTableProvider → PruningEngine → ListingTable → DataFusion
/// ```
///
/// Our job: Prune segments intelligently
/// DataFusion's job: Execute query on pruned segments
#[derive(Debug)]
pub struct ArkaTableProvider {
    schema: SchemaRef,
    pruning_engine: Arc<PruningEngine>,
    table_name: String,
}

impl ArkaTableProvider {
    pub fn new(table_name: String, schema: SchemaRef, pruning_engine: Arc<PruningEngine>) -> Self {
        Self {
            schema,
            pruning_engine,
            table_name,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for ArkaTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Step 1: Prune using DataFusion's PruningPredicate API
        // This automatically handles complex expressions using min/max statistics
        let pruned_paths = self
            .pruning_engine
            .prune_with_datafusion(filters, self.schema.clone())?;

        // Step 2: Handle empty result
        if pruned_paths.is_empty() {
            return Ok(Arc::new(EmptyExec::new(self.schema.clone())));
        }

        // Step 3: Create ListingTable with pruned files
        let urls: Vec<_> = pruned_paths
            .iter()
            .map(|p| ListingTableUrl::parse(&format!("file://{}", p.display())))
            .collect::<DataFusionResult<Vec<_>>>()?;

        let file_format = Arc::new(ArrowFormat::default());
        let listing_options = ListingOptions::new(file_format).with_file_extension("arrow");

        let config = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(listing_options)
            .with_schema(self.schema.clone());

        let listing_table = ListingTable::try_new(config)?;

        // Step 4: Delegate to DataFusion
        listing_table.scan(state, projection, filters, limit).await
    }
}
