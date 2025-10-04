use super::metadata::{TableIdentifier, TableMetadata, TableType};
use crate::{ArkTable, ArkConfig};
use arrow::array::{Array, ArrayRef, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef, TimeUnit};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Arka catalog managing table metadata and instances
///
/// ## Architecture
///
/// ```text
/// ┌─────────────────────────────────────────────┐
/// │  ArkCatalog                                 │
/// │                                             │
/// │  In-Memory:                                 │
/// │  tables: DashMap<TableIdentifier, State>   │
/// │                                             │
/// │  Persistent:                                │
/// │  {base_path}/catalog.arrow                  │
/// │  - TableMetadata as Arrow RecordBatch       │
/// │  - One row per table                        │
/// │                                             │
/// │  On Startup:                                │
/// │  1. Read catalog.arrow                      │
/// │  2. Reconstruct TableMetadata               │
/// │  3. Create ArkTable instances               │
/// │  4. Load indexes from disk segments         │
/// └─────────────────────────────────────────────┘
/// ```
pub struct ArkCatalog {
    /// In-memory table registry
    tables: DashMap<TableIdentifier, CatalogEntry>,

    /// Base path for catalog storage
    base_path: PathBuf,
}

/// Internal catalog entry
struct CatalogEntry {
    metadata: TableMetadata,
    table: Arc<ArkTable>,
}

impl ArkCatalog {
    /// Create a new catalog
    ///
    /// # Arguments
    /// * `base_path` - Root directory for catalog and table data
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self, CatalogError> {
        let base_path = base_path.as_ref().to_path_buf();

        // Create base directory if not exists
        fs::create_dir_all(&base_path)
            .map_err(|e| CatalogError::Io(format!("Failed to create base path: {}", e)))?;

        Ok(Self {
            tables: DashMap::new(),
            base_path,
        })
    }

    /// Load catalog from disk
    ///
    /// Reads catalog.arrow and reconstructs table registry.
    /// If catalog file doesn't exist, starts with empty catalog.
    pub fn load(&mut self) -> Result<(), CatalogError> {
        let catalog_path = self.catalog_path();

        if !catalog_path.exists() {
            // No catalog file = empty catalog (first run)
            return Ok(());
        }

        let file = File::open(&catalog_path)
            .map_err(|e| CatalogError::Io(format!("Failed to open catalog: {}", e)))?;

        let reader = FileReader::try_new(file, None)
            .map_err(|e| CatalogError::Arrow(format!("Failed to read catalog: {}", e)))?;

        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| CatalogError::Arrow(format!("Failed to read batch: {}", e)))?;

            self.load_batch(batch)?;
        }

        Ok(())
    }

    /// Save catalog to disk
    ///
    /// Writes all table metadata to catalog.arrow as a single RecordBatch.
    pub fn save(&self) -> Result<(), CatalogError> {
        if self.tables.is_empty() {
            // No tables = no catalog file needed
            return Ok(());
        }

        let batch = self.serialize_to_batch()?;
        let catalog_path = self.catalog_path();

        let file = File::create(&catalog_path)
            .map_err(|e| CatalogError::Io(format!("Failed to create catalog: {}", e)))?;

        let mut writer = FileWriter::try_new(file, &batch.schema())
            .map_err(|e| CatalogError::Arrow(format!("Failed to create writer: {}", e)))?;

        writer
            .write(&batch)
            .map_err(|e| CatalogError::Arrow(format!("Failed to write batch: {}", e)))?;

        writer
            .finish()
            .map_err(|e| CatalogError::Arrow(format!("Failed to finish writing: {}", e)))?;

        Ok(())
    }

    /// Register a new table
    ///
    /// # Arguments
    /// * `metadata` - Table metadata (schema, partition spec, properties)
    ///
    /// # Returns
    /// Arc to the created ArkTable instance
    pub fn register_table(
        &self,
        metadata: TableMetadata,
    ) -> Result<Arc<ArkTable>, CatalogError> {
        let identifier = metadata.identifier.clone();

        // Check if table already exists
        if self.tables.contains_key(&identifier) {
            return Err(CatalogError::TableExists(identifier.qualified_name()));
        }

        // Create table directory
        let table_path = PathBuf::from(&metadata.location);
        fs::create_dir_all(&table_path)
            .map_err(|e| CatalogError::Io(format!("Failed to create table directory: {}", e)))?;

        // Create ArkTable instance
        // Use append_only by default (TODO: respect table_type from metadata)
        let config = match metadata.table_type {
            TableType::AppendOnly => ArkConfig::append_only_events(),
            TableType::PrimaryKey => {
                // Extract primary key column index
                let pk_column = metadata.primary_key.as_ref()
                    .and_then(|pk_name| {
                        metadata.schema.fields()
                            .iter()
                            .position(|f| f.name() == pk_name)
                    })
                    .unwrap_or(0);
                ArkConfig::cdc_enabled_table(vec![pk_column])
            },
            TableType::MaterializedView => ArkConfig::append_only_events(),
        };

        let table = Arc::new(ArkTable::new(
            identifier.name.clone(),
            metadata.schema.clone(),
            Arc::new(config),
            metadata.location.clone(),
        ).map_err(|e| CatalogError::Io(e.to_string()))?);

        // Start background buffer flusher for disk persistence
        table.start_buffer_flusher(metadata.location.clone())
            .map_err(|e| CatalogError::Io(e.to_string()))?;

        // Store in catalog
        self.tables.insert(
            identifier.clone(),
            CatalogEntry {
                metadata,
                table: table.clone(),
            },
        );

        // Persist catalog
        self.save()?;

        Ok(table)
    }

    /// Get table by identifier
    pub fn get_table(&self, identifier: &TableIdentifier) -> Option<Arc<ArkTable>> {
        self.tables.get(identifier).map(|entry| entry.table.clone())
    }

    /// Get table by simple name (assumes "default" namespace)
    pub fn get_table_by_name(&self, name: &str) -> Option<Arc<ArkTable>> {
        let identifier = TableIdentifier::new(name);
        self.get_table(&identifier)
    }

    /// Get table metadata
    pub fn get_metadata(&self, identifier: &TableIdentifier) -> Option<TableMetadata> {
        self.tables
            .get(identifier)
            .map(|entry| entry.metadata.clone())
    }

    /// List all tables
    pub fn list_tables(&self) -> Vec<TableIdentifier> {
        self.tables.iter().map(|entry| entry.key().clone()).collect()
    }

    /// Drop a table
    ///
    /// Removes from catalog but does NOT delete data files.
    pub fn drop_table(&self, identifier: &TableIdentifier) -> Result<(), CatalogError> {
        self.tables
            .remove(identifier)
            .ok_or_else(|| CatalogError::TableNotFound(identifier.qualified_name()))?;

        // Persist catalog
        self.save()?;

        Ok(())
    }

    /// Get path to catalog file
    fn catalog_path(&self) -> PathBuf {
        self.base_path.join("catalog.arrow")
    }

    /// Serialize catalog to Arrow RecordBatch
    fn serialize_to_batch(&self) -> Result<RecordBatch, CatalogError> {
        let num_tables = self.tables.len();

        let mut namespace_jsons: Vec<String> = Vec::with_capacity(num_tables);
        let mut table_names: Vec<String> = Vec::with_capacity(num_tables);
        let mut schema_jsons: Vec<String> = Vec::with_capacity(num_tables);
        let mut partition_spec_jsons: Vec<Option<String>> = Vec::with_capacity(num_tables);
        let mut properties_jsons: Vec<String> = Vec::with_capacity(num_tables);
        let mut locations: Vec<String> = Vec::with_capacity(num_tables);
        let mut primary_keys: Vec<Option<String>> = Vec::with_capacity(num_tables);
        let mut table_types: Vec<String> = Vec::with_capacity(num_tables);
        let mut created_ats: Vec<i64> = Vec::with_capacity(num_tables);
        let mut updated_ats: Vec<i64> = Vec::with_capacity(num_tables);

        for entry in self.tables.iter() {
            let metadata = &entry.metadata;

            // Serialize namespace as JSON array
            let namespace_json = serde_json::to_string(&metadata.identifier.namespace)
                .map_err(|e| CatalogError::Serialization(format!("Namespace: {}", e)))?;
            namespace_jsons.push(namespace_json);

            table_names.push(metadata.identifier.name.clone());

            // Serialize schema to JSON using Arrow IPC format
            let schema_json = schema_to_json(&metadata.schema)?;
            schema_jsons.push(schema_json);

            // Serialize partition spec to JSON
            let partition_spec_json = metadata
                .partition_spec
                .as_ref()
                .map(|spec| serde_json::to_string(spec))
                .transpose()
                .map_err(|e| CatalogError::Serialization(format!("PartitionSpec: {}", e)))?;
            partition_spec_jsons.push(partition_spec_json);

            // Serialize properties to JSON
            let properties_json = serde_json::to_string(&metadata.properties)
                .map_err(|e| CatalogError::Serialization(format!("Properties: {}", e)))?;
            properties_jsons.push(properties_json);

            locations.push(metadata.location.clone());
            primary_keys.push(metadata.primary_key.clone());
            table_types.push(metadata.table_type.as_str().to_string());
            created_ats.push(metadata.created_at);
            updated_ats.push(metadata.updated_at);
        }

        // Build Arrow arrays
        let columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(namespace_jsons)),
            Arc::new(StringArray::from(table_names)),
            Arc::new(StringArray::from(schema_jsons)),
            Arc::new(StringArray::from(partition_spec_jsons)),
            Arc::new(StringArray::from(properties_jsons)),
            Arc::new(StringArray::from(locations)),
            Arc::new(StringArray::from(primary_keys)),
            Arc::new(StringArray::from(table_types)),
            Arc::new(TimestampMicrosecondArray::from(created_ats)),
            Arc::new(TimestampMicrosecondArray::from(updated_ats)),
        ];

        let schema = catalog_schema();
        RecordBatch::try_new(Arc::new(schema), columns)
            .map_err(|e| CatalogError::Arrow(format!("Failed to create batch: {}", e)))
    }

    /// Load tables from RecordBatch
    fn load_batch(&mut self, batch: RecordBatch) -> Result<(), CatalogError> {
        let namespace_json_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CatalogError::Deserialization("Invalid namespace column".into()))?;

        let table_name_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CatalogError::Deserialization("Invalid table_name column".into()))?;

        let schema_json_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CatalogError::Deserialization("Invalid schema_json column".into()))?;

        let partition_spec_json_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CatalogError::Deserialization("Invalid partition_spec_json column".into()))?;

        let properties_json_array = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CatalogError::Deserialization("Invalid properties_json column".into()))?;

        let location_array = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CatalogError::Deserialization("Invalid location column".into()))?;

        let primary_key_array = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CatalogError::Deserialization("Invalid primary_key column".into()))?;

        let table_type_array = batch
            .column(7)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CatalogError::Deserialization("Invalid table_type column".into()))?;

        let created_at_array = batch
            .column(8)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| CatalogError::Deserialization("Invalid created_at column".into()))?;

        let updated_at_array = batch
            .column(9)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| CatalogError::Deserialization("Invalid updated_at column".into()))?;

        for i in 0..batch.num_rows() {
            // Parse namespace from JSON
            let namespace_json = namespace_json_array.value(i);
            let namespace: Vec<String> = serde_json::from_str(namespace_json)
                .map_err(|e| CatalogError::Deserialization(format!("Namespace: {}", e)))?;

            let table_name = table_name_array.value(i).to_string();

            let identifier = TableIdentifier::with_namespace(namespace, table_name);

            // Deserialize schema using helper
            let schema_json = schema_json_array.value(i);
            let schema = schema_from_json(schema_json)?;

            // Deserialize partition spec
            let partition_spec = if partition_spec_json_array.is_null(i) {
                None
            } else {
                let spec_json = partition_spec_json_array.value(i);
                Some(serde_json::from_str(spec_json)
                    .map_err(|e| CatalogError::Deserialization(format!("PartitionSpec: {}", e)))?)
            };

            // Deserialize properties
            let properties_json = properties_json_array.value(i);
            let properties = serde_json::from_str(properties_json)
                .map_err(|e| CatalogError::Deserialization(format!("Properties: {}", e)))?;

            let location = location_array.value(i).to_string();

            let primary_key = if primary_key_array.is_null(i) {
                None
            } else {
                Some(primary_key_array.value(i).to_string())
            };

            let table_type_str = table_type_array.value(i);
            let table_type = TableType::from_str(table_type_str)
                .ok_or_else(|| CatalogError::Deserialization(format!("Invalid table type: {}", table_type_str)))?;

            let created_at = created_at_array.value(i);
            let updated_at = updated_at_array.value(i);

            // Reconstruct metadata (schema is already Arc from schema_from_json)
            let metadata = TableMetadata {
                identifier: identifier.clone(),
                schema,
                partition_spec,
                properties,
                location,
                primary_key,
                table_type,
                created_at,
                updated_at,
            };

            // Create ArkTable instance with appropriate config
            let config = match metadata.table_type {
                TableType::AppendOnly => ArkConfig::append_only_events(),
                TableType::PrimaryKey => {
                    let pk_column = metadata.primary_key.as_ref()
                        .and_then(|pk_name| {
                            metadata.schema.fields()
                                .iter()
                                .position(|f| f.name() == pk_name)
                        })
                        .unwrap_or(0);
                    ArkConfig::cdc_enabled_table(vec![pk_column])
                },
                TableType::MaterializedView => ArkConfig::append_only_events(),
            };

            let table = Arc::new(ArkTable::new(
                metadata.identifier.name.clone(),
                metadata.schema.clone(),
                Arc::new(config),
                metadata.location.clone(),
            ).map_err(|e| CatalogError::Io(e.to_string()))?);

            // Start background buffer flusher for disk persistence
            table.start_buffer_flusher(metadata.location.clone())
                .map_err(|e| CatalogError::Io(e.to_string()))?;

            self.tables.insert(
                identifier,
                CatalogEntry { metadata, table },
            );
        }

        Ok(())
    }
}

/// Catalog schema for Arrow IPC persistence
fn catalog_schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        Field::new("namespace_json", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("schema_json", DataType::Utf8, false),
        Field::new("partition_spec_json", DataType::Utf8, true),
        Field::new("properties_json", DataType::Utf8, false),
        Field::new("location", DataType::Utf8, false),
        Field::new("primary_key", DataType::Utf8, true),
        Field::new("table_type", DataType::Utf8, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ])
}

/// Serialize Arrow Schema to JSON using Arrow IPC format
fn schema_to_json(schema: &SchemaRef) -> Result<String, CatalogError> {
    // Use Arrow's schema serialization
    let json = serde_json::json!({
        "fields": schema.fields().iter().map(|f| {
            serde_json::json!({
                "name": f.name(),
                "type": format!("{:?}", f.data_type()),
                "nullable": f.is_nullable(),
            })
        }).collect::<Vec<_>>(),
    });

    serde_json::to_string(&json)
        .map_err(|e| CatalogError::Serialization(format!("Schema JSON: {}", e)))
}

/// Deserialize Arrow Schema from JSON
fn schema_from_json(json_str: &str) -> Result<Arc<ArrowSchema>, CatalogError> {
    // For now, use a simplified deserialization
    // In production, you'd want full Arrow IPC format support
    let json: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| CatalogError::Deserialization(format!("Schema JSON: {}", e)))?;

    let fields_json = json.get("fields")
        .and_then(|v| v.as_array())
        .ok_or_else(|| CatalogError::Deserialization("Missing 'fields' in schema".into()))?;

    let fields: Result<Vec<Field>, CatalogError> = fields_json
        .iter()
        .map(|f| {
            let name = f.get("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| CatalogError::Deserialization("Missing field name".into()))?;

            let type_str = f.get("type")
                .and_then(|v| v.as_str())
                .ok_or_else(|| CatalogError::Deserialization("Missing field type".into()))?;

            let nullable = f.get("nullable")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);

            // Simple type parsing (expand this for production)
            let data_type = match type_str {
                "Int64" => DataType::Int64,
                "Utf8" => DataType::Utf8,
                "Boolean" => DataType::Boolean,
                "Float64" => DataType::Float64,
                _ if type_str.starts_with("Timestamp") => {
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                }
                _ => return Err(CatalogError::Deserialization(
                    format!("Unsupported type: {}", type_str)
                )),
            };

            Ok(Field::new(name, data_type, nullable))
        })
        .collect();

    Ok(Arc::new(ArrowSchema::new(fields?)))
}

/// Catalog errors
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("Table already exists: {0}")]
    TableExists(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("I/O error: {0}")]
    Io(String),

    #[error("Arrow error: {0}")]
    Arrow(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use tempfile::TempDir;

    fn create_test_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn test_catalog_create_and_register() {
        let temp_dir = TempDir::new().unwrap();
        let catalog = ArkCatalog::new(temp_dir.path()).unwrap();

        let metadata = TableMetadata::new(
            TableIdentifier::new("users"),
            create_test_schema(),
            temp_dir.path().join("users").to_str().unwrap().to_string(),
            TableType::PrimaryKey,
        );

        let table = catalog.register_table(metadata).unwrap();
        assert!(table.name == "users");

        // Verify table is retrievable
        let retrieved = catalog.get_table_by_name("users").unwrap();
        assert!(Arc::ptr_eq(&table, &retrieved));
    }

    #[test]
    fn test_catalog_persistence() {
        let temp_dir = TempDir::new().unwrap();

        // Create catalog and register table
        {
            let catalog = ArkCatalog::new(temp_dir.path()).unwrap();
            let metadata = TableMetadata::new(
                TableIdentifier::new("users"),
                create_test_schema(),
                temp_dir.path().join("users").to_str().unwrap().to_string(),
                TableType::PrimaryKey,
            );
            catalog.register_table(metadata).unwrap();
        }

        // Load catalog in new instance
        {
            let mut catalog = ArkCatalog::new(temp_dir.path()).unwrap();
            catalog.load().unwrap();

            let table = catalog.get_table_by_name("users").unwrap();
            assert!(table.name == "users");

            let metadata = catalog.get_metadata(&TableIdentifier::new("users")).unwrap();
            assert_eq!(metadata.table_type, TableType::PrimaryKey);
        }
    }

    #[test]
    fn test_list_and_drop_tables() {
        let temp_dir = TempDir::new().unwrap();
        let catalog = ArkCatalog::new(temp_dir.path()).unwrap();

        catalog.register_table(TableMetadata::new(
            TableIdentifier::new("users"),
            create_test_schema(),
            temp_dir.path().join("users").to_str().unwrap().to_string(),
            TableType::PrimaryKey,
        )).unwrap();

        catalog.register_table(TableMetadata::new(
            TableIdentifier::new("events"),
            create_test_schema(),
            temp_dir.path().join("events").to_str().unwrap().to_string(),
            TableType::AppendOnly,
        )).unwrap();

        let tables = catalog.list_tables();
        assert_eq!(tables.len(), 2);

        catalog.drop_table(&TableIdentifier::new("users")).unwrap();
        let tables = catalog.list_tables();
        assert_eq!(tables.len(), 1);
    }
}
