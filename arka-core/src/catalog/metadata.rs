use arrow::datatypes::Schema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Iceberg-aligned table identifier
///
/// Follows Iceberg catalog spec for namespace + table naming.
/// Examples:
/// - Simple: namespace=["default"], name="users"
/// - Hierarchical: namespace=["analytics", "production"], name="events"
#[derive(Debug, Clone, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableIdentifier {
    /// Namespace path (e.g., ["default"] or ["analytics", "prod"])
    pub namespace: Vec<String>,

    /// Table name within the namespace
    pub name: String,
}

impl TableIdentifier {
    /// Create a simple table identifier in the "default" namespace
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            namespace: vec!["default".to_string()],
            name: name.into(),
        }
    }

    /// Create a table identifier with custom namespace
    pub fn with_namespace(namespace: Vec<String>, name: impl Into<String>) -> Self {
        Self {
            namespace,
            name: name.into(),
        }
    }

    /// Get the full qualified name (e.g., "analytics.prod.users")
    pub fn qualified_name(&self) -> String {
        let mut parts = self.namespace.clone();
        parts.push(self.name.clone());
        parts.join(".")
    }
}

/// Iceberg partition transform functions
///
/// Defines how source column values are transformed into partition values.
/// Follows Iceberg partition spec: https://iceberg.apache.org/spec/#partitioning
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Transform {
    /// Use source value as-is (no transformation)
    Identity,

    /// Extract year from timestamp
    Year,

    /// Extract month from timestamp
    Month,

    /// Extract day from timestamp
    Day,

    /// Extract hour from timestamp
    Hour,

    /// Hash bucket by N buckets
    Bucket(i32),

    /// Truncate string/integer to width N
    Truncate(i32),
}

/// Iceberg partition field
///
/// Defines a single partition field in the partition spec.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionField {
    /// Source column ID in the schema (1-indexed in Iceberg)
    pub source_id: i32,

    /// Unique field ID for this partition field
    pub field_id: i32,

    /// Name of the partition field
    pub name: String,

    /// Transform to apply to source column
    pub transform: Transform,
}

impl PartitionField {
    /// Create a new partition field
    pub fn new(source_id: i32, field_id: i32, name: String, transform: Transform) -> Self {
        Self {
            source_id,
            field_id,
            name,
            transform,
        }
    }

    /// Create an identity partition (no transform)
    pub fn identity(source_id: i32, field_id: i32, name: String) -> Self {
        Self::new(source_id, field_id, name, Transform::Identity)
    }
}

/// Iceberg partition specification
///
/// Defines how a table is partitioned. Multiple partition specs can exist
/// for schema evolution (e.g., changing partitioning strategy over time).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSpec {
    /// Unique spec ID (0 = unpartitioned in Iceberg)
    pub spec_id: i32,

    /// List of partition fields
    pub fields: Vec<PartitionField>,
}

impl PartitionSpec {
    /// Create an unpartitioned spec
    pub fn unpartitioned() -> Self {
        Self {
            spec_id: 0,
            fields: vec![],
        }
    }

    /// Create a new partition spec with fields
    pub fn new(spec_id: i32, fields: Vec<PartitionField>) -> Self {
        Self { spec_id, fields }
    }

    /// Check if this spec is unpartitioned
    pub fn is_unpartitioned(&self) -> bool {
        self.fields.is_empty()
    }
}

/// Table type classification
///
/// Determines how the table is managed and what operations are supported.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum TableType {
    /// Append-only table (no updates/deletes, no primary key)
    AppendOnly,

    /// Primary key table (supports CDC operations)
    PrimaryKey,

    /// Materialized view (derived from other tables)
    MaterializedView,
}

impl TableType {
    pub fn as_str(&self) -> &'static str {
        match self {
            TableType::AppendOnly => "append_only",
            TableType::PrimaryKey => "primary_key",
            TableType::MaterializedView => "materialized_view",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "append_only" => Some(TableType::AppendOnly),
            "primary_key" => Some(TableType::PrimaryKey),
            "materialized_view" => Some(TableType::MaterializedView),
            _ => None,
        }
    }
}

/// Complete table metadata (Iceberg-aligned + Arka extensions)
///
/// This struct contains all metadata needed to:
/// 1. Recreate an ArkTable on server restart
/// 2. Sync to Iceberg catalog in the future
/// 3. Support partition pruning and query optimization
#[derive(Debug, Clone, Serialize)]
pub struct TableMetadata {
    /// Table identifier (namespace + name)
    pub identifier: TableIdentifier,

    /// Arrow schema for this table
    /// Note: Serialized separately in catalog.rs using schema_to_json
    #[serde(skip)]
    pub schema: Arc<Schema>,

    /// Partition specification (optional)
    pub partition_spec: Option<PartitionSpec>,

    /// Iceberg table properties (arbitrary key-value pairs)
    /// Standard properties:
    /// - "write.format.default" = "parquet"
    /// - "write.metadata.compression-codec" = "gzip"
    /// - "commit.retry.num-retries" = "4"
    pub properties: HashMap<String, String>,

    /// Base path for table data (segments, metadata)
    pub location: String,

    // ===== Arka-specific fields (not in Iceberg spec) =====

    /// Primary key column name (for PrimaryKey tables)
    pub primary_key: Option<String>,

    /// Table type classification
    pub table_type: TableType,

    /// Creation timestamp (Unix epoch microseconds)
    pub created_at: i64,

    /// Last update timestamp (Unix epoch microseconds)
    pub updated_at: i64,
}

impl TableMetadata {
    /// Create new table metadata
    pub fn new(
        identifier: TableIdentifier,
        schema: Arc<Schema>,
        location: String,
        table_type: TableType,
    ) -> Self {
        let now = chrono::Utc::now().timestamp_micros();

        Self {
            identifier,
            schema,
            partition_spec: None,
            properties: HashMap::new(),
            location,
            primary_key: None,
            table_type,
            created_at: now,
            updated_at: now,
        }
    }

    /// Builder pattern: set partition spec
    pub fn with_partition_spec(mut self, spec: PartitionSpec) -> Self {
        self.partition_spec = Some(spec);
        self
    }

    /// Builder pattern: set primary key
    pub fn with_primary_key(mut self, key: String) -> Self {
        self.primary_key = Some(key);
        self
    }

    /// Builder pattern: add property
    pub fn with_property(mut self, key: String, value: String) -> Self {
        self.properties.insert(key, value);
        self
    }

    /// Get the partition spec or unpartitioned default
    pub fn partition_spec(&self) -> &PartitionSpec {
        static UNPARTITIONED: PartitionSpec = PartitionSpec {
            spec_id: 0,
            fields: vec![],
        };

        self.partition_spec.as_ref().unwrap_or(&UNPARTITIONED)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_table_identifier() {
        let id = TableIdentifier::new("users");
        assert_eq!(id.namespace, vec!["default"]);
        assert_eq!(id.name, "users");
        assert_eq!(id.qualified_name(), "default.users");

        let id = TableIdentifier::with_namespace(
            vec!["analytics".into(), "prod".into()],
            "events"
        );
        assert_eq!(id.qualified_name(), "analytics.prod.events");
    }

    #[test]
    fn test_partition_spec() {
        let spec = PartitionSpec::unpartitioned();
        assert!(spec.is_unpartitioned());
        assert_eq!(spec.spec_id, 0);

        let fields = vec![
            PartitionField::new(1, 1000, "event_date".into(), Transform::Day),
            PartitionField::new(2, 1001, "user_bucket".into(), Transform::Bucket(16)),
        ];
        let spec = PartitionSpec::new(1, fields);
        assert!(!spec.is_unpartitioned());
        assert_eq!(spec.fields.len(), 2);
    }

    #[test]
    fn test_table_metadata() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let metadata = TableMetadata::new(
            TableIdentifier::new("users"),
            schema,
            "/data/users".into(),
            TableType::PrimaryKey,
        )
        .with_primary_key("id".into())
        .with_property("write.format.default".into(), "parquet".into());

        assert_eq!(metadata.identifier.name, "users");
        assert_eq!(metadata.table_type, TableType::PrimaryKey);
        assert_eq!(metadata.primary_key, Some("id".into()));
        assert_eq!(metadata.properties.get("write.format.default"), Some(&"parquet".to_string()));
    }

    #[test]
    fn test_table_type_serialization() {
        assert_eq!(TableType::AppendOnly.as_str(), "append_only");
        assert_eq!(TableType::from_str("primary_key"), Some(TableType::PrimaryKey));
        assert_eq!(TableType::from_str("invalid"), None);
    }
}
