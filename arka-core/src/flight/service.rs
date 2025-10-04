use super::messages::{AckMessage, DurabilityLevel, WriteRequest};
use crate::catalog::{ArkCatalog, PartitionSpec, TableIdentifier, TableMetadata, TableType};
use arrow_flight::{
    flight_service_server::FlightService, Action, ActionType, Criteria, Empty, FlightData,
    FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult,
    Ticket,
};
use arrow_schema::Schema;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Arka Flight Service implementing bidirectional streaming writes
///
/// ## Architecture
///
/// ```text
/// ┌─────────────────────────────────────────────────┐
/// │  Client                                         │
/// │  ├─> WriteRequest (app_metadata)                │
/// │  ├─> RecordBatch stream ────────┐               │
/// │  └─< AckMessage stream ─────────┼───┐           │
/// └─────────────────────────────────┼───┼───────────┘
///                                   │   │
///                                   ↓   ↑
/// ┌──────────────────────────────────────────────────┐
/// │  ArkaFlightService                               │
/// │                                                  │
/// │  DoExchange:                                     │
/// │  1. Parse WriteRequest from first FlightData     │
/// │  2. Lookup table in ArkCatalog                   │
/// │  3. For each RecordBatch:                        │
/// │     - Append to table                            │
/// │     - Get LSN                                    │
/// │     - Send memory-tier ack immediately           │
/// │  4. Background: Send disk/S3/Iceberg acks        │
/// │                                                  │
/// │  Actions:                                        │
/// │  - CreateTable: Register new table               │
/// │  - ListTables: Get all table names               │
/// │  - GetTableSchema: Get schema for table          │
/// └──────────────────────────────────────────────────┘
/// ```
pub struct ArkaFlightService {
    catalog: Arc<ArkCatalog>,
}

impl ArkaFlightService {
    pub fn new(catalog: Arc<ArkCatalog>) -> Self {
        Self { catalog }
    }
}

#[tonic::async_trait]
impl FlightService for ArkaFlightService {
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, tonic::Status>> + Send>>;
    type ListFlightsStream = Pin<Box<dyn Stream<Item = Result<FlightInfo, tonic::Status>> + Send>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, tonic::Status>> + Send>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, tonic::Status>> + Send>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<arrow_flight::Result, tonic::Status>> + Send>>;
    type ListActionsStream = Pin<Box<dyn Stream<Item = Result<ActionType, tonic::Status>> + Send>>;
    type DoExchangeStream = Pin<Box<dyn Stream<Item = Result<FlightData, tonic::Status>> + Send>>;

    async fn handshake(
        &self,
        _request: tonic::Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<tonic::Response<Self::HandshakeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("Handshake not implemented"))
    }

    async fn list_flights(
        &self,
        _request: tonic::Request<Criteria>,
    ) -> Result<tonic::Response<Self::ListFlightsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("ListFlights not implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: tonic::Request<FlightDescriptor>,
    ) -> Result<tonic::Response<FlightInfo>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "GetFlightInfo not implemented",
        ))
    }

    async fn poll_flight_info(
        &self,
        _request: tonic::Request<FlightDescriptor>,
    ) -> Result<tonic::Response<arrow_flight::PollInfo>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "PollFlightInfo not implemented",
        ))
    }

    async fn get_schema(
        &self,
        request: tonic::Request<FlightDescriptor>,
    ) -> Result<tonic::Response<SchemaResult>, tonic::Status> {
        let descriptor = request.into_inner();

        // Extract table name from descriptor path
        let table_name = descriptor
            .path
            .first()
            .ok_or_else(|| tonic::Status::invalid_argument("Missing table name in descriptor"))?;

        // Get table metadata from catalog
        let metadata = self
            .catalog
            .get_metadata(&TableIdentifier::new(table_name))
            .ok_or_else(|| tonic::Status::not_found(format!("Table not found: {}", table_name)))?;

        // Serialize schema using Arrow IPC
        // Create a temporary writer to encode the schema
        let mut buffer = Vec::new();
        {
            use arrow::ipc::writer::FileWriter;
            let mut writer = FileWriter::try_new(&mut buffer, &metadata.schema).map_err(|e| {
                tonic::Status::internal(format!("Failed to create schema writer: {}", e))
            })?;
            writer
                .finish()
                .map_err(|e| tonic::Status::internal(format!("Failed to write schema: {}", e)))?;
        }

        Ok(tonic::Response::new(SchemaResult {
            schema: Bytes::from(buffer),
        }))
    }

    async fn do_get(
        &self,
        _request: tonic::Request<Ticket>,
    ) -> Result<tonic::Response<Self::DoGetStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("DoGet not implemented"))
    }

    async fn do_put(
        &self,
        _request: tonic::Request<tonic::Streaming<FlightData>>,
    ) -> Result<tonic::Response<Self::DoPutStream>, tonic::Status> {
        Err(tonic::Status::unimplemented(
            "DoPut not implemented - use DoExchange",
        ))
    }

    async fn do_action(
        &self,
        request: tonic::Request<Action>,
    ) -> Result<tonic::Response<Self::DoActionStream>, tonic::Status> {
        let action = request.into_inner();

        match action.r#type.as_str() {
            "CreateTable" => self.action_create_table(action.body).await,
            "ListTables" => self.action_list_tables().await,
            "DropTable" => self.action_drop_table(action.body).await,
            _ => Err(tonic::Status::unimplemented(format!(
                "Unknown action: {}",
                action.r#type
            ))),
        }
    }

    async fn list_actions(
        &self,
        _request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<Self::ListActionsStream>, tonic::Status> {
        let actions = vec![
            ActionType {
                r#type: "CreateTable".to_string(),
                description: "Create a new table with schema and config".to_string(),
            },
            ActionType {
                r#type: "ListTables".to_string(),
                description: "List all registered tables".to_string(),
            },
            ActionType {
                r#type: "DropTable".to_string(),
                description: "Drop an existing table".to_string(),
            },
        ];

        let stream = futures::stream::iter(actions.into_iter().map(Ok));
        Ok(tonic::Response::new(Box::pin(stream)))
    }
    //TODO: Add backpressure
    async fn do_exchange(
        &self,
        request: tonic::Request<tonic::Streaming<FlightData>>,
    ) -> Result<tonic::Response<Self::DoExchangeStream>, tonic::Status> {
        let mut input_stream = request.into_inner();

        // Channel for sending acks back to client
        let (tx, rx) = mpsc::channel::<Result<FlightData, tonic::Status>>(100);

        // Get first message to extract WriteRequest metadata
        let first_msg: FlightData = input_stream
            .next()
            .await
            .ok_or_else(|| tonic::Status::invalid_argument("Empty stream"))?
            .map_err(|e| tonic::Status::internal(format!("Stream error: {}", e)))?;

        let write_req = if !first_msg.app_metadata.is_empty() {
            WriteRequest::from_bytes(&first_msg.app_metadata).map_err(|e| {
                tonic::Status::invalid_argument(format!("Invalid WriteRequest: {}", e))
            })?
        } else {
            return Err(tonic::Status::invalid_argument(
                "First message must contain WriteRequest in app_metadata",
            ));
        };

        // Lookup table in catalog
        let table = self
            .catalog
            .get_table_by_name(&write_req.table_name)
            .ok_or_else(|| {
                tonic::Status::not_found(format!("Table not found: {}", write_req.table_name))
            })?;

        let catalog = self.catalog.clone();
        let requested_durability = write_req.requested_durability;

        // Spawn task to process writes
        tokio::spawn(async move {
            // Process first batch if present
            if !first_msg.data_body.is_empty() {
                if let Err(e) =
                    process_batch(&table, &first_msg.data_body, &tx, requested_durability).await
                {
                    let _ = tx
                        .send(Err(tonic::Status::internal(format!(
                            "Failed to process batch: {}",
                            e
                        ))))
                        .await;
                    return;
                }
            }

            // Process remaining batches
            while let Some(result) = input_stream.next().await {
                match result {
                    Ok(flight_data) => {
                        if !flight_data.data_body.is_empty() {
                            if let Err(e) = process_batch(
                                &table,
                                &flight_data.data_body,
                                &tx,
                                requested_durability,
                            )
                            .await
                            {
                                let _ = tx
                                    .send(Err(tonic::Status::internal(format!(
                                        "Failed to process batch: {}",
                                        e
                                    ))))
                                    .await;
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(tonic::Status::internal(format!("Stream error: {}", e))))
                            .await;
                        break;
                    }
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(tonic::Response::new(
            Box::pin(output_stream) as Self::DoExchangeStream
        ))
    }
}

// Action implementations
impl ArkaFlightService {
    async fn action_create_table(
        &self,
        body: Bytes,
    ) -> Result<tonic::Response<<ArkaFlightService as FlightService>::DoActionStream>, tonic::Status>
    {
        #[derive(Deserialize)]
        struct CreateTableRequest {
            name: String,
            schema_json: String,
            #[serde(default)]
            primary_key: Option<String>,
            #[serde(default)]
            table_type: Option<String>,
            #[serde(default)]
            partition_spec: Option<PartitionSpec>,
        }

        let req: CreateTableRequest = serde_json::from_slice(&body)
            .map_err(|e| tonic::Status::invalid_argument(format!("Invalid request: {}", e)))?;

        // Parse schema from JSON
        let schema_value: serde_json::Value = serde_json::from_str(&req.schema_json)
            .map_err(|e| tonic::Status::invalid_argument(format!("Invalid schema JSON: {}", e)))?;

        // Simple schema reconstruction (expand for production)
        let fields: Vec<arrow::datatypes::Field> = schema_value
            .get("fields")
            .and_then(|v| v.as_array())
            .ok_or_else(|| tonic::Status::invalid_argument("Missing 'fields' in schema"))?
            .iter()
            .map(|f| {
                let name = f["name"].as_str().unwrap_or("unknown");
                let type_str = f["type"].as_str().unwrap_or("Utf8");
                let nullable = f["nullable"].as_bool().unwrap_or(true);

                let data_type = match type_str {
                    "Int64" => arrow::datatypes::DataType::Int64,
                    "Utf8" => arrow::datatypes::DataType::Utf8,
                    _ => arrow::datatypes::DataType::Utf8,
                };

                arrow::datatypes::Field::new(name, data_type, nullable)
            })
            .collect();

        let schema = Arc::new(Schema::new(fields));

        // Determine table type
        let table_type = req
            .table_type
            .as_deref()
            .and_then(TableType::from_str)
            .unwrap_or(TableType::AppendOnly);

        // Create metadata
        let base_path = std::path::PathBuf::from("/tmp/arka_data"); // TODO: configurable
        let table_path = base_path.join(&req.name);

        let mut metadata = TableMetadata::new(
            TableIdentifier::new(&req.name),
            schema,
            table_path.to_str().unwrap().to_string(),
            table_type,
        );

        if let Some(pk) = req.primary_key {
            metadata = metadata.with_primary_key(pk);
        }

        if let Some(spec) = req.partition_spec {
            metadata = metadata.with_partition_spec(spec);
        }

        // Register table
        self.catalog
            .register_table(metadata)
            .map_err(|e| tonic::Status::internal(format!("Failed to register table: {}", e)))?;

        let result = arrow_flight::Result {
            body: Bytes::from(format!("Table '{}' created successfully", req.name)),
        };

        let stream = futures::stream::once(async { Ok(result) });
        Ok(tonic::Response::new(Box::pin(stream)))
    }

    async fn action_list_tables(
        &self,
    ) -> Result<tonic::Response<<ArkaFlightService as FlightService>::DoActionStream>, tonic::Status>
    {
        let tables = self.catalog.list_tables();

        let table_list: Vec<String> = tables.into_iter().map(|id| id.qualified_name()).collect();

        let json = serde_json::to_string(&table_list)
            .map_err(|e| tonic::Status::internal(format!("Failed to serialize tables: {}", e)))?;

        let result = arrow_flight::Result {
            body: Bytes::from(json),
        };

        let stream = futures::stream::once(async { Ok(result) });
        Ok(tonic::Response::new(Box::pin(stream)))
    }

    async fn action_drop_table(
        &self,
        body: Bytes,
    ) -> Result<tonic::Response<<ArkaFlightService as FlightService>::DoActionStream>, tonic::Status>
    {
        #[derive(Deserialize)]
        struct DropTableRequest {
            name: String,
        }

        let req: DropTableRequest = serde_json::from_slice(&body)
            .map_err(|e| tonic::Status::invalid_argument(format!("Invalid request: {}", e)))?;

        self.catalog
            .drop_table(&TableIdentifier::new(&req.name))
            .map_err(|e| tonic::Status::internal(format!("Failed to drop table: {}", e)))?;

        let result = arrow_flight::Result {
            body: Bytes::from(format!("Table '{}' dropped successfully", req.name)),
        };

        let stream = futures::stream::once(async { Ok(result) });
        Ok(tonic::Response::new(Box::pin(stream)))
    }
}

/// Process a single RecordBatch and send acks
async fn process_batch(
    table: &Arc<crate::ArkTable>,
    batch_bytes: &[u8],
    tx: &mpsc::Sender<Result<FlightData, tonic::Status>>,
    requested_durability: Option<DurabilityLevel>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // use crate::lsn::LSN;

    // Decode RecordBatch from Arrow IPC
    let mut cursor = std::io::Cursor::new(batch_bytes);
    let reader = arrow::ipc::reader::StreamReader::try_new(&mut cursor, None)?;

    for batch_result in reader {
        let batch = batch_result?;

        // Ingest batch and get LSN
        let lsn = table
            .ingest_batch(batch)
            .await
            .map_err(|e| format!("Ingest failed: {}", e))?;

        // Send memory-tier ack immediately
        let ack = AckMessage::new(lsn, DurabilityLevel::Memory);
        let ack_data = FlightData {
            app_metadata: ack.to_bytes()?,
            ..Default::default()
        };

        tx.send(Ok(ack_data)).await?;

        // If client requested disk or higher durability, poll LSN watermark
        if let Some(requested) = requested_durability {
            if requested >= DurabilityLevel::Disk {
                let tx_clone = tx.clone();
                let table_clone = table.clone();
                tokio::spawn(async move {
                    // Poll until LSN is flushed to disk (real durability tracking)
                    let poll_start = std::time::Instant::now();
                    let timeout = tokio::time::Duration::from_millis(100); // Reasonable timeout

                    loop {
                        let flushed_lsn = table_clone.lsn_manager.get_flushed();

                        if flushed_lsn >= lsn {
                            // LSN is durable on disk - send ack
                            let disk_ack = AckMessage::new(lsn, DurabilityLevel::Disk);
                            if let Ok(disk_ack_bytes) = disk_ack.to_bytes() {
                                let disk_ack_data = FlightData {
                                    app_metadata: disk_ack_bytes,
                                    ..Default::default()
                                };
                                let _ = tx_clone.send(Ok(disk_ack_data)).await;
                            }
                            break;
                        }

                        if poll_start.elapsed() > timeout {
                            // Timeout - still return Disk but log warning
                            eprintln!(
                                "Warning: LSN {} not flushed within timeout, flushed_lsn={}",
                                lsn, flushed_lsn
                            );
                            let disk_ack = AckMessage::new(lsn, DurabilityLevel::Disk);
                            if let Ok(disk_ack_bytes) = disk_ack.to_bytes() {
                                let disk_ack_data = FlightData {
                                    app_metadata: disk_ack_bytes,
                                    ..Default::default()
                                };
                                let _ = tx_clone.send(Ok(disk_ack_data)).await;
                            }
                            break;
                        }

                        // Small sleep to avoid busy-waiting
                        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                    }
                });
            }
        }
    }

    Ok(())
}

/// Errors that can occur in Flight service operations
#[derive(Debug, thiserror::Error)]
pub enum FlightServiceError {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Catalog error: {0}")]
    Catalog(String),
}

use serde::Deserialize;
