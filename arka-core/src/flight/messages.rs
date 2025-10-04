use serde::{Deserialize, Serialize};

/// Durability tier for write acknowledgments
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum DurabilityLevel {
    /// Data in memory buffer (fastest, ~0.1ms)
    Memory = 0,

    /// Data flushed to local disk (fsync, ~10ms)
    Disk = 1,

    /// Data uploaded to object storage (S3/R2, ~100ms)
    ObjectStorage = 2,

    /// Data committed to Iceberg table (visible in queries, ~500ms)
    IcebergCommitted = 3,
}

impl DurabilityLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            DurabilityLevel::Memory => "memory",
            DurabilityLevel::Disk => "disk",
            DurabilityLevel::ObjectStorage => "object_storage",
            DurabilityLevel::IcebergCommitted => "iceberg_committed",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "memory" => Some(DurabilityLevel::Memory),
            "disk" => Some(DurabilityLevel::Disk),
            "object_storage" => Some(DurabilityLevel::ObjectStorage),
            "iceberg_committed" => Some(DurabilityLevel::IcebergCommitted),
            _ => None,
        }
    }
}

/// Write request metadata sent in FlightData.app_metadata
///
/// This is sent as the first message in a DoExchange stream to specify
/// table and durability preferences.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteRequest {
    /// Target table name (assumes "default" namespace)
    pub table_name: String,

    /// Requested durability level for notifications
    /// Client will receive acks when data reaches this tier
    #[serde(default)]
    pub requested_durability: Option<DurabilityLevel>,

    /// Optional idempotency key for deduplication
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

impl WriteRequest {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            requested_durability: None,
            idempotency_key: None,
        }
    }

    pub fn with_durability(mut self, level: DurabilityLevel) -> Self {
        self.requested_durability = Some(level);
        self
    }

    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Serialize to JSON bytes for app_metadata
    pub fn to_bytes(&self) -> Result<bytes::Bytes, serde_json::Error> {
        let json = serde_json::to_vec(self)?;
        Ok(bytes::Bytes::from(json))
    }

    /// Deserialize from app_metadata bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// Acknowledgment message sent from server to client
///
/// Sent via FlightData.app_metadata to notify client about write progress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AckMessage {
    /// LSN assigned to this write
    pub lsn: u64,

    /// Durability tier reached
    pub durability_level: DurabilityLevel,

    /// Timestamp when this tier was reached (Unix epoch microseconds)
    pub timestamp_us: i64,

    /// Optional error message
    #[serde(default)]
    pub error: Option<String>,
}

impl AckMessage {
    pub fn new(lsn: u64, durability_level: DurabilityLevel) -> Self {
        Self {
            lsn,
            durability_level,
            timestamp_us: chrono::Utc::now().timestamp_micros(),
            error: None,
        }
    }

    pub fn with_error(mut self, error: impl Into<String>) -> Self {
        self.error = Some(error.into());
        self
    }

    /// Serialize to JSON bytes for app_metadata
    pub fn to_bytes(&self) -> Result<bytes::Bytes, serde_json::Error> {
        let json = serde_json::to_vec(self)?;
        Ok(bytes::Bytes::from(json))
    }

    /// Deserialize from app_metadata bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_request_serialization() {
        let req = WriteRequest::new("users")
            .with_durability(DurabilityLevel::Disk)
            .with_idempotency_key("batch-123");

        let bytes = req.to_bytes().unwrap();
        let decoded = WriteRequest::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.table_name, "users");
        assert_eq!(decoded.requested_durability, Some(DurabilityLevel::Disk));
        assert_eq!(decoded.idempotency_key, Some("batch-123".to_string()));
    }

    #[test]
    fn test_ack_message_serialization() {
        let ack = AckMessage::new(12345, DurabilityLevel::Memory);

        let bytes = ack.to_bytes().unwrap();
        let decoded = AckMessage::from_bytes(&bytes).unwrap();

        assert_eq!(decoded.lsn, 12345);
        assert_eq!(decoded.durability_level, DurabilityLevel::Memory);
        assert!(decoded.error.is_none());
    }

    #[test]
    fn test_durability_level_ordering() {
        assert!(DurabilityLevel::Memory < DurabilityLevel::Disk);
        assert!(DurabilityLevel::Disk < DurabilityLevel::ObjectStorage);
        assert!(DurabilityLevel::ObjectStorage < DurabilityLevel::IcebergCommitted);
    }
}
