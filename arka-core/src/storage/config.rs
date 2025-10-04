use std::time::Duration;

/// Durability level for segment writes
///
/// Controls the trade-off between write latency and crash safety.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityLevel {
    /// Write with fsync - data is durable on disk (5-10ms latency)
    ///
    /// Use this when:
    /// - Data loss is unacceptable
    /// - Writes are infrequent
    /// - Latency is acceptable
    ///
    /// Performance: ~200 writes/sec
    Durable,

    /// Write without fsync - data is buffered by OS (microseconds latency)
    ///
    /// Use this when:
    /// - High throughput is critical
    /// - You have a background flusher
    /// - Can tolerate data loss on power failure (before OS flushes)
    ///
    /// Performance: 100k+ writes/sec
    Buffered,
}

impl Default for DurabilityLevel {
    fn default() -> Self {
        // Conservative default: ensure durability
        Self::Durable
    }
}

/// Configuration for SegmentWriter
#[derive(Debug, Clone)]
pub struct SegmentWriterConfig {
    /// Durability level for writes
    pub durability: DurabilityLevel,
}

impl Default for SegmentWriterConfig {
    fn default() -> Self {
        Self {
            durability: DurabilityLevel::Durable,
        }
    }
}

impl SegmentWriterConfig {
    /// Create config with buffered writes (no fsync)
    pub fn buffered() -> Self {
        Self {
            durability: DurabilityLevel::Buffered,
        }
    }

    /// Create config with durable writes (with fsync)
    pub fn durable() -> Self {
        Self {
            durability: DurabilityLevel::Durable,
        }
    }
}

/// Configuration for WriteBuffer
///
/// Controls memory usage and flush behavior for the write buffer.
#[derive(Debug, Clone)]
pub struct WriteBufferConfig {
    /// Maximum total bytes of unflushed data across all buffers
    ///
    /// When this limit is exceeded, new writes will block (backpressure)
    /// until the flusher makes space.
    ///
    /// Recommended: 256 MB - 1 GB depending on available memory
    pub max_unflushed_bytes: usize,

    /// Maximum size of a single buffer before it's frozen
    ///
    /// When current buffer exceeds this size, it's frozen and queued for flush.
    ///
    /// Recommended: 64 MB - 128 MB
    pub max_buffer_size_bytes: usize,

    /// Maximum number of batches in a single buffer
    ///
    /// Even if size limit not reached, freeze when batch count hits this.
    ///
    /// Recommended: 1000 - 10000 batches
    pub max_batches_per_buffer: usize,

    /// How frequently to flush buffers (time-based trigger)
    ///
    /// Even if size/count limits not reached, flush every interval.
    /// This bounds write latency for low-throughput scenarios.
    ///
    /// Recommended: 100ms - 1s
    pub flush_interval: Duration,
}

impl Default for WriteBufferConfig {
    fn default() -> Self {
        Self {
            // 512 MB total unflushed (like SlateDB's max_unflushed_bytes)
            max_unflushed_bytes: 512 * 1024 * 1024,

            // 64 MB per buffer (like SlateDB's l0_sst_size_bytes)
            max_buffer_size_bytes: 64 * 1024 * 1024,

            // 1000 batches max per buffer
            max_batches_per_buffer: 1000,

            // Flush every 100ms (like SlateDB's flush_interval)
            flush_interval: Duration::from_millis(100),
        }
    }
}

impl WriteBufferConfig {
    /// Create config optimized for high throughput
    pub fn high_throughput() -> Self {
        Self {
            max_unflushed_bytes: 1024 * 1024 * 1024,  // 1 GB
            max_buffer_size_bytes: 128 * 1024 * 1024, // 128 MB
            max_batches_per_buffer: 10000,
            flush_interval: Duration::from_millis(500), // Flush less frequently
        }
    }

    /// Create config optimized for low latency
    pub fn low_latency() -> Self {
        Self {
            max_unflushed_bytes: 128 * 1024 * 1024,  // 128 MB
            max_buffer_size_bytes: 16 * 1024 * 1024, // 16 MB
            max_batches_per_buffer: 100,
            flush_interval: Duration::from_millis(50), // Flush more frequently
        }
    }

    /// Create config for testing (small buffers, fast flushes)
    #[cfg(test)]
    pub fn test() -> Self {
        Self {
            max_unflushed_bytes: 10 * 1024 * 1024, // 10 MB
            max_buffer_size_bytes: 1024 * 1024,    // 1 MB
            max_batches_per_buffer: 10,
            flush_interval: Duration::from_millis(10),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_durability_level_default() {
        assert_eq!(DurabilityLevel::default(), DurabilityLevel::Durable);
    }

    #[test]
    fn test_segment_writer_config() {
        let buffered = SegmentWriterConfig::buffered();
        assert_eq!(buffered.durability, DurabilityLevel::Buffered);

        let durable = SegmentWriterConfig::durable();
        assert_eq!(durable.durability, DurabilityLevel::Durable);
    }

    #[test]
    fn test_write_buffer_config_defaults() {
        let config = WriteBufferConfig::default();
        assert_eq!(config.max_unflushed_bytes, 512 * 1024 * 1024);
        assert_eq!(config.max_buffer_size_bytes, 64 * 1024 * 1024);
        assert_eq!(config.max_batches_per_buffer, 1000);
        assert_eq!(config.flush_interval, Duration::from_millis(100));
    }

    #[test]
    fn test_write_buffer_config_presets() {
        let high = WriteBufferConfig::high_throughput();
        assert!(high.max_unflushed_bytes > WriteBufferConfig::default().max_unflushed_bytes);

        let low = WriteBufferConfig::low_latency();
        assert!(low.flush_interval < WriteBufferConfig::default().flush_interval);
    }
}
