use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::path::PathBuf;
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct LSN(pub u64);

impl LSN {
    pub const ZERO: LSN = LSN(0);

    pub fn next(&self) -> LSN {
        LSN(self.0 + 1)
    }
}

#[derive(Debug)]
pub enum DataLocation {
    Memory,
    Disk,
    Iceberg,
}

#[derive(Debug)]
pub struct LagInfo {
    pub unflushed_to_disk: u64,
    pub unflushed_to_iceberg: u64,
    pub total_pending: u64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Checkpoint {
    pub next_lsn: u64,
    pub wal_durable: u64,           // NEW: Track WAL persistence like Fluss
    pub flushed_to_disk: u64,
    pub flushed_to_iceberg: u64,
    pub last_applied_cdc: u64,
}

pub struct LSNcoordinator {
    next_lsn: AtomicU64,

    // NEW: Track WAL durability separately (like Fluss high watermark)
    wal_durable_lsn: AtomicU64,

    flushed_to_disk_lsn: AtomicU64,
    flushed_to_iceberg_lsn: AtomicU64,
    last_applied_cdc_lsn: AtomicU64,

    // NEW: Notifications like Moonlink
    notify_tx: watch::Sender<LSN>,

    // NEW: Checkpoint persistence like Fluss
    checkpoint_path: Option<PathBuf>,
}

impl LSNcoordinator {
    pub fn new() -> Self {
        let (tx, _rx) = watch::channel(LSN::ZERO);
        Self {
            next_lsn: AtomicU64::new(1),
            wal_durable_lsn: AtomicU64::new(0),
            flushed_to_disk_lsn: AtomicU64::new(0),
            flushed_to_iceberg_lsn: AtomicU64::new(0),
            last_applied_cdc_lsn: AtomicU64::new(0),
            notify_tx: tx,
            checkpoint_path: None,
        }
    }

    pub fn new_with_checkpoint_path(path: PathBuf) -> Self {
        let (tx, _rx) = watch::channel(LSN::ZERO);
        Self {
            next_lsn: AtomicU64::new(1),
            wal_durable_lsn: AtomicU64::new(0),
            flushed_to_disk_lsn: AtomicU64::new(0),
            flushed_to_iceberg_lsn: AtomicU64::new(0),
            last_applied_cdc_lsn: AtomicU64::new(0),
            notify_tx: tx,
            checkpoint_path: Some(path),
        }
    }

    /// Get notification channel for LSN updates
    pub fn subscribe(&self) -> watch::Receiver<LSN> {
        self.notify_tx.subscribe()
    }

    /// Assign LSN for incoming record
    pub fn assign_lsn(&self) -> LSN {
        LSN(self.next_lsn.fetch_add(1, Ordering::SeqCst))
    }

    /// Assign LSN range for batch (fixed edge case handling)
    pub fn assign_batch_lsn(&self, count: u64) -> Result<(LSN, LSN), &'static str> {
        if count == 0 {
            return Err("Cannot assign LSN for empty batch");
        }
        let start = self.next_lsn.fetch_add(count, Ordering::SeqCst);
        Ok((LSN(start), LSN(start + count - 1)))
    }

    /// NEW: Mark LSN as durable in WAL (like Fluss updateHighWatermark)
    pub fn mark_wal_durable(&self, up_to_lsn: LSN) {
        let old = self.wal_durable_lsn.fetch_max(up_to_lsn.0, Ordering::SeqCst);
        // Notify if we advanced
        if up_to_lsn.0 > old {
            let _ = self.notify_tx.send(up_to_lsn);
        }
    }

    /// Mark data as flushed to disk
    pub fn mark_flushed_to_disk(&self, up_to_lsn: LSN) {
        self.flushed_to_disk_lsn
            .fetch_max(up_to_lsn.0, Ordering::SeqCst);
    }

    /// Mark data as flushed to Iceberg
    pub fn mark_flushed_to_iceberg(&self, up_to_lsn: LSN) {
        self.flushed_to_iceberg_lsn
            .fetch_max(up_to_lsn.0, Ordering::SeqCst);
    }

    /// Mark CDC as applied (FIXED: was updating wrong field!)
    pub fn mark_cdc_applied(&self, up_to_lsn: LSN) {
        self.last_applied_cdc_lsn
            .fetch_max(up_to_lsn.0, Ordering::SeqCst);
    }

    /// Check where data for given LSN lives
    pub fn locate_lsn(&self, lsn: LSN) -> DataLocation {
        let iceberg_lsn = self.flushed_to_iceberg_lsn.load(Ordering::SeqCst);
        let disk_lsn = self.flushed_to_disk_lsn.load(Ordering::SeqCst);

        if lsn.0 <= iceberg_lsn {
            DataLocation::Iceberg
        } else if lsn.0 <= disk_lsn {
            DataLocation::Disk
        } else {
            DataLocation::Memory
        }
    }

    /// Check if CDC should be applied to this LSN (improved logic)
    pub fn should_apply_cdc(&self, target_lsn: LSN) -> bool {
        target_lsn.0 > self.last_applied_cdc_lsn.load(Ordering::SeqCst)
    }

    /// NEW: Check if LSN is duplicate (for recovery deduplication like Moonlink)
    pub fn is_duplicate(&self, lsn: LSN) -> bool {
        lsn.0 <= self.wal_durable_lsn.load(Ordering::SeqCst)
    }

    /// NEW: Validate LSN monotonicity
    pub fn validate_lsn(&self, lsn: LSN) -> bool {
        lsn.0 <= self.next_lsn.load(Ordering::SeqCst)
    }

    /// Get current lag metrics
    pub fn get_lag(&self) -> LagInfo {
        let current = self.next_lsn.load(Ordering::SeqCst);
        let disk = self.flushed_to_disk_lsn.load(Ordering::SeqCst);
        let iceberg = self.flushed_to_iceberg_lsn.load(Ordering::SeqCst);

        LagInfo {
            unflushed_to_disk: current - disk,
            unflushed_to_iceberg: current - iceberg,
            total_pending: current - iceberg,
        }
    }

    /// Create checkpoint data structure
    pub fn checkpoint(&self) -> Checkpoint {
        Checkpoint {
            next_lsn: self.next_lsn.load(Ordering::SeqCst),
            wal_durable: self.wal_durable_lsn.load(Ordering::SeqCst),
            flushed_to_disk: self.flushed_to_disk_lsn.load(Ordering::SeqCst),
            flushed_to_iceberg: self.flushed_to_iceberg_lsn.load(Ordering::SeqCst),
            last_applied_cdc: self.last_applied_cdc_lsn.load(Ordering::SeqCst),
        }
    }

    /// NEW: Persist checkpoint atomically (like Fluss)
    pub async fn persist_checkpoint(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(path) = &self.checkpoint_path {
            let checkpoint = self.checkpoint();
            let json = serde_json::to_string_pretty(&checkpoint)?;

            // Atomic write like Fluss: write to temp file then rename
            let temp_path = path.with_extension("tmp");
            tokio::fs::write(&temp_path, json).await?;
            tokio::fs::rename(&temp_path, path).await?;

            Ok(())
        } else {
            Err("No checkpoint path configured".into())
        }
    }

    /// Recover from checkpoint
    pub fn recover_from(checkpoint: Checkpoint) -> Self {
        let (tx, _rx) = watch::channel(LSN(checkpoint.wal_durable));
        Self {
            next_lsn: AtomicU64::new(checkpoint.next_lsn),
            wal_durable_lsn: AtomicU64::new(checkpoint.wal_durable),
            flushed_to_disk_lsn: AtomicU64::new(checkpoint.flushed_to_disk),
            flushed_to_iceberg_lsn: AtomicU64::new(checkpoint.flushed_to_iceberg),
            last_applied_cdc_lsn: AtomicU64::new(checkpoint.last_applied_cdc),
            notify_tx: tx,
            checkpoint_path: None,
        }
    }

    /// NEW: Load checkpoint from file (like Fluss)
    pub async fn load_checkpoint(path: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        if path.exists() {
            let json = tokio::fs::read_to_string(&path).await?;
            let checkpoint: Checkpoint = serde_json::from_str(&json)?;

            let (tx, _rx) = watch::channel(LSN(checkpoint.wal_durable));
            Ok(Self {
                next_lsn: AtomicU64::new(checkpoint.next_lsn),
                wal_durable_lsn: AtomicU64::new(checkpoint.wal_durable),
                flushed_to_disk_lsn: AtomicU64::new(checkpoint.flushed_to_disk),
                flushed_to_iceberg_lsn: AtomicU64::new(checkpoint.flushed_to_iceberg),
                last_applied_cdc_lsn: AtomicU64::new(checkpoint.last_applied_cdc),
                notify_tx: tx,
                checkpoint_path: Some(path),
            })
        } else {
            Ok(Self::new_with_checkpoint_path(path))
        }
    }

    /// Get current WAL durable LSN (for testing)
    pub fn get_wal_durable_lsn(&self) -> LSN {
        LSN(self.wal_durable_lsn.load(Ordering::SeqCst))
    }
}
