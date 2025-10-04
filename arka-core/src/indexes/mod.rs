pub mod pk_index;
pub mod segment_index;

use crate::storage::segment::SegmentId;

/// Location of a row within the storage system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RowLocation {
    /// Which segment contains this row
    pub segment_id: SegmentId,

    /// Row index within that segment (0-based)
    pub row_index: u32,
}

impl RowLocation {
    pub fn new(segment_id: SegmentId, row_index: u32) -> Self {
        Self {
            segment_id,
            row_index,
        }
    }
}
