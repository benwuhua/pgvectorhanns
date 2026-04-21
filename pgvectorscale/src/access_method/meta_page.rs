//! Meta page (Block 0) for hanns_hnsw index.
//!
//! Uses raw page content area for meta page storage (simple and reliable).
//! Data pages use PageAddItem for proper PG page management.

use pgrx::prelude::*;
use pgrx::PgRelation;
use rkyv::{Archive, Deserialize, Serialize};

use super::distance::DistanceType;

pub const META_BLOCK: pg_sys::BlockNumber = 0;
const HANNS_MAGIC: u32 = 0x48414E53; // "HANS"
const HANNS_VERSION: u32 = 1;

/// Index metadata persisted in Block 0.
#[derive(Archive, Serialize, Deserialize, Clone, Debug)]
pub struct MetaPage {
    pub magic_number: u32,
    pub version: u32,
    pub distance_type: u16,
    pub num_dimensions: u32,
    pub m: u32,
    pub ef_construction: u32,
    pub ef_search: u32,
    pub num_vectors: u64,
    pub index_data_num_blocks: u32,
}

impl MetaPage {
    pub fn new(
        dim: u32,
        m: u32,
        ef_construction: u32,
        ef_search: u32,
        distance_type: DistanceType,
    ) -> Self {
        Self {
            magic_number: HANNS_MAGIC,
            version: HANNS_VERSION,
            distance_type: distance_type as u16,
            num_dimensions: dim,
            m,
            ef_construction,
            ef_search,
            num_vectors: 0,
            index_data_num_blocks: 0,
        }
    }

    /// Create block 0 and write meta data (first time).
    pub unsafe fn write_new(&self, index_rel: &PgRelation) {
        let bytes = rkyv::to_bytes::<_, 256>(self).expect("meta page serialize");
        self.write_bytes(index_rel, &bytes, true);
    }

    /// Overwrite block 0 meta data (update block count etc).
    pub unsafe fn write_update(&self, index_rel: &PgRelation) {
        let bytes = rkyv::to_bytes::<_, 256>(self).expect("meta page serialize");
        self.write_bytes(index_rel, &bytes, false);
    }

    unsafe fn write_bytes(&self, index_rel: &PgRelation, bytes: &[u8], is_new: bool) {
        let buffer = if is_new {
            // Extend to create block 0
            let _lock = pg_sys::LockRelationForExtension(
                index_rel.as_ptr(),
                pg_sys::ExclusiveLock as pg_sys::LOCKMODE,
            );
            let buf = pg_sys::ReadBufferExtended(
                index_rel.as_ptr(),
                pg_sys::ForkNumber::MAIN_FORKNUM,
                pg_sys::InvalidBlockNumber,
                pg_sys::ReadBufferMode::RBM_NORMAL,
                std::ptr::null_mut(),
            );
            pg_sys::UnlockRelationForExtension(
                index_rel.as_ptr(),
                pg_sys::ExclusiveLock as pg_sys::LOCKMODE,
            );
            buf
        } else {
            // Block 0 exists, read it
            pg_sys::ReadBuffer(index_rel.as_ptr(), META_BLOCK)
        };

        pg_sys::LockBuffer(buffer, pg_sys::BUFFER_LOCK_EXCLUSIVE as i32);
        let page = pg_sys::BufferGetPage(buffer);

        // Initialize page fresh each time (meta page has single item)
        pg_sys::PageInit(page, pg_sys::BLCKSZ as usize, 0);

        // Add meta data as item 1
        let offset = pg_sys::PageAddItemExtended(
            page,
            bytes.as_ptr() as _,
            bytes.len(),
            pg_sys::InvalidOffsetNumber,
            0,
        );
        assert!(offset != pg_sys::InvalidOffsetNumber, "meta PageAddItem failed");

        pg_sys::MarkBufferDirty(buffer);
        pg_sys::UnlockReleaseBuffer(buffer);
    }

    /// Read meta page from Block 0.
    pub unsafe fn read(index_rel: &PgRelation) -> Result<Self, String> {
        let buffer = pg_sys::ReadBuffer(index_rel.as_ptr(), META_BLOCK);
        pg_sys::LockBuffer(buffer, pg_sys::BUFFER_LOCK_SHARE as i32);
        let page = pg_sys::BufferGetPage(buffer);

        let item_id = pg_sys::PageGetItemId(page, 1);
        let item = pg_sys::PageGetItem(page, item_id);
        let item_len = (*item_id).lp_len() as usize;
        let data = std::slice::from_raw_parts(item as *const u8, item_len);

        let archived = rkyv::archived_root::<MetaPage>(data);
        let meta: MetaPage = archived.deserialize(&mut rkyv::Infallible).unwrap();

        pg_sys::UnlockReleaseBuffer(buffer);

        if meta.magic_number != HANNS_MAGIC {
            return Err(format!("invalid magic: {:x}", meta.magic_number));
        }
        Ok(meta)
    }
}
