//! Index cache: process-local in-memory cache for hanns HnswIndex instances.

use hanns::HnswIndex;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use pgrx::prelude::*;
use pgrx::PgRelation;
use std::collections::HashMap;
use std::sync::Arc;

type CacheMap = HashMap<pg_sys::Oid, Arc<HnswIndex>>;

static INDEX_CACHE: Lazy<RwLock<CacheMap>> = Lazy::new(|| RwLock::new(HashMap::new()));

pub fn init() {
    extern "C" {
        fn CacheRegisterRelcacheCallback(
            callback: Option<unsafe extern "C" fn(pg_sys::Datum, pg_sys::Oid)>,
            arg: pg_sys::Datum,
        );
    }
    unsafe {
        CacheRegisterRelcacheCallback(Some(relcache_callback), pg_sys::Datum::from(0));
    }
}

pub fn get_or_load(index_rel: &PgRelation) -> Result<Arc<HnswIndex>, String> {
    let oid = unsafe { (*index_rel.as_ptr()).rd_id };

    // Fast path
    {
        let cache = INDEX_CACHE.read();
        if let Some(idx) = cache.get(&oid) {
            return Ok(Arc::clone(idx));
        }
    }

    // Slow path: load from pages
    let t_total = std::time::Instant::now();
    let meta = unsafe { crate::access_method::meta_page::MetaPage::read(index_rel)? };

    // Read serialized data from pages
    let t_read = std::time::Instant::now();
    let data = unsafe { read_index_data_from_pages(index_rel, &meta) };
    let read_elapsed = t_read.elapsed();

    // Deserialize in a dedicated thread with 32MB stack to avoid PG backend
    // stack limitations. PG backends use ~2MB stacks; hanns deserialize has
    // deep recursion and SIMD that can segfault in constrained stacks.
    let t_deser = std::time::Instant::now();
    pgrx::warning!("index_cache: starting deserialize in thread, data_len={}", data.len());

    let data_clone = data.clone();
    let builder = std::thread::Builder::new()
        .name("hanns-deser".into())
        .stack_size(32 * 1024 * 1024); // 32MB stack
    let handle = builder.spawn(move || {
        HnswIndex::fast_deserialize_from_bytes(&data_clone)
    }).map_err(|e| format!("thread spawn: {e}"))?;

    let index = match handle.join() {
        Ok(Ok(idx)) => idx,
        Ok(Err(e)) => return Err(format!("deserialize: {e}")),
        Err(_) => return Err("deserialize panicked (thread join failed)".into()),
    };
    pgrx::warning!(
        "index_cache: deserialize done in {:?}, data_len={}",
        t_deser.elapsed(), data.len()
    );

    // Verify: try a dummy search to confirm index is valid
    let dim = meta.num_dimensions as usize;
    let dummy_query = vec![0.0f32; dim];
    let dummy_req = hanns::SearchRequest { top_k: 1, nprobe: 10, ..Default::default() };
    match index.search(&dummy_query, &dummy_req) {
        Ok(r) => pgrx::warning!("index_cache: verify search ok, {} results", r.ids.len()),
        Err(e) => pgrx::warning!("index_cache: verify search FAILED: {}", e),
    }
    let deser_elapsed = t_deser.elapsed();

    pgrx::warning!(
        "index_cache: load {} bytes: read={:?}, deserialize={:?}, total={:?}",
        data.len(), read_elapsed, deser_elapsed, t_total.elapsed()
    );

    let arc = Arc::new(index);
    {
        let mut cache = INDEX_CACHE.write();
        cache.insert(oid, Arc::clone(&arc));
    }
    Ok(arc)
}

unsafe fn read_index_data_from_pages(
    index_rel: &PgRelation,
    meta: &crate::access_method::meta_page::MetaPage,
) -> Vec<u8> {
    let num_blocks = meta.index_data_num_blocks as usize;
    let mut result = Vec::with_capacity(num_blocks * 8100);

    for i in 0..num_blocks {
        let block_no = 1 + i as pg_sys::BlockNumber;
        let buffer = pg_sys::ReadBuffer(index_rel.as_ptr(), block_no);
        pg_sys::LockBuffer(buffer, pg_sys::BUFFER_LOCK_SHARE as i32);
        let page = pg_sys::BufferGetPage(buffer);

        let max_offset = pg_sys::PageGetMaxOffsetNumber(page);
        if max_offset == 0 {
            pg_sys::UnlockReleaseBuffer(buffer);
            continue;
        }

        let item_id = pg_sys::PageGetItemId(page, 1);
        let item = pg_sys::PageGetItem(page, item_id);
        let item_len = (*item_id).lp_len() as usize;
        let data = std::slice::from_raw_parts(item as *const u8, item_len);
        result.extend_from_slice(data);

        pg_sys::UnlockReleaseBuffer(buffer);
    }

    pgrx::warning!(
        "index_cache: loaded {} blocks, {} bytes total",
        num_blocks, result.len());
    result
}

unsafe extern "C" fn relcache_callback(_arg: pg_sys::Datum, relid: pg_sys::Oid) {
    if relid != pg_sys::InvalidOid {
        INDEX_CACHE.write().remove(&relid);
    }
}
