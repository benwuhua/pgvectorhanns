//! Index build and insert callbacks for hanns_hnsw access method.
//!
//! Build: collect all vectors → hanns HnswIndex bulk build → serialize to PG pages.
//! Insert: TODO (Phase 2) — for now, reindex is needed after inserts.

use pgrx::prelude::*;
use pgrx::PgRelation;
use pgrx::warning;
use std::os::raw::c_void;

use crate::access_method::distance::DistanceType;
use crate::access_method::meta_page::MetaPage;

/// Build state passed to the heap scan callback.
struct BuildState {
    vectors: Vec<f32>,
    ids: Vec<i64>,
    dim: usize,
    ntuples: usize,
}

#[pg_guard]
pub extern "C-unwind" fn ambuild(
    heaprel: pg_sys::Relation,
    indexrel: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    let _heap_relation = unsafe { PgRelation::from_pg(heaprel) };
    let index_relation = unsafe { PgRelation::from_pg(indexrel) };

    // Determine dimensions from the index tuple descriptor
    let tupdesc = index_relation.tuple_desc();
    let dim = get_num_dimensions_from_attribute(&tupdesc, 0)
        .expect("could not determine vector dimensions") as usize;

    let distance_type = unsafe { get_distance_type(indexrel) };

    // Read index options (m, ef_construction, ef_search)
    let opts = crate::access_method::options::TSVIndexOptions::from_relation(&index_relation);
    let m = opts.get_m() as u32;
    let ef_construction = opts.get_ef_construction();
    let ef_search = opts.get_ef_search();

    // Collect all vectors via heap scan
    let mut state = BuildState {
        vectors: Vec::new(),
        ids: Vec::new(),
        dim,
        ntuples: 0,
    };

    unsafe {
        pg_sys::IndexBuildHeapScan(
            heaprel,
            indexrel,
            index_info,
            Some(build_callback),
            &mut state as *mut _ as *mut c_void,
        );
    }

    // Build hanns HNSW index
    let t0 = std::time::Instant::now();
    let config = hanns::api::IndexConfig {
        index_type: hanns::api::IndexType::Hnsw,
        metric_type: distance_type.to_hanns_metric(),
        dim,
        data_type: hanns::api::DataType::Float,
        params: hanns::api::IndexParams {
            ef_construction: Some(ef_construction as usize),
            ef_search: Some(ef_search as usize),
            m: Some(m as usize),
            // Use ml=0 to keep all nodes on level 0 (single-layer HNSW).
            // Multi-layer mode (ml=1/ln(m)) reduces recall from 99.4% to 91.5%
            // on 50K vectors because the multi-level traversal misses some nodes.
            // Single-layer is simpler, more reliable, and sufficient for <1M scale.
            ml: Some(0.0),
            ..Default::default()
        },
    };
    let mut hnsw = hanns::HnswIndex::new(&config).expect("failed to create HnswIndex");
    warning!("hanns build: index created in {:?}", t0.elapsed());

    // Train is a no-op for HNSW but required by the API
    let t1 = std::time::Instant::now();
    hnsw.train(&state.vectors).expect("hnsw train failed");
    warning!("hanns build: train in {:?}", t1.elapsed());

    let t2 = std::time::Instant::now();
    hnsw.add(&state.vectors, Some(&state.ids))
        .expect("hnsw add failed");
    warning!("hanns build: add {} vectors in {:?}", state.ntuples, t2.elapsed());

    // Serialize and write to PG pages
    let t3 = std::time::Instant::now();
    let serialized = hnsw.serialize_to_bytes().expect("hnsw serialize failed");
    warning!("hanns build: serialize {} bytes in {:?}", serialized.len(), t3.elapsed());

    let mut meta = MetaPage::new(dim as u32, m, ef_construction, ef_search, distance_type);
    meta.num_vectors = state.ntuples as u64;

    unsafe {
        // Write meta page first (creates block 0), then write data (blocks 1..N),
        // then update meta with the block count.
        // We write meta twice: once to create block 0, once to set index_data_num_blocks.
        meta.num_vectors = state.ntuples as u64;
        meta.index_data_num_blocks = 0; // will be set properly below
        meta.write_new(&index_relation);
        let t4 = std::time::Instant::now();
        write_index_data(&index_relation, &serialized, &mut meta);
        warning!("hanns build: write {} pages in {:?}", meta.index_data_num_blocks, t4.elapsed());
        // Overwrite meta page with final block count
        meta.write_update(&index_relation);
    }

    let mut result = unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0() };
    result.heap_tuples = state.ntuples as f64;
    result.index_tuples = state.ntuples as f64;
    result.into_pg()
}

unsafe extern "C-unwind" fn build_callback(
    _index: pg_sys::Relation,
    ctid: pg_sys::ItemPointer,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    _tuple_is_alive: bool,
    state: *mut c_void,
) {
    let state = &mut *(state as *mut BuildState);

    // Skip NULL values
    if unsafe { *isnull.add(0) } {
        return;
    }

    let datum = unsafe { *values.add(0) };
    let (vec_dim, vec_data) = unsafe { datum_to_vector_components(datum) };

    if vec_dim as usize != state.dim {
        panic!(
            "vector dimension mismatch: expected {}, got {}",
            state.dim, vec_dim
        );
    }

    state.vectors.extend_from_slice(&vec_data);

    // Encode ctid as i64
    let ctid_val = unsafe { *ctid };
    let block = pgrx::itemptr::item_pointer_get_block_number(&ctid_val) as i64;
    let offset = ctid_val.ip_posid as i64;
    state.ids.push((block << 16) | offset);

    state.ntuples += 1;
}

#[pg_guard]
pub extern "C-unwind" fn ambuildempty(_indexrel: pg_sys::Relation) {
    // TODO: create empty meta page
}

#[pg_guard]
pub unsafe extern "C-unwind" fn aminsert(
    _indexrel: pg_sys::Relation,
    _values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    _heap_tid: pg_sys::ItemPointer,
    _heaprel: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck::Type,
    _index_unchanged: bool,
    _index_info: *mut pg_sys::IndexInfo,
) -> bool {
    // TODO: implement single-tuple insert
    // For MVP, inserts require REINDEX
    panic!("hanns_hnsw: INSERT into existing index not yet supported. Use REINDEX after data changes.");
}

/// Read distance type from the index's operator class support function.
unsafe fn get_distance_type(indexrel: pg_sys::Relation) -> DistanceType {
    let procinfo = pg_sys::index_getprocinfo(indexrel, 1, 1);
    if procinfo.is_null() {
        return DistanceType::Cosine; // default
    }
    let datum = pg_sys::OidFunctionCall0Coll((*procinfo).fn_oid, pg_sys::InvalidOid);
    let type_code = datum.value() as i16;
    DistanceType::from_u16(type_code as u16)
}

/// Extract dimensions and f32 slice from a pgvector Datum.
unsafe fn datum_to_vector_components(datum: pg_sys::Datum) -> (u16, Vec<f32>) {
    let varlena = pg_sys::pg_detoast_datum(datum.cast_mut_ptr());
    let ptr = varlena as *const u8;
    // pgvector layout: int32 vl_len_, uint16 dim, uint16 unused, float x[]
    let dim = *(ptr.add(4) as *const u16);
    let slice = std::slice::from_raw_parts(ptr.add(8) as *const f32, dim as usize);
    (dim, slice.to_vec())
}

/// Get number of dimensions from the index tuple descriptor attribute.
fn get_num_dimensions_from_attribute(
    tupdesc: &pgrx::PgTupleDesc<'_>,
    attr_idx: usize,
) -> Option<u32> {
    let attr = tupdesc.get(attr_idx)?;
    let typmod = attr.type_mod();
    // pgvector stores dimensions directly in typmod (e.g., vector(128) → typmod = 128)
    if typmod > 0 {
        Some(typmod as u32)
    } else {
        None
    }
}

/// Write serialized index data to PG pages (blocks 1..N).
unsafe fn write_index_data(
    index_rel: &PgRelation,
    data: &[u8],
    meta: &mut MetaPage,
) {
    // Each page can hold ~8100 bytes of payload via PageAddItem
    let usable_per_page: usize = 8100;
    let mut offset = 0;
    let mut blocks_written: u32 = 0;

    while offset < data.len() {
        let chunk_len = std::cmp::min(usable_per_page, data.len() - offset);
        let chunk = &data[offset..offset + chunk_len];

        // Extend the relation by one block
        let buffer = {
            let _lock = pg_sys::LockRelationForExtension(
                index_rel.as_ptr(),
                pg_sys::ExclusiveLock as pg_sys::LOCKMODE,
            );
            pg_sys::ReadBufferExtended(
                index_rel.as_ptr(),
                pg_sys::ForkNumber::MAIN_FORKNUM,
                pg_sys::InvalidBlockNumber, // extend
                pg_sys::ReadBufferMode::RBM_NORMAL,
                std::ptr::null_mut(),
            )
        };
        pg_sys::LockBuffer(buffer, pg_sys::BUFFER_LOCK_EXCLUSIVE as i32);

        let page = pg_sys::BufferGetPage(buffer);
        pg_sys::PageInit(page, pg_sys::BLCKSZ as usize, 0);

        // Add chunk as a page item using PG standard page management
        let off = pg_sys::PageAddItemExtended(
            page,
            chunk.as_ptr() as _,
            chunk.len(),
            pg_sys::InvalidOffsetNumber,
            0,
        );
        assert!(off != pg_sys::InvalidOffsetNumber, "PageAddItem failed for data block");

        pg_sys::MarkBufferDirty(buffer);
        pg_sys::UnlockReleaseBuffer(buffer);

        offset += chunk_len;
        blocks_written += 1;
    }

    meta.index_data_num_blocks = blocks_written;
}

#[cfg(any(feature = "pg17", feature = "pg18"))]
#[pg_guard]
pub unsafe extern "C-unwind" fn ambuildphasename(phase: i64) -> *mut std::os::raw::c_char {
    match phase {
        0 => c"building graph".as_ptr() as *mut _,
        _ => std::ptr::null_mut(),
    }
}
