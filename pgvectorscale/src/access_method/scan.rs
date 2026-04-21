//! Index scan callbacks for hanns_hnsw access method.
//!
//! Search: load hanns index from PG pages (with in-memory cache) → search → return results.

use pgrx::prelude::*;
use pgrx::PgRelation;
use std::os::raw::c_void;

/// Scan state stored as the index scan's opaque data.
struct HnswScanState {
    results: Vec<(i64, f32)>, // (ctid_as_i64, distance)
    pos: usize,
}

#[pg_guard]
pub extern "C-unwind" fn ambeginscan(
    index_relation: pg_sys::Relation,
    _nkeys: ::std::os::raw::c_int,
    _norderbys: ::std::os::raw::c_int,
) -> pg_sys::IndexScanDesc {
    unsafe {
        let scan = pg_sys::RelationGetIndexScan(index_relation, 0, 1);
        (*scan).xs_recheck = false;
        (*scan).opaque = std::ptr::null_mut();
        scan
    }
}

#[pg_guard]
pub extern "C-unwind" fn amrescan(
    scan: pg_sys::IndexScanDesc,
    _keys: pg_sys::ScanKey,
    _nkeys: ::std::os::raw::c_int,
    orderbys: pg_sys::ScanKey,
    _norderbys: ::std::os::raw::c_int,
) {
    unsafe {
        pgrx::warning!("scan: amrescan called, orderbys={:?}", orderbys);
        if orderbys.is_null() {
            return;
        }

        let index_rel = PgRelation::from_pg((*scan).indexRelation);
        let orderby = &*orderbys;
        let query_datum = orderby.sk_argument;

        // Extract query vector from pgvector Datum
        let varlena = pg_sys::pg_detoast_datum(query_datum.cast_mut_ptr());
        let ptr = varlena as *const u8;
        let dim = *(ptr.add(4) as *const u16);
        pgrx::warning!("scan: query dim={}, varlena bytes: {:02x}{:02x}{:02x}{:02x} {:02x}{:02x}{:02x}{:02x}",
            dim, *ptr.add(0), *ptr.add(1), *ptr.add(2), *ptr.add(3),
            *ptr.add(4), *ptr.add(5), *ptr.add(6), *ptr.add(7));
        let query_vec = std::slice::from_raw_parts(ptr.add(8) as *const f32, dim as usize);

        // Load index from cache or pages
        let hnsw = crate::access_method::index_cache::get_or_load(&index_rel)
            .expect("failed to load hanns index");

        // Search
        let ef_search = crate::access_method::guc::ef_search();
        let top_k = 100; // fetch enough for LIMIT to filter
        let req = hanns::SearchRequest {
            top_k,
            nprobe: ef_search,
            filter: None,
            params: None,
            radius: None,
        };

        pgrx::warning!("scan: about to search, query_len={}", query_vec.len());
        let result = hnsw.search(query_vec, &req).expect("hanns search failed");
        pgrx::warning!("scan: search ok, {} results", result.ids.len());

        // Convert to scan state
        let mut state = Box::new(HnswScanState {
            results: Vec::with_capacity(result.ids.len()),
            pos: 0,
        });

        for (i, &id) in result.ids.iter().enumerate() {
            state.results.push((id, result.distances[i]));
        }

        (*scan).opaque = Box::into_raw(state) as *mut c_void;
    }
}

#[pg_guard]
pub extern "C-unwind" fn amgettuple(
    scan: pg_sys::IndexScanDesc,
    _direction: pg_sys::ScanDirection::Type,
) -> bool {
    unsafe {
        if (*scan).opaque.is_null() {
            return false;
        }

        let state = &mut *((*scan).opaque as *mut HnswScanState);

        if state.pos >= state.results.len() {
            return false;
        }

        let (id, _dist) = state.results[state.pos];
        state.pos += 1;

        // Decode i64 back to ctid
        let block = (id >> 16) as u32;
        let offset = (id & 0xFFFF) as u16;

        (*scan).xs_heaptid = pg_sys::ItemPointerData {
            ip_blkid: pg_sys::BlockIdData {
                bi_hi: (block >> 16) as u16,
                bi_lo: (block & 0xFFFF) as u16,
            },
            ip_posid: offset,
        };

        true
    }
}

#[pg_guard]
pub extern "C-unwind" fn amendscan(scan: pg_sys::IndexScanDesc) {
    unsafe {
        if !(*scan).opaque.is_null() {
            let _state = Box::from_raw((*scan).opaque as *mut HnswScanState);
            // dropped here
        }
        (*scan).opaque = std::ptr::null_mut();
    }
}
