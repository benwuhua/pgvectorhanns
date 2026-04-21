//! GUC parameters for hanns_hnsw access method.

use pgrx::{pg_sys::AsPgCStr, *};

pub static HANNS_EF_SEARCH: GucSetting<i32> = GucSetting::<i32>::new(40);

pub fn init() {
    GucRegistry::define_int_guc(
        unsafe { std::ffi::CStr::from_ptr("hanns_hnsw.ef_search".as_pg_cstr()) },
        unsafe {
            std::ffi::CStr::from_ptr(
                "The search beam width for HNSW queries".as_pg_cstr(),
            )
        },
        unsafe {
            std::ffi::CStr::from_ptr(
                "Higher value increases recall at the cost of speed.".as_pg_cstr(),
            )
        },
        &HANNS_EF_SEARCH,
        1,
        10000,
        GucContext::Userset,
        GucFlags::default(),
    );
}

/// Get the current ef_search GUC value.
pub fn ef_search() -> usize {
    HANNS_EF_SEARCH.get() as usize
}
