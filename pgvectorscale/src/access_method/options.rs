//! Index options for hanns_hnsw access method.
//!
//! Replaces pgvectorscale's DiskANN options (num_neighbors, max_alpha, storage_layout)
//! with HNSW parameters: m, ef_construction, ef_search.

use memoffset::*;
use pgrx::{pg_sys::AsPgCStr, prelude::*, set_varsize_4b, PgRelation};

//DO NOT derive Clone for this struct. The varlena header requires careful handling.
#[derive(Debug, PartialEq)]
#[repr(C)]
pub struct TSVIndexOptions {
    /* varlena header (do not touch directly!) */
    #[allow(dead_code)]
    vl_len_: i32,

    pub m: i32,
    pub ef_construction: u32,
    pub ef_search: u32,
    pub num_dimensions: u32,
}

pub const M_DEFAULT_SENTINEL: i32 = -1;
pub const EF_CONSTRUCTION_DEFAULT: u32 = 200;
pub const EF_SEARCH_DEFAULT: u32 = 40;
pub const NUM_DIMENSIONS_DEFAULT_SENTINEL: u32 = 0;

impl TSVIndexOptions {
    pub fn from_relation(relation: &PgRelation) -> PgBox<TSVIndexOptions> {
        if relation.rd_index.is_null() {
            panic!("'{}' is not a hanns_hnsw index", relation.name())
        } else if relation.rd_options.is_null() {
            // use defaults
            let mut ops = unsafe { PgBox::<TSVIndexOptions>::alloc0() };
            ops.m = M_DEFAULT_SENTINEL;
            ops.ef_construction = EF_CONSTRUCTION_DEFAULT;
            ops.ef_search = EF_SEARCH_DEFAULT;
            ops.num_dimensions = NUM_DIMENSIONS_DEFAULT_SENTINEL;
            unsafe {
                set_varsize_4b(
                    ops.as_ptr().cast(),
                    std::mem::size_of::<TSVIndexOptions>() as i32,
                );
            }
            ops.into_pg_boxed()
        } else {
            unsafe { PgBox::from_pg(relation.rd_options as *mut TSVIndexOptions) }
        }
    }

    pub fn get_m(&self) -> i32 {
        if self.m == M_DEFAULT_SENTINEL {
            16 // sensible default for HNSW
        } else {
            if self.m < 2 {
                panic!("m must be >= 2, or -1 for default")
            }
            self.m
        }
    }

    pub fn get_ef_construction(&self) -> u32 {
        self.ef_construction
    }

    pub fn get_ef_search(&self) -> u32 {
        self.ef_search
    }

    pub fn get_num_dimensions(&self) -> u32 {
        self.num_dimensions
    }
}

static mut RELOPT_KIND_TSV: pg_sys::relopt_kind::Type = 0;

#[allow(clippy::unneeded_field_pattern)] // b/c of offset_of!()
#[pg_guard]
pub unsafe extern "C-unwind" fn amoptions(
    reloptions: pg_sys::Datum,
    validate: bool,
) -> *mut pg_sys::bytea {
    fn make_relopt_parse_elt(
        optname: &str,
        opttype: pg_sys::relopt_type::Type,
        offset: i32,
    ) -> pg_sys::relopt_parse_elt {
        #[cfg(not(feature = "pg18"))]
        {
            pg_sys::relopt_parse_elt {
                optname: optname.as_pg_cstr(),
                opttype,
                offset,
            }
        }
        #[cfg(feature = "pg18")]
        {
            pg_sys::relopt_parse_elt {
                optname: optname.as_pg_cstr(),
                opttype,
                offset,
                isset_offset: 0,
            }
        }
    }

    let tab: [pg_sys::relopt_parse_elt; 4] = [
        make_relopt_parse_elt(
            "m",
            pg_sys::relopt_type::RELOPT_TYPE_INT,
            offset_of!(TSVIndexOptions, m) as i32,
        ),
        make_relopt_parse_elt(
            "ef_construction",
            pg_sys::relopt_type::RELOPT_TYPE_INT,
            offset_of!(TSVIndexOptions, ef_construction) as i32,
        ),
        make_relopt_parse_elt(
            "ef_search",
            pg_sys::relopt_type::RELOPT_TYPE_INT,
            offset_of!(TSVIndexOptions, ef_search) as i32,
        ),
        make_relopt_parse_elt(
            "num_dimensions",
            pg_sys::relopt_type::RELOPT_TYPE_INT,
            offset_of!(TSVIndexOptions, num_dimensions) as i32,
        ),
    ];

    let rdopts = pg_sys::build_reloptions(
        reloptions,
        validate,
        RELOPT_KIND_TSV,
        std::mem::size_of::<TSVIndexOptions>(),
        tab.as_ptr(),
        tab.len() as i32,
    );

    rdopts as *mut pg_sys::bytea
}

/// # Safety
pub unsafe fn init() {
    RELOPT_KIND_TSV = pg_sys::add_reloption_kind();

    pg_sys::add_int_reloption(
        RELOPT_KIND_TSV,
        "m".as_pg_cstr(),
        "HNSW max connections per layer".as_pg_cstr(),
        M_DEFAULT_SENTINEL,
        -1,
        128,
        pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
    );

    pg_sys::add_int_reloption(
        RELOPT_KIND_TSV,
        "ef_construction".as_pg_cstr(),
        "HNSW build beam width".as_pg_cstr(),
        EF_CONSTRUCTION_DEFAULT as _,
        1,
        1000,
        pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
    );

    pg_sys::add_int_reloption(
        RELOPT_KIND_TSV,
        "ef_search".as_pg_cstr(),
        "HNSW search beam width".as_pg_cstr(),
        EF_SEARCH_DEFAULT as _,
        1,
        1000,
        pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
    );

    pg_sys::add_int_reloption(
        RELOPT_KIND_TSV,
        "num_dimensions".as_pg_cstr(),
        "The number of dimensions to index (0 to index all dimensions)".as_pg_cstr(),
        0,
        0,
        5000,
        pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE,
    );
}
