//! pgvector vector type Datum handling.

use pgrx::*;
use std::mem::MaybeUninit;

// pgvector internal representation: varlena header + dim + unused + float[]
#[repr(C)]
#[derive(Debug)]
pub struct PgVectorInternal {
    vl_len_: i32, /* varlena header (do not touch directly!) */
    pub dim: i16, /* number of dimensions */
    unused: MaybeUninit<i16>,
    pub x: pg_sys::__IncompleteArrayField<std::os::raw::c_float>,
}

impl PgVectorInternal {
    pub fn to_slice(&self) -> &[f32] {
        let dim = self.dim;
        unsafe { self.x.as_slice(dim as _) }
    }
}

/// Extract (dim, f32_slice) from a pgvector Datum.
///
/// # Safety
/// Caller must ensure datum is a valid pgvector varlena.
pub unsafe fn datum_to_vector_slice(datum: pg_sys::Datum) -> (u16, Vec<f32>) {
    let varlena = pg_sys::pg_detoast_datum(datum.cast_mut_ptr());
    let ptr = varlena as *const u8;
    let dim = *(ptr.add(4) as *const u16);
    let slice = std::slice::from_raw_parts(ptr.add(8) as *const f32, dim as usize);
    (dim, slice.to_vec())
}
