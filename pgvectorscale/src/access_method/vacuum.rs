//! Vacuum callbacks (stubs for MVP).

use pgrx::prelude::*;

#[pg_guard]
pub unsafe extern "C-unwind" fn ambulkdelete(
    _info: *mut pg_sys::IndexVacuumInfo,
    _stats: *mut pg_sys::IndexBulkDeleteResult,
    _callback: pg_sys::IndexBulkDeleteCallback,
    _callback_state: *mut std::os::raw::c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    // TODO: implement soft delete via hanns bitset
    let result = PgBox::<pg_sys::IndexBulkDeleteResult>::alloc0();
    result.into_pg()
}

#[pg_guard]
pub unsafe extern "C-unwind" fn amvacuumcleanup(
    _info: *mut pg_sys::IndexVacuumInfo,
    _stats: *mut pg_sys::IndexBulkDeleteResult,
) -> *mut pg_sys::IndexBulkDeleteResult {
    let result = PgBox::<pg_sys::IndexBulkDeleteResult>::alloc0();
    result.into_pg()
}
