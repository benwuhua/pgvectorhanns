use pgrx::*;
mod build;
pub mod distance;
pub mod guc;
pub mod index_cache;
mod meta_page;
mod node;
pub mod options;
pub mod pg_vector;
mod scan;
pub mod stats;
mod storage;
mod vacuum;

/// Access method support function numbers
pub const HANNS_DISTANCE_TYPE_PROC: u16 = 1;

#[pg_extern(sql = "
    CREATE OR REPLACE FUNCTION hanns_hnsw_amhandler(internal) RETURNS index_am_handler PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';

    DO $$
    DECLARE
        c int;
    BEGIN
        SELECT count(*)
        INTO c
        FROM pg_catalog.pg_am a
        WHERE a.amname = 'hanns_hnsw';

        IF c = 0 THEN
            CREATE ACCESS METHOD hanns_hnsw TYPE INDEX HANDLER hanns_hnsw_amhandler;
        END IF;
    END;
    $$;
")]
fn amhandler(_fcinfo: pg_sys::FunctionCallInfo) -> PgBox<pg_sys::IndexAmRoutine> {
    let mut amroutine =
        unsafe { PgBox::<pg_sys::IndexAmRoutine>::alloc_node(pg_sys::NodeTag::T_IndexAmRoutine) };

    amroutine.amstrategies = 0;
    amroutine.amsupport = 1;

    amroutine.amcanorder = false;
    amroutine.amcanorderbyop = true;
    amroutine.amcanbackward = false; /* can change direction mid-scan */
    amroutine.amcanunique = false;
    amroutine.amcanmulticol = true;
    amroutine.amoptionalkey = true;
    amroutine.amsearcharray = false;
    amroutine.amsearchnulls = false;
    amroutine.amstorage = false;
    amroutine.amclusterable = false;
    amroutine.ampredlocks = false;
    amroutine.amcanparallel = false; //TODO
    amroutine.amcaninclude = false; //TODO
    amroutine.amoptsprocnum = 0;
    amroutine.amusemaintenanceworkmem = false; /* not used during VACUUM */
    amroutine.amkeytype = pg_sys::InvalidOid;

    amroutine.amvalidate = Some(amvalidate);
    amroutine.ambuild = Some(build::ambuild);
    amroutine.ambuildempty = Some(build::ambuildempty);
    amroutine.aminsert = Some(build::aminsert);
    amroutine.ambulkdelete = Some(vacuum::ambulkdelete);
    amroutine.amvacuumcleanup = Some(vacuum::amvacuumcleanup);
    amroutine.amcostestimate = Some(amcostestimate);
    amroutine.amoptions = Some(options::amoptions);
    amroutine.ambeginscan = Some(scan::ambeginscan);
    amroutine.amrescan = Some(scan::amrescan);
    amroutine.amgettuple = Some(scan::amgettuple);
    amroutine.amgetbitmap = None;
    amroutine.amendscan = Some(scan::amendscan);

    amroutine.ambuildphasename = Some(build::ambuildphasename);

    #[cfg(all(any(feature = "pg17", feature = "pg18"), feature = "build_parallel"))]
    {
        amroutine.amcanbuildparallel = true;
    }

    amroutine.into_pg_boxed()
}

// Operator class SQL — idempotent install/upgrade
extension_sql!(
    r#"
DO $$
DECLARE
  have_cos_ops int;
  have_l2_ops int;
  have_ip_ops int;
BEGIN
    -- Has cosine operator class been installed previously?
    SELECT count(*)
    INTO have_cos_ops
    FROM pg_catalog.pg_opclass c
    WHERE c.opcname = 'vector_cosine_ops'
    AND c.opcmethod = (SELECT oid FROM pg_catalog.pg_am am WHERE am.amname = 'hanns_hnsw')
    AND c.opcnamespace = (SELECT oid FROM pg_catalog.pg_namespace where nspname='@extschema@');

    -- Has L2 operator class been installed previously?
    SELECT count(*)
    INTO have_l2_ops
    FROM pg_catalog.pg_opclass c
    WHERE c.opcname = 'vector_l2_ops'
    AND c.opcmethod = (SELECT oid FROM pg_catalog.pg_am am WHERE am.amname = 'hanns_hnsw')
    AND c.opcnamespace = (SELECT oid FROM pg_catalog.pg_namespace where nspname='@extschema@');

    -- Has inner product operator class been installed previously?
    SELECT count(*)
    INTO have_ip_ops
    FROM pg_catalog.pg_opclass c
    WHERE c.opcname = 'vector_ip_ops'
    AND c.opcmethod = (SELECT oid FROM pg_catalog.pg_am am WHERE am.amname = 'hanns_hnsw')
    AND c.opcnamespace = (SELECT oid FROM pg_catalog.pg_namespace where nspname='@extschema@');

    IF have_cos_ops = 0 THEN
        CREATE OPERATOR CLASS vector_cosine_ops DEFAULT
        FOR TYPE vector USING hanns_hnsw AS
	        OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops,
            FUNCTION 1 distance_type_cosine();
    ELSIF have_cos_ops > 0 THEN
        -- Upgrade: ensure the support function is registered
        BEGIN
            INSERT INTO pg_amproc (amprocfamily, amproclefttype, amprocrighttype, amprocnum, amproc)
            SELECT c.opcfamily, c.opcintype, c.opcintype, 1, '@extschema@.distance_type_cosine'::regproc
            FROM pg_opclass c, pg_am a
            WHERE a.oid = c.opcmethod AND c.opcname = 'vector_cosine_ops' AND a.amname = 'hanns_hnsw'
            ON CONFLICT DO NOTHING;
        EXCEPTION WHEN OTHERS THEN
            NULL;
        END;
    END IF;

    IF have_l2_ops = 0 THEN
        CREATE OPERATOR CLASS vector_l2_ops
        FOR TYPE vector USING hanns_hnsw AS
            OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
            FUNCTION 1 distance_type_l2();
    END IF;

    IF have_ip_ops = 0 THEN
        CREATE OPERATOR CLASS vector_ip_ops
        FOR TYPE vector USING hanns_hnsw AS
            OPERATOR 1 <#> (vector, vector) FOR ORDER BY float_ops,
            FUNCTION 1 distance_type_inner_product();
    END IF;
END;
$$;
"#,
    name = "hanns_hnsw_ops_operator",
    requires = [
        amhandler,
        distance_type_cosine,
        distance_type_l2,
        distance_type_inner_product,
    ]
);

#[pg_guard]
pub extern "C-unwind" fn amvalidate(_opclassoid: pg_sys::Oid) -> bool {
    true
}

/// Minimal cost estimator for hanns_hnsw index scans.
#[pg_guard]
pub unsafe extern "C-unwind" fn amcostestimate(
    _root: *mut pg_sys::PlannerInfo,
    _path: *mut pg_sys::IndexPath,
    _loop_count: f64,
    _index_startup_cost: *mut pg_sys::Cost,
    index_total_cost: *mut pg_sys::Cost,
    _index_selectivity: *mut pg_sys::Selectivity,
    _index_correlation: *mut f64,
    _index_pages: *mut f64,
) {
    // HNSW search is fast — use a low fixed cost to encourage index use
    *index_total_cost = 1.0;
}
