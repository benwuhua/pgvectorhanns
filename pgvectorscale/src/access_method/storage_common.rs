//! Storage common utilities.

/// Get number of index attributes (always 1 for vector-only index).
pub fn get_num_index_attributes(_index_rel: &pgrx::PgRelation) -> usize {
    1
}
