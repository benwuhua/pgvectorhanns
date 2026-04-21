//! Node placeholder — hanns manages its own node structure internally.
//! Kept for minimal compatibility.

use crate::util::ItemPointer;
use pgrx::PgRelation;

use super::stats::{StatsNodeModify, StatsNodeRead};

pub trait ReadableNode {
    type Node<'a>;
    unsafe fn read<'a, S: StatsNodeRead>(
        _index: &'a PgRelation,
        _index_pointer: ItemPointer,
        _stats: &mut S,
    ) -> Self::Node<'a>;
}

pub trait WriteableNode {
    type Node<'a>;
    unsafe fn modify<'a, S: StatsNodeModify>(
        _index: &'a PgRelation,
        _index_pointer: ItemPointer,
        _stats: &mut S,
    ) -> Self::Node<'a>;
}
