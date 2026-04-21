//! Stats trait stubs for util/ compatibility.
//!
//! hanns manages its own internal statistics. These traits exist
//! solely to satisfy util/ module type bounds.

//! Stats trait stubs for util/ compatibility.

pub trait StatsNodeRead {
    fn get_node_count(&self) -> u64;
    fn record_read(&mut self) {}
}

pub trait StatsNodeWrite: StatsNodeRead {
    fn record_write(&mut self) {}
}

pub trait StatsNodeModify: StatsNodeRead {}

pub trait WriteStats {
    fn write_stats(&mut self, _stats: &dyn StatsNodeRead) {}
}

pub trait StatsHeapNodeRead {
    fn get_num_heap_tuples(&self) -> f64 {
        0.0
    }
    fn record_heap_read(&mut self) {}
}

/// Default no-op stats implementation for insert operations.
#[derive(Default)]
pub struct InsertStats;

impl StatsNodeRead for InsertStats {
    fn get_node_count(&self) -> u64 {
        0
    }
}

impl StatsNodeWrite for InsertStats {
    fn record_write(&mut self) {}
}

impl StatsNodeModify for InsertStats {}

impl StatsHeapNodeRead for InsertStats {}
