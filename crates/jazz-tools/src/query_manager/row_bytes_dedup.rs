//! Cross-subscription deduplication of loaded row content.
//!
//! Persistent storage backends deserialize a fresh, private `Arc<[u8]>` for
//! every row load, so N subscriptions over the same rows retain N copies of
//! identical bytes — on blob-heavy stores that is the dominant per-subscriber
//! memory cost (see `tests/row_bytes_sharing.rs`).
//!
//! This cache canonicalizes row content AFTER it was read: the storage read
//! and decode happen exactly as before (no staleness questions — a row's
//! content for a given batch id is immutable in the CRDT history), and only
//! byte-identical content is collapsed onto one shared allocation. Entries
//! are `Weak`, so the cache never retains bytes on its own: content dies with
//! its last subscriber.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use crate::object::ObjectId;
use crate::query_manager::types::RowBytes;
use crate::row_histories::BatchId;

/// Purge dead weak entries after this many `dedup` calls.
const PURGE_EVERY_OPS: usize = 4096;

#[derive(Debug, Default)]
pub(super) struct RowBytesDedup {
    map: HashMap<(ObjectId, BatchId), Weak<[u8]>>,
    ops_since_purge: usize,
}

impl RowBytesDedup {
    /// Return the canonical shared bytes for `(row_id, batch_id)`.
    ///
    /// On a hit with byte-identical content the freshly loaded allocation is
    /// dropped in favor of the canonical one. On a miss (or a dead entry)
    /// the fresh allocation becomes the canonical one. Content that differs
    /// from the cached bytes for the same key (which immutable batches make
    /// impossible in practice) defensively replaces the entry — the freshly
    /// read bytes always win.
    pub(super) fn dedup(
        &mut self,
        row_id: ObjectId,
        batch_id: BatchId,
        fresh: RowBytes,
    ) -> RowBytes {
        self.ops_since_purge += 1;
        if self.ops_since_purge >= PURGE_EVERY_OPS {
            self.ops_since_purge = 0;
            self.map.retain(|_, weak| weak.strong_count() > 0);
        }

        let key = (row_id, batch_id);
        if let Some(existing) = self.map.get(&key).and_then(Weak::upgrade)
            && existing.as_ref() == fresh.as_ref()
        {
            return RowBytes::from_arc(existing);
        }
        self.map.insert(key, Arc::downgrade(fresh.as_arc()));
        fresh
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(fill: u8, len: usize) -> RowBytes {
        RowBytes::from(vec![fill; len])
    }

    #[test]
    fn second_load_shares_the_first_allocation() {
        let mut dedup = RowBytesDedup::default();
        let row = ObjectId::new();
        let batch = BatchId::new();

        let first = dedup.dedup(row, batch, bytes(7, 64));
        let second = dedup.dedup(row, batch, bytes(7, 64));
        assert!(
            Arc::ptr_eq(first.as_arc(), second.as_arc()),
            "identical content for the same (row, batch) must share one allocation"
        );
    }

    #[test]
    fn dead_entries_are_replaced_by_fresh_loads() {
        let mut dedup = RowBytesDedup::default();
        let row = ObjectId::new();
        let batch = BatchId::new();

        let first = dedup.dedup(row, batch, bytes(7, 64));
        drop(first); // last strong ref gone — weak entry is dead
        let second = dedup.dedup(row, batch, bytes(7, 64));
        let third = dedup.dedup(row, batch, bytes(7, 64));
        assert!(
            Arc::ptr_eq(second.as_arc(), third.as_arc()),
            "a dead entry must be replaced and the replacement shared"
        );
    }

    #[test]
    fn differing_content_never_serves_the_cached_bytes() {
        let mut dedup = RowBytesDedup::default();
        let row = ObjectId::new();
        let batch = BatchId::new();

        let first = dedup.dedup(row, batch, bytes(7, 64));
        let different = dedup.dedup(row, batch, bytes(9, 64));
        assert!(
            !Arc::ptr_eq(first.as_arc(), different.as_arc()),
            "defensive: differing bytes for one key must not alias"
        );
        assert_eq!(different.as_ref(), &[9u8; 64][..], "fresh bytes win");
    }

    #[test]
    fn distinct_batches_of_one_row_do_not_alias() {
        let mut dedup = RowBytesDedup::default();
        let row = ObjectId::new();

        let v1 = dedup.dedup(row, BatchId::new(), bytes(1, 32));
        let v2 = dedup.dedup(row, BatchId::new(), bytes(2, 32));
        assert!(!Arc::ptr_eq(v1.as_arc(), v2.as_arc()));
        assert_eq!(v1.as_ref(), &[1u8; 32][..]);
        assert_eq!(v2.as_ref(), &[2u8; 32][..]);
    }

    #[test]
    fn purge_drops_dead_entries() {
        let mut dedup = RowBytesDedup::default();
        let row = ObjectId::new();
        for _ in 0..PURGE_EVERY_OPS {
            // Every returned RowBytes is dropped immediately: all entries die.
            let _ = dedup.dedup(row, BatchId::new(), bytes(3, 16));
        }
        // The purge on the threshold op cleared everything dead before it.
        assert!(
            dedup.map.len() <= 1,
            "purge must clear dead weak entries (len={})",
            dedup.map.len()
        );
    }
}
