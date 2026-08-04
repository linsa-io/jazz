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
//!
//! That last sentence was only true of the VALUE. `RowBytes` used to be an
//! `Arc<[u8]>`, whose counters and payload live in one allocation, so a `Weak`
//! left here kept the payload RESIDENT until the next sweep even though its
//! last subscriber was long gone — and the sweep only runs from `dedup`, so an
//! idle server never ran it at all. `RowBytes` now boxes its payload, which
//! leaves a stale entry pinning the Arc header rather than a megabyte of blob
//! (see the note on the type, and `a_stale_entry_pins_only_the_header` below).

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use crate::object::ObjectId;
use crate::query_manager::types::RowBytes;
use crate::row_histories::BatchId;

/// Purge dead weak entries after this many `dedup` calls.
const PURGE_EVERY_OPS: usize = 4096;

#[derive(Debug, Default)]
pub(super) struct RowBytesDedup {
    map: HashMap<(ObjectId, BatchId), Weak<Box<[u8]>>>,
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
            && existing.as_ref().as_ref() == fresh.as_ref()
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

    /// The regression gate for the retention bug.
    ///
    /// It asserts the SHAPE rather than a byte count, because a unit test cannot observe
    /// deallocation: `jazz-tools` installs mimalloc globally, so a counting allocator
    /// cannot be layered underneath one here. What it can pin is the property that makes
    /// the deallocation certain — the payload sits behind a `Box`, so `RowBytes` is one
    /// word wide and the bytes die with the last strong reference no matter how long a
    /// `Weak` outlives them. Going back to `Arc<[u8]>` makes it a two-word fat pointer and
    /// fails here.
    ///
    /// The byte-level evidence is a measurement, not a test: serving 5.2 MiB of blob rows
    /// left 5.16 MiB allocated-but-dead before this change and 0.01 MiB after, and a
    /// subscriber-attached upload run dropped from 742 to 554 MiB RSS.
    #[test]
    fn a_stale_entry_pins_only_the_header() {
        use std::mem::size_of;

        assert_eq!(
            size_of::<RowBytes>(),
            size_of::<usize>(),
            "RowBytes must stay one word wide: a fat `Arc<[u8]>` puts the payload in the \
             same allocation as the weak count, so a stale dedup entry keeps it resident",
        );

        let mut dedup = RowBytesDedup::default();
        let row = ObjectId::new();
        let batch = BatchId::new();
        let payload = dedup.dedup(row, batch, bytes(3, 4096));

        // The map holds a Weak, so the only strong reference is the caller's.
        assert_eq!(Arc::strong_count(payload.as_arc()), 1);
        drop(payload);

        // Dropping it runs `Box`'s destructor — the entry survives until the next sweep
        // but has nothing behind it.
        let revived = dedup.dedup(row, batch, bytes(3, 4096));
        assert_eq!(
            Arc::strong_count(revived.as_arc()),
            1,
            "a dead entry must be replaced by the fresh load, not resurrected",
        );
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
