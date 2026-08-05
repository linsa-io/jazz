//! What one history scan costs, measured on a real store.
//!
//! A settle pass on a device degraded from 150ms to 900ms after large blob rows landed,
//! with the CPU profile pointing at `decode_history_row_bytes_in_table`. That leaf is
//! reached from `scan_history_row_batches`, which decodes EVERY history entry of a row.
//! Whether that is cheap or ruinous depends on data this repo's fixtures do not have:
//! a row with eleven thousand heartbeat revisions, and a row holding a megabyte blob.
//!
//! So point it at a store captured from a device:
//!   cargo run --release --example store_probe --features sqlite -- <path.sqlite>
//!
//! It reports, per table, what a single scan of the worst row costs — which is the price
//! the write path pays per row it touches.

use std::collections::BTreeMap;
use std::time::Instant;

use jazz_tools::storage::{SqliteStorage, Storage};

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: store_probe <path-to-store.sqlite>");

    let storage = SqliteStorage::open(&path).expect("open store");
    let locators = storage.scan_row_locators().expect("scan row locators");
    println!("rows in store: {}", locators.len());

    // Group rows by the table that owns their history.
    let mut by_table: BTreeMap<String, Vec<_>> = BTreeMap::new();
    for (object_id, locator) in locators {
        by_table
            .entry(locator.table.to_string())
            .or_default()
            .push(object_id);
    }

    println!(
        "\n{:<24} {:>6} {:>10} {:>12} {:>12} {:>12}",
        "table", "rows", "entries", "bytes", "worst scan", "worst row"
    );

    for (table, row_ids) in &by_table {
        let mut entries_total = 0usize;
        let mut bytes_total = 0usize;
        let mut worst_micros = 0u128;
        let mut worst_entries = 0usize;
        let mut worst_bytes = 0usize;

        for object_id in row_ids {
            let started = Instant::now();
            let Ok(history) = storage.scan_history_row_batches(table.as_str(), *object_id) else {
                continue;
            };
            let micros = started.elapsed().as_micros();

            let bytes: usize = history.iter().map(|batch| batch.data.as_ref().len()).sum();
            entries_total += history.len();
            bytes_total += bytes;

            if micros > worst_micros {
                worst_micros = micros;
                worst_entries = history.len();
                worst_bytes = bytes;
            }
        }

        println!(
            "{:<24} {:>6} {:>10} {:>9.1} MB {:>9.1} ms {:>5} e/{:.1} MB",
            table,
            row_ids.len(),
            entries_total,
            bytes_total as f64 / 1_048_576.0,
            worst_micros as f64 / 1000.0,
            worst_entries,
            worst_bytes as f64 / 1_048_576.0,
        );
    }
}
