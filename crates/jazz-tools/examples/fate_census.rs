//! Census of persisted batch fates / sealed submissions / local batch records
//! in a store COPY — written for the 2026-08-09 production conveyor incident.
//!
//!   cargo run --release --example fate_census --features sqlite,rocksdb -- <store-path>
//!
//! Answers: what does the restart sweep walk, how many entries read as
//! "needing settlement" at the EdgeServer target, and what shape are they.

use std::collections::BTreeMap;
use std::path::Path;

use jazz_tools::DurabilityTier;
use jazz_tools::storage::{RocksDBStorage, SqliteStorage, Storage};
use jazz_tools::sync_manager::SyncManager;

fn open_store(path: &str) -> Box<dyn Storage> {
    if path.ends_with(".sqlite") {
        Box::new(SqliteStorage::open(path).expect("open sqlite store"))
    } else {
        let dir = Path::new(path);
        let rocks = dir.join("jazz.rocksdb");
        let target = if rocks.exists() {
            rocks
        } else {
            dir.to_path_buf()
        };
        Box::new(RocksDBStorage::open(&target, 64 << 20).expect("open rocksdb store"))
    }
}

fn batch_birth(batch_id_bytes: &[u8]) -> String {
    if batch_id_bytes.len() < 6 {
        return "?".into();
    }
    let mut ms: u64 = 0;
    for b in &batch_id_bytes[..6] {
        ms = (ms << 8) | *b as u64;
    }
    let secs = (ms / 1000) as i64;
    // no chrono dep in examples — day-resolution by hand from unix seconds
    format!("unix:{secs}")
}

fn main() {
    let path = std::env::args()
        .nth(1)
        .expect("usage: fate_census <store-path>");
    let store = open_store(&path);
    let target = DurabilityTier::EdgeServer;

    let fates = store.scan_authoritative_batch_fates().unwrap_or_default();
    let submissions = store.scan_sealed_batch_submissions().unwrap_or_default();
    let records = store.scan_local_batch_records().unwrap_or_default();

    println!("fates total:        {}", fates.len());
    println!("sealed submissions: {}", submissions.len());
    println!("local batch records:{}", records.len());

    let mut by_shape: BTreeMap<String, usize> = BTreeMap::new();
    let mut unsettled_births: Vec<u64> = Vec::new();
    for fate in &fates {
        let needs = SyncManager::fate_needs_settlement_at(Some(fate), target);
        let variant = format!("{fate:?}");
        let variant_name = variant
            .split(|c| c == ' ' || c == '(' || c == '{')
            .next()
            .unwrap_or("?");
        let key = format!("{variant_name} needs_settlement={needs}");
        *by_shape.entry(key).or_default() += 1;
        if needs {
            let b = fate.batch_id();
            let bytes = b.0;
            let mut ms: u64 = 0;
            for x in &bytes[..6] {
                ms = (ms << 8) | *x as u64;
            }
            unsettled_births.push(ms);
        }
    }
    println!("\nfates by shape:");
    for (k, v) in &by_shape {
        println!("  {v:>8}  {k}");
    }

    let mut sub_needs = 0usize;
    for sub in &submissions {
        let fate = fates.iter().find(|f| f.batch_id() == sub.batch_id);
        if matches!(fate, None) || SyncManager::fate_needs_settlement_at(fate, target) {
            sub_needs += 1;
        }
    }
    println!("\nsubmissions reading as pending: {sub_needs}");

    // Birth histogram of pending submissions (uuid7 ms prefix of the batch id),
    // plus whether the field-stuck batch is among them.
    let mut per_hour: BTreeMap<u64, usize> = BTreeMap::new();
    let stuck: [u8; 16] = [
        1, 159, 231, 114, 162, 196, 124, 144, 136, 79, 49, 137, 217, 119, 186, 77,
    ];
    let mut stuck_found = false;
    for sub in &submissions {
        let fate = fates.iter().find(|f| f.batch_id() == sub.batch_id);
        if !(matches!(fate, None) || SyncManager::fate_needs_settlement_at(fate, target)) {
            continue;
        }
        let bytes = sub.batch_id.0;
        if bytes == stuck {
            stuck_found = true;
        }
        let mut ms: u64 = 0;
        for x in &bytes[..6] {
            ms = (ms << 8) | *x as u64;
        }
        *per_hour.entry(ms / 3_600_000).or_default() += 1;
    }
    println!("pending submissions by hour (unix-hour: count):");
    for (h, n) in &per_hour {
        println!("  {}  ({} UTC)  {}", h, h % 24, n);
    }
    println!("field-stuck batch present: {stuck_found}");

    if !unsettled_births.is_empty() {
        unsettled_births.sort_unstable();
        let mut by_day: BTreeMap<u64, usize> = BTreeMap::new();
        for ms in &unsettled_births {
            *by_day.entry(ms / 86_400_000).or_default() += 1;
        }
        println!("\nunsettled fates by day (unix-day: count):");
        for (day, count) in &by_day {
            println!(
                "  day {} ({}): {}",
                day,
                batch_birth(&((day * 86_400_000) as u128).to_be_bytes()[10..]),
                count
            );
        }
        println!(
            "first unsettled birth ms={} last={}",
            unsettled_births.first().unwrap(),
            unsettled_births.last().unwrap()
        );
    }

    // A couple of raw samples of unsettled fates for shape inspection.
    println!("\nsample unsettled fates:");
    for fate in fates
        .iter()
        .filter(|f| SyncManager::fate_needs_settlement_at(Some(f), target))
        .take(5)
    {
        println!("  {fate:?}");
    }
}
// (appended) — quick min/max birth over all fates for store-age determination
