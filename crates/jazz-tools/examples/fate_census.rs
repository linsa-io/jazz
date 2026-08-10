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
    // Which rejection SOURCE produced them: code/reason name the call site.
    let mut by_reason: BTreeMap<String, usize> = BTreeMap::new();
    for fate in &fates {
        if let jazz_tools::batch_fate::BatchFate::Rejected { code, reason, .. } = fate {
            let short: String = reason.chars().take(70).collect();
            *by_reason.entry(format!("{code} | {short}")).or_default() += 1;
        }
    }
    // When were the rejected batches born (uuid7 ms prefix)? Answers whether a
    // denial wave predates or follows a given deploy.
    let mut rejected_by_hour: BTreeMap<u64, usize> = BTreeMap::new();
    for fate in &fates {
        if matches!(fate, jazz_tools::batch_fate::BatchFate::Rejected { .. }) {
            let bytes = fate.batch_id().0;
            let mut ms: u64 = 0;
            for x in &bytes[..6] {
                ms = (ms << 8) | *x as u64;
            }
            *rejected_by_hour.entry(ms / 3_600_000).or_default() += 1;
        }
    }
    println!("\nrejected batches by birth hour (unix-hour, UTC hour-of-day, count):");
    for (h, n) in &rejected_by_hour {
        println!("  {h}  {:02}:00 UTC  {n}", h % 24);
    }

    println!("\nrejected fates by code|reason:");
    for (k, v) in &by_reason {
        println!("  {v:>6}  {k}");
    }

    // Attribute rejected batches to actual rows: walk the users table's
    // history and match batch ids against the rejected fates.
    let rejected_ids: std::collections::HashSet<_> = fates
        .iter()
        .filter(|f| matches!(f, jazz_tools::batch_fate::BatchFate::Rejected { .. }))
        .map(|f| f.batch_id())
        .collect();
    let mut per_table: BTreeMap<String, usize> = BTreeMap::new();
    let mut per_row: BTreeMap<String, usize> = BTreeMap::new();
    let mut per_branch: BTreeMap<String, usize> = BTreeMap::new();
    let mut inserts = 0usize;
    let mut updates = 0usize;
    if let Ok(locators) = store.scan_row_locators() {
        println!("\nrow locators: {}", locators.len());
        for (row_id, locator) in locators {
            let table = locator.table.to_string();
            let Ok(history) = store.scan_history_row_batches(&table, row_id) else {
                continue;
            };
            for h in &history {
                if rejected_ids.contains(&h.batch_id) {
                    *per_table.entry(table.clone()).or_default() += 1;
                    *per_row.entry(format!("{table}/{row_id}")).or_default() += 1;
                    *per_branch.entry(h.branch.to_string()).or_default() += 1;
                    if h.parents.is_empty() {
                        inserts += 1;
                    } else {
                        updates += 1;
                    }
                }
            }
        }
    }
    println!("rejected batches found in history: per-table {per_table:?}");
    println!("  parentless (row-creating): {inserts}, with parents (updates): {updates}");
    println!("  branches: {per_branch:?}");
    let mut rows: Vec<_> = per_row.into_iter().collect();
    rows.sort_by_key(|(_, n)| std::cmp::Reverse(*n));
    for (row, n) in rows.iter().take(6) {
        println!("  {row}: {n}");
    }

    // Does this store hold the diverged user's row at all, and on which
    // branches? Discriminates "row never accepted here" from "row exists but
    // the writer cannot see it".
    if let Ok(locators) = store.scan_row_locators() {
        let needle = "6d7a605e";
        let mut found = false;
        for (row_id, locator) in &locators {
            if !format!("{row_id}").starts_with(needle) {
                continue;
            }
            found = true;
            println!("\nROW {row_id} table={} ", locator.table.as_str());
            if let Ok(history) = store.scan_history_row_batches(locator.table.as_str(), *row_id) {
                let mut per_branch: BTreeMap<String, (usize, usize)> = BTreeMap::new();
                for h in &history {
                    let e = per_branch.entry(h.branch.to_string()).or_default();
                    e.0 += 1;
                    if h.parents.is_empty() {
                        e.1 += 1;
                    }
                }
                println!("  history entries per branch (total, parentless): {per_branch:?}");
                let rejected_here = history
                    .iter()
                    .filter(|h| rejected_ids.contains(&h.batch_id))
                    .count();
                println!("  of those, rejected-fate batches present: {rejected_here}");
            }
        }
        if !found {
            println!("\nROW {needle}...: NOT PRESENT in this store at all");
        }
        // Branch histogram across the whole store: is that device-looking
        // branch the norm here or an outlier?
        let mut branches: BTreeMap<String, usize> = BTreeMap::new();
        for (row_id, locator) in &locators {
            if let Ok(history) = store.scan_history_row_batches(locator.table.as_str(), *row_id) {
                for h in &history {
                    *branches.entry(h.branch.to_string()).or_default() += 1;
                }
            }
        }
        println!("\nbranches across the store (history entries each):");
        for (b, n) in branches.iter().take(10) {
            println!("  {b}: {n}");
        }

        // How many users rows exist here in total?
        let users_rows = locators
            .iter()
            .filter(|(_, l)| l.table.as_str() == "users")
            .count();
        println!("users rows in store: {users_rows}");
    }

    // History length per row: the multiplier on every parentless write.
    if let Ok(locators) = store.scan_row_locators() {
        let mut lengths: Vec<(usize, String, String)> = Vec::new();
        for (row_id, locator) in &locators {
            if let Ok(history) = store.scan_history_row_batches(locator.table.as_str(), *row_id) {
                lengths.push((
                    history.len(),
                    locator.table.to_string(),
                    format!("{row_id}"),
                ));
            }
        }
        lengths.sort_by_key(|(n, _, _)| std::cmp::Reverse(*n));
        let total: usize = lengths.iter().map(|(n, _, _)| n).sum();
        println!(
            "\nhistory entries total: {total} across {} rows",
            lengths.len()
        );
        println!("longest histories:");
        for (n, table, row) in lengths.iter().take(6) {
            println!("  {n:>6}  {table}  {}", &row[..8]);
        }
    }

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
