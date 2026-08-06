//! Diagnose a row whose history chain has diverged between stores.
//!
//! A row's sync is a parent-linked chain of batches. A server that is missing a link
//! rejects every later batch with `ParentNotFound`, forever — the engine has no repair
//! protocol. Before building one, this says exactly what is broken and whether it is
//! repairable: which batches each store holds for the row, which parents are referenced
//! but absent, and whether the UNION of all the stores closes the chain.
//!
//! It only ever READS through the `Storage` trait — but the RocksDB backend takes the
//! directory lock on open, so point it at copies, never at a live server's store.
//!
//!   cargo run --release --example chain_doctor --features sqlite,rocksdb -- \
//!       --row <row-id> [--table <name>] <store-path>...
//!
//! A store path ending in `.sqlite` opens with the SQLite backend; a directory is
//! expected to hold `jazz.rocksdb` inside it (or to BE a RocksDB dir) and opens with the
//! RocksDB backend.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use jazz_tools::ObjectId;
use jazz_tools::storage::{RocksDBStorage, SqliteStorage, Storage};

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
        // RocksDB has no read-only open here, so it takes the LOCK: only ever point this
        // at a COPY, never at a store a server is using.
        Box::new(RocksDBStorage::open(&target, 64 << 20).expect("open rocksdb store"))
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut row_id: Option<ObjectId> = None;
    let mut table: Option<String> = None;
    let mut stores: Vec<String> = Vec::new();
    let mut it = args.into_iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--row" => {
                row_id = Some(ObjectId::from_uuid(
                    it.next()
                        .expect("--row needs a value")
                        .parse()
                        .expect("row id is a uuid"),
                ))
            }
            "--table" => table = Some(it.next().expect("--table needs a value")),
            other => stores.push(other.to_string()),
        }
    }
    let row_id = row_id.expect("--row is required");
    assert!(!stores.is_empty(), "at least one store path is required");

    // Which batches does each store hold, and what do they parent on?
    let mut per_store: Vec<(String, BTreeMap<String, Vec<String>>)> = Vec::new();
    let mut union_batches: BTreeSet<String> = BTreeSet::new();
    let mut union_parents: BTreeSet<String> = BTreeSet::new();
    let mut resolved_table = table.clone();

    for path in &stores {
        let store = open_store(path);
        let table_name = match resolved_table.clone() {
            Some(t) => t,
            None => {
                let locator = store
                    .load_row_locator(row_id)
                    .ok()
                    .flatten()
                    .map(|l| l.table.to_string());
                match locator {
                    Some(t) => {
                        resolved_table = Some(t.clone());
                        t
                    }
                    None => {
                        per_store.push((path.clone(), BTreeMap::new()));
                        continue;
                    }
                }
            }
        };
        let history = store
            .scan_history_row_batches(&table_name, row_id)
            .unwrap_or_default();
        let mut batches = BTreeMap::new();
        for row in &history {
            let id = row.batch_id.to_string();
            let parents: Vec<String> = row.parents.iter().map(|p| p.to_string()).collect();
            union_batches.insert(id.clone());
            for p in &parents {
                union_parents.insert(p.clone());
            }
            batches.insert(id, parents);
        }
        per_store.push((path.clone(), batches));
    }

    let table_name = resolved_table.unwrap_or_else(|| "<unknown>".into());
    println!("row {row_id}  table {table_name}\n");
    println!("{:<52} {:>8} {:>10}", "store", "batches", "missing");
    for (path, batches) in &per_store {
        let referenced: BTreeSet<&String> = batches.values().flatten().collect();
        let missing = referenced
            .iter()
            .filter(|p| !batches.contains_key(p.as_str()))
            .count();
        let name = path.rsplit('/').next().unwrap_or(path);
        println!("{name:<52} {:>8} {missing:>10}", batches.len());
    }

    let union_missing: Vec<&String> = union_parents
        .iter()
        .filter(|p| !union_batches.contains(p.as_str()))
        .collect();
    println!(
        "\nunion: {} batches, {} referenced parents, {} STILL missing",
        union_batches.len(),
        union_parents.len(),
        union_missing.len()
    );
    if union_missing.is_empty() {
        println!("=> the union CLOSES the chain: a graft from these stores can repair the row");
    } else {
        println!(
            "=> unrepairable from these stores alone; first missing: {:?}",
            union_missing.first()
        );
    }
}
