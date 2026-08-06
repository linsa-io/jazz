//! Apply the history graft to real stores. See `storage::graft` for what it writes and
//! refuses; see `chain_doctor` for the read-only diagnosis that should precede this.
//!
//!   cargo run --release --example graft_row --features sqlite,rocksdb -- \
//!       --into <target-store> --row <row-id> [--table <name>] <source-store>...
//!
//! Every store must be an OFFLINE copy. Paths ending in `.sqlite` open with the SQLite
//! backend; directories open with RocksDB (which takes the lock — pointing this at a
//! store a server is using fails, by design).

use std::path::Path;

use jazz_tools::ObjectId;
use jazz_tools::storage::graft::graft_row_history;
use jazz_tools::storage::{RocksDBStorage, SqliteStorage, Storage};

fn open_source(path: &str) -> Box<dyn Storage> {
    if path.ends_with(".sqlite") {
        Box::new(SqliteStorage::open(path).expect("open sqlite source"))
    } else {
        let dir = Path::new(path);
        let rocks = dir.join("jazz.rocksdb");
        let target = if rocks.exists() {
            rocks
        } else {
            dir.to_path_buf()
        };
        Box::new(RocksDBStorage::open(&target, 64 << 20).expect("open rocksdb source"))
    }
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut into: Option<String> = None;
    let mut row_id: Option<ObjectId> = None;
    let mut table: Option<String> = None;
    let mut sources: Vec<String> = Vec::new();
    let mut it = args.into_iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--into" => into = Some(it.next().expect("--into needs a value")),
            "--row" => {
                row_id = Some(ObjectId::from_uuid(
                    it.next()
                        .expect("--row needs a value")
                        .parse()
                        .expect("row id is a uuid"),
                ))
            }
            "--table" => table = Some(it.next().expect("--table needs a value")),
            other => sources.push(other.to_string()),
        }
    }
    let into = into.expect("--into is required");
    let row_id = row_id.expect("--row is required");
    assert!(!sources.is_empty(), "at least one source store is required");

    let sources: Vec<(String, Box<dyn Storage>)> = sources
        .into_iter()
        .map(|path| {
            let store = open_source(&path);
            (path, store)
        })
        .collect();

    let table = table
        .or_else(|| {
            sources.iter().find_map(|(_, store)| {
                store
                    .load_row_locator(row_id)
                    .ok()
                    .flatten()
                    .map(|locator| locator.table.to_string())
            })
        })
        .expect("--table not given and no source locates the row");

    // The target opens per backend the same way; grafting needs it mutable.
    if into.ends_with(".sqlite") {
        let mut target = SqliteStorage::open(&into).expect("open sqlite target");
        run(&mut target, &sources, &table, row_id);
        target.flush().expect("flush target");
    } else {
        let dir = Path::new(&into);
        let rocks = dir.join("jazz.rocksdb");
        let path = if rocks.exists() {
            rocks
        } else {
            dir.to_path_buf()
        };
        let mut target = RocksDBStorage::open(&path, 256 << 20).expect("open rocksdb target");
        run(&mut target, &sources, &table, row_id);
        target.flush().expect("flush target");
    }
}

fn run<T: Storage>(
    target: &mut T,
    sources: &[(String, Box<dyn Storage>)],
    table: &str,
    row_id: ObjectId,
) {
    println!("row {row_id}  table {table}");
    for (path, source) in sources {
        let report = graft_row_history(target, source.as_ref(), table, row_id)
            .unwrap_or_else(|error| panic!("graft from {path} failed: {error}"));
        let name = path.rsplit('/').next().unwrap_or(path);
        println!(
            "{name:<52} grafted {:>5}  present {:>6}  fates {:>4}  stripped {:>4}",
            report.batches_grafted,
            report.batches_already_present,
            report.fates_copied,
            report.stripped_copies_skipped
        );
    }
}
