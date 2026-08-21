//! Runs the frontier rule against a real client store captured from the field.
//!
//! Point it at a copy of a device's jazz sqlite file:
//!   FIELD_STORE=/path/to/client.sqlite cargo test -p jazz-tools --features test \
//!     --test field_repair_probe -- --ignored --nocapture
//!
//! Ignored by default because it needs that file. It is the only check that measures the fix
//! against data the engine actually produced, rather than against a fixture we wrote.

#![cfg(feature = "sqlite")]

use jazz_tools::storage::{SqliteStorage, Storage};

#[ignore = "needs FIELD_STORE pointing at a captured client store"]
#[test]
fn a_captured_client_store_collapses_its_frontier() {
    let Ok(path) = std::env::var("FIELD_STORE") else {
        panic!("set FIELD_STORE to a captured client sqlite file");
    };
    let storage = SqliteStorage::open(&path).expect("the captured store should open");

    let table = std::env::var("FIELD_TABLE").unwrap_or_else(|_| "users".to_string());
    let branch = std::env::var("FIELD_BRANCH").expect("set FIELD_BRANCH");
    let row_hex = std::env::var("FIELD_ROW").expect("set FIELD_ROW (32 hex chars)");

    let mut bytes = [0u8; 16];
    for (index, slot) in bytes.iter_mut().enumerate() {
        *slot = u8::from_str_radix(&row_hex[index * 2..index * 2 + 2], 16).expect("hex row id");
    }
    let row_id = jazz_tools::object::ObjectId::from_uuid(uuid::Uuid::from_bytes(bytes));

    let history = storage
        .scan_history_row_batches(&table, row_id)
        .expect("history should read");
    let on_branch = history.iter().filter(|row| row.branch == branch).count();
    let visible = history
        .iter()
        .filter(|row| row.branch == branch && row.state.is_visible())
        .count();
    let parentless = history
        .iter()
        .filter(|row| row.branch == branch && row.state.is_visible() && row.parents.is_empty())
        .count();

    let stored_tips = storage
        .scan_row_branch_tip_ids(&table, &branch, row_id)
        .expect("tip ids should read")
        .len();

    // The read above answers from the STORED visible-region entry, which is what the device is
    // serving today — it is not recomputed until the row is next applied to. Dropping that entry
    // makes the same call fall through to the computation, which is what the next arrival will
    // do, so this measures the repair rather than the backlog.
    let mut storage = storage;
    storage
        .delete_visible_region_row(&table, &branch, row_id)
        .expect("the captured entry should drop");
    let tips = storage
        .scan_row_branch_tip_ids(&table, &branch, row_id)
        .expect("recomputed tip ids should read");
    println!(
        "field store: stored frontier {stored_tips} tips -> recomputed {} tips",
        tips.len()
    );

    println!(
        "field store: {on_branch} batches on branch ({visible} visible, {parentless} parentless) \
         -> frontier {} tips",
        tips.len()
    );

    assert!(
        tips.len() <= 2,
        "a real captured row must collapse. It holds {on_branch} batches of which {parentless} \
         are parentless snapshots, and the frontier came back {} wide. Before this fix that \
         number was the count of everything ever delivered.",
        tips.len()
    );
}
