//! A backref written in the CURRENT generation must be visible to a read of its parent.
//!
//! MEASURED on the dev stack 2026-08-18. A user renamed their handle; the server wrote the
//! new row correctly and the device received it, yet every screen kept showing the old one.
//! Dumping both stores settled where it went wrong:
//!
//! ```text
//!   server store   unique_names on TWO generations
//!                  dev-53710882d8e0-main: ekke, timer      (early August)
//!                  dev-b32dae47bbd9-main: ekka, timer3     (that afternoon)
//!   device store   holds BOTH, timer3 present and indexed on uniqueName/userId/takenAtMs
//!   users row      148 versions, all on dev-b32dae47bbd9-main
//!   the app        renders `timer` — the row from the OLDER generation
//! ```
//!
//! This gate was written expecting to be RED — the hypothesis being that the read resolved
//! in the older generation. It came back GREEN, and that is what it now records: the
//! include returns BOTH rows, `["old", "new"]`, oldest first. The engine loses nothing.
//!
//! Which moves the defect downstream, to two places that are not the engine. The consumer
//! picked `.at(0)` off that array and therefore rendered the OLDER handle; and the older
//! row survives at all because the authority operates in the current generation and never
//! saw it, so a rename that deletes "every row this user owns" deleted only the ones its
//! own generation could see. Both are worth knowing, and neither is fixed here.
//!
//! Keeping the test as a guard: an include spanning a generation crossing must keep
//! returning the row written after it, whatever anyone downstream then does with the
//! array.
//!
//! `SqliteStorage` on purpose. `MemoryStorage` keeps visible entries as structs and never
//! executes the locator ladder, so the whole cross-generation raw-table family is invisible
//! to it and this gate would pass there while production stayed broken — the lesson entry
//! 27 was itself written to record.

use super::*;
use crate::storage::SqliteStorage;

fn handles_schema_v1() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("users").column("name", ColumnType::Text))
        .table(
            TableSchema::builder("handles")
                .column("userId", ColumnType::Uuid)
                .column("label", ColumnType::Text),
        )
        .build()
}

/// The same two tables, plus one more — enough to hash differently and mint a new
/// generation, while leaving `users` and `handles` rows byte-identical so nothing but the
/// family can differ.
fn handles_schema_v2() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("users").column("name", ColumnType::Text))
        .table(
            TableSchema::builder("handles")
                .column("userId", ColumnType::Uuid)
                .column("label", ColumnType::Text),
        )
        .table(TableSchema::builder("tags").column("label", ColumnType::Text))
        .build()
}

fn runtime_over<S: Storage>(
    schema: Schema,
    app_name: &str,
    storage: S,
) -> RuntimeCore<S, NoopScheduler> {
    let app_id = AppId::from_name(app_name);
    let mut schema_manager =
        SchemaManager::new(SyncManager::new(), schema, app_id, "dev", "main").unwrap();
    crate::schema_manager::rehydrate_schema_manager_from_catalogue(
        &mut schema_manager,
        &storage,
        app_id,
    )
    .expect("rehydrate from the persisted catalogue");
    let mut core = new_test_core(schema_manager, storage, NoopScheduler);
    core.immediate_tick();
    core
}

/// `users where id = U` with its `handles` backref included.
fn user_with_handles(core: &mut TestCore2, user_id: ObjectId) -> Query {
    core.schema_manager_mut()
        .query_manager_mut()
        .query("users")
        .filter_eq("id", Value::Uuid(user_id))
        .with_array("handlesViaUser", |sub| {
            sub.from("handles").correlate("userId", "users.id")
        })
        .build()
}

type TestCore2 = RuntimeCore<SqliteStorage, NoopScheduler>;

/// Every handle label the include returned, in order.
fn included_labels(rows: &[(ObjectId, Vec<Value>)]) -> Vec<String> {
    let mut out = Vec::new();
    for (_, values) in rows {
        for value in values {
            let Value::Array(handles) = value else {
                continue;
            };
            for handle in handles {
                let Value::Row { values, .. } = handle else {
                    continue;
                };
                if let Some(Value::Text(label)) = values.get(1) {
                    out.push(label.clone());
                }
            }
        }
    }
    out
}

#[test]
fn a_backref_written_after_a_generation_crossing_is_visible_to_its_parent() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let path = dir.path().join("backref.sqlite");
    let storage = SqliteStorage::open(&path).expect("sqlite storage should open");

    let mut core = runtime_over(handles_schema_v1(), "defect27-backref", storage);

    let ((user_id, _), _) = insert_and_wait_for_batch(
        &mut core,
        "users",
        HashMap::from([("name".to_string(), Value::Text("vlad".to_string()))]),
        None,
        DurabilityTier::Local,
    )
    .expect("the user inserts under generation A");
    let (_, _) = insert_and_wait_for_batch(
        &mut core,
        "handles",
        HashMap::from([
            ("userId".to_string(), Value::Uuid(user_id)),
            ("label".to_string(), Value::Text("old".to_string())),
        ]),
        None,
        DurabilityTier::Local,
    )
    .expect("the first handle inserts under generation A");
    core.batched_tick();
    core.immediate_tick();

    // The crossing: same store, rehydrated under a schema that hashes differently — the
    // way every production deployment does it.
    let storage = core.into_storage();
    let mut core = runtime_over(handles_schema_v2(), "defect27-backref", storage);

    let (_, _) = insert_and_wait_for_batch(
        &mut core,
        "handles",
        HashMap::from([
            ("userId".to_string(), Value::Uuid(user_id)),
            ("label".to_string(), Value::Text("new".to_string())),
        ]),
        None,
        DurabilityTier::Local,
    )
    .expect("the second handle inserts under generation B");
    core.batched_tick();
    core.immediate_tick();

    let query = user_with_handles(&mut core, user_id);
    // The shared executor is typed for the MemoryStorage test core; this gate must run on
    // SqliteStorage, so it drives the same future directly.
    let rows = {
        let waker = noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        let mut future = core.query_with_propagation(
            query,
            None,
            ReadDurabilityOptions::default(),
            crate::sync_manager::QueryPropagation::Full,
        );
        match Pin::new(&mut future).poll(&mut cx) {
            Poll::Ready(Ok(results)) => results,
            Poll::Ready(Err(err)) => panic!("query should succeed: {err:?}"),
            Poll::Pending => panic!("query should resolve immediately"),
        }
    };
    let labels = included_labels(&rows);

    eprintln!("handles the include returned: {labels:?}");
    assert!(
        labels.iter().any(|label| label == "new"),
        "a backref row written in the CURRENT generation must be visible to a read of its \
         parent — the parent is in that generation too, and the row is present and indexed. \
         Got {labels:?}. Measured live as a renamed handle that every screen kept rendering \
         at its previous value while the new row sat on the device, in the current \
         generation, indexed and unread."
    );
}
