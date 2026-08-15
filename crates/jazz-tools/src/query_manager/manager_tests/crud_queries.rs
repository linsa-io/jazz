use super::*;

#[test]
fn insert_and_get() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, handle.row_id);
    assert_eq!(results[0].1[0], Value::Text("Alice".into()));
    assert_eq!(results[0].1[1], Value::Integer(100));
}

#[test]
fn insert_and_query() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let bob = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(50)],
        )
        .unwrap();
    let charlie = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

    // Query all
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 3);
    assert!(results.iter().any(|(id, values)| {
        *id == alice.row_id && values == &vec![Value::Text("Alice".into()), Value::Integer(100)]
    }));
    assert!(results.iter().any(|(id, values)| {
        *id == bob.row_id && values == &vec![Value::Text("Bob".into()), Value::Integer(50)]
    }));
    assert!(results.iter().any(|(id, values)| {
        *id == charlie.row_id && values == &vec![Value::Text("Charlie".into()), Value::Integer(75)]
    }));

    // Query with filter
    let query = qm
        .query("users")
        .filter_ge("score", Value::Integer(75))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().any(|(id, values)| {
        *id == alice.row_id && values == &vec![Value::Text("Alice".into()), Value::Integer(100)]
    }));
    assert!(results.iter().any(|(id, values)| {
        *id == charlie.row_id && values == &vec![Value::Text("Charlie".into()), Value::Integer(75)]
    }));
    assert!(
        results.iter().all(|(id, _)| *id != bob.row_id),
        "Bob should not match score >= 75 filter"
    );
}

#[test]
fn query_with_sort_and_limit() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Bob".into()), Value::Integer(50)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Charlie".into()), Value::Integer(75)],
    )
    .unwrap();

    let query = qm.query("users").order_by_desc("score").limit(2).build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].1[0], Value::Text("Alice".into())); // 100
    assert_eq!(results[1].1[0], Value::Text("Charlie".into())); // 75
}

#[test]
fn query_order_by_id_is_deterministic() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let h1 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let h2 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(50)],
        )
        .unwrap();
    let h3 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

    let query = qm.query("users").order_by("id").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 3);

    let actual: Vec<_> = results.iter().map(|(id, _)| *id).collect();
    let mut expected = vec![h1.row_id, h2.row_id, h3.row_id];
    expected.sort();
    assert_eq!(actual, expected);
}

#[test]
fn query_without_order_by_defaults_to_id_ascending() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let h1 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let h2 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(50)],
        )
        .unwrap();
    let h3 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 3);

    let actual: Vec<_> = results.iter().map(|(id, _)| *id).collect();
    let mut expected = vec![h1.row_id, h2.row_id, h3.row_id];
    expected.sort();
    assert_eq!(actual, expected);
}

#[test]
fn insert_returns_handle_with_batch_id() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Handle should reference a query-visible row with inserted content.
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    let inserted = results
        .iter()
        .find(|(id, _)| *id == handle.row_id)
        .expect("Inserted row should be query-visible");
    assert_eq!(inserted.1[0], Value::Text("Alice".into()));
    assert_eq!(inserted.1[1], Value::Integer(100));

    // Handle should have a valid logical batch identity
    assert!(handle.batch_id.0 != [0; 16]);
}

#[test]
fn insert_materializes_visible_and_history_rows() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let descriptor = schema
        .get(&TableName::new("users"))
        .unwrap()
        .columns
        .clone();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    let branch = get_branch(&qm);
    let visible = storage.scan_visible_region("users", &branch).unwrap();
    let history = storage
        .scan_history_region(
            "users",
            &branch,
            HistoryScan::Row {
                row_id: handle.row_id,
            },
        )
        .unwrap();

    assert_eq!(visible.len(), 1);
    assert_eq!(history, visible);

    let stored = &history[0];
    assert_eq!(stored.row_id, handle.row_id);
    assert_eq!(stored.branch, branch);
    assert_eq!(stored.state, RowState::VisibleDirect);
    assert!(!stored.is_deleted);
    assert_eq!(
        decode_row(&descriptor, &stored.data).unwrap(),
        vec![Value::Text("Alice".into()), Value::Integer(100)]
    );
}

#[test]
fn row_is_indexed_after_insert() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Row should be immediately query-visible via indexed column lookup.
    let query = qm
        .query("users")
        .filter_eq("score", Value::Integer(100))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, handle.row_id);
}

#[test]
fn index_persistence_via_insert() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Test".into()), Value::Integer(42)],
        )
        .unwrap();

    // Verify row is query-visible via both indexed columns.
    let by_name = qm
        .query("users")
        .filter_eq("name", Value::Text("Test".into()))
        .build();
    let by_name_results = execute_query(&mut qm, &mut storage, by_name).unwrap();
    assert_eq!(by_name_results.len(), 1);
    assert_eq!(by_name_results[0].0, handle.row_id);

    let by_score = qm
        .query("users")
        .filter_eq("score", Value::Integer(42))
        .build();
    let by_score_results = execute_query(&mut qm, &mut storage, by_score).unwrap();
    assert_eq!(by_score_results.len(), 1);
    assert_eq!(by_score_results[0].0, handle.row_id);
}

#[test]
fn can_register_query_immediately() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Can register a query subscription immediately
    let query = qm.query("users").build();
    let sub_id = qm
        .subscribe(query)
        .expect("Registering a simple query should succeed");

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Registered subscription should receive updates");
    assert_eq!(delta.added.len(), 1);
}

#[test]
fn query_reads_visible_region_after_legacy_commit_history_is_removed() {
    let schema = test_schema();
    let (mut writer_qm, mut storage) = create_query_manager(SyncManager::new(), schema.clone());
    let _branch = get_branch(&writer_qm);

    let handle = writer_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    let mut reader_qm = QueryManager::new(SyncManager::new());
    reader_qm.set_current_schema(schema, "dev", "main");

    let query = reader_qm.query("users").build();
    let rows = execute_query(&mut reader_qm, &mut storage, query)
        .expect("visible-row query should succeed without legacy object-backed storage");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, handle.row_id);
    assert_eq!(
        rows[0].1,
        vec![Value::Text("Alice".into()), Value::Integer(100)]
    );
}

#[test]
fn multiple_inserts_all_visible_in_query() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Multiple inserts
    let h1 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let h2 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(50)],
        )
        .unwrap();
    let h3 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

    // Query returns all rows with expected identities and payload values.
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 3);
    let row_1 = results
        .iter()
        .find(|(id, _)| *id == h1.row_id)
        .expect("Alice row should be present");
    let row_2 = results
        .iter()
        .find(|(id, _)| *id == h2.row_id)
        .expect("Bob row should be present");
    let row_3 = results
        .iter()
        .find(|(id, _)| *id == h3.row_id)
        .expect("Charlie row should be present");
    assert_eq!(row_1.1[0], Value::Text("Alice".into()));
    assert_eq!(row_2.1[0], Value::Text("Bob".into()));
    assert_eq!(row_3.1[0], Value::Text("Charlie".into()));
    assert_eq!(row_1.1[1], Value::Integer(100));
    assert_eq!(row_2.1[1], Value::Integer(50));
    assert_eq!(row_3.1[1], Value::Integer(75));

    // Sorted query works
    let query = qm.query("users").order_by_desc("score").limit(2).build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].1[0], Value::Text("Alice".into())); // 100
    assert_eq!(results[1].1[0], Value::Text("Charlie".into())); // 75
}

#[test]
fn synced_insert_materializes_visible_and_history_rows() {
    use crate::query_manager::encoding::encode_row;
    use std::collections::HashMap;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let descriptor = schema
        .get(&TableName::new("users"))
        .unwrap()
        .columns
        .clone();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);
    let row_id = crate::object::ObjectId::new();

    let mut metadata = HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "users".to_string());
    put_test_row_metadata(&mut storage, row_id, metadata);

    let row_data = encode_row(
        &descriptor,
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    let commit = stored_row_commit(smallvec![], row_data, 1000, row_id.to_string());
    receive_row_commit(&mut qm, &mut storage, row_id, &branch, commit);

    qm.process(&mut storage);

    let visible = storage.scan_visible_region("users", &branch).unwrap();
    let history = storage
        .scan_history_region("users", &branch, HistoryScan::Row { row_id })
        .unwrap();

    assert_eq!(visible.len(), 1);
    assert_eq!(history, visible);

    let stored = &history[0];
    assert_eq!(stored.row_id, row_id);
    assert_eq!(stored.branch, branch);
    assert_eq!(stored.state, RowState::VisibleDirect);
    assert!(!stored.is_deleted);
    assert_eq!(
        decode_row(&descriptor, &stored.data).unwrap(),
        vec![Value::Text("Alice".into()), Value::Integer(100)]
    );
}

#[test]
fn lens_transform_failure_drops_row_instead_of_fallback() {
    use crate::query_manager::encoding::encode_row;
    use std::collections::HashMap;

    // Build a live schema without registering a lens path to current.
    // Rows from that branch should be dropped at materialization time.
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let mut live_schema = Schema::new();
    live_schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
            ColumnDescriptor::new("email", ColumnType::Text),
        ])
        .into(),
    );
    let live_descriptor = live_schema
        .get(&TableName::new("users"))
        .expect("live schema table should exist")
        .columns
        .clone();
    qm.add_live_schema(live_schema);

    let current_branch = get_branch(&qm);
    let live_branch = qm
        .all_query_branches()
        .into_iter()
        .find(|b| b != &current_branch)
        .expect("live schema branch should exist");

    let row_id = ObjectId::new();
    let mut metadata = HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "users".to_string());
    put_test_row_metadata(&mut storage, row_id, metadata);

    let live_data = encode_row(
        &live_descriptor,
        &[
            Value::Text("Alice".into()),
            Value::Integer(100),
            Value::Text("alice@example.com".into()),
        ],
    )
    .unwrap();
    let commit = stored_row_commit(smallvec![], live_data, 1000, row_id.to_string());
    receive_row_commit(&mut qm, &mut storage, row_id, &live_branch, commit);
    qm.process(&mut storage);

    assert!(
        qm.row_is_indexed_on_branch(&storage, "users", &live_branch, row_id),
        "row should be indexed on live branch before subscription settle"
    );

    let sub_id = qm.subscribe(qm.query("users").build()).unwrap();
    qm.process(&mut storage);

    let results = qm.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        0,
        "row from branch with failed lens transform should be dropped"
    );
}

#[test]
fn permissive_local_runtime_without_loaded_policies_returns_all_rows() {
    let sync_manager = SyncManager::new();
    // Use the regular test_schema which has no policies
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Bob".into()), Value::Integer(200)],
    )
    .unwrap();

    // Without a loaded policy bundle, local session-scoped reads stay permissive.
    let session = PolicySession::new("some_user");
    let query = qm.query("users").build();
    let sub_id = qm
        .subscribe_with_session(query, Some(session), None)
        .unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    let update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .unwrap();

    assert_eq!(
        update.delta.added.len(),
        2,
        "policy-less local runtimes should keep returning rows until a compiled bundle is loaded"
    );
}

/// A row from a branch whose schema differs ONLY IN OTHER TABLES must be
/// served without any lens.
///
/// Production 2026-08-15: the deployed migration removed one table
/// (`chat_activities`). Branch identity is whole-schema identity, so every
/// untouched table's rows — users, chats, messages — landed behind a crossing
/// whose only correct projection is identity. Clients whose stores lack a
/// registered lens (fresh store, catalogue synced without one) dropped every
/// pre-migration row at materialization: the sign-in account query settled
/// empty for an identity whose row was delivered and confirmed, and the
/// rpc-server hydrated identity tables at zero. The neighbouring drop test
/// pins the OPPOSITE case (genuinely differing descriptors, no lens — drop);
/// this one pins the identity case: identical descriptors need no lens.
#[test]
fn a_row_from_an_identical_table_on_another_schema_branch_is_served_without_a_lens() {
    use crate::query_manager::encoding::encode_row;
    use std::collections::HashMap;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // The old world: `users` IDENTICAL to the current schema; the hash differs
    // only because another table exists there (the one the migration removed).
    let mut old_schema = Schema::new();
    old_schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ])
        .into(),
    );
    old_schema.insert(
        TableName::new("chat_activities"),
        RowDescriptor::new(vec![ColumnDescriptor::new("kind", ColumnType::Text)]).into(),
    );
    let old_descriptor = old_schema
        .get(&TableName::new("users"))
        .expect("old schema users table exists")
        .columns
        .clone();
    qm.add_live_schema(old_schema);

    let current_branch = get_branch(&qm);
    let old_branch = qm
        .all_query_branches()
        .into_iter()
        .find(|b| b != &current_branch)
        .expect("old schema branch should exist");

    let row_id = ObjectId::new();
    let mut metadata = HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "users".to_string());
    put_test_row_metadata(&mut storage, row_id, metadata);

    let old_data = encode_row(
        &old_descriptor,
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    let commit = stored_row_commit(smallvec![], old_data, 1000, row_id.to_string());
    receive_row_commit(&mut qm, &mut storage, row_id, &old_branch, commit);
    qm.process(&mut storage);

    assert!(
        qm.row_is_indexed_on_branch(&storage, "users", &old_branch, row_id),
        "row should be indexed on the old branch, or this gates nothing"
    );

    let sub_id = qm.subscribe(qm.query("users").build()).unwrap();
    qm.process(&mut storage);

    let results = qm.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        1,
        "a row from an identical table on another schema branch was dropped — an \
         untouched table must not need a lens to cross a schema change"
    );
    // Content, not just presence: identity must serve the exact bytes, or a
    // misdecoding fallback would pass this gate silently.
    assert_eq!(results[0].1[0], Value::Text("Alice".into()));
    assert_eq!(results[0].1[1], Value::Integer(100));
}

/// A row delivered BEFORE the catalogue knows its origin schema must still be
/// readable once the catalogue catches up.
///
/// Production 2026-08-15, the second face of the crossing (defect 20): a
/// fresh client store receives the user's pre-migration `users` row before
/// catalogue sync delivers the old schema. The write-context ladder falls
/// through to the any-decoding-descriptor fallback and places the visible
/// bytes in the CURRENT schema's raw table, while the row locator keeps the
/// server-stamped origin hash — and every read path resolves the raw table
/// through the locator, so the row is delivered, confirmed, indexed and
/// unreadable. Measured live: the account query flips true off the in-flight
/// delta (the app reaches chats), the next materialization misses, and the
/// user is kicked back to welcome.
#[test]
fn a_row_delivered_before_the_catalogue_knows_its_schema_is_still_readable() {
    use crate::query_manager::encoding::encode_row;
    use std::collections::HashMap;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // A local write first: it seeds the catalogue with the CURRENT schema
    // (the app persists its own schema at boot) and doubles as the control
    // row that must remain visible throughout.
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("local".into()), Value::Integer(1)],
    )
    .unwrap();
    qm.process(&mut storage);

    // The old world's schema: `users` IDENTICAL, hash differs by an extra
    // table. NOT registered anywhere yet — the row will arrive first.
    let mut old_schema = Schema::new();
    old_schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ])
        .into(),
    );
    old_schema.insert(
        TableName::new("chat_activities"),
        RowDescriptor::new(vec![ColumnDescriptor::new("kind", ColumnType::Text)]).into(),
    );
    let old_hash = crate::query_manager::types::SchemaHash::compute(&old_schema);
    let old_branch = crate::query_manager::types::ComposedBranchName::new("dev", old_hash, "main")
        .to_branch_name();

    // The delivery: server-stamped metadata (locator carries the origin
    // hash), row bytes on the old branch.
    let row_id = ObjectId::new();
    let mut metadata = HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "users".to_string());
    metadata.insert(
        MetadataKey::OriginSchemaHash.to_string(),
        old_hash.to_string(),
    );
    put_test_row_metadata(&mut storage, row_id, metadata);
    let old_descriptor = old_schema
        .get(&TableName::new("users"))
        .expect("old users table exists")
        .columns
        .clone();
    let old_data = encode_row(
        &old_descriptor,
        &[Value::Text("delivered".into()), Value::Integer(100)],
    )
    .unwrap();
    let commit = stored_row_commit(smallvec![], old_data, 1000, row_id.to_string());
    receive_row_commit(&mut qm, &mut storage, row_id, old_branch.as_str(), commit);
    qm.process(&mut storage);

    // Catalogue sync catches up: the old schema becomes known and its branch
    // queryable (identity activation).
    qm.add_live_schema(old_schema);
    qm.process(&mut storage);

    let sub_id = qm.subscribe(qm.query("users").build()).unwrap();
    qm.process(&mut storage);
    let results = qm.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        2,
        "the delivered row vanished: a row that arrived before its schema was \
         catalogued must be readable once the catalogue catches up (only the \
         local control row is visible)"
    );
    let names: Vec<_> = results
        .iter()
        .map(|(_, values)| values[0].clone())
        .collect();
    assert!(
        names.contains(&Value::Text("delivered".into())),
        "the delivered row's content must be served (got: {names:?})"
    );
}
