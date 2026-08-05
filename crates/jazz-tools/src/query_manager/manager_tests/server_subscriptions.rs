use super::*;

fn owned_items_schema() -> Schema {
    let mut schema = Schema::new();
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
    ]);
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]));
    schema.insert(
        TableName::new("items"),
        TableSchema::with_policies(descriptor, policies),
    );
    schema
}

/// Confirm the given payloads as the receiver would, so the server's delivery bookkeeping
/// advances. Claims are applied on the receiver's confirmation, not at queue time, so a
/// test that drains the outbox has to say the rows arrived — otherwise the peer looks like
/// one still owed rows and is rightly re-offered them.
fn confirm_delivered(
    qm: &mut crate::query_manager::manager::QueryManager,
    entries: &[crate::sync_manager::OutboxEntry],
) {
    use crate::sync_manager::{Destination, SyncPayload};
    let confirmed: Vec<_> = entries
        .iter()
        .filter_map(|entry| match (&entry.destination, &entry.payload) {
            (Destination::Client(client_id), SyncPayload::RowBatchNeeded { row, .. }) => Some((
                *client_id,
                row.row_id,
                crate::object::BranchName::new(row.branch.as_str()),
                row.batch_id,
            )),
            _ => None,
        })
        .collect();
    qm.sync_manager_mut().confirm_client_deliveries(&confirmed);
}

#[test]
fn server_builds_query_graph_on_subscription() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    // Server has existing data: 3 users, 2 with score > 50
    let handle1 = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let _handle2 = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();
    let handle3 = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();
    server_qm.process(&mut storage);

    // Add a client
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, client_id);

    // Client sends QuerySubscription for score > 50
    let query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(query),
            session: None,
            required_tier: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    server_qm.process(&mut storage);

    // Server should send RowBatchNeeded for matching users (Alice, Charlie)
    let outbox = server_qm.sync_manager_mut().take_outbox();

    let row_updates: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Client(id) if id == client_id))
        .filter_map(|e| match &e.payload {
            SyncPayload::RowBatchNeeded { row, .. } => Some(row.row_id),
            _ => None,
        })
        .collect();

    assert_eq!(
        row_updates.len(),
        2,
        "Should send 2 RowBatchNeeded messages for matching users"
    );

    let sent_ids: std::collections::HashSet<_> = row_updates.into_iter().collect();

    assert!(sent_ids.contains(&handle1.row_id), "Alice should be sent");
    assert!(sent_ids.contains(&handle3.row_id), "Charlie should be sent");
}

#[test]
fn initial_query_settled_is_not_queued_behind_later_query_scope_rows() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let bob = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();
    let _charlie = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();
    server_qm.process(&mut storage);

    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, client_id);
    server_qm.sync_manager_mut().take_outbox();

    let small_query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(90))
        .build();
    let broad_query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(0))
        .build();

    for (query_id, query) in [(QueryId(1), small_query), (QueryId(2), broad_query)] {
        server_qm.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::QuerySubscription {
                query_id,
                query: Box::new(query),
                session: None,
                required_tier: None,
                propagation: crate::sync_manager::QueryPropagation::Full,
                policy_context_tables: vec![],
            },
        });
    }

    server_qm.process(&mut storage);

    let outbox = server_qm.sync_manager_mut().take_outbox();
    let first_settled_idx = outbox
        .iter()
        .position(|entry| {
            matches!(
                entry,
                crate::sync_manager::OutboxEntry {
                    destination: Destination::Client(id),
                    payload: SyncPayload::QuerySettled { query_id: QueryId(1), .. },
                } if *id == client_id
            )
        })
        .expect("first query should settle");
    let second_query_only_row_idx = outbox
        .iter()
        .position(|entry| {
            matches!(
                entry,
                crate::sync_manager::OutboxEntry {
                    destination: Destination::Client(id),
                    payload: SyncPayload::RowBatchNeeded { row, .. },
                } if *id == client_id && row.row_id == bob.row_id
            )
        })
        .expect("second query should queue its additional rows");

    assert!(
        first_settled_idx < second_query_only_row_idx,
        "query 1 settlement should follow query 1 rows ({}) before query 2 rows ({}); outbox={outbox:?}",
        alice.row_id,
        bob.row_id
    );
}

#[test]
fn server_subscription_does_not_repeat_same_scope_same_tier_settlement() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    server_qm.process(&mut storage);

    let client_id = ClientId::new();
    connect_client(&mut server_qm, &storage, client_id);
    let _ = server_qm.sync_manager_mut().take_outbox();

    let query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(query),
            session: None,
            required_tier: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    server_qm.process(&mut storage);
    let outbox = server_qm.sync_manager_mut().take_outbox();
    assert!(
        outbox.iter().any(|entry| matches!(
            entry,
            crate::sync_manager::OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::QuerySettled { query_id: QueryId(1), .. },
            } if *id == client_id
        )),
        "initial settlement should still be emitted"
    );

    server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(10)],
        )
        .unwrap();
    server_qm.process(&mut storage);

    let outbox = server_qm.sync_manager_mut().take_outbox();
    assert!(
        outbox.iter().all(|entry| !matches!(
            entry,
            crate::sync_manager::OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::QuerySettled { query_id: QueryId(1), .. },
            } if *id == client_id
        )),
        "dirty graph passes with unchanged scope and unchanged tier should not resend QuerySettled"
    );
}

#[test]
fn duplicate_server_subscription_does_not_replay_same_scope_same_tier_settlement() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    server_qm.process(&mut storage);

    let client_id = ClientId::new();
    connect_client(&mut server_qm, &storage, client_id);
    let _ = server_qm.sync_manager_mut().take_outbox();

    let query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let mut outbox = Vec::new();
    for _ in 0..2 {
        server_qm.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::QuerySubscription {
                query_id: QueryId(1),
                query: Box::new(query.clone()),
                session: None,
                required_tier: None,
                propagation: crate::sync_manager::QueryPropagation::Full,
                policy_context_tables: vec![],
            },
        });

        server_qm.process(&mut storage);

        // Confirm between the registrations. A peer with rows still outstanding is owed a
        // re-offer, and the second registration would rightly give it one; this test is
        // about the other case — a peer that has everything and merely re-registers.
        let delivered = server_qm.sync_manager_mut().take_outbox();
        confirm_delivered(&mut server_qm, &delivered);
        outbox.extend(delivered);
    }
    let settled_count = outbox
        .iter()
        .filter(|entry| {
            matches!(
                entry,
                crate::sync_manager::OutboxEntry {
                    destination: Destination::Client(id),
                    payload: SyncPayload::QuerySettled { query_id: QueryId(1), .. },
                } if *id == client_id
            )
        })
        .count();

    assert_eq!(
        settled_count, 1,
        "re-registering an equivalent active subscription should not replay an unchanged settlement"
    );
}

#[test]
fn pending_duplicate_server_subscription_is_compiled_once() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    server_qm.process(&mut storage);

    let client_id = ClientId::new();
    connect_client(&mut server_qm, &storage, client_id);
    let _ = server_qm.sync_manager_mut().take_outbox();

    let query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    for _ in 0..2 {
        server_qm.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::QuerySubscription {
                query_id: QueryId(1),
                query: Box::new(query.clone()),
                session: None,
                required_tier: None,
                propagation: crate::sync_manager::QueryPropagation::Full,
                policy_context_tables: vec![],
            },
        });
    }

    server_qm.process(&mut storage);

    let outbox = server_qm.sync_manager_mut().take_outbox();
    let settled_count = outbox
        .iter()
        .filter(|entry| {
            matches!(
                entry,
                crate::sync_manager::OutboxEntry {
                    destination: Destination::Client(id),
                    payload: SyncPayload::QuerySettled { query_id: QueryId(1), .. },
                } if *id == client_id
            )
        })
        .count();

    assert_eq!(
        settled_count, 1,
        "duplicate pending registrations for the same client/query should compile and settle once"
    );
}

#[test]
fn server_subscription_reads_visible_region_after_legacy_commit_history_is_removed() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};
    use uuid::Uuid;

    let schema = test_schema();
    let (mut writer_qm, mut storage) = create_query_manager(SyncManager::new(), schema.clone());
    let _branch = get_branch(&writer_qm);

    let handle = writer_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(75)],
        )
        .unwrap();
    writer_qm.process(&mut storage);

    let (mut server_qm, _) = create_query_manager(SyncManager::new(), schema);
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, client_id);

    let query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(query),
            session: None,
            required_tier: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    server_qm.process(&mut storage);

    let outbox = server_qm.sync_manager_mut().take_outbox();
    let row_updates: Vec<_> = outbox
        .iter()
        .filter(|entry| matches!(entry.destination, Destination::Client(id) if id == client_id))
        .filter_map(|entry| match &entry.payload {
            SyncPayload::RowBatchNeeded { row, .. } => Some(row.row_id),
            _ => None,
        })
        .collect();

    assert_eq!(
        row_updates.len(),
        1,
        "server subscription should settle from visible rows without legacy object-backed storage"
    );
    assert_eq!(row_updates[0], handle.row_id);
}

#[test]
fn server_authorizes_subscription_sync_scope_without_rechecking_output_scope() {
    use crate::query_manager::session::Session;
    use crate::sync_manager::{
        ClientId, InboxEntry, QueryId, QueryPropagation, Source, SyncPayload,
    };

    let mut server_qm = QueryManager::new(SyncManager::new());
    server_qm.set_current_schema(owned_items_schema(), "dev", "main");
    let inner = seeded_memory_storage(&server_qm.schema_context().current_schema);
    let mut storage = CountingCatalogueUpsertsStorage::with_inner(inner);

    for index in 0..4 {
        server_qm
            .insert(
                &mut storage,
                "items",
                &[
                    Value::Text("alice".to_string()),
                    Value::Text(format!("Item {index}")),
                ],
            )
            .expect("insert item");
    }
    server_qm.process(&mut storage);

    let client_id = ClientId::new();
    connect_client(&mut server_qm, &storage, client_id);
    let _ = server_qm.sync_manager_mut().take_outbox();
    storage.reset_visible_query_loads();

    let query = server_qm.query("items").limit(4).build();
    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(query),
            session: Some(Session::new("alice")),
            required_tier: None,
            propagation: QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    server_qm.process(&mut storage);

    assert!(
        storage.visible_query_loads() <= 12,
        "server subscription should not re-run an extra output-scope authorization pass, got {} visible row loads",
        storage.visible_query_loads()
    );
}

#[test]
fn authorization_schema_context_is_reused_for_matching_env_and_user_branch() {
    let schema = owned_items_schema();
    let schema_hash = crate::query_manager::types::SchemaHash::compute(&schema);
    let mut server_qm = QueryManager::new(SyncManager::new());
    server_qm.set_current_schema(schema.clone(), "dev", "main");
    server_qm.set_authorization_schema(schema.clone());
    server_qm.set_known_schemas(std::sync::Arc::new(HashMap::from([(schema_hash, schema)])));

    let (_, first_context) = server_qm
        .authorization_schema_for_context("dev", "main")
        .expect("authorization context should be available");
    let (_, second_context) = server_qm
        .authorization_schema_for_context("dev", "main")
        .expect("authorization context should be cached");

    assert!(
        std::sync::Arc::ptr_eq(&first_context, &second_context),
        "authorization schema context should be reused for repeated subscription settlement"
    );
    assert_eq!(server_qm.authorization_context_cache.len(), 1);

    server_qm.set_known_schemas(std::sync::Arc::new(HashMap::new()));
    assert!(
        server_qm.authorization_context_cache.is_empty(),
        "known schema changes must invalidate cached authorization contexts"
    );
}

#[test]
fn local_stale_recompile_failure_drops_subscription_and_reports_failure() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let sub_id = qm.subscribe(qm.query("users").build()).unwrap();
    qm.process(&mut storage);
    let _ = qm.take_updates();

    {
        let sub = qm
            .subscriptions
            .get_mut(&sub_id)
            .expect("subscription should exist");
        sub.query = QueryBuilder::new("no_such_table").build();
        sub.needs_recompile = true;
    }

    qm.process(&mut storage);

    assert!(
        !qm.subscriptions.contains_key(&sub_id),
        "failed stale recompile should drop the local subscription"
    );

    let failures = qm.take_failed_subscriptions();
    assert_eq!(
        failures.len(),
        1,
        "expected exactly one reported local subscription failure"
    );
    assert_eq!(failures[0].subscription_id, sub_id);
    assert!(
        failures[0].reason.contains("no_such_table"),
        "failure reason should include compile context: {}",
        failures[0].reason
    );
}

#[test]
fn server_sends_error_for_uncompilable_query_subscription() {
    use crate::sync_manager::{
        ClientId, Destination, InboxEntry, QueryId, Source, SyncError, SyncPayload,
    };
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, client_id);

    // Query references a table that does not exist in schema.
    let invalid_query = QueryBuilder::new("no_such_table").build();
    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(42),
            query: Box::new(invalid_query),
            session: None,
            required_tier: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    server_qm.process(&mut storage);

    let outbox = server_qm.sync_manager_mut().take_outbox();
    let (code, reason) = outbox
        .iter()
        .find_map(|entry| match (&entry.destination, &entry.payload) {
            (
                Destination::Client(id),
                SyncPayload::Error(SyncError::QuerySubscriptionRejected {
                    query_id,
                    code,
                    reason,
                }),
            ) if *id == client_id && *query_id == QueryId(42) => {
                Some((code.clone(), reason.clone()))
            }
            _ => None,
        })
        .expect("Server should send an error payload when query subscription compilation fails");
    assert_eq!(code, "query_compilation_failed");
    assert!(
        reason.contains("query_id 42"),
        "error reason should include query id context: {reason}"
    );
    assert!(
        reason.contains("no_such_table"),
        "error reason should include compile error context: {reason}"
    );
}

#[test]
fn server_stale_recompile_failure_drops_subscription_and_notifies_client() {
    use crate::sync_manager::{
        ClientId, Destination, InboxEntry, QueryId, ServerId, Source, SyncError, SyncPayload,
    };
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    let upstream_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut server_qm, &storage, upstream_id);
    let _ = server_qm.sync_manager_mut().take_outbox();

    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, client_id);

    let valid_query = server_qm.query("users").build();
    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(7),
            query: Box::new(valid_query),
            session: None,
            required_tier: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });
    server_qm.process(&mut storage);
    let _ = server_qm.sync_manager_mut().take_outbox();

    {
        let sub = server_qm
            .server_subscriptions
            .get_mut(&(client_id, QueryId(7)))
            .expect("server subscription should exist");
        sub.query = QueryBuilder::new("no_such_table").build();
        sub.needs_recompile = true;
    }

    server_qm.process(&mut storage);

    assert!(
        !server_qm
            .server_subscriptions
            .contains_key(&(client_id, QueryId(7))),
        "failed stale recompile should drop the server subscription"
    );
    assert!(
        !server_qm
            .sync_manager()
            .get_client(client_id)
            .expect("client should still exist")
            .queries
            .contains_key(&QueryId(7)),
        "client query scope should be cleared after fail-fast drop"
    );

    let outbox = server_qm.sync_manager_mut().take_outbox();
    let (rejection_code, rejection_reason) = outbox
        .iter()
        .find_map(|entry| match (&entry.destination, &entry.payload) {
            (
                Destination::Client(id),
                SyncPayload::Error(SyncError::QuerySubscriptionRejected {
                    query_id,
                    code,
                    reason,
                }),
            ) if *id == client_id && *query_id == QueryId(7) => {
                Some((code.clone(), reason.clone()))
            }
            _ => None,
        })
        .expect("client should receive QuerySubscriptionRejected on stale recompile failure");
    assert_eq!(rejection_code, "query_recompile_failed");
    assert!(
        rejection_reason.contains("query recompilation failed for query_id 7"),
        "rejection should include query id context: {rejection_reason}"
    );
    assert!(
        rejection_reason.contains("no_such_table"),
        "rejection should include compile error context: {rejection_reason}"
    );

    assert!(
        outbox.iter().any(|entry| matches!(
            (&entry.destination, &entry.payload),
            (
                Destination::Server(id),
                SyncPayload::QueryUnsubscription { query_id }
            ) if *id == upstream_id && *query_id == QueryId(7)
        )),
        "stale recompile failure should forward QueryUnsubscription upstream"
    );
}

#[test]
fn server_pushes_new_matches() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    // Server has 1 user initially
    let _handle1 = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    server_qm.process(&mut storage);

    // Add client and subscribe
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, client_id);

    let query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(query),
            session: None,
            required_tier: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    server_qm.process(&mut storage);

    // Clear initial outbox, confirming it as the receiver would.
    let initial = server_qm.sync_manager_mut().take_outbox();
    confirm_delivered(&mut server_qm, &initial);

    // Insert new matching user
    let handle2 = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();
    server_qm.process(&mut storage);

    // Should send RowBatchNeeded for new matching user
    let outbox = server_qm.sync_manager_mut().take_outbox();

    let row_updates: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Client(id) if id == client_id))
        .filter_map(|e| match &e.payload {
            SyncPayload::RowBatchNeeded { row, .. } => Some(row.row_id),
            _ => None,
        })
        .collect();

    assert_eq!(
        row_updates.len(),
        1,
        "Should send 1 RowBatchNeeded for new matching user"
    );

    assert_eq!(
        row_updates[0], handle2.row_id,
        "Should send Charlie's ObjectId"
    );
}

#[test]
fn server_subscription_telemetry_tracks_grouping_and_unsubscribe_lifecycle() {
    use crate::sync_manager::{
        ClientId, InboxEntry, QueryId, QueryPropagation, Source, SyncPayload,
    };

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    let repeated_query = server_qm.query("users").build();
    let repeated_query_json = serde_json::to_string(&repeated_query).unwrap();
    let filtered_query = server_qm
        .query("users")
        .filter_eq("name", Value::Text("Alice".into()))
        .build();

    let client_a = ClientId::new();
    let client_b = ClientId::new();
    let client_c = ClientId::new();
    for client_id in [client_a, client_b, client_c] {
        connect_client(&mut server_qm, &storage, client_id);
    }

    for (client_id, query_id, query, propagation) in [
        (
            client_a,
            QueryId(1),
            repeated_query.clone(),
            QueryPropagation::Full,
        ),
        (
            client_b,
            QueryId(2),
            repeated_query.clone(),
            QueryPropagation::Full,
        ),
        (
            client_c,
            QueryId(3),
            repeated_query.clone(),
            QueryPropagation::LocalOnly,
        ),
        (
            client_c,
            QueryId(4),
            filtered_query.clone(),
            QueryPropagation::Full,
        ),
    ] {
        server_qm.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::QuerySubscription {
                query_id,
                query: Box::new(query),
                session: None,
                required_tier: None,
                propagation,
                policy_context_tables: vec![],
            },
        });
    }

    server_qm.process(&mut storage);

    let telemetry = server_qm.server_subscription_telemetry();
    assert_eq!(telemetry.len(), 3);
    assert!(telemetry.iter().any(|group| {
        group.count == 2 && group.propagation == QueryPropagation::Full && group.table == "users"
    }));
    assert!(
        telemetry
            .iter()
            .any(|group| { group.count == 1 && group.propagation == QueryPropagation::LocalOnly })
    );
    assert!(
        telemetry
            .iter()
            .any(|group| { group.count == 1 && group.query.contains("\"name\"") })
    );

    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_b),
        payload: SyncPayload::QueryUnsubscription {
            query_id: QueryId(2),
        },
    });
    server_qm.process(&mut storage);

    let telemetry_after_unsubscribe = server_qm.server_subscription_telemetry();
    assert!(telemetry_after_unsubscribe.iter().any(|group| {
        group.count == 1
            && group.propagation == QueryPropagation::Full
            && group.query == repeated_query_json
    }));
}

#[test]
fn server_does_not_push_non_matching() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    // Add client and subscribe to score > 50
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, client_id);

    let query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(query),
            session: None,
            required_tier: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    server_qm.process(&mut storage);
    let _ = server_qm.sync_manager_mut().take_outbox();

    // Insert non-matching user (score = 30)
    let _handle = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();
    server_qm.process(&mut storage);

    // Should NOT send RowBatchNeeded for non-matching user
    let outbox = server_qm.sync_manager_mut().take_outbox();

    let row_updates: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Client(id) if id == client_id))
        .filter(|e| matches!(e.payload, SyncPayload::RowBatchNeeded { .. }))
        .collect();

    assert_eq!(
        row_updates.len(),
        0,
        "Should NOT send RowBatchNeeded for non-matching user"
    );
}

/// A peer whose transport dropped and came back must be re-offered what it missed.
///
/// This is the shape a phone produces and the one no other test reaches. A brief network gap
/// does not tear down the client's subscription — the transport reconnects underneath it and
/// replays the SAME query id. The server then finds an equivalent, already-settled
/// subscription and answers from the cached scope without re-deriving anything.
/// Re-derivation is the only place that sets `force_resend`, so a row the peer never
/// confirmed is otherwise never offered again.
///
/// Every existing offline test reconnects by building a fresh client, which mints a NEW
/// query id; the server has no prior state for that key, re-derives, and force-resends
/// everything as a side effect. They are green for a reason that does not apply in the field.
#[test]
fn a_transport_reconnect_re_offers_a_row_the_peer_never_confirmed() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    server_qm.process(&mut storage);

    let client_id = ClientId::new();
    connect_client(&mut server_qm, &storage, client_id);
    let _ = server_qm.sync_manager_mut().take_outbox();

    let query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let subscribe = |qm: &mut crate::query_manager::manager::QueryManager| {
        qm.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::QuerySubscription {
                query_id: QueryId(1),
                query: Box::new(query.clone()),
                session: None,
                required_tier: None,
                propagation: crate::sync_manager::QueryPropagation::Full,
                policy_context_tables: vec![],
            },
        });
    };

    subscribe(&mut server_qm);
    server_qm.process(&mut storage);
    let initial = server_qm.sync_manager_mut().take_outbox();
    confirm_delivered(&mut server_qm, &initial);

    // The gap: a message arrives while the peer cannot receive it. Draining the outbox
    // without confirming is what a dead socket does to the payload.
    let missed = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("sent while away".into()), Value::Integer(150)],
        )
        .unwrap();
    server_qm.process(&mut storage);
    let dropped = server_qm.sync_manager_mut().take_outbox();
    assert!(
        dropped.iter().any(|entry| matches!(
            entry,
            crate::sync_manager::OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::RowBatchNeeded { row, .. },
            } if *id == client_id && row.row_id == missed.row_id
        )),
        "precondition: the row should have been offered to the subscribed peer once"
    );
    drop(dropped);

    // The peer is back. Nothing is signalled by hand: the server still believes the old
    // socket is alive — a three-second drop tells TCP nothing — so no disconnect was ever
    // recorded. What IS true is that the peer never confirmed the row.
    subscribe(&mut server_qm);
    server_qm.process(&mut storage);

    let after = server_qm.sync_manager_mut().take_outbox();
    assert!(
        after.iter().any(|entry| matches!(
            entry,
            crate::sync_manager::OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::RowBatchNeeded { row, .. },
            } if *id == client_id && row.row_id == missed.row_id
        )),
        "the peer reconnected and the row it missed was never offered again — the server \
         recognised the replayed subscription as equivalent and already settled, so nothing \
         re-derived its scope and nothing set force_resend. This is the message that never \
         arrives after a brief network drop."
    );
}

/// The re-offer stops once the peer is caught up.
///
/// If the trigger stayed true after confirmation, every resubscribe — and a phone
/// resubscribes on every foreground — would re-send. This pins the other half.
#[test]
fn the_re_offer_stops_once_the_peer_is_caught_up() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    let client_id = ClientId::new();
    connect_client(&mut server_qm, &storage, client_id);
    let _ = server_qm.sync_manager_mut().take_outbox();

    let query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let subscribe = |qm: &mut crate::query_manager::manager::QueryManager| {
        qm.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::QuerySubscription {
                query_id: QueryId(1),
                query: Box::new(query.clone()),
                session: None,
                required_tier: None,
                propagation: crate::sync_manager::QueryPropagation::Full,
                policy_context_tables: vec![],
            },
        });
    };

    subscribe(&mut server_qm);
    server_qm.process(&mut storage);

    server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("delivered".into()), Value::Integer(150)],
        )
        .unwrap();
    server_qm.process(&mut storage);
    let delivered = server_qm.sync_manager_mut().take_outbox();
    confirm_delivered(&mut server_qm, &delivered);

    subscribe(&mut server_qm);
    server_qm.process(&mut storage);

    let after = server_qm.sync_manager_mut().take_outbox();
    let re_offered = after
        .iter()
        .filter(|entry| {
            matches!(
                entry,
                crate::sync_manager::OutboxEntry {
                    destination: Destination::Client(id),
                    payload: SyncPayload::RowBatchNeeded { .. },
                } if *id == client_id
            )
        })
        .count();
    assert_eq!(
        re_offered, 0,
        "a peer that confirmed everything was re-offered {re_offered} rows on a plain \
         resubscribe — the trigger never clears, so every foreground re-sends"
    );
}

/// The re-offer carries what the peer is missing, not everything it can see.
#[test]
fn the_re_offer_carries_only_what_the_peer_is_missing() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    let client_id = ClientId::new();
    connect_client(&mut server_qm, &storage, client_id);
    let _ = server_qm.sync_manager_mut().take_outbox();

    let query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let subscribe = |qm: &mut crate::query_manager::manager::QueryManager| {
        qm.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::QuerySubscription {
                query_id: QueryId(1),
                query: Box::new(query.clone()),
                session: None,
                required_tier: None,
                propagation: crate::sync_manager::QueryPropagation::Full,
                policy_context_tables: vec![],
            },
        });
    };

    subscribe(&mut server_qm);
    server_qm.process(&mut storage);

    let held = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("already held".into()), Value::Integer(150)],
        )
        .unwrap();
    server_qm.process(&mut storage);
    let delivered = server_qm.sync_manager_mut().take_outbox();
    confirm_delivered(&mut server_qm, &delivered);

    let missed = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("sent while away".into()), Value::Integer(160)],
        )
        .unwrap();
    server_qm.process(&mut storage);
    let _dropped = server_qm.sync_manager_mut().take_outbox();

    subscribe(&mut server_qm);
    server_qm.process(&mut storage);

    let after = server_qm.sync_manager_mut().take_outbox();
    let offered: Vec<_> = after
        .iter()
        .filter_map(|entry| match (&entry.destination, &entry.payload) {
            (Destination::Client(id), SyncPayload::RowBatchNeeded { row, .. })
                if *id == client_id =>
            {
                Some(row.row_id)
            }
            _ => None,
        })
        .collect();

    assert!(
        offered.contains(&missed.row_id),
        "the unconfirmed row was not re-offered"
    );
    assert!(
        !offered.contains(&held.row_id),
        "a row the peer already confirmed was re-sent as part of the recovery — the \
         re-offer ships the whole scope instead of what is missing, so one outstanding row \
         costs a full retransmission on every resubscribe"
    );
}

/// A batch a later batch supersedes must stop being owed.
///
/// A re-offer can only ship the CURRENT row, so an unconfirmed batch that a newer one has
/// replaced can never be confirmed — nothing will send it again. If the entry survives that,
/// the peer counts as owed rows forever: every registration re-derives, and the entry leaks
/// until the client is reaped, which never happens while it stays connected.
#[test]
fn a_superseded_unconfirmed_batch_stops_being_owed() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    let client_id = ClientId::new();
    connect_client(&mut server_qm, &storage, client_id);
    let _ = server_qm.sync_manager_mut().take_outbox();

    let query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let subscribe = |qm: &mut crate::query_manager::manager::QueryManager| {
        qm.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::QuerySubscription {
                query_id: QueryId(1),
                query: Box::new(query.clone()),
                session: None,
                required_tier: None,
                propagation: crate::sync_manager::QueryPropagation::Full,
                policy_context_tables: vec![],
            },
        });
    };

    subscribe(&mut server_qm);
    server_qm.process(&mut storage);

    let row = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("first".into()), Value::Integer(150)],
        )
        .unwrap();
    server_qm.process(&mut storage);
    let _dropped = server_qm.sync_manager_mut().take_outbox();

    // Edited while the peer is still away: the first batch can never be sent again.
    server_qm
        .update(
            &mut storage,
            row.row_id,
            &[Value::Text("second".into()), Value::Integer(160)],
        )
        .unwrap();
    server_qm.process(&mut storage);
    let _ = server_qm.sync_manager_mut().take_outbox();

    // The peer returns, is handed the current row, and confirms it.
    subscribe(&mut server_qm);
    server_qm.process(&mut storage);
    let delivered = server_qm.sync_manager_mut().take_outbox();
    assert!(
        delivered.iter().any(|entry| matches!(
            entry,
            crate::sync_manager::OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::RowBatchNeeded { .. },
            } if *id == client_id
        )),
        "precondition: the returning peer should have been re-offered the row"
    );
    confirm_delivered(&mut server_qm, &delivered);

    subscribe(&mut server_qm);
    server_qm.process(&mut storage);

    let after = server_qm.sync_manager_mut().take_outbox();
    let re_offered = after
        .iter()
        .filter(|entry| {
            matches!(
                entry,
                crate::sync_manager::OutboxEntry {
                    destination: Destination::Client(id),
                    payload: SyncPayload::RowBatchNeeded { .. },
                } if *id == client_id
            )
        })
        .count();
    assert_eq!(
        re_offered, 0,
        "the caught-up peer was re-offered {re_offered} rows"
    );
    assert!(
        !server_qm
            .sync_manager()
            .client_has_undelivered_payloads(client_id),
        "the peer is caught up but is still recorded as owed rows — the entry outlives \
         every chance to confirm it, so it leaks until the client is reaped, and a \
         connected client is never reaped"
    );
}
