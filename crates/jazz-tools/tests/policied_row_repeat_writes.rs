#![cfg(feature = "test")]
//! Defect-27 gate: repeated updates to ONE policied row must all reach the server.
//!
//! Live shape (2026-08-15/16, production): a React Native client writes a presence
//! heartbeat every 10s — `update(users, <own row>, { onlineTimeUpdatedAtMs: now })` on a
//! table whose `allowUpdate` is `whereOld(owner is session).whereNew(same)`. The client's
//! local copy advances every beat; the SERVER's copy freezes and only ever advances by one
//! beat per server PROCESS lifetime (three forced restarts, three single advances). No
//! failure, no park, no ancestor request, no denial is logged at any level. An app restart
//! moves nothing.
//!
//! The invariant this gate pins: a local write to a policied row reaches the server
//! without requiring a server restart, for every write, not just the first.

mod support;

use std::time::Duration;

use jazz_tools::query_manager::types::{permissions, policy_expr as pe};
use jazz_tools::row_input;
use jazz_tools::server::JazzServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, QueryBuilder, SchemaBuilder, TableSchema, Value,
};
use support::wait_for_query;

/// `users`-shaped table: readable by its owner, updatable only by its owner, with the
/// old-row arm the live schema uses (`whereOld` + `whereNew`).
fn policied_users_schema() -> jazz_tools::Schema {
    let owner_is_session = pe::eq("owner_id", pe::session("user_id"));
    let users_policies = permissions(|p| {
        p.allow_read().where_(owner_is_session.clone());
        p.allow_insert().where_(owner_is_session.clone());
        p.allow_update()
            .where_old(owner_is_session.clone())
            .where_new(owner_is_session);
    });

    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("owner_id", ColumnType::Text)
                .column("online_time_updated_at_ms", ColumnType::BigInt)
                .policies(users_policies),
        )
        .build()
}

const USER: &str = "heartbeat-user";
const BEATS: i64 = 5;

#[tokio::test]
async fn every_heartbeat_on_a_policied_row_reaches_the_server_not_just_the_first() {
    let schema = policied_users_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;

    let writer = JazzClient::connect(server.make_client_context_for_user(schema.clone(), USER))
        .await
        .expect("connect writer");
    let reader = JazzClient::connect(server.make_client_context_for_user(schema, USER))
        .await
        .expect("connect reader");

    let query = QueryBuilder::new("users")
        .select(&["online_time_updated_at_ms"])
        .build();
    for (client, what) in [(&writer, "writer"), (&reader, "reader")] {
        wait_for_query(
            client,
            query.clone(),
            Some(DurabilityTier::EdgeServer),
            Duration::from_secs(30),
            &format!("{what} query readiness"),
            |_| Some(()),
        )
        .await;
    }

    let (row_id, _, insert_batch) = writer
        .insert(
            "users",
            row_input!("owner_id" => USER, "online_time_updated_at_ms" => 0i64),
        )
        .expect("seed the presence row");
    writer
        .wait_for_batch(insert_batch, DurabilityTier::EdgeServer)
        .await
        .expect("the seeding insert must reach the server");

    // The heartbeat: the same row, updated over and over. Each beat must reach the
    // server on its own — nothing here restarts anything.
    for beat in 1..=BEATS {
        let batch_id = writer
            .update(
                row_id,
                vec![(
                    "online_time_updated_at_ms".to_string(),
                    Value::BigInt(beat * 10_000),
                )],
            )
            .unwrap_or_else(|err| panic!("beat {beat} must be accepted locally: {err:?}"));

        writer
            .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
            .await
            .unwrap_or_else(|err| {
                panic!(
                    "beat {beat} never reached the server (batch {batch_id:?}): {err:?} — \
                     live, only the first write of a server's lifetime lands and every \
                     later beat is dropped silently"
                )
            });

        // Independent of the writer's own bookkeeping: a second client sees the beat
        // only if the server actually holds it.
        let expected = vec![Value::BigInt(beat * 10_000)];
        wait_for_query(
            &reader,
            query.clone(),
            Some(DurabilityTier::EdgeServer),
            Duration::from_secs(20),
            &format!("the peer sees beat {beat} through the server"),
            |rows| {
                rows.iter()
                    .any(|(id, values)| *id == row_id && values == &expected)
                    .then_some(())
            },
        )
        .await;
    }

    writer.shutdown().await.expect("shutdown writer");
    reader.shutdown().await.expect("shutdown reader");
    server.shutdown().await;
}
