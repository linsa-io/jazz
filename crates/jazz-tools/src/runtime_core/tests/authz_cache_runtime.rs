//! Runtime-level differential coverage for the cross-tick authorization verdict cache.
//!
//! A live subscription with a session runs explicit authorization filtering on every
//! settle. In debug builds every cache hit is re-verified against a fresh policy
//! evaluation inside `provenance_row_matches_current_select_policy`, so stale verdicts
//! panic the test rather than silently leak or hide rows. The assertions below
//! additionally pin the visible behavior: revocation and grants through the policy's
//! dependency table must take effect on the very next settle.

use super::*;
use crate::query_manager::relation_ir::{
    ColumnRef, JoinCondition, JoinKind, PredicateCmpOp, PredicateExpr, RelExpr, RowIdRef, ValueRef,
};

/// Structural (runtime) schema: no policies.
fn cache_teams_structural_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("teams").column("name", ColumnType::Text))
        .table(
            TableSchema::builder("user_team_edges")
                .column("user_id", ColumnType::Text)
                .column("team_id", ColumnType::Uuid),
        )
        .build()
}

/// Authorization schema: a team is visible when an edge for the session's user points
/// at it. Deliberately references ONLY `user_team_edges`, so writes to `teams` leave
/// other teams' verdicts valid — that is what makes cache hits possible at all.
fn cache_teams_auth_schema() -> Schema {
    let team_select_policy = PolicyExpr::ExistsRel {
        rel: RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: TableName::new("user_team_edges"),
            }),
            predicate: PredicateExpr::And(vec![
                PredicateExpr::Cmp {
                    left: ColumnRef::scoped("user_team_edges", "user_id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::SessionRef(vec!["user_id".into()]),
                },
                PredicateExpr::Cmp {
                    left: ColumnRef::scoped("user_team_edges", "team_id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::RowId(RowIdRef::Outer),
                },
            ]),
        },
    };

    SchemaBuilder::new()
        .table(
            TableSchema::builder("teams")
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(team_select_policy)
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("user_team_edges")
                .column("user_id", ColumnType::Text)
                .column("team_id", ColumnType::Uuid)
                .policies(TablePolicies::new().with_insert(PolicyExpr::True)),
        )
        .build()
}

fn team_ids(core: &mut TestCore, sub_id: QuerySubscriptionId) -> Vec<ObjectId> {
    let mut ids: Vec<ObjectId> = core
        .schema_manager_mut()
        .query_manager_mut()
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    ids.sort();
    ids
}

fn insert_edge(core: &mut TestCore, user: &str, team_id: ObjectId) -> ObjectId {
    let ((id, _), _) = core
        .insert(
            "user_team_edges",
            HashMap::from([
                ("user_id".to_string(), Value::Text(user.into())),
                ("team_id".to_string(), Value::Uuid(team_id)),
            ]),
            None,
        )
        .unwrap();
    id
}

#[test]
fn authz_cache_serves_hits_and_tracks_policy_dep_writes() {
    let mut core =
        create_runtime_with_schema(cache_teams_structural_schema(), "authz-cache-runtime");
    core.schema_manager_mut()
        .query_manager_mut()
        .set_authorization_schema(cache_teams_auth_schema());

    let sub = core
        .schema_manager_mut()
        .query_manager_mut()
        .subscribe_with_session(Query::new("teams"), Some(Session::new("alice")), None)
        .unwrap();
    core.immediate_tick();

    // Alice can see T1 through her edge.
    let ((team1, _), _) = core
        .insert(
            "teams",
            HashMap::from([("name".to_string(), Value::Text("one".into()))]),
            None,
        )
        .unwrap();
    let edge1 = insert_edge(&mut core, "alice", team1);
    core.immediate_tick();
    assert_eq!(team_ids(&mut core, sub), vec![team1]);

    // A second team without an edge: the settle re-checks T1 (cache hit, parity
    // verified in debug) and evaluates T2 to invisible.
    let hits_before = core
        .schema_manager_mut()
        .query_manager_mut()
        .authz_cache_hit_count();
    let ((team2, _), _) = core
        .insert(
            "teams",
            HashMap::from([("name".to_string(), Value::Text("two".into()))]),
            None,
        )
        .unwrap();
    core.immediate_tick();
    assert_eq!(team_ids(&mut core, sub), vec![team1].into_iter().chain([]).collect::<Vec<_>>());
    let hits_after = core
        .schema_manager_mut()
        .query_manager_mut()
        .authz_cache_hit_count();
    // Skipped when the kill switch is on — used to baseline behavior without the cache.
    if std::env::var_os("JAZZ_AUTHZ_CACHE_DISABLE").is_none() {
        assert!(
            hits_after > hits_before,
            "the unchanged team's verdict should have been served from the cache \
             (before {hits_before}, after {hits_after})",
        );
    }

    // Granting through the dependency table must invalidate and grant on the next
    // settle. A cache that missed the user_team_edges dependency would keep T2 hidden
    // — and the debug parity assert would fire before this assertion even runs.
    insert_edge(&mut core, "alice", team2);
    core.immediate_tick();
    let mut expected = vec![team1, team2];
    expected.sort();
    assert_eq!(team_ids(&mut core, sub), expected);

    // Revocation through the dependency table. The subscription's maintained result
    // set only refreshes on a settle, and an edge delete alone does not force one —
    // that is upstream behavior, the same with this cache disabled
    // (JAZZ_AUTHZ_CACHE_DISABLE=1). What the cache must guarantee: at the NEXT settle
    // the dropped edge is reflected — a stale verdict here would both fail this
    // assertion and trip the debug parity assert.
    core.delete(edge1, None).unwrap();
    core.update(
        team2,
        vec![("name".to_string(), Value::Text("two-renamed".into()))],
        None,
    )
    .unwrap();
    core.immediate_tick();
    assert_eq!(team_ids(&mut core, sub), vec![team2]);
}
