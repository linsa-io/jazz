//! Differential harness for the incremental index scan.
//!
//! Live subscriptions are driven through randomized insert/update/delete workloads.
//! Every settle runs through `IndexScanNode::scan`, whose debug-build parity assertion
//! compares each incremental result against a full rescan — so any divergence panics
//! right here. On top of that the subscription's maintained result set is compared
//! against an independently tracked model after every step.

use super::*;
use std::collections::HashMap as StdHashMap;

/// Deterministic PRNG so failures reproduce byte for byte.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }

    fn pick(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
}

fn documents_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("documents")
                .column("owner_id", ColumnType::Text)
                .column("title", ColumnType::Text),
        )
        .build()
}

fn subscription_ids(core: &mut TestCore, sub_id: QuerySubscriptionId) -> Vec<ObjectId> {
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

#[test]
fn incremental_scan_matches_model_under_randomized_writes() {
    let mut core = create_runtime_with_schema(documents_schema(), "incremental-scan-app");

    // One filtered subscription (Eq condition on the scanned column) and one
    // unfiltered (All condition) stay live for the whole workload.
    let hot_sub = core
        .schema_manager_mut()
        .query_manager_mut()
        .subscribe(
            QueryBuilder::new("documents")
                .filter_eq("title", Value::Text("hot".into()))
                .build(),
        )
        .unwrap();
    let all_sub = core
        .schema_manager_mut()
        .query_manager_mut()
        .subscribe(Query::new("documents"))
        .unwrap();
    core.immediate_tick();

    let mut rng = Lcg(0x5eed_1234_9876_abcd);
    // Model of the table: id -> Some(title) for live rows, None for deleted ones.
    let mut model: StdHashMap<ObjectId, Option<String>> = StdHashMap::new();

    for step in 0..300 {
        let live_ids: Vec<ObjectId> = model
            .iter()
            .filter_map(|(id, title)| title.is_some().then_some(*id))
            .collect();

        match rng.pick(10) {
            // Insert, titles split between matching and non-matching.
            0..=3 => {
                let title = if rng.pick(2) == 0 { "hot" } else { "cold" };
                let ((id, _), _) = core
                    .insert("documents", document_insert_values("owner", title), None)
                    .unwrap();
                model.insert(id, Some(title.to_string()));
            }
            // Flip the title of a live row in or out of the filtered set.
            4..=7 if !live_ids.is_empty() => {
                let id = live_ids[rng.pick(live_ids.len())];
                let current = model[&id].clone().unwrap();
                let next = if current == "hot" { "cold" } else { "hot" };
                core.update(
                    id,
                    vec![("title".to_string(), Value::Text(next.to_string()))],
                    None,
                )
                .unwrap();
                model.insert(id, Some(next.to_string()));
            }
            // Delete a live row.
            _ if !live_ids.is_empty() => {
                let id = live_ids[rng.pick(live_ids.len())];
                core.delete(id, None).unwrap();
                model.insert(id, None);
            }
            _ => continue,
        }
        core.immediate_tick();

        let mut expected_hot: Vec<ObjectId> = model
            .iter()
            .filter_map(|(id, title)| {
                (title.as_deref() == Some("hot")).then_some(*id)
            })
            .collect();
        expected_hot.sort();
        let mut expected_all: Vec<ObjectId> = model
            .iter()
            .filter_map(|(id, title)| title.is_some().then_some(*id))
            .collect();
        expected_all.sort();

        assert_eq!(
            subscription_ids(&mut core, hot_sub),
            expected_hot,
            "filtered subscription diverged from the model at step {step}",
        );
        assert_eq!(
            subscription_ids(&mut core, all_sub),
            expected_all,
            "unfiltered subscription diverged from the model at step {step}",
        );
    }

    core.schema_manager_mut()
        .query_manager_mut()
        .unsubscribe_with_sync(hot_sub);
    core.schema_manager_mut()
        .query_manager_mut()
        .unsubscribe_with_sync(all_sub);
}

#[test]
fn incremental_scan_survives_interleaved_full_dirty() {
    // Interleaves row-precise updates with events that force full rescans (new
    // subscriptions compile mid-stream), verifying the two modes hand off cleanly.
    let mut core = create_runtime_with_schema(documents_schema(), "incremental-scan-app");

    let sub = core
        .schema_manager_mut()
        .query_manager_mut()
        .subscribe(
            QueryBuilder::new("documents")
                .filter_eq("title", Value::Text("hot".into()))
                .build(),
        )
        .unwrap();
    core.immediate_tick();

    let mut hot_ids: Vec<ObjectId> = Vec::new();
    for round in 0..10 {
        let ((id, _), _) = core
            .insert("documents", document_insert_values("owner", "hot"), None)
            .unwrap();
        hot_ids.push(id);
        core.immediate_tick();

        // A new subscription compiles fresh scan nodes (full scan baseline) while the
        // existing one keeps taking incremental deltas.
        let extra = core
            .schema_manager_mut()
            .query_manager_mut()
            .subscribe(Query::new("documents"))
            .unwrap();
        core.immediate_tick();
        assert_eq!(
            subscription_ids(&mut core, extra).len(),
            hot_ids.len(),
            "fresh subscription should see every row at round {round}",
        );
        core.schema_manager_mut()
            .query_manager_mut()
            .unsubscribe_with_sync(extra);

        let mut expected = hot_ids.clone();
        expected.sort();
        assert_eq!(subscription_ids(&mut core, sub), expected);
    }
}
