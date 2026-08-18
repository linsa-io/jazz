//! What a wedged row costs the log.
//!
//! A row that cannot be applied is retried on every arrival and on every pass of the
//! parked queue, and each failure used to print the same WARN.

use super::*;

/// A wedged row must be named once, not counted out one attempt at a time.
///
/// A row that cannot be applied is retried on every arrival and on every pass of the
/// parked queue, and each failure used to emit the same WARN. Production 2026-08-18: one
/// row produced 130,634 identical lines in three hours, peaking at 49,606 a minute — and
/// not one of them said the row was stuck. The volume was its own outage surface (it was
/// shipped to Loki at that rate), and the incident was found from a CPU graph rather than
/// from the log that had been describing it all along.
///
/// So the first failure per `(row, branch)` stays loud and complete, repeats are counted
/// rather than printed, and the count rides on the summary. The line that matters is the
/// one naming the row and what it has cost.
#[test]
fn a_wedged_row_is_named_once_not_once_per_attempt() {
    let warn_lines = captured_warns(|| {
        let schema = crate::runtime_core::tests::test_schema();
        let mut server =
            crate::runtime_core::tests::create_runtime_with_schema(schema, "wedged-row-logging");
        let client_id = ClientId::new();
        server.add_client(client_id, Some(Session::new("writer")));

        let ((row_id, _), _) = server
            .insert(
                "users",
                HashMap::from([
                    ("id".to_string(), Value::Uuid(ObjectId::new())),
                    ("name".to_string(), Value::Text("v0".to_string())),
                ]),
                None,
            )
            .expect("seed the row");
        server.batched_tick();
        server.immediate_tick();
        let branch = crate::storage::sole_branch_name(server.storage())
            .expect("branch registry readable")
            .expect("the seeded row registered a branch");
        let columns = &crate::runtime_core::tests::test_schema()
            [&crate::query_manager::types::TableName::new("users")]
            .columns;

        // Twelve arrivals for the SAME row, each declaring parents this authority does not
        // hold: twelve `ParentNotFound` refusals, which is the shape that produced 130,634
        // identical lines in production.
        for round in 0..12 {
            let orphaned = crate::row_histories::StoredRowBatch::new(
                row_id,
                branch.as_str(),
                vec![
                    crate::row_histories::BatchId::new(),
                    crate::row_histories::BatchId::new(),
                ],
                crate::query_manager::encoding::encode_row(
                    columns,
                    &crate::runtime_core::tests::user_row_values(
                        row_id,
                        &format!("diverged-{round}"),
                    ),
                )
                .expect("row encodes"),
                crate::metadata::RowProvenance::for_insert(row_id.to_string(), 9_000 + round),
                HashMap::new(),
                crate::row_histories::RowState::VisibleDirect,
                None,
            );
            server.park_sync_message(InboxEntry {
                source: Source::Client(client_id),
                payload: SyncPayload::RowBatchCreated {
                    metadata: None,
                    row: orphaned,
                },
            });
            server.batched_tick();
            server.immediate_tick();
        }
    });

    let repeats = warn_lines
        .lines()
        .filter(|line| line.contains("failed to apply synced row batch"))
        .count();
    eprintln!("WARN lines for a wedged row: {repeats}");
    assert!(
        repeats <= 2,
        "a row that keeps failing must be named once, not once per attempt — got {repeats} \
         WARN lines. At production rates this multiplier was 49,606 lines a minute for a \
         single row, none of which said the row was stuck."
    );
}

/// WARN-level output produced while `body` runs.
fn captured_warns(body: impl FnOnce()) -> String {
    use std::sync::{Arc, Mutex};

    #[derive(Clone)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for SharedWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("the log buffer lock is not poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    let buffer = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(SharedWriter(Arc::clone(&buffer)))
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .finish();
    tracing::subscriber::with_default(subscriber, body);
    String::from_utf8(
        buffer
            .lock()
            .expect("the log buffer lock is not poisoned")
            .clone(),
    )
    .expect("captured logs are utf-8")
}
