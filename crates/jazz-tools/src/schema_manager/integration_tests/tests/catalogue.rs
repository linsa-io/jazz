use super::*;

#[test]
fn schema_manager_persists_current_schema_only_once_per_storage() {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    let mut manager =
        SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();
    let mut storage = CountingCatalogueUpsertsStorage::new();

    manager
        .insert(
            &mut storage,
            "users",
            HashMap::from([
                ("id".to_string(), Value::Uuid(ObjectId::new())),
                ("name".to_string(), Value::Text("Alice".into())),
            ]),
            None,
            None,
        )
        .expect("first insert should succeed");
    let first_upserts = storage.catalogue_upserts();
    assert!(
        first_upserts >= 1,
        "first insert should persist current schema/catalogue state"
    );

    manager
        .insert(
            &mut storage,
            "users",
            HashMap::from([
                ("id".to_string(), Value::Uuid(ObjectId::new())),
                ("name".to_string(), Value::Text("Bob".into())),
            ]),
            None,
            None,
        )
        .expect("second insert should succeed");

    assert_eq!(
        storage.catalogue_upserts(),
        first_upserts,
        "schema-aware writes should not reload and repersist the unchanged current schema",
    );
}

#[test]
fn catalogue_schema_persistence() {
    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .build();

    let mut manager =
        SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main").unwrap();

    // Persist schema
    let mut storage = MemoryStorage::new();
    let object_id = manager.persist_schema(&mut storage);

    // Verify ObjectId is deterministic (based on schema hash)
    let schema_hash = SchemaHash::compute(&v2);
    assert_eq!(object_id, schema_hash.to_object_id());

    // Verify schema was encoded correctly by decoding it back
    let encoded = encode_schema(&v2);
    let decoded = decode_schema(&encoded).unwrap();

    // Decoded schema should match original
    assert_eq!(decoded.len(), v2.len());
    assert!(decoded.contains_key(&TableName::new("users")));
}

#[test]
fn catalogue_lens_persistence() {
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .build();

    let mut manager =
        SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main").unwrap();
    let lens = manager.add_live_schema(v1).unwrap().clone();

    // Persist lens
    let mut storage = MemoryStorage::new();
    let object_id = manager.persist_lens(&mut storage, &lens);

    // Verify ObjectId is deterministic
    assert_eq!(object_id, lens.object_id());

    // Verify lens transform was encoded correctly
    let encoded = encode_lens_transform(&lens.forward);
    let decoded = decode_lens_transform(&encoded).unwrap();

    assert_eq!(decoded.ops.len(), lens.forward.ops.len());
}

#[test]
fn catalogue_process_schema_update() {
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .build();

    // Client B starts with v1 schema
    let mut manager_b =
        SchemaManager::new(SyncManager::new(), v1.clone(), test_app_id(), "dev", "main").unwrap();

    // Simulate receiving v2 schema from catalogue sync
    let v2_hash = SchemaHash::compute(&v2);
    let v2_object_id = v2_hash.to_object_id();
    let v2_encoded = encode_schema(&v2);

    let mut metadata = HashMap::new();
    metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueSchema.to_string(),
    );
    metadata.insert(
        MetadataKey::AppId.to_string(),
        test_app_id().uuid().to_string(),
    );
    metadata.insert(MetadataKey::SchemaHash.to_string(), v2_hash.to_string());

    // Process the catalogue update
    manager_b
        .process_catalogue_update(v2_object_id, &metadata, &v2_encoded)
        .unwrap();

    // v2 should now be pending (no lens path yet)
    assert!(manager_b.context().is_pending(&v2_hash));
    assert!(!manager_b.context().is_live(&v2_hash));
}

#[test]
fn catalogue_lens_activates_pending_schema() {
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .build();

    // Client B starts with v1 schema as current
    let mut manager_b =
        SchemaManager::new(SyncManager::new(), v1.clone(), test_app_id(), "dev", "main").unwrap();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);

    // First, receive v2 schema (will be pending - no lens)
    let v2_object_id = v2_hash.to_object_id();
    let v2_encoded = encode_schema(&v2);
    let mut schema_metadata = HashMap::new();
    schema_metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueSchema.to_string(),
    );
    schema_metadata.insert(
        MetadataKey::AppId.to_string(),
        test_app_id().uuid().to_string(),
    );
    schema_metadata.insert(MetadataKey::SchemaHash.to_string(), v2_hash.to_string());

    manager_b
        .process_catalogue_update(v2_object_id, &schema_metadata, &v2_encoded)
        .unwrap();

    assert!(manager_b.context().is_pending(&v2_hash));

    // Now receive the lens v1->v2
    let lens = generate_lens(&v1, &v2);
    let lens_object_id = lens.object_id();
    let lens_encoded = encode_lens_transform(&lens.forward);

    let mut lens_metadata = HashMap::new();
    lens_metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueLens.to_string(),
    );
    lens_metadata.insert(
        MetadataKey::AppId.to_string(),
        test_app_id().uuid().to_string(),
    );
    lens_metadata.insert(MetadataKey::SourceHash.to_string(), v1_hash.to_string());
    lens_metadata.insert(MetadataKey::TargetHash.to_string(), v2_hash.to_string());

    manager_b
        .process_catalogue_update(lens_object_id, &lens_metadata, &lens_encoded)
        .unwrap();

    // v2 should now be live because lens_path() traverses backward lenses too.
    // Current schema is v1. For pending v2 to become live, we need a path from v2 to v1.
    // The lens is registered as (v1, v2), but lens_path() BFS considers backward direction,
    // so it finds v2 -> v1 via the backward transform.
    assert!(manager_b.get_lens(&v1_hash, &v2_hash).is_some());

    // v2 should now be live (no longer pending)
    assert!(!manager_b.context().is_pending(&v2_hash));
    assert!(manager_b.context().is_live(&v2_hash));
    assert_eq!(manager_b.all_branches().len(), 2);
}

#[test]
fn catalogue_draft_lens_does_not_activate_pending_schema() {
    let v1 = SchemaBuilder::new()
        .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("org_id", ColumnType::Uuid),
        )
        .build();

    let mut schema_manager =
        SchemaManager::new(SyncManager::new(), v1.clone(), test_app_id(), "dev", "main").unwrap();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);

    let v2_object_id = v2_hash.to_object_id();
    let v2_encoded = encode_schema(&v2);
    let mut schema_metadata = HashMap::new();
    schema_metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueSchema.to_string(),
    );
    schema_metadata.insert(
        MetadataKey::AppId.to_string(),
        test_app_id().uuid().to_string(),
    );
    schema_metadata.insert(MetadataKey::SchemaHash.to_string(), v2_hash.to_string());

    schema_manager
        .process_catalogue_update(v2_object_id, &schema_metadata, &v2_encoded)
        .unwrap();

    assert!(
        schema_manager.context().is_pending(&v2_hash),
        "v2 should be pending before any lens arrives"
    );

    let lens = generate_lens(&v1, &v2);
    assert!(lens.is_draft(), "v1 -> v2 should be a draft lens");

    let lens_object_id = lens.object_id();
    let lens_encoded = encode_lens_transform(&lens.forward);
    let mut lens_metadata = HashMap::new();
    lens_metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueLens.to_string(),
    );
    lens_metadata.insert(
        MetadataKey::AppId.to_string(),
        test_app_id().uuid().to_string(),
    );
    lens_metadata.insert(MetadataKey::SourceHash.to_string(), v1_hash.to_string());
    lens_metadata.insert(MetadataKey::TargetHash.to_string(), v2_hash.to_string());

    schema_manager
        .process_catalogue_update(lens_object_id, &lens_metadata, &lens_encoded)
        .unwrap();

    assert!(
        schema_manager.get_lens(&v1_hash, &v2_hash).is_some(),
        "draft lens should still be registered"
    );
    assert!(
        schema_manager.context().is_pending(&v2_hash),
        "draft lens should not activate pending schema v2"
    );
    assert!(
        !schema_manager.context().is_live(&v2_hash),
        "draft lens should not make v2 live"
    );
    assert_eq!(
        schema_manager.all_branches().len(),
        1,
        "only the current schema branch should be active"
    );
}

#[test]
fn query_manager_queues_catalogue_updates() {
    use crate::query_manager::manager::QueryManager;

    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    let mut qm = QueryManager::new(SyncManager::new());
    qm.set_current_schema(v1.clone(), "dev", "main");
    let mut storage = MemoryStorage::new();

    // Initially no pending catalogue updates
    assert!(qm.take_pending_catalogue_updates().is_empty());

    // Inject two real catalogue schema objects via sync path.
    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .build();
    let v2_hash = SchemaHash::compute(&v2);
    let object_id_v2 = v2_hash.to_object_id();
    let encoded_schema_v2 = encode_schema(&v2);

    let v3 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text)
                .nullable_column("role", ColumnType::Text),
        )
        .build();
    let v3_hash = SchemaHash::compute(&v3);
    let object_id_v3 = v3_hash.to_object_id();
    let encoded_schema_v3 = encode_schema(&v3);

    let mut metadata_v2 = HashMap::new();
    metadata_v2.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueSchema.to_string(),
    );
    metadata_v2.insert(
        MetadataKey::AppId.to_string(),
        test_app_id().uuid().to_string(),
    );
    metadata_v2.insert(MetadataKey::SchemaHash.to_string(), v2_hash.to_string());

    let mut metadata_v3 = HashMap::new();
    metadata_v3.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueSchema.to_string(),
    );
    metadata_v3.insert(
        MetadataKey::AppId.to_string(),
        test_app_id().uuid().to_string(),
    );
    metadata_v3.insert(MetadataKey::SchemaHash.to_string(), v3_hash.to_string());

    ingest_remote_catalogue_object(
        &mut qm,
        &mut storage,
        object_id_v2,
        metadata_v2,
        encoded_schema_v2.clone(),
        1,
    );
    ingest_remote_catalogue_object(
        &mut qm,
        &mut storage,
        object_id_v3,
        metadata_v3,
        encoded_schema_v3.clone(),
        2,
    );

    qm.process(&mut storage);

    let pending = qm.take_pending_catalogue_updates();
    assert_eq!(pending.len(), 2, "two catalogue objects should be queued");
    assert_eq!(pending[0].object_id, object_id_v2);
    assert_eq!(pending[1].object_id, object_id_v3);

    for update in &pending {
        assert_eq!(
            update.metadata.get(MetadataKey::Type.as_str()),
            Some(&ObjectType::CatalogueSchema.to_string())
        );
        assert_eq!(
            update.metadata.get(MetadataKey::AppId.as_str()),
            Some(&test_app_id().uuid().to_string())
        );
    }
    assert_eq!(pending[0].content, encoded_schema_v2);
    assert_eq!(pending[1].content, encoded_schema_v3);

    // Queue should be drained after take().
    assert!(qm.take_pending_catalogue_updates().is_empty());
}

#[test]
fn catalogue_non_matching_app_id_is_ignored() {
    // v1: id, name, birthday(Timestamp)
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .column("birthday", ColumnType::Timestamp),
        )
        .build();
    // v2: id, name, birthday(Text)
    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .column("birthday", ColumnType::Text),
        )
        .build();
    // v3: id, name, birthday(nullable Text)
    let v3 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("birthday", ColumnType::Text),
        )
        .build();
    // v4: id, name, email(Text)
    let v4 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .column("email", ColumnType::Text),
        )
        .build();

    let mut manager =
        SchemaManager::new(SyncManager::new(), v1.clone(), test_app_id(), "dev", "main").unwrap();

    for schema in [v1, v2, v3, v4] {
        let hash = SchemaHash::compute(&schema);
        let before = (
            manager.all_branches().len(),
            manager.context().is_live(&hash),
            manager.context().is_pending(&hash),
            manager.is_schema_known(&hash),
        );

        let mut metadata = HashMap::new();
        metadata.insert(
            MetadataKey::Type.to_string(),
            ObjectType::CatalogueSchema.to_string(),
        );
        metadata.insert(
            MetadataKey::AppId.to_string(),
            AppId::from_name("other-app").uuid().to_string(),
        );
        metadata.insert(MetadataKey::SchemaHash.to_string(), hash.to_string());

        manager
            .process_catalogue_update(hash.to_object_id(), &metadata, &encode_schema(&schema))
            .unwrap();

        let after = (
            manager.all_branches().len(),
            manager.context().is_live(&hash),
            manager.context().is_pending(&hash),
            manager.is_schema_known(&hash),
        );
        assert_eq!(
            after,
            before,
            "mismatched app_id should not mutate schema state for hash {}",
            hash.short()
        );
    }
}

#[test]
fn catalogue_unknown_type_is_ignored() {
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .column("birthday", ColumnType::Timestamp),
        )
        .build();
    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .column("email", ColumnType::Text),
        )
        .build();
    let v2_hash = SchemaHash::compute(&v2);

    let mut manager =
        SchemaManager::new(SyncManager::new(), v1.clone(), test_app_id(), "dev", "main").unwrap();
    let before_branches = manager.all_branches().len();

    let mut metadata = HashMap::new();
    // Unknown type should be ignored
    metadata.insert(
        MetadataKey::Type.to_string(),
        "CatalogueBogusType".to_string(),
    );
    metadata.insert(
        MetadataKey::AppId.to_string(),
        test_app_id().uuid().to_string(),
    );

    manager
        .process_catalogue_update(v2_hash.to_object_id(), &metadata, &encode_schema(&v2))
        .unwrap();

    assert_eq!(manager.all_branches().len(), before_branches);
    assert!(!manager.context().is_pending(&v2_hash));
    assert!(!manager.context().is_live(&v2_hash));
    assert!(!manager.is_schema_known(&v2_hash));
}

#[test]
fn catalogue_same_schema_push_is_noop() {
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .column("birthday", ColumnType::Timestamp),
        )
        .build();
    let v1_hash = SchemaHash::compute(&v1);

    let mut manager =
        SchemaManager::new(SyncManager::new(), v1.clone(), test_app_id(), "dev", "main").unwrap();

    let before = (
        manager.all_branches().len(),
        manager.context().is_live(&v1_hash),
        manager.context().is_pending(&v1_hash),
        manager.is_schema_known(&v1_hash),
    );

    let mut metadata = HashMap::new();
    metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueSchema.to_string(),
    );
    metadata.insert(
        MetadataKey::AppId.to_string(),
        test_app_id().uuid().to_string(),
    );
    metadata.insert(MetadataKey::SchemaHash.to_string(), v1_hash.to_string());

    manager
        .process_catalogue_update(v1_hash.to_object_id(), &metadata, &encode_schema(&v1))
        .unwrap();
    manager
        .process_catalogue_update(v1_hash.to_object_id(), &metadata, &encode_schema(&v1))
        .unwrap();

    let after = (
        manager.all_branches().len(),
        manager.context().is_live(&v1_hash),
        manager.context().is_pending(&v1_hash),
        manager.is_schema_known(&v1_hash),
    );
    assert_eq!(after, before, "same-schema pushes should be idempotent");
}

#[test]
fn catalogue_schema_malformed_payload_errors_deterministically() {
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .column("birthday", ColumnType::Timestamp),
        )
        .build();
    let mut manager =
        SchemaManager::new(SyncManager::new(), v1, test_app_id(), "dev", "main").unwrap();

    let target_hash = SchemaHash::from_bytes([9; 32]);
    let before_branches = manager.all_branches().len();
    let before = (
        manager.context().is_live(&target_hash),
        manager.context().is_pending(&target_hash),
        manager.is_schema_known(&target_hash),
    );
    let mut metadata = HashMap::new();
    metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueSchema.to_string(),
    );
    metadata.insert(
        MetadataKey::AppId.to_string(),
        test_app_id().uuid().to_string(),
    );
    metadata.insert(MetadataKey::SchemaHash.to_string(), target_hash.to_string());

    let err = manager
        .process_catalogue_update(ObjectId::new(), &metadata, b"\xFF")
        .expect_err("malformed schema payload should return decode-path error");
    assert_eq!(
        err,
        crate::schema_manager::SchemaError::SchemaNotFound(SchemaHash::from_bytes([0; 32]))
    );
    let err_again = manager
        .process_catalogue_update(ObjectId::new(), &metadata, b"\xFF")
        .expect_err("second malformed schema payload should fail identically");
    assert_eq!(err_again, err);
    assert_eq!(
        manager.all_branches().len(),
        before_branches,
        "failed decode must not mutate branch state"
    );
    let after = (
        manager.context().is_live(&target_hash),
        manager.context().is_pending(&target_hash),
        manager.is_schema_known(&target_hash),
    );
    assert_eq!(
        after, before,
        "failed schema decode must not change known/pending/live state"
    );
}

#[test]
fn catalogue_lens_malformed_payload_errors_deterministically() {
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .column("birthday", ColumnType::Timestamp),
        )
        .build();
    let mut manager =
        SchemaManager::new(SyncManager::new(), v1, test_app_id(), "dev", "main").unwrap();

    let before_branches = manager.all_branches().len();
    let source = SchemaHash::from_bytes([1; 32]);
    let target = SchemaHash::from_bytes([2; 32]);
    let before = (
        manager.context().is_live(&target),
        manager.context().is_pending(&target),
    );
    let mut metadata = HashMap::new();
    metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueLens.to_string(),
    );
    metadata.insert(
        MetadataKey::AppId.to_string(),
        test_app_id().uuid().to_string(),
    );
    metadata.insert(MetadataKey::SourceHash.to_string(), source.to_string());
    metadata.insert(MetadataKey::TargetHash.to_string(), target.to_string());

    let err = manager
        .process_catalogue_update(ObjectId::new(), &metadata, b"\xFF")
        .expect_err("malformed lens payload should return decode-path error");
    assert_eq!(
        err,
        crate::schema_manager::SchemaError::LensNotFound { source, target }
    );
    let err_again = manager
        .process_catalogue_update(ObjectId::new(), &metadata, b"\xFF")
        .expect_err("second malformed lens payload should fail identically");
    assert_eq!(err_again, err);
    assert!(
        manager.get_lens(&source, &target).is_none(),
        "failed lens decode must not register any lens"
    );
    assert_eq!(
        manager.all_branches().len(),
        before_branches,
        "failed lens decode must not mutate branch state"
    );
    let after = (
        manager.context().is_live(&target),
        manager.context().is_pending(&target),
    );
    assert_eq!(
        after, before,
        "failed lens decode must not activate any schema"
    );
}

#[test]
fn e2e_catalogue_sync_with_data_query() {
    use crate::sync_manager::{InboxEntry, ServerId, Source, SyncPayload};

    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .build();

    let v2_hash = SchemaHash::compute(&v2);

    // === Client A: v2 schema, knows about v1 ===
    let mut client_a =
        SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main").unwrap();
    let lens = client_a.add_live_schema(v1.clone()).unwrap().clone();
    // add_live_schema now automatically updates QueryManager

    // === Client A persists schema and lens to catalogue ===
    let mut io_a = MemoryStorage::new();
    let schema_object_id = client_a.persist_schema(&mut io_a);
    let lens_object_id = client_a.persist_lens(&mut io_a, &lens);

    // Verify deterministic ObjectIds
    assert_eq!(schema_object_id, v2_hash.to_object_id());
    assert_eq!(lens_object_id, lens.object_id());

    // === Client B: v1 schema (old client) ===
    let mut client_b =
        SchemaManager::new(SyncManager::new(), v1.clone(), test_app_id(), "dev", "main").unwrap();
    let mut io_b = MemoryStorage::new();

    // Initially B only knows about v1
    assert_eq!(client_b.all_branches().len(), 1);
    assert!(!client_b.context().is_live(&v2_hash));

    // Wire both clients to a shared upstream to exercise real outbox/inbox sync path.
    let upstream_server_id = ServerId::new();
    client_a
        .query_manager_mut()
        .sync_manager_mut()
        .add_server_with_storage(upstream_server_id, false, &io_a);
    client_b
        .query_manager_mut()
        .sync_manager_mut()
        .add_server_with_storage(upstream_server_id, false, &io_b);
    client_b
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();

    // === Transfer catalogue objects via real outbox/inbox sync payloads ===
    // These were queued as part of initial add_server sync from client A.
    let catalogue_outbox = client_a
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    let schema_msg = catalogue_outbox
        .iter()
        .find(|e| {
            matches!(
                &e.payload,
                SyncPayload::CatalogueEntryUpdated { entry } if entry.object_id == schema_object_id
            )
        })
        .expect("Client A should emit schema catalogue object");
    let lens_msg = catalogue_outbox
        .iter()
        .find(|e| {
            matches!(
                &e.payload,
                SyncPayload::CatalogueEntryUpdated { entry } if entry.object_id == lens_object_id
            )
        })
        .expect("Client A should emit lens catalogue object");

    client_b
        .query_manager_mut()
        .sync_manager_mut()
        .push_inbox(InboxEntry {
            source: Source::Server(upstream_server_id),
            payload: schema_msg.payload.clone(),
        });
    client_b.process(&mut io_b);

    // v2 is pending (no lens yet)
    assert!(client_b.context().is_pending(&v2_hash));

    client_b
        .query_manager_mut()
        .sync_manager_mut()
        .push_inbox(InboxEntry {
            source: Source::Server(upstream_server_id),
            payload: lens_msg.payload.clone(),
        });
    client_b.process(&mut io_b);

    // Now v2 should be live (lens path v2->v1 exists via backward lens)
    assert!(!client_b.context().is_pending(&v2_hash));
    assert!(client_b.context().is_live(&v2_hash));
    assert_eq!(client_b.all_branches().len(), 2);

    // QueryManager is automatically updated when schemas activate via process()

    // === Client A writes a row on v2 branch ===
    let id = ObjectId::new();
    let id_val = Value::Uuid(id);
    let name = Value::Text("Alice".into());
    let email = Value::Text("alice@example.com".into());

    let row_handle = client_a
        .insert(
            &mut io_a,
            "users",
            HashMap::from([
                ("id".to_string(), id_val.clone()),
                ("name".to_string(), name.clone()),
                ("email".to_string(), email.clone()),
            ]),
            None,
            None,
        )
        .unwrap();
    client_a.process(&mut io_a);

    // === Real A -> upstream -> B row sync via outbox/inbox ===
    let outbox_a = client_a
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    let row_msg = outbox_a
        .iter()
        .find(|e| {
            matches!(
                &e.payload,
                SyncPayload::RowBatchCreated { row, .. } if row.row_id == row_handle.row_id
            )
        })
        .expect("Client A should emit RowBatchCreated for the row");

    client_b
        .query_manager_mut()
        .sync_manager_mut()
        .push_inbox(InboxEntry {
            source: Source::Server(upstream_server_id),
            payload: row_msg.payload.clone(),
        });
    client_b.process(&mut io_b);

    // Query across both branches; row should be visible transformed into current (v1) shape.
    let v1_branch = client_b.branch_name().to_string();
    let v2_branch = format!("dev-{}-main", v2_hash.short());
    let query = QueryBuilder::new("users")
        .branches(&[&v1_branch, &v2_branch])
        .build();
    let results = execute_query(&mut client_b, &mut io_b, query);
    assert_eq!(results.len(), 1, "Client B should observe the synced row");

    // Validate query-visible row identity and payload content.
    let row_values = &results[0].1;
    assert!(
        row_values.len() >= 2,
        "Synced row should include user payload values"
    );
    assert!(
        row_values.iter().any(|v| v == &id_val),
        "Synced row should retain id value"
    );
    assert!(
        row_values.iter().any(|v| {
            matches!(
                v,
                Value::Text(t) if t.contains("Alice") || t.contains("alice@example.com")
            )
        }),
        "Synced row should retain text payload from source row"
    );
    assert_eq!(results[0].0, row_handle.row_id);
}

#[test]
fn e2e_multi_hop_catalogue_activation() {
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .build();

    let v3 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text)
                .nullable_column("role", ColumnType::Text),
        )
        .build();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);
    let v3_hash = SchemaHash::compute(&v3);

    // Client starts with v1 schema
    let mut client =
        SchemaManager::new(SyncManager::new(), v1.clone(), test_app_id(), "dev", "main").unwrap();

    // Receive v2 schema - becomes pending
    let v2_encoded = encode_schema(&v2);
    let mut v2_metadata = HashMap::new();
    v2_metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueSchema.to_string(),
    );
    v2_metadata.insert(
        MetadataKey::AppId.to_string(),
        test_app_id().uuid().to_string(),
    );
    v2_metadata.insert(MetadataKey::SchemaHash.to_string(), v2_hash.to_string());

    client
        .process_catalogue_update(v2_hash.to_object_id(), &v2_metadata, &v2_encoded)
        .unwrap();

    assert!(client.context().is_pending(&v2_hash));
    assert!(!client.context().is_pending(&v3_hash)); // Not received yet

    // Receive v3 schema - also becomes pending
    let v3_encoded = encode_schema(&v3);
    let mut v3_metadata = HashMap::new();
    v3_metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueSchema.to_string(),
    );
    v3_metadata.insert(
        MetadataKey::AppId.to_string(),
        test_app_id().uuid().to_string(),
    );
    v3_metadata.insert(MetadataKey::SchemaHash.to_string(), v3_hash.to_string());

    client
        .process_catalogue_update(v3_hash.to_object_id(), &v3_metadata, &v3_encoded)
        .unwrap();

    assert!(client.context().is_pending(&v2_hash));
    assert!(client.context().is_pending(&v3_hash));

    // Receive lens v1->v2
    // This should activate v2 (v2 can reach v1 via backward lens)
    let lens_v1_v2 = generate_lens(&v1, &v2);
    let lens_v1_v2_encoded = encode_lens_transform(&lens_v1_v2.forward);
    let mut lens_v1_v2_metadata = HashMap::new();
    lens_v1_v2_metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueLens.to_string(),
    );
    lens_v1_v2_metadata.insert(
        MetadataKey::AppId.to_string(),
        test_app_id().uuid().to_string(),
    );
    lens_v1_v2_metadata.insert(MetadataKey::SourceHash.to_string(), v1_hash.to_string());
    lens_v1_v2_metadata.insert(MetadataKey::TargetHash.to_string(), v2_hash.to_string());

    client
        .process_catalogue_update(
            lens_v1_v2.object_id(),
            &lens_v1_v2_metadata,
            &lens_v1_v2_encoded,
        )
        .unwrap();

    // v2 should now be live
    assert!(!client.context().is_pending(&v2_hash));
    assert!(client.context().is_live(&v2_hash));

    // v3 should still be pending (no path to v1 yet)
    assert!(client.context().is_pending(&v3_hash));

    // Receive lens v2->v3
    // This should activate v3 (v3 -> v2 via backward, then v2 -> v1 via backward)
    let lens_v2_v3 = generate_lens(&v2, &v3);
    let lens_v2_v3_encoded = encode_lens_transform(&lens_v2_v3.forward);
    let mut lens_v2_v3_metadata = HashMap::new();
    lens_v2_v3_metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueLens.to_string(),
    );
    lens_v2_v3_metadata.insert(
        MetadataKey::AppId.to_string(),
        test_app_id().uuid().to_string(),
    );
    lens_v2_v3_metadata.insert(MetadataKey::SourceHash.to_string(), v2_hash.to_string());
    lens_v2_v3_metadata.insert(MetadataKey::TargetHash.to_string(), v3_hash.to_string());

    client
        .process_catalogue_update(
            lens_v2_v3.object_id(),
            &lens_v2_v3_metadata,
            &lens_v2_v3_encoded,
        )
        .unwrap();

    // v3 should now be live (2-hop path: v3 -> v2 -> v1)
    assert!(!client.context().is_pending(&v3_hash));
    assert!(client.context().is_live(&v3_hash));

    // All three schemas should now be live
    assert_eq!(client.all_branches().len(), 3);
}

#[test]
fn e2e_server_learns_schema_via_catalogue_sync() {
    // === Client schema (what we want to sync to server) ===
    let client_schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column("id", ColumnType::Uuid)
                .column("content", ColumnType::Text),
        )
        .build();

    let _client_schema_hash = SchemaHash::compute(&client_schema);

    // === Setup client with schema ===
    let mut client = SchemaManager::new(
        SyncManager::new(),
        client_schema.clone(),
        test_app_id(),
        "dev",
        "main",
    )
    .unwrap();

    // === Setup server with same schema ===
    // NOTE: In the current architecture, server needs to have the schema
    // to process queries. Schema sync via catalogue is for adding NEW
    // schema versions (migrations), not for initializing from scratch.
    //
    // The server's QueryManager needs the schema to:
    // 1. Decode row data
    // 2. Build query graphs
    // 3. Maintain indices
    //
    // So for now, server must be initialized with the same base schema.
    // Catalogue sync handles schema EVOLUTION, not schema BOOTSTRAPPING.
    let mut server = SchemaManager::new(
        SyncManager::new(),
        client_schema.clone(), // Server must have base schema
        test_app_id(),
        "dev",
        "main",
    )
    .unwrap();

    let mut io_client = MemoryStorage::new();
    let mut io_server = MemoryStorage::new();

    // === Network topology ===
    let client_id = ClientId::new();
    let server_id = ServerId::new();

    server
        .query_manager_mut()
        .sync_manager_mut()
        .add_client_with_storage(&io_server, client_id);
    server
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(client_id, ClientRole::Admin);
    client
        .query_manager_mut()
        .sync_manager_mut()
        .add_server_with_storage(server_id, false, &io_client);

    // Clear initial sync
    client.query_manager_mut().sync_manager_mut().take_outbox();
    server.query_manager_mut().sync_manager_mut().take_outbox();

    // === Client persists schema to catalogue ===
    let schema_obj_id = client.persist_schema(&mut io_client);
    client.process(&mut io_client);

    // Transfer to server
    let outbox = client.query_manager_mut().sync_manager_mut().take_outbox();
    for entry in &outbox {
        if let SyncPayload::CatalogueEntryUpdated { entry } = &entry.payload
            && entry.object_id == schema_obj_id
        {
            server
                .query_manager_mut()
                .sync_manager_mut()
                .push_inbox(InboxEntry {
                    source: Source::Client(client_id),
                    payload: SyncPayload::CatalogueEntryUpdated {
                        entry: entry.clone(),
                    },
                });
        }
    }

    server.process(&mut io_server);

    // Process catalogue updates
    let updates = server.query_manager_mut().take_pending_catalogue_updates();
    for update in &updates {
        let _ =
            server.process_catalogue_update(update.object_id, &update.metadata, &update.content);
    }

    // === Create a note on the server ===
    let note_id = server
        .insert(
            &mut io_server,
            "notes",
            HashMap::from([
                ("id".to_string(), Value::Uuid(ObjectId::new())),
                ("content".to_string(), Value::Text("Hello World".into())),
            ]),
            None,
            None,
        )
        .unwrap()
        .row_id;

    server.query_manager_mut().sync_manager_mut().take_outbox();

    // === Client subscribes to notes ===
    let query = QueryBuilder::new("notes")
        .branch(client.branch_name().to_string())
        .build();

    let _sub_id = client
        .query_manager_mut()
        .subscribe_with_sync(query, None, None)
        .unwrap();

    client.process(&mut io_client);

    // Forward subscription to server
    let client_outbox = client.query_manager_mut().sync_manager_mut().take_outbox();
    for entry in &client_outbox {
        if matches!(entry.payload, SyncPayload::QuerySubscription { .. }) {
            server
                .query_manager_mut()
                .sync_manager_mut()
                .push_inbox(InboxEntry {
                    source: Source::Client(client_id),
                    payload: entry.payload.clone(),
                });
        }
    }

    server.process(&mut io_server);

    // === Server should send the note to client ===
    let server_outbox = server.query_manager_mut().sync_manager_mut().take_outbox();

    let note_sent = server_outbox.iter().any(|e| {
        if let SyncPayload::RowBatchNeeded { row, .. } = &e.payload {
            row.row_id == note_id
        } else {
            false
        }
    });

    assert!(
        note_sent,
        "Server should send the note to client after query subscription"
    );
}

/// A permissions head that arrives again, byte-identical to the one already
/// applied, must not be applied a second time.
///
/// This is not tidiness. `apply_permissions_head` ends in
/// `QueryManager::set_authorization_schema` (query_manager/manager.rs:750),
/// which bumps the authz generation, clears the authorization context cache
/// AND marks EVERY local and server subscription for recompilation
/// (`mark_subscriptions_for_recompile`, :884). On an rpc-server holding one
/// subscription per connected device, a redundant apply rebuilds all of their
/// graphs.
///
/// The existing guard rejects only a STRICTLY older head
/// (`current_head.version > head.version`, `process_catalogue_permissions_head`);
/// an equal head falls through and re-applies, and the legacy path
/// (`process_catalogue_permissions_legacy`) has no version guard at all.
///
/// That is load-bearing for the cold-boot generation fix: once the catalogue is
/// re-fed to the schema layer — at boot by
/// `rehydrate_schema_manager_from_catalogue`, and over the wire once the
/// SyncManager stops swallowing entries whose bytes it has already persisted —
/// the same head legitimately arrives more than once, on every peer connect.
#[test]
fn catalogue_same_permissions_head_push_is_noop() {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();
    let schema_hash = SchemaHash::compute(&schema);

    let mut manager =
        SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();
    let mut storage = MemoryStorage::new();

    manager
        .publish_permissions_bundle(&mut storage, schema_hash, HashMap::new(), None)
        .expect("publishing the first permissions bundle should succeed");

    let applied_generation = manager.query_manager().authz_schema_generation();
    assert!(
        applied_generation > 0,
        "publishing a bundle should have applied it"
    );

    // The exact bytes a peer re-sends on connect: whatever the store holds.
    let head_object_id = SchemaManager::permissions_head_object_id_for(test_app_id());
    let head_entry = storage
        .load_catalogue_entry(head_object_id)
        .expect("loading the persisted head entry should succeed")
        .expect("publishing a bundle should have persisted a head entry");

    manager
        .process_catalogue_update(
            head_entry.object_id,
            &head_entry.metadata,
            &head_entry.content,
        )
        .expect("re-processing the persisted head should succeed");

    assert_eq!(
        manager.query_manager().authz_schema_generation(),
        applied_generation,
        "a permissions head identical to the one already applied must not be \
         re-applied — each apply clears the authorization cache and marks every \
         subscription for recompilation (query_manager/manager.rs:750-757)",
    );
}

/// A generation whose catalogue entry is ALREADY in this node's storage, but
/// absent from its in-memory schema manager, must still be learned when a peer
/// re-sends it.
///
/// `SyncManager::persist_catalogue_entry` (sync_manager/sync_logic.rs:199-234)
/// answers exactly one question — "did storage change?" — and that single bool
/// gates two unrelated things: forwarding the entry onward, and handing it to
/// the schema layer through `pending_catalogue_updates` (inbox.rs:2027, :2628).
/// For an entry whose bytes are already on disk the answer is "no", so the
/// schema layer never sees it, the generation stays unknown to the running
/// process, its branch never enters the query universe
/// (`SchemaContext::all_branch_names`, context.rs:212) and every row under it
/// is unreadable at every durability tier.
///
/// MEASURED (rpc-server, 2026-08-16): on every restart after the 2026-08-07
/// migration the server served only the current generation — users 2 of 22,
/// chat_members 4 of 11, chats 2 of 4 — and every membership check 403'd.
///
/// The connect replay in this fixture is load-bearing, not decoration:
/// `add_client_with_storage` (sync_manager/mod.rs:771) seeds
/// `SyncManager::catalogue_entries` from storage wholesale, so by the time the
/// peer's copy arrives the entry is already in the SyncManager's own map. A fix
/// that keys on "new to the SyncManager" therefore cures nothing in the
/// production topology; the question that has to be asked is "has the SCHEMA
/// LAYER been handed this entry?".
///
/// MemoryStorage is adequate here because the assertion is on the branch
/// universe, a schema-layer fact. Serving rows across generations is pinned
/// separately, on SqliteStorage, by
/// runtime_core/tests/cold_boot_generation_universe.rs.
#[test]
fn catalogue_entry_already_in_storage_is_still_handed_to_the_schema_layer() {
    let notes = || {
        TableSchema::builder("notes")
            .column("id", ColumnType::Uuid)
            .column("content", ColumnType::Text)
    };
    let v1 = SchemaBuilder::new().table(notes()).build();
    // Generation B: an added table moves the schema hash and therefore the
    // composed branch name — the shape of the production migration.
    let v2 = SchemaBuilder::new()
        .table(notes())
        .table(TableSchema::builder("tags").column("label", ColumnType::Text))
        .build();
    let v2_hash = SchemaHash::compute(&v2);

    // The peer that published generation B.
    let mut publisher =
        SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main").unwrap();
    let mut io_publisher = MemoryStorage::new();
    let v2_object_id = publisher.persist_schema(&mut io_publisher);
    let v2_entry = io_publisher
        .load_catalogue_entry(v2_object_id)
        .expect("loading the published schema entry should succeed")
        .expect("persist_schema should have written a catalogue entry");

    // The node under test, immediately after a restart: generation B's entry is
    // on its disk — it persisted it before the restart — but the manager it just
    // constructed knows only the declared schema.
    let mut io_server = MemoryStorage::new();
    io_server
        .upsert_catalogue_entry(&v2_entry)
        .expect("seeding the node's storage should succeed");
    let mut server =
        SchemaManager::new(SyncManager::new(), v1, test_app_id(), "dev", "main").unwrap();
    assert!(
        !server.context().is_live(&v2_hash),
        "the fixture is only meaningful if the manager starts out ignorant of generation B"
    );

    let client_id = ClientId::new();
    server
        .query_manager_mut()
        .sync_manager_mut()
        .add_client_with_storage(&io_server, client_id);
    server
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(client_id, ClientRole::Admin);
    server.query_manager_mut().sync_manager_mut().take_outbox();

    // The peer re-sends generation B, byte-identical to what storage holds.
    server
        .query_manager_mut()
        .sync_manager_mut()
        .push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::CatalogueEntryUpdated {
                entry: v2_entry.clone(),
            },
        });
    server.process(&mut io_server);

    assert!(
        server.context().is_live(&v2_hash),
        "a catalogue entry that arrives over the wire must reach the schema layer \
         even when this node's storage already holds those exact bytes — otherwise \
         the generation is on disk and unreadable forever (`SyncManager::persist_catalogue_entry`)"
    );
    assert_eq!(
        server.all_branches().len(),
        2,
        "learning generation B must widen the query universe to both branches"
    );

    // Anti-storm half: having been handed it once, the node must not hand the
    // same entry to the schema layer again on every subsequent peer connect —
    // each redundant permissions-head apply rebuilds every subscription's graph
    // (query_manager/manager.rs:750-757).
    server
        .query_manager_mut()
        .sync_manager_mut()
        .push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::CatalogueEntryUpdated { entry: v2_entry },
        });
    server.query_manager_mut().process(&mut io_server);
    assert!(
        server
            .query_manager_mut()
            .take_pending_catalogue_updates()
            .is_empty(),
        "a re-send of an entry the schema layer has already been handed must not \
         be handed to it a second time"
    );
}

/// The production sequence the guard is really for: publish locally, then have a
/// peer echo the entry straight back through the SyncManager.
///
/// `catalogue_same_permissions_head_push_is_noop` calls
/// `process_catalogue_update` directly, so it proves the guard and nothing about
/// the path that reaches it. This runs the whole path:
/// `publish_permissions_bundle` → `persist_current_permissions` →
/// `SyncManager::upsert_catalogue_entry` (which deliberately does NOT record the
/// entry as handed to the schema layer — the schema layer is where it came from)
/// → a client connect, which replays the catalogue out of storage → that client
/// offering the same entry back → `SchemaManager::process`.
///
/// Every device reconnect replays this. A bump here is a full authorization
/// rebuild plus a recompile of every subscription the process holds, once per
/// reconnect, on a server that holds one per connected device.
#[test]
fn a_locally_published_head_echoed_back_by_a_peer_does_not_rebuild_authorization() {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();
    let schema_hash = SchemaHash::compute(&schema);

    let mut server =
        SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();
    let mut io_server = MemoryStorage::new();

    server
        .publish_permissions_bundle(&mut io_server, schema_hash, HashMap::new(), None)
        .expect("publishing the permissions bundle should succeed");
    let applied_generation = server.query_manager().authz_schema_generation();
    assert!(
        applied_generation > 0,
        "publishing a bundle should have applied it"
    );

    let client_id = ClientId::new();
    server
        .query_manager_mut()
        .sync_manager_mut()
        .add_client_with_storage(&io_server, client_id);
    server
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(client_id, ClientRole::Admin);
    server.query_manager_mut().sync_manager_mut().take_outbox();

    let head_entry = io_server
        .load_catalogue_entry(SchemaManager::permissions_head_object_id_for(test_app_id()))
        .expect("loading the persisted head entry should succeed")
        .expect("publishing a bundle should have persisted a head entry");

    for reconnect in 1..=2 {
        server
            .query_manager_mut()
            .sync_manager_mut()
            .push_inbox(InboxEntry {
                source: Source::Client(client_id),
                payload: SyncPayload::CatalogueEntryUpdated {
                    entry: head_entry.clone(),
                },
            });
        server.process(&mut io_server);

        assert_eq!(
            server.query_manager().authz_schema_generation(),
            applied_generation,
            "reconnect {reconnect}: a peer echoing back the head this node published must not \
             rebuild the authorization schema — that clears the authz cache and marks every \
             subscription for recompilation (query_manager/manager.rs:750-757)",
        );
    }
}
